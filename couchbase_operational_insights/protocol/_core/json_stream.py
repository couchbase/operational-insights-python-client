#  Copyright 2016-2025. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from __future__ import annotations

from concurrent.futures import Future
from queue import Empty as QueueEmpty
from queue import Full as QueueFull
from queue import Queue
from typing import TYPE_CHECKING, Callable, Iterator, Optional

import ijson

from couchbase_operational_insights.common._core.json_parsing import JsonStreamConfig, ParsedResult, ParsedResultType
from couchbase_operational_insights.common._core.json_token_parser_base import JsonTokenParsingError
from couchbase_operational_insights.common.logging import LogLevel
from couchbase_operational_insights.protocol._core.json_token_parser import JsonTokenParser

if TYPE_CHECKING:
    from couchbase_operational_insights.protocol._core.request_context import StreamingRequestContext


class JsonStream:
    DEFAULT_HTTP_STREAM_BUFFER_SIZE = 2**16

    def __init__(
        self,
        http_stream_iter: Iterator[bytes],
        *,
        stream_config: Optional[JsonStreamConfig] = None,
        logger_handler: Optional[Callable[[str, LogLevel], None]] = None,
    ) -> None:
        # HTTP stream handling
        if stream_config is None:
            stream_config = JsonStreamConfig()
        self._http_stream_iter = http_stream_iter
        self._http_stream_buffer_size = stream_config.http_stream_buffer_size
        self._http_response_buffer = bytearray()
        self._http_stream_exhausted = False

        # logging
        self._log_handler = logger_handler

        # results handling
        self._buffered_row_max = stream_config.buffered_row_max
        self._buffered_row_threshold = int(self._buffered_row_max * stream_config.buffered_row_threshold_percent)
        self._json_stream_parser = None
        self._buffer_entire_result = stream_config.buffer_entire_result
        handler = None if self._buffer_entire_result is True else self._handle_json_result
        self._json_token_parser = JsonTokenParser(handler)
        self._token_stream_exhausted = False
        self._results_queue: Queue[ParsedResult] = Queue()
        self._queue_timeout = stream_config.queue_timeout
        self._notify_on_results_or_error: Optional[Future[ParsedResultType]] = None

    @property
    def http_stream_exhausted(self) -> bool:
        """
        **INTERNAL**
        """
        return self._http_stream_exhausted

    @property
    def token_stream_exhausted(self) -> bool:
        """
        **INTERNAL**
        """
        return self._token_stream_exhausted

    def _continue_processing(self, request_context: Optional[StreamingRequestContext] = None) -> bool:
        """
        **INTERNAL**
        """
        if self._token_stream_exhausted:
            return False
        if self._buffer_entire_result:
            return True
        if request_context is not None and (request_context.cancelled or request_context.timed_out):
            return False
        if self._results_queue.qsize() >= self._buffered_row_threshold:
            return False
        return True

    def _put(self, result: ParsedResult) -> None:
        """
        **INTERNAL**
        """
        while True:
            try:
                self._results_queue.put(result, timeout=self._queue_timeout)
                break
            except QueueFull:
                self._log_message('Encountered QueueFull error', LogLevel.ERROR)
                pass

    def _handle_json_result(self, row: bytes) -> None:
        """
        **INTERNAL**
        """
        if self._notify_on_results_or_error is not None and not self._notify_on_results_or_error.done():
            self._handle_notification(ParsedResultType.ROW)

        self._put(ParsedResult(row, ParsedResultType.ROW))

    def _handle_notification(self, result_type: ParsedResultType) -> None:
        if self._notify_on_results_or_error is None or self._notify_on_results_or_error.done():
            return

        self._notify_on_results_or_error.set_result(result_type)

    def _log_message(self, message: str, level: LogLevel) -> None:
        if self._log_handler is not None:
            self._log_handler(message, level)

    def _process_token_stream(self, request_context: Optional[StreamingRequestContext] = None) -> None:
        """
        **INTERNAL**
        """
        if self._json_stream_parser is None:
            self._json_stream_parser = ijson.parse(self, buf_size=self._http_stream_buffer_size)

        while self._continue_processing(request_context=request_context):
            try:
                _, event, value = next(self._json_stream_parser)  # type: ignore[call-overload]
                self._json_token_parser.parse_token(event, value)
            except StopIteration:
                self._token_stream_exhausted = True
            except JsonTokenParsingError as ex:
                ex_str = str(ex)
                self._log_message(f'JSON token parsing error encountered: {ex_str}', LogLevel.ERROR)
                self._token_stream_exhausted = True
                self._put(ParsedResult(ex_str.encode('utf-8'), ParsedResultType.ERROR))
                self._handle_notification(ParsedResultType.ERROR)
                return
            except ijson.common.IncompleteJSONError as ex:
                ex_str = str(ex)
                self._log_message(f'Incomplete JSON error encountered: {ex_str}', LogLevel.ERROR)
                self._token_stream_exhausted = True
                self._put(ParsedResult(ex_str.encode('utf-8'), ParsedResultType.ERROR))
                self._handle_notification(ParsedResultType.ERROR)
                return
            except ijson.common.JSONError as ex:  # also covers the python backend's UnexpectedSymbol
                ex_str = str(ex)
                self._log_message(f'JSON error encountered: {ex_str}', LogLevel.ERROR)
                self._token_stream_exhausted = True
                self._put(ParsedResult(ex_str.encode('utf-8'), ParsedResultType.ERROR))
                self._handle_notification(ParsedResultType.ERROR)
                return

        if self._token_stream_exhausted:
            result_type = ParsedResultType.ERROR if self._json_token_parser.has_errors else ParsedResultType.END
            self._put(ParsedResult(self._json_token_parser.get_result(), result_type))
            self._handle_notification(result_type)

    def read(self, size: Optional[int] = -1) -> bytes:
        """
        **INTERNAL**
        """
        if size is None or size == 0 or self._http_stream_exhausted:
            return b''

        while not self._http_stream_exhausted:
            if size >= 0 and len(self._http_response_buffer) > size:
                break
            try:
                chunk = next(self._http_stream_iter)
                self._http_response_buffer += chunk
            except StopIteration:
                self._http_stream_exhausted = True
                break

        if size == -1:
            data = bytes(self._http_response_buffer[:])
            del self._http_response_buffer[:]
        else:
            end = min(size, len(self._http_response_buffer))
            data = bytes(self._http_response_buffer[:end])
            del self._http_response_buffer[:end]
        return data

    def get_result(self, timeout: float) -> Optional[ParsedResult]:
        try:
            return self._results_queue.get(timeout=timeout)
        except QueueEmpty:
            self._log_message(f'Results queue empty after waiting {timeout} seconds', LogLevel.WARNING)
            return None

    def get_result_nowait(self) -> Optional[ParsedResult]:
        try:
            return self._results_queue.get_nowait()
        except QueueEmpty:
            return None

    def start_parsing(
        self,
        request_context: Optional[StreamingRequestContext] = None,
        notify_on_results_or_error: Optional[Future[ParsedResultType]] = None,
    ) -> None:
        if self._json_stream_parser is not None:
            self._log_message('JSON stream parser already exists', LogLevel.WARNING)
            return
        self._notify_on_results_or_error = notify_on_results_or_error
        self._process_token_stream(request_context=request_context)

    def continue_parsing(
        self,
        request_context: Optional[StreamingRequestContext] = None,
    ) -> None:
        self._process_token_stream(request_context=request_context)
