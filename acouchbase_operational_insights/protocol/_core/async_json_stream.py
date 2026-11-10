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

from typing import Any, AsyncIterator, Callable, Optional, Tuple

import ijson
from anyio import EndOfStream, Event, create_memory_object_stream

from acouchbase_operational_insights.protocol._core.async_json_token_parser import AsyncJsonTokenParser
from couchbase_operational_insights.common._core.json_parsing import JsonStreamConfig, ParsedResult, ParsedResultType
from couchbase_operational_insights.common._core.json_token_parser_base import JsonTokenParsingError
from couchbase_operational_insights.common.errors import OperationalInsightsError
from couchbase_operational_insights.common.logging import LogLevel


class AsyncJsonStream:
    def __init__(
        self,
        http_stream_iter: AsyncIterator[bytes],
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

        # logging
        self._log_handler = logger_handler

        # results handling
        self._send_stream, self._receive_stream = create_memory_object_stream[ParsedResult](
            max_buffer_size=stream_config.buffered_row_max
        )
        self._parsed_events = ijson.sendable_list()
        self._parsed_event_idx = 0
        self._json_parser = ijson.parse_coro(self._parsed_events)
        self._json_parser_closed = False
        self._parsing_started = False
        self._buffer_entire_result = stream_config.buffer_entire_result
        handler = None if self._buffer_entire_result is True else self._handle_json_result
        self._json_token_parser = AsyncJsonTokenParser(handler)
        self._token_stream_exhausted = False
        self._has_results_or_errors_evt = Event()
        self._results_or_errors_type = ParsedResultType.UNKNOWN

    @property
    def has_results_or_errors(self) -> Event:
        """
        **INTERNAL**
        """
        return self._has_results_or_errors_evt

    @property
    def results_or_errors_type(self) -> ParsedResultType:
        """
        **INTERNAL**
        """
        return self._results_or_errors_type

    @property
    def token_stream_exhausted(self) -> bool:
        """
        **INTERNAL**
        """
        return self._token_stream_exhausted

    def _continue_processing(self) -> bool:
        """
        **INTERNAL**
        """
        if self._token_stream_exhausted:
            return False
        if self._buffer_entire_result:
            return True

        stats = self._receive_stream.statistics()
        if stats.current_buffer_used >= stats.max_buffer_size:
            return False
        return True

    def _log_message(self, message: str, level: LogLevel) -> None:
        if self._log_handler is not None:
            self._log_handler(message, level)

    async def _send_to_stream(self, result: ParsedResult, close: Optional[bool] = False) -> None:
        """
        **INTERNAL**
        """
        await self._send_stream.send(result)
        if close is True:
            await self._send_stream.aclose()

    async def _handle_json_result(self, row: bytes) -> None:
        """
        **INTERNAL**
        """
        if not self._has_results_or_errors_evt.is_set():
            self._handle_notification(ParsedResultType.ROW)
        await self._send_to_stream(ParsedResult(row, ParsedResultType.ROW))

    def _handle_notification(self, result_type: Optional[ParsedResultType] = None) -> None:
        if self._has_results_or_errors_evt.is_set():
            return

        if result_type is None:
            self._results_or_errors_type = ParsedResultType.END
            self._has_results_or_errors_evt.set()
            return

        self._results_or_errors_type = result_type
        self._has_results_or_errors_evt.set()

    async def _read_chunk(self) -> bytes:
        """
        **INTERNAL**
        """
        if not self._http_response_buffer:
            try:
                self._http_response_buffer += await self._http_stream_iter.__anext__()
            except StopAsyncIteration:
                return b''
        data = bytes(self._http_response_buffer[: self._http_stream_buffer_size])
        del self._http_response_buffer[: self._http_stream_buffer_size]
        return data

    async def _next_event(self) -> Optional[Tuple[str, Any]]:
        """
        **INTERNAL**

        Chunks are pushed into ijson rather than letting ``ijson.parse_async`` await the HTTP read: its C backend does
        not deliver cancellation into the read, so a read timeout escapes as a bare ``CancelledError``.
        """
        while self._parsed_event_idx >= len(self._parsed_events):
            if self._json_parser_closed:
                return None
            del self._parsed_events[:]
            self._parsed_event_idx = 0
            chunk = await self._read_chunk()
            if chunk:
                self._json_parser.send(chunk)
            else:
                self._json_parser_closed = True
                self._json_parser.close()
        _, event, value = self._parsed_events[self._parsed_event_idx]
        self._parsed_event_idx += 1
        return event, value

    async def _process_token_stream(self) -> None:
        """
        **INTERNAL**
        """
        while self._continue_processing():
            try:
                next_event = await self._next_event()
                if next_event is None:
                    self._token_stream_exhausted = True
                    break
                await self._json_token_parser.parse_token(*next_event)
            except JsonTokenParsingError as ex:
                await self._handle_parsing_error('JSON token parsing error', ex)
                return
            except ijson.common.IncompleteJSONError as ex:
                await self._handle_parsing_error('Incomplete JSON error', ex)
                return
            except ijson.common.JSONError as ex:
                await self._handle_parsing_error('JSON error', ex)
                return

        if self._token_stream_exhausted:
            result_type = ParsedResultType.ERROR if self._json_token_parser.has_errors else ParsedResultType.END
            await self._send_to_stream(ParsedResult(self._json_token_parser.get_result(), result_type), close=True)
            self._handle_notification(result_type)

    async def _handle_parsing_error(self, description: str, ex: Exception) -> None:
        """
        **INTERNAL**
        """
        self.close_parser()
        ex_str = str(ex)
        self._log_message(f'{description} encountered: {ex_str}', LogLevel.ERROR)
        self._token_stream_exhausted = True
        await self._send_to_stream(ParsedResult(ex_str.encode('utf-8'), ParsedResultType.ERROR), close=True)
        self._handle_notification(ParsedResultType.ERROR)

    async def close_on_stream_error(self) -> None:
        """
        **INTERNAL**
        """
        self.close_parser()
        self._token_stream_exhausted = True
        self._handle_notification(ParsedResultType.ERROR)
        await self._send_stream.aclose()

    def close_parser(self) -> None:
        """
        **INTERNAL**

        An unclosed parser on ijson's python backend prints an ignored ``IncompleteJSONError`` when garbage collected.
        """
        if self._json_parser_closed:
            return
        self._json_parser_closed = True
        try:
            self._json_parser.close()
        except ijson.common.JSONError:
            pass  # nosec - expected, the stream stopped before the end of the document

    async def get_result(self) -> ParsedResult:
        try:
            return await self._receive_stream.receive()
        except EndOfStream as ex:
            raise OperationalInsightsError(ex, 'AsyncJsonStream has been closed.') from None

    async def start_parsing(self) -> None:
        if self._parsing_started:
            self._log_message('JSON stream parser already exists', LogLevel.WARNING)
            return
        self._parsing_started = True
        await self._process_token_stream()

    async def continue_parsing(self) -> None:
        await self._process_token_stream()
