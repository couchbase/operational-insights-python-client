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

import json
import math
from asyncio import CancelledError, Task
from types import TracebackType
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Type, Union
from uuid import uuid4

import anyio
from httpx2 import Response as HttpCoreResponse
from httpx2 import TimeoutException

from acouchbase_operational_insights.protocol._core.anyio_utils import AsyncBackend, current_async_library, get_time
from acouchbase_operational_insights.protocol._core.async_json_stream import AsyncJsonStream
from acouchbase_operational_insights.protocol._core.net_utils import get_request_ip_async
from couchbase_operational_insights.common._core import JsonStreamConfig, ParsedResult, ParsedResultType
from couchbase_operational_insights.common._core.error_context import ErrorContext
from couchbase_operational_insights.common.backoff_calculator import DefaultBackoffCalculator
from couchbase_operational_insights.common.errors import OperationalInsightsError
from couchbase_operational_insights.common.logging import LogLevel
from couchbase_operational_insights.common.request import RequestState
from couchbase_operational_insights.protocol._core.request import (
    FetchResultsRequest,
    HttpRequest,
    QueryRequest,
    StartQueryRequest,
)
from couchbase_operational_insights.protocol.connection import DEFAULT_TIMEOUTS
from couchbase_operational_insights.protocol.errors import ErrorMapper

if TYPE_CHECKING:
    from acouchbase_operational_insights.protocol._core.client_adapter import _AsyncClientAdapter


class AsyncRequestContext:
    def __init__(
        self,
        client_adapter: _AsyncClientAdapter,
        request: HttpRequest,
        supports_cancellation: Optional[bool] = None,
        backend: Optional[AsyncBackend] = None,
    ) -> None:
        self._id = str(uuid4())
        self._client_adapter = client_adapter
        self._request = request
        self._backend = backend or current_async_library()
        self._backoff_calc = DefaultBackoffCalculator()
        self._error_context = ErrorContext(num_attempts=0, method=request.method)
        if isinstance(request, (QueryRequest, StartQueryRequest)):
            self._error_context.set_statement(request.get_request_statement())
        self._request_state = RequestState.NotStarted
        self._supports_cancellation = False if supports_cancellation is None else supports_cancellation
        self._request_error: Optional[Union[BaseException, Exception]] = None
        connect_timeout = self._client_adapter.connection_details.get_connect_timeout()
        self._connect_deadline = get_time() + connect_timeout
        self._cancel_scope_deadline_updated = False
        self._shutdown = False
        self._request_deadline = math.inf

    @property
    def cancelled(self) -> bool:
        if not self._supports_cancellation:
            return False
        self._check_timed_out()
        return self._request_state in [RequestState.Cancelled, RequestState.AsyncCancelledPriorToTimeout]

    @property
    def error_context(self) -> ErrorContext:
        return self._error_context

    @property
    def is_shutdown(self) -> bool:
        return self._shutdown

    @property
    def request_error(self) -> Optional[Union[BaseException, Exception]]:
        return self._request_error

    @property
    def request_state(self) -> RequestState:
        return self._request_state

    @property
    def retry_limit_exceeded(self) -> bool:
        return self._error_context.num_attempts > self._request.max_retries

    @property
    def timed_out(self) -> bool:
        self._check_timed_out()
        return self._request_state == RequestState.Timeout

    def calculate_backoff(self) -> float:
        return self._backoff_calc.calculate_backoff(self._error_context.num_attempts) / 1000

    async def check_for_http_status_error(
        self,
        status_code: int,
        ignore_not_found_status: Optional[bool] = False,
        close_handler: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        ctx = str(self._error_context)
        err = ErrorMapper.maybe_get_error_from_status_code(
            status_code, ctx, ignore_not_found_status=ignore_not_found_status
        )
        if err is None:
            return
        if close_handler is not None:
            await close_handler()
        raise err

    def create_response_task(self, fn: Callable[..., Coroutine[Any, Any, Any]], *args: object) -> Task[Any]:
        if self._backend is None or self._backend.backend_lib != 'asyncio':
            raise RuntimeError('Must use the asyncio backend to create a response task.')
        if self._backend.loop is None:
            raise RuntimeError('Async backend loop is not initialized.')
        task_name = f'{self._id}-response-task'
        task: Task[Any] = self._backend.loop.create_task(fn(*args), name=task_name)
        self._response_task = task
        return task

    async def initialize(self) -> None:
        if self._request_state == RequestState.ResetAndNotStarted:
            current_time = get_time()
            self.log_message(
                'Request is a retry, skipping initialization',
                LogLevel.DEBUG,
                message_data={'current_time': f'{current_time}', 'request_deadline': f'{self._request_deadline}'},
            )
            return
        await self.__aenter__()
        self._request_state = RequestState.Started
        # we set the request timeout once the context is initialized in order to create the deadline
        # closer to when the upstream logic will begin to use the request context
        timeouts = self._request.get_request_timeouts() or {}
        current_time = get_time()
        self._request_deadline = current_time + (timeouts.get('read', None) or DEFAULT_TIMEOUTS['query_timeout'])
        message_data = {'current_time': f'{current_time}', 'request_deadline': f'{self._request_deadline}'}
        self.log_message('Request context initialized', LogLevel.DEBUG, message_data=message_data)

    def log_message(
        self,
        message: str,
        log_level: LogLevel,
        message_data: Optional[Dict[str, str]] = None,
        append_ctx: Optional[bool] = True,
    ) -> None:
        if append_ctx is True:
            message = f'{message}: ctx={self._id}'
        if message_data is not None:
            message_data_str = ', '.join(f'{k}={v}' for k, v in message_data.items())
            message = f'{message}, {message_data_str}'
        self._client_adapter.log_message(message, log_level)

    def okay_to_delay_and_retry(self, delay: float) -> bool:
        # calling self.timed_out will call _check_timed_out, so we don't need to call it again
        if self.timed_out:
            return False
        elif self._supports_cancellation and self._request_state == RequestState.Cancelled:
            return False

        current_time = get_time()
        delay_time = current_time + delay
        will_time_out = self._request_deadline < delay_time
        if will_time_out:
            self._request_state = RequestState.Timeout
            message_data = {
                'current_time': f'{current_time}',
                'delay_time': f'{delay_time}',
                'request_deadline': f'{self._request_deadline}',
            }
            self.log_message('Request will timeout after delay', LogLevel.DEBUG, message_data=message_data)
            return False
        elif self.retry_limit_exceeded:
            self._request_state = RequestState.Error
            message_data = {
                'num_attempts': f'{self.error_context.num_attempts}',
                'max_retries': f'{self._request.max_retries}',
            }
            self.log_message('Request has exceeded max retries', LogLevel.DEBUG, message_data=message_data)
            return False
        elif self._supports_cancellation:
            # _reset_stream() _should_ exist, but surround w/ try/except just in case
            try:
                self._reset_stream()  # type: ignore[attr-defined]
            except AttributeError:
                pass  # nosec

        return True

    async def process_response(
        self,
        core_response: HttpCoreResponse,
        close_handler: Callable[[], Awaitable[None]],
        handle_context_shutdown: Optional[bool] = False,
        ignore_not_found_status: Optional[bool] = False,
    ) -> Any:
        # we have all the data, close the core response/stream
        await close_handler()
        await self.check_for_http_status_error(
            core_response.status_code, ignore_not_found_status=ignore_not_found_status
        )
        try:
            json_response = core_response.json()
        except json.JSONDecodeError:
            await self._process_error(str(core_response.text), handle_context_shutdown=handle_context_shutdown)
        else:
            if 'errors' in json_response:
                await self._process_error(json_response['errors'], handle_context_shutdown=handle_context_shutdown)
            return json_response

    async def reraise_after_shutdown(self, err: Exception) -> None:
        try:
            raise err
        except Exception as ex:
            await self.shutdown(type(ex), ex, ex.__traceback__)
            raise ex from None

    async def send_request(
        self, enable_trace_handling: Optional[bool] = False, ignore_not_found_status: Optional[bool] = False
    ) -> HttpCoreResponse:
        self._error_context.update_num_attempts()
        ip = await get_request_ip_async(self._request.url.host, self._request.url.port, self.log_message)
        if self._request.path and not self._request.path.isspace():
            req_path = f'{self._request.path}'
        else:
            req_path = self._client_adapter.operational_insights_path
        if enable_trace_handling is True and hasattr(self, '_trace_handler'):
            self._request.update_url(ip, req_path).add_trace_to_extensions(self._trace_handler)
        else:
            self._request.update_url(ip, req_path)
        self._error_context.update_request_context(self._request, path=req_path)
        message_data = {
            'url': f'{self._request.url.get_formatted_url()}',
            'request_deadline': f'{self._request_deadline}',
        }

        if isinstance(self._request, (QueryRequest, StartQueryRequest)):
            message_data['body'] = f'{self._request.body}'

        stream = hasattr(self._request, 'should_stream') and self._request.should_stream is True
        message_data['streaming'] = str(stream)

        self.log_message('HTTP request', LogLevel.DEBUG, message_data=message_data)
        response = await self._client_adapter.send_request(self._request, stream=stream)
        self._error_context.update_response_context(response)
        message_data = {
            'status_code': f'{response.status_code}',
            'last_dispatched_to': f'{self._error_context.last_dispatched_to}',
            'last_dispatched_from': f'{self._error_context.last_dispatched_from}',
            'request_deadline': f'{self._request_deadline}',
        }
        self.log_message('HTTP response', LogLevel.DEBUG, message_data=message_data)
        return response

    async def shutdown(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> None:
        if self.is_shutdown:
            self.log_message('Request context already shutdown', LogLevel.WARNING)
            return
        if hasattr(self, '_taskgroup'):
            await self.__aexit__(exc_type, exc_val, exc_tb)
        else:
            self._maybe_set_request_error(exc_type, exc_val)

        if RequestState.is_okay(self._request_state):
            self._request_state = RequestState.Completed
        self._shutdown = True
        self.log_message('Request context shutdown complete', LogLevel.INFO)

    def _check_timed_out(self) -> None:
        if self._request_state in (RequestState.Timeout, RequestState.Error):
            return

        if self._supports_cancellation and self._request_state == RequestState.Cancelled:
            return

        if hasattr(self, '_request_deadline') is False:
            return

        current_time = get_time()
        timed_out = current_time >= self._request_deadline
        if timed_out:
            message_data = {'current_time': f'{current_time}', 'request_deadline': f'{self._request_deadline}'}
            self.log_message('Request has timed out', LogLevel.DEBUG, message_data=message_data)
            if self._supports_cancellation and self._request_state == RequestState.Cancelled:
                self._request_state = RequestState.AsyncCancelledPriorToTimeout
            else:
                self._request_state = RequestState.Timeout

    def _maybe_set_request_error(
        self, exc_type: Optional[Type[BaseException]] = None, exc_val: Optional[BaseException] = None
    ) -> None:
        self._check_timed_out()
        if exc_val is None:
            return
        if not RequestState.is_timeout_or_cancelled(self._request_state):
            # This handles httpx timeouts
            if exc_type is not None and issubclass(exc_type, TimeoutException):
                self._request_state = RequestState.Timeout
            elif issubclass(type(exc_val), TimeoutException):
                self._request_state = RequestState.Timeout
            elif isinstance(exc_val, CancelledError):
                self._request_state = RequestState.Cancelled
            else:
                self._request_state = RequestState.Error
            self._request_error = exc_val

    async def _process_error(
        self, json_data: Union[str, List[Dict[str, Any]]], handle_context_shutdown: Optional[bool] = False
    ) -> None:
        self._request_state = RequestState.Error
        if isinstance(json_data, str):
            self._request_error = ErrorMapper.build_error_from_http_status_code(json_data, self._error_context)
        elif not isinstance(json_data, list):
            self._request_error = OperationalInsightsError(
                'Cannot parse error response; expected JSON array', context=str(self._error_context)
            )
        else:
            self._request_error = ErrorMapper.build_error_from_json(json_data, self._error_context)
        if handle_context_shutdown is True:
            await self.reraise_after_shutdown(self._request_error)

        raise self._request_error

    async def __aenter__(self) -> AsyncRequestContext:
        self._taskgroup = anyio.create_task_group()
        message_data = {'cancel_scope': f'{id(self._taskgroup.cancel_scope):x}'}
        self.log_message('Task group created', LogLevel.DEBUG, message_data=message_data)
        await self._taskgroup.__aenter__()
        return self

    # TODO(PYCO-72): Possible improvement to handling async RequestContext.__aexit__
    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> Optional[bool]:
        try:
            await self._taskgroup.__aexit__(exc_type, exc_val, exc_tb)
        except BaseException:
            pass  # we handle the error when the context is shutdown (which is what calls __aexit__())
        finally:
            self._maybe_set_request_error(exc_type, exc_val)
            del self._taskgroup
            return None  # noqa: B012


class AsyncStreamingRequestContext(AsyncRequestContext):
    def __init__(
        self,
        client_adapter: _AsyncClientAdapter,
        request: Union[FetchResultsRequest, QueryRequest],
        stream_config: Optional[JsonStreamConfig] = None,
        backend: Optional[AsyncBackend] = None,
    ) -> None:
        super().__init__(client_adapter, request, supports_cancellation=True, backend=backend)
        self._stream_config = stream_config or JsonStreamConfig()
        self._json_stream: AsyncJsonStream
        self._stage_completed: Optional[anyio.Event] = None
        self._deserializer = request.deserializer

    @property
    def has_stage_completed(self) -> bool:
        return self._stage_completed is not None and self._stage_completed.is_set()

    @property
    def okay_to_iterate(self) -> bool:
        self._check_timed_out()
        return RequestState.okay_to_iterate(self._request_state)

    @property
    def okay_to_stream(self) -> bool:
        self._check_timed_out()
        return RequestState.okay_to_stream(self._request_state)

    @property
    def results_or_errors_type(self) -> ParsedResultType:
        return self._json_stream.results_or_errors_type

    def cancel_request(self, fn: Optional[Callable[..., Coroutine[Any, Any, Any]]] = None, *args: object) -> None:
        if fn is not None:
            self._taskgroup.start_soon(fn, *args)
        if self._request_state == RequestState.Timeout:
            return
        self._taskgroup.cancel_scope.cancel()
        self._request_state = RequestState.Cancelled

    def deserialize_result(self, result: bytes) -> Any:
        if not self._deserializer:
            raise RuntimeError('No deserializer found for this request context.')
        return self._deserializer.deserialize(result)

    async def finish_processing_stream(self) -> None:
        if not self.has_stage_completed:
            await self._wait_for_stage_to_complete()

        while not self._json_stream.token_stream_exhausted:
            self._start_next_stage(self._json_stream.continue_parsing, reset_previous_stage=True)
            await self._wait_for_stage_to_complete()

    async def get_result_from_stream(self) -> ParsedResult:
        return await self._json_stream.get_result()

    def maybe_continue_to_process_stream(self) -> None:
        if not self.has_stage_completed:
            return

        if self._json_stream.token_stream_exhausted:
            return

        self._start_next_stage(self._json_stream.continue_parsing, reset_previous_stage=True)

    async def process_streaming_response(
        self,
        close_handler: Callable[[], Awaitable[None]],
        raw_response: Optional[ParsedResult] = None,
        handle_context_shutdown: Optional[bool] = False,
    ) -> Any:
        if raw_response is None:
            raw_response = await self._json_stream.get_result()
            if raw_response is None:
                await close_handler()
                raise OperationalInsightsError(
                    message='Received unexpected empty result from JsonStream.', context=str(self._error_context)
                )

        if raw_response.value is None:
            await close_handler()
            raise OperationalInsightsError(
                message='Received unexpected empty result from JsonStream.', context=str(self._error_context)
            )

        # we have all the data, close the core response/stream
        await close_handler()

        try:
            json_response = json.loads(raw_response.value)
        except json.JSONDecodeError:
            await self._process_error(str(raw_response.value), handle_context_shutdown=handle_context_shutdown)
        else:
            if 'errors' in json_response:
                await self._process_error(json_response['errors'], handle_context_shutdown=handle_context_shutdown)
            return json_response

    def start_stream(self, core_response: HttpCoreResponse) -> None:
        if hasattr(self, '_json_stream'):
            self.log_message('JSON stream already exists', LogLevel.WARNING)
            return

        self._json_stream = AsyncJsonStream(
            core_response.aiter_bytes(), stream_config=self._stream_config, logger_handler=self.log_message
        )
        self._start_next_stage(self._json_stream.start_parsing)

    async def wait_for_results_or_errors(self) -> None:
        await self._json_stream.has_results_or_errors.wait()
        if self._json_stream.results_or_errors_type == ParsedResultType.ROW:
            # we move to iterating rows
            self._request_state = RequestState.StreamingResults

    async def _execute(self, fn: Callable[..., Awaitable[Any]], *args: object) -> None:
        await fn(*args)
        if self._stage_completed is not None:
            self._stage_completed.set()

    def _reset_stream(self) -> None:
        if hasattr(self, '_json_stream'):
            del self._json_stream
        self._request_state = RequestState.ResetAndNotStarted
        self._stage_completed = None
        self._cancel_scope_deadline_updated = False

    def _start_next_stage(
        self, fn: Callable[..., Awaitable[Any]], *args: object, reset_previous_stage: Optional[bool] = False
    ) -> None:
        if self._stage_completed is not None:
            if reset_previous_stage is True:
                self._stage_completed = None
            else:
                raise RuntimeError('Task already running in this context.')

        self._stage_completed = anyio.Event()
        self._taskgroup.start_soon(self._execute, fn, *args)

    async def _trace_handler(self, event_name: str, _: str) -> None:
        if event_name == 'connection.connect_tcp.complete':
            # after connection is established, we need to update the cancel_scope deadline to match the query_timeout
            self._update_cancel_scope_deadline(self._request_deadline, is_absolute=True)
            self._cancel_scope_deadline_updated = True
        elif self._cancel_scope_deadline_updated is False and event_name.endswith('send_request_headers.started'):
            # if the socket is reused, we won't get the connect_tcp.complete event,
            # so the deadline at the next closest event
            self._update_cancel_scope_deadline(self._request_deadline, is_absolute=True)
            self._cancel_scope_deadline_updated = True

    def _update_cancel_scope_deadline(self, deadline: float, is_absolute: Optional[bool] = False) -> None:
        new_deadline = deadline if is_absolute else get_time() + deadline
        current_time = get_time()
        if current_time >= new_deadline:
            self.log_message(
                'Deadline already exceeded, cancelling request',
                LogLevel.DEBUG,
                message_data={
                    'current_time': f'{current_time}',
                    'new_deadline': f'{new_deadline}',
                },
            )
            self._taskgroup.cancel_scope.cancel()
        else:
            self.log_message(
                f'Updating cancel scope deadline: {self._taskgroup.cancel_scope.deadline} -> {new_deadline}',
                LogLevel.DEBUG,
            )
            self._taskgroup.cancel_scope.deadline = new_deadline

    async def _wait_for_stage_to_complete(self) -> None:
        if self._stage_completed is None:
            return
        await self._stage_completed.wait()
