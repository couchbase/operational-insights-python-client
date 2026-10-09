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

from typing import Any, Optional

from httpx2 import Response as HttpCoreResponse

from acouchbase_operational_insights.protocol._core.request_context import AsyncStreamingRequestContext
from acouchbase_operational_insights.protocol._core.retries import AsyncRetryHandler
from couchbase_operational_insights.common._core import ParsedResult, ParsedResultType
from couchbase_operational_insights.common._core.query import build_query_metadata
from couchbase_operational_insights.common.errors import InternalSDKError, OperationalInsightsError, TimeoutError
from couchbase_operational_insights.common.logging import LogLevel
from couchbase_operational_insights.common.query import QueryMetadata


class AsyncHttpStreamingResponse:
    def __init__(self, request_context: AsyncStreamingRequestContext, request_id: Optional[str] = None) -> None:
        self._metadata: Optional[QueryMetadata] = None
        self._core_response: HttpCoreResponse
        # Goal is to treat the AsyncHttpStreamingResponse as a "task group"
        self._request_context = request_context
        self._request_id = request_id

    async def _close_in_background(self) -> None:
        """
        **INTERNAL**
        """
        await self.close()

    async def _handle_iteration_abort(self) -> None:
        """
        **INTERNAL**
        """
        await self.close()
        if self._request_context.cancelled:
            self._request_context.log_message('Request canceled, aborting iteration', LogLevel.DEBUG)
            await self._request_context.shutdown()
            raise StopAsyncIteration
        elif self._request_context.timed_out:
            err = TimeoutError(
                message='Unable to complete iteration. Request timed out.',
                context=str(self._request_context.error_context),
            )
            await self._request_context.reraise_after_shutdown(err)
        else:
            self._request_context.log_message('Aborting iteration', LogLevel.DEBUG)
            await self._request_context.shutdown()
            raise StopAsyncIteration

    async def _process_response(
        self, raw_response: Optional[ParsedResult] = None, handle_context_shutdown: Optional[bool] = False
    ) -> None:
        """
        **INTERNAL**
        """
        json_response = await self._request_context.process_streaming_response(
            self.close, raw_response=raw_response, handle_context_shutdown=handle_context_shutdown
        )
        await self.set_metadata(json_data=json_response)

    async def close(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_core_response'):
            await self._core_response.aclose()
            self._request_context.log_message('HTTP core response closed', LogLevel.INFO)
            del self._core_response

    def cancel(self) -> None:
        """
        **INTERNAL**
        """
        self._request_context.log_message('AsyncHttpStreamingResponse cancelling request in background', LogLevel.DEBUG)
        self._request_context.cancel_request(self._close_in_background)

    async def cancel_async(self) -> None:
        """
        **INTERNAL**
        """
        self._request_context.log_message('AsyncHttpStreamingResponse cancelling request', LogLevel.DEBUG)
        await self.close()
        self._request_context.cancel_request()
        await self._request_context.shutdown()

    def get_metadata(self) -> QueryMetadata:
        """
        **INTERNAL**
        """
        if self._metadata is None:
            raise RuntimeError('Query metadata is only available after all rows have been iterated.')
        return self._metadata

    async def set_metadata(self, json_data: Optional[Any] = None, raw_metadata: Optional[bytes] = None) -> None:
        """
        **INTERNAL**
        """
        try:
            self._metadata = QueryMetadata(
                build_query_metadata(
                    json_data=json_data,
                    raw_metadata=raw_metadata,
                    request_id=self._request_id,
                    log_fn=self._request_context.log_message,
                )
            )
            await self._request_context.shutdown()
        except (OperationalInsightsError, ValueError) as err:
            await self._request_context.reraise_after_shutdown(err)
        except Exception as ex:
            internal_err = InternalSDKError(cause=ex, message=str(ex), context=str(self._request_context.error_context))
            await self._request_context.reraise_after_shutdown(internal_err)
        finally:
            await self.close()

    async def get_next_row(self) -> Any:
        """
        **INTERNAL**
        """
        if not (
            hasattr(self, '_core_response')
            and self._core_response is not None
            and self._request_context.okay_to_iterate
        ):
            await self._handle_iteration_abort()

        self._request_context.maybe_continue_to_process_stream()
        raw_response = await self._request_context.get_result_from_stream()
        if raw_response.result_type == ParsedResultType.ROW:
            if raw_response.value is None:
                await self.close()
                raise OperationalInsightsError(
                    message='Unexpected empty row response while streaming.',
                    context=str(self._request_context.error_context),
                )
            return self._request_context.deserialize_result(raw_response.value)
        elif raw_response.result_type in [ParsedResultType.ERROR, ParsedResultType.UNKNOWN]:
            await self._process_response(raw_response=raw_response, handle_context_shutdown=True)
        elif raw_response.result_type == ParsedResultType.END:
            await self.set_metadata(raw_metadata=raw_response.value)
            raise StopAsyncIteration
        else:
            await self._process_response(raw_response=raw_response, handle_context_shutdown=True)

    @AsyncRetryHandler.with_retries
    async def send_request(self) -> None:
        """
        **INTERNAL**
        """
        if not self._request_context.okay_to_stream:
            raise RuntimeError('Query has been canceled or previously executed.')

        # start cancel scope
        await self._request_context.initialize()
        self._core_response = await self._request_context.send_request()
        self._request_context.start_stream(self._core_response)
        # block until we either know we have rows or we have an error
        await self._request_context.wait_for_results_or_errors()

        # it is possible we have "rows" but the HTTP status code is indicative of an error (e.g. 404, 503, etc.),
        # so we need to check for HTTP status errors before allowing iteration to continue
        await self._request_context.check_for_http_status_error(
            self._core_response.status_code, close_handler=self.close
        )
        if not self._request_context.okay_to_iterate:
            await self._request_context.finish_processing_stream()
            await self._process_response()

    async def shutdown(self) -> None:
        """
        **INTERNAL**
        """
        await self.close()
        await self._request_context.shutdown()
