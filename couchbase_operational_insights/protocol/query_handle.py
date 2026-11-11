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

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Optional

from couchbase_operational_insights.common._core.query_handle import (
    QueryHandleStatusResponse,
    serialize_query_handle,
)
from couchbase_operational_insights.common.errors import OperationalInsightsError
from couchbase_operational_insights.common.query_handle import BlockingQueryHandle as _CoreBlockingQueryHandle
from couchbase_operational_insights.common.query_handle import (
    BlockingQueryResultHandle as _CoreBlockingQueryResultHandle,
)
from couchbase_operational_insights.common.query_handle import BlockingQueryStatus as _CoreBlockingQueryStatus
from couchbase_operational_insights.common.result import BlockingQueryResult
from couchbase_operational_insights.protocol._core.client_adapter import _ClientAdapter
from couchbase_operational_insights.protocol._core.request import _RequestBuilder
from couchbase_operational_insights.protocol._core.request_context import RequestContext, StreamingRequestContext
from couchbase_operational_insights.protocol._core.response import HttpResponse
from couchbase_operational_insights.protocol.streaming import HttpStreamingResponse

if TYPE_CHECKING:
    from couchbase_operational_insights.common._core import JsonStreamConfig
    from couchbase_operational_insights.options import FetchResultsOptions


class BlockingQueryHandle(_CoreBlockingQueryHandle):
    def __init__(
        self,
        client_adapter: _ClientAdapter,
        request_builder: _RequestBuilder,
        request_id: str,
        handle: str,
        tp_executor: ThreadPoolExecutor,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> None:
        super().__init__()
        self._client_adapter = client_adapter
        self._request_builder = request_builder
        self._tp_executor = tp_executor
        self._stream_config = stream_config
        self._request_id = request_id
        self._handle = handle

    def fetch_status(self, options: Optional[Any] = None, **kwargs: Any) -> BlockingQueryStatus:
        server_req = self._request_builder.build_request_from_handle(self._handle)
        request_context = RequestContext(self._client_adapter, server_req)
        resp = HttpResponse(request_context)
        resp.send_request()
        if resp.json_response is None:
            raise OperationalInsightsError(message='HTTP response does not contain JSON data.')

        status_response = self._get_handle_status_response(resp)
        return BlockingQueryStatus(
            self._client_adapter,
            self._request_builder,
            self._tp_executor,
            status_response,
            stream_config=self._stream_config,
        )

    def cancel(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        cancel_req = self._request_builder.build_cancel_request(self._request_id)
        request_context = RequestContext(self._client_adapter, cancel_req)
        resp = HttpResponse(request_context, has_no_body_response=True, request_id=self._request_id)
        resp.send_request()

    def to_serialized(self) -> str:
        return serialize_query_handle(self._request_id, self._handle)

    def _get_handle_status_response(self, resp: HttpResponse) -> QueryHandleStatusResponse:
        if resp.json_response is None:
            raise OperationalInsightsError(message='HTTP response does not contain JSON data.')

        return QueryHandleStatusResponse.from_server(self._request_id, resp.json_response)


class BlockingQueryResultHandle(_CoreBlockingQueryResultHandle):
    def __init__(
        self,
        client_adapter: _ClientAdapter,
        request_builder: _RequestBuilder,
        tp_executor: ThreadPoolExecutor,
        request_id: str,
        handle: str,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> None:
        super().__init__()
        self._client_adapter = client_adapter
        self._request_builder = request_builder
        self._tp_executor = tp_executor
        self._request_id = request_id
        self._handle = handle
        self._stream_config = stream_config

    def fetch_results(self, options: Optional[FetchResultsOptions] = None, **kwargs: Any) -> BlockingQueryResult:
        server_req = self._request_builder.build_fetch_results_request(self._handle, options, **kwargs)
        request_context = StreamingRequestContext(
            self._client_adapter, server_req, self._tp_executor, stream_config=self._stream_config
        )
        resp = HttpStreamingResponse(request_context, request_id=self._request_id)
        resp.send_request()
        return BlockingQueryResult(resp)

    def discard_results(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        req = self._request_builder.build_discard_results_request(self._handle)
        request_context = RequestContext(self._client_adapter, req)
        resp = HttpResponse(request_context, has_no_body_response=True, request_id=self._request_id)
        resp.send_request()


class BlockingQueryStatus(_CoreBlockingQueryStatus):
    def __init__(
        self,
        client_adapter: _ClientAdapter,
        request_builder: _RequestBuilder,
        tp_executor: ThreadPoolExecutor,
        status_resp: QueryHandleStatusResponse,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> None:
        super().__init__()
        self._client_adapter = client_adapter
        self._request_builder = request_builder
        self._tp_executor = tp_executor
        self._status_resp = status_resp
        self._stream_config = stream_config

    def results_ready(self) -> bool:
        return self._status_resp.handle is not None

    def result_handle(self) -> BlockingQueryResultHandle:
        if self._status_resp.handle is None:
            raise OperationalInsightsError(message='Query is not ready. Handle is not available.')

        return BlockingQueryResultHandle(
            self._client_adapter,
            self._request_builder,
            self._tp_executor,
            self._status_resp.request_id,
            self._status_resp.handle,
            stream_config=self._stream_config,
        )

    def __repr__(self) -> str:
        return f'BlockingQueryStatus({self._status_resp.get_details()})'

    def __str__(self) -> str:
        return self.__repr__()
