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

from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Union

from couchbase_operational_insights.common._core.query_handle import get_query_handle_fields
from couchbase_operational_insights.common.result import BlockingQueryResult
from couchbase_operational_insights.protocol._core.client_adapter import _ClientAdapter
from couchbase_operational_insights.protocol._core.request import _RequestBuilder
from couchbase_operational_insights.protocol._core.request_context import RequestContext, StreamingRequestContext
from couchbase_operational_insights.protocol._core.response import HttpResponse
from couchbase_operational_insights.protocol.query_handle import BlockingQueryHandle
from couchbase_operational_insights.protocol.streaming import HttpStreamingResponse

if TYPE_CHECKING:
    from couchbase_operational_insights.protocol.database import Database


class Scope:
    def __init__(self, database: Database, scope_name: str) -> None:
        self._database = database
        self._scope_name = scope_name
        self._request_builder = _RequestBuilder(self._database.client_adapter, self._database.name, self.name)

    @property
    def client_adapter(self) -> _ClientAdapter:
        """
        **INTERNAL**
        """
        return self._database.client_adapter

    @property
    def name(self) -> str:
        """
        str: The name of this :class:`~couchbase_operational_insights.protocol.scope.Scope` instance.
        """
        return self._scope_name

    @property
    def threadpool_executor(self) -> ThreadPoolExecutor:
        """
        **INTERNAL**
        """
        return self._database.threadpool_executor

    def execute_query(
        self, statement: str, *args: object, **kwargs: object
    ) -> Union[BlockingQueryResult, Future[BlockingQueryResult]]:
        req = self._request_builder.build_query_request(statement, *args, **kwargs)
        lazy_execute = req.options.pop('lazy_execute', None)
        stream_config = req.options.pop('stream_config', None)
        request_context = StreamingRequestContext(
            self.client_adapter, req, self.threadpool_executor, stream_config=stream_config
        )
        resp = HttpStreamingResponse(request_context, lazy_execute=lazy_execute)

        def _execute_query(http_response: HttpStreamingResponse) -> BlockingQueryResult:
            http_response.send_request()
            return BlockingQueryResult(http_response)

        if request_context.cancel_enabled is True:
            if lazy_execute is True:
                raise RuntimeError(
                    (
                        'Cannot cancel, via cancel token, a query that is executed lazily.'
                        ' Queries executed lazily can be cancelled only after iteration begins.'
                    )
                )
            return request_context.send_request_in_background(_execute_query, resp)
        else:
            if lazy_execute is not True:
                resp.send_request()
            return BlockingQueryResult(resp)

    def start_query(self, statement: str, *args: object, **kwargs: object) -> BlockingQueryHandle:
        base_req = self._request_builder.build_start_query_request(statement, *args, **kwargs)
        stream_config = base_req.options.pop('stream_config', None)
        request_context = RequestContext(self.client_adapter, base_req)
        resp = HttpResponse(request_context)
        resp.send_request()
        request_id, handle = get_query_handle_fields(resp.json_response)
        return BlockingQueryHandle(
            self.client_adapter,
            self._request_builder,
            request_id,
            handle,
            self.threadpool_executor,
            stream_config=stream_config,
        )
