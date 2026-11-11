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

import sys
from typing import TYPE_CHECKING, Awaitable, Optional

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from acouchbase_operational_insights.protocol._core.anyio_utils import current_async_library
from acouchbase_operational_insights.protocol._core.client_adapter import _AsyncClientAdapter
from acouchbase_operational_insights.protocol._core.request_context import (
    AsyncRequestContext,
    AsyncStreamingRequestContext,
)
from acouchbase_operational_insights.protocol._core.response import AsyncHttpResponse
from acouchbase_operational_insights.protocol.query_handle import AsyncQueryHandle
from acouchbase_operational_insights.protocol.streaming import AsyncHttpStreamingResponse
from acouchbase_operational_insights.result import AsyncQueryResult
from couchbase_operational_insights.common._core.query_handle import get_query_handle_fields
from couchbase_operational_insights.common.logging import LogLevel
from couchbase_operational_insights.protocol._core.request import _RequestBuilder

if TYPE_CHECKING:
    from acouchbase_operational_insights.protocol.database import AsyncDatabase
    from couchbase_operational_insights.common._core import JsonStreamConfig


class AsyncScope:
    def __init__(self, database: AsyncDatabase, scope_name: str) -> None:
        self._database = database
        self._scope_name = scope_name
        self._request_builder = _RequestBuilder(self.client_adapter, self._database.name, self.name)
        self._backend = current_async_library()

    @property
    def client_adapter(self) -> _AsyncClientAdapter:
        """
        **INTERNAL**
        """
        return self._database.client_adapter

    @property
    def name(self) -> str:
        """
        str: The name of this :class:`~acouchbase_operational_insights.protocol.scope.Scope` instance.
        """
        return self._scope_name

    async def _create_client(self) -> None:
        """
        **INTERNAL**
        """
        await self.client_adapter.create_client()

    async def _execute_query(self, http_resp: AsyncHttpStreamingResponse) -> AsyncQueryResult:
        if not self.client_adapter.has_client:
            self.client_adapter.log_message(
                'Cluster does not have a connection.  Creating the client.', LogLevel.WARNING
            )
            await self._create_client()
        await http_resp.send_request()
        return AsyncQueryResult(http_resp)

    def execute_query(self, statement: str, *args: object, **kwargs: object) -> Awaitable[AsyncQueryResult]:
        req = self._request_builder.build_query_request(statement, *args, **kwargs)
        stream_config = req.options.pop('stream_config', None)
        request_context = AsyncStreamingRequestContext(
            client_adapter=self.client_adapter, request=req, stream_config=stream_config, backend=self._backend
        )
        resp = AsyncHttpStreamingResponse(request_context)
        if self._backend.backend_lib == 'asyncio':
            return request_context.create_response_task(self._execute_query, resp)
        return self._execute_query(resp)

    async def _start_query(
        self, http_resp: AsyncHttpResponse, stream_config: Optional[JsonStreamConfig]
    ) -> AsyncQueryHandle:
        if not self.client_adapter.has_client:
            self.client_adapter.log_message(
                'Cluster does not have a connection.  Creating the client.', LogLevel.WARNING
            )
            await self._create_client()
        await http_resp.send_request()
        request_id, handle = get_query_handle_fields(http_resp.json_response)
        return AsyncQueryHandle(
            self.client_adapter, self._request_builder, request_id, handle, stream_config=stream_config
        )

    def start_query(self, statement: str, *args: object, **kwargs: object) -> Awaitable[AsyncQueryHandle]:
        req = self._request_builder.build_start_query_request(statement, *args, **kwargs)
        stream_config = req.options.pop('stream_config', None)
        request_context = AsyncRequestContext(self.client_adapter, req, backend=self._backend)
        resp = AsyncHttpResponse(request_context)
        if self._backend.backend_lib == 'asyncio':
            return request_context.create_response_task(self._start_query, resp, stream_config)
        return self._start_query(resp, stream_config)


Scope: TypeAlias = AsyncScope
