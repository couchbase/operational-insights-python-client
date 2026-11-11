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

import atexit
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional, Union
from uuid import uuid4

from couchbase_operational_insights.common._core.query_handle import get_query_handle_fields
from couchbase_operational_insights.common.logging import LogLevel
from couchbase_operational_insights.common.result import BlockingQueryResult
from couchbase_operational_insights.protocol._core.client_adapter import _ClientAdapter
from couchbase_operational_insights.protocol._core.request import _RequestBuilder
from couchbase_operational_insights.protocol._core.request_context import RequestContext, StreamingRequestContext
from couchbase_operational_insights.protocol._core.response import HttpResponse
from couchbase_operational_insights.protocol.query_handle import BlockingQueryHandle
from couchbase_operational_insights.protocol.streaming import HttpStreamingResponse

if TYPE_CHECKING:
    from couchbase_operational_insights.common._core import JsonStreamConfig
    from couchbase_operational_insights.common.credential import Credential
    from couchbase_operational_insights.options import ClusterOptions


class Cluster:
    def __init__(
        self, http_endpoint: str, credential: Credential, options: Optional[ClusterOptions] = None, **kwargs: object
    ) -> None:
        self._cluster_id = str(uuid4())
        kwargs['cluster_id'] = self._cluster_id
        self._client_adapter = _ClientAdapter(http_endpoint, credential, options, **kwargs)
        self._request_builder = _RequestBuilder(self._client_adapter)
        self._create_client()
        # TODO(PYCO-75):  make a custom ThreadPoolExecutor, so that we can override submit and have a way to get
        #        a "plain" future as the docs say we should create a future via an executor
        #        The RequestContext generates a future that enables some background processing
        # Allow the default max_workers which is (as of Python 3.8): min(32, os.cpu_count() + 4).
        # We can add an option later if we see a need
        self._tp_executor_prefix = f'pycboi-tpe-{self._cluster_id[:8]}'
        self._tp_executor = ThreadPoolExecutor(thread_name_prefix=self._tp_executor_prefix)
        self._client_adapter.log_message(f'Created ThreadPoolExecutor({self._tp_executor_prefix})', LogLevel.INFO)
        self._tp_executor_shutdown_called = False
        atexit.register(self._shutdown_executor)

    @property
    def client_adapter(self) -> _ClientAdapter:
        """
        **INTERNAL**
        """
        return self._client_adapter

    @property
    def cluster_id(self) -> str:
        """
        **INTERNAL**
        """
        return self._cluster_id

    @property
    def has_client(self) -> bool:
        """
        bool: Indicator on if the cluster HTTP client has been created or not.
        """
        return self._client_adapter.has_client

    @property
    def threadpool_executor(self) -> ThreadPoolExecutor:
        """
        **INTERNAL**
        """
        return self._tp_executor

    def _shutdown(self) -> None:
        """
        **INTERNAL**
        """
        atexit.unregister(self._shutdown_executor)
        self._client_adapter.close_client()
        self._client_adapter.reset_client()
        self._shutdown_executor()

    def _create_client(self) -> None:
        """
        **INTERNAL**
        """
        self._client_adapter.create_client()

    def _shutdown_executor(self) -> None:
        if self._tp_executor_shutdown_called is False:
            if not sys.is_finalizing():
                self._client_adapter.log_message(
                    f'Shutting down ThreadPoolExecutor({self._tp_executor_prefix})', LogLevel.INFO
                )
            self._tp_executor.shutdown()
        self._tp_executor_shutdown_called = True

    def shutdown(self) -> None:
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        if self.has_client:
            self._shutdown()
        else:
            self._client_adapter.log_message('Cluster does not have a connection, no need to shutdown.', LogLevel.INFO)

    def set_credential(self, credential: Credential) -> None:
        self._client_adapter.update_credential(credential)

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
        return self.create_query_handle(request_id, handle, stream_config=stream_config)

    def create_query_handle(
        self, request_id: str, handle: str, stream_config: Optional[JsonStreamConfig] = None
    ) -> BlockingQueryHandle:
        """
        **INTERNAL**
        """
        return BlockingQueryHandle(
            self._client_adapter,
            self._request_builder,
            request_id,
            handle,
            self._tp_executor,
            stream_config=stream_config,
        )

    @classmethod
    def create_instance(
        cls, http_endpoint: str, credential: Credential, options: Optional[ClusterOptions], **kwargs: object
    ) -> Cluster:
        return cls(http_endpoint, credential, options, **kwargs)
