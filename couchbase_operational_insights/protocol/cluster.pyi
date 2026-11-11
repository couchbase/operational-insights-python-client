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

import sys
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional, overload

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from couchbase_operational_insights import JSONType
from couchbase_operational_insights.common._core import JsonStreamConfig
from couchbase_operational_insights.common.credential import Credential
from couchbase_operational_insights.common.result import BlockingQueryResult
from couchbase_operational_insights.options import (
    ClusterOptions,
    ClusterOptionsKwargs,
    QueryOptions,
    QueryOptionsKwargs,
    StartQueryOptions,
    StartQueryOptionsKwargs,
)
from couchbase_operational_insights.protocol._core.client_adapter import _ClientAdapter
from couchbase_operational_insights.protocol.query_handle import BlockingQueryHandle

class Cluster:
    @overload
    def __init__(self, http_endpoint: str, credential: Credential) -> None: ...
    @overload
    def __init__(self, http_endpoint: str, credential: Credential, options: ClusterOptions) -> None: ...
    @overload
    def __init__(self, http_endpoint: str, credential: Credential, **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...
    @overload
    def __init__(
        self,
        http_endpoint: str,
        credential: Credential,
        options: ClusterOptions,
        **kwargs: Unpack[ClusterOptionsKwargs],
    ) -> None: ...
    @property
    def client_adapter(self) -> _ClientAdapter: ...
    @property
    def connected(self) -> bool: ...
    @property
    def threadpool_executor(self) -> ThreadPoolExecutor: ...
    def create_query_handle(
        self, request_id: str, handle: str, stream_config: Optional[JsonStreamConfig] = None
    ) -> BlockingQueryHandle: ...
    @overload
    def execute_query(self, statement: str) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, options: QueryOptions) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, **kwargs: Unpack[QueryOptionsKwargs]) -> BlockingQueryResult: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> BlockingQueryResult: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> BlockingQueryResult: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, **kwargs: str
    ) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, *args: JSONType, **kwargs: str) -> BlockingQueryResult: ...
    @overload
    def execute_query(self, statement: str, enable_cancel: bool) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(self, statement: str, enable_cancel: bool, *args: JSONType) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, enable_cancel: bool
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, enable_cancel: bool, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, enable_cancel: bool, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self,
        statement: str,
        options: QueryOptions,
        enable_cancel: bool,
        *args: JSONType,
        **kwargs: Unpack[QueryOptionsKwargs],
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self,
        statement: str,
        options: QueryOptions,
        *args: JSONType,
        enable_cancel: bool,
        **kwargs: Unpack[QueryOptionsKwargs],
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, enable_cancel: bool, *args: JSONType, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, enable_cancel: bool, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, enable_cancel: bool, *args: JSONType, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, *args: JSONType, enable_cancel: bool, **kwargs: str
    ) -> Future[BlockingQueryResult]: ...
    @overload
    def start_query(self, statement: str) -> BlockingQueryHandle: ...
    @overload
    def start_query(self, statement: str, options: StartQueryOptions) -> BlockingQueryHandle: ...
    @overload
    def start_query(self, statement: str, **kwargs: Unpack[StartQueryOptionsKwargs]) -> BlockingQueryHandle: ...
    @overload
    def start_query(
        self, statement: str, options: StartQueryOptions, **kwargs: Unpack[StartQueryOptionsKwargs]
    ) -> BlockingQueryHandle: ...
    @overload
    def start_query(
        self, statement: str, options: StartQueryOptions, *args: JSONType, **kwargs: Unpack[StartQueryOptionsKwargs]
    ) -> BlockingQueryHandle: ...
    @overload
    def start_query(
        self, statement: str, options: StartQueryOptions, *args: JSONType, **kwargs: str
    ) -> BlockingQueryHandle: ...
    @overload
    def start_query(self, statement: str, *args: JSONType, **kwargs: str) -> BlockingQueryHandle: ...
    def set_credential(self, credential: Credential) -> None: ...
    def shutdown(self) -> None: ...
    @overload
    @classmethod
    def create_instance(cls, http_endpoint: str, credential: Credential) -> Cluster: ...
    @overload
    @classmethod
    def create_instance(cls, http_endpoint: str, credential: Credential, options: ClusterOptions) -> Cluster: ...
    @overload
    @classmethod
    def create_instance(
        cls, http_endpoint: str, credential: Credential, **kwargs: Unpack[ClusterOptionsKwargs]
    ) -> Cluster: ...
    @overload
    @classmethod
    def create_instance(
        cls, http_endpoint: str, credential: Credential, options: ClusterOptions, **kwargs: Unpack[ClusterOptionsKwargs]
    ) -> Cluster: ...
