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
from typing import Awaitable, Optional, overload

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from acouchbase_operational_insights import JSONType
from acouchbase_operational_insights.credential import Credential
from acouchbase_operational_insights.options import (
    ClusterOptions,
    ClusterOptionsKwargs,
    QueryOptions,
    QueryOptionsKwargs,
    StartQueryOptions,
    StartQueryOptionsKwargs,
)
from acouchbase_operational_insights.protocol._core.client_adapter import _AsyncClientAdapter
from acouchbase_operational_insights.protocol.database import AsyncDatabase
from acouchbase_operational_insights.protocol.query_handle import AsyncQueryHandle
from acouchbase_operational_insights.result import AsyncQueryResult
from couchbase_operational_insights.common._core import JsonStreamConfig

class AsyncCluster:
    @overload
    def __init__(self, endpoint: str, credential: Credential) -> None: ...
    @overload
    def __init__(self, endpoint: str, credential: Credential, options: ClusterOptions) -> None: ...
    @overload
    def __init__(self, endpoint: str, credential: Credential, **kwargs: Unpack[ClusterOptionsKwargs]) -> None: ...
    @overload
    def __init__(
        self, endpoint: str, credential: Credential, options: ClusterOptions, **kwargs: Unpack[ClusterOptionsKwargs]
    ) -> None: ...
    @property
    def client_adapter(self) -> _AsyncClientAdapter: ...
    @property
    def connected(self) -> bool: ...
    def create_query_handle(
        self, request_id: str, handle: str, stream_config: Optional[JsonStreamConfig] = None
    ) -> AsyncQueryHandle: ...
    async def set_credential(self, credential: Credential) -> None: ...
    async def shutdown(self) -> None: ...
    def database(self, name: str) -> AsyncDatabase: ...
    @overload
    def execute_query(self, statement: str) -> Awaitable[AsyncQueryResult]: ...
    @overload
    def execute_query(self, statement: str, options: QueryOptions) -> Awaitable[AsyncQueryResult]: ...
    @overload
    def execute_query(self, statement: str, **kwargs: Unpack[QueryOptionsKwargs]) -> Awaitable[AsyncQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> Awaitable[AsyncQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, **kwargs: Unpack[QueryOptionsKwargs]
    ) -> Awaitable[AsyncQueryResult]: ...
    @overload
    def execute_query(
        self, statement: str, options: QueryOptions, *args: JSONType, **kwargs: str
    ) -> Awaitable[AsyncQueryResult]: ...
    @overload
    def execute_query(self, statement: str, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryResult]: ...
    @overload
    def start_query(self, statement: str) -> Awaitable[AsyncQueryHandle]: ...
    @overload
    def start_query(self, statement: str, options: StartQueryOptions) -> Awaitable[AsyncQueryHandle]: ...
    @overload
    def start_query(self, statement: str, **kwargs: Unpack[StartQueryOptionsKwargs]) -> Awaitable[AsyncQueryHandle]: ...
    @overload
    def start_query(
        self, statement: str, options: StartQueryOptions, **kwargs: Unpack[StartQueryOptionsKwargs]
    ) -> Awaitable[AsyncQueryHandle]: ...
    @overload
    def start_query(
        self, statement: str, options: StartQueryOptions, *args: JSONType, **kwargs: Unpack[StartQueryOptionsKwargs]
    ) -> Awaitable[AsyncQueryHandle]: ...
    @overload
    def start_query(
        self, statement: str, options: StartQueryOptions, *args: JSONType, **kwargs: str
    ) -> Awaitable[AsyncQueryHandle]: ...
    @overload
    def start_query(self, statement: str, *args: JSONType, **kwargs: str) -> Awaitable[AsyncQueryHandle]: ...
    @overload
    @classmethod
    def create_instance(cls, endpoint: str, credential: Credential) -> AsyncCluster: ...
    @overload
    @classmethod
    def create_instance(cls, endpoint: str, credential: Credential, options: ClusterOptions) -> AsyncCluster: ...
    @overload
    @classmethod
    def create_instance(
        cls, endpoint: str, credential: Credential, **kwargs: Unpack[ClusterOptionsKwargs]
    ) -> AsyncCluster: ...
    @overload
    @classmethod
    def create_instance(
        cls, endpoint: str, credential: Credential, options: ClusterOptions, **kwargs: Unpack[ClusterOptionsKwargs]
    ) -> AsyncCluster: ...
