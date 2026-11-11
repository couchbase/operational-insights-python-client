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

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Optional

from couchbase_operational_insights.common._core.query_handle import QueryHandle, QueryResultHandle, QueryStatus
from couchbase_operational_insights.common.options import FetchResultsOptions
from couchbase_operational_insights.common.result import AsyncQueryResult, BlockingQueryResult

if TYPE_CHECKING:
    from acouchbase_operational_insights.cluster import AsyncCluster
    from acouchbase_operational_insights.protocol.cluster import AsyncCluster as _AsyncCluster
    from couchbase_operational_insights.cluster import Cluster
    from couchbase_operational_insights.protocol.cluster import Cluster as _Cluster


class AsyncQueryHandle(QueryHandle):
    @abstractmethod
    def cancel(self, options: Optional[Any] = None, **kwargs: Any) -> Awaitable[None]:
        """
        Cancel the query associated with the QueryHandle.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_status(self, options: Optional[Any] = None, **kwargs: Any) -> Awaitable[AsyncQueryStatus]:
        raise NotImplementedError

    @abstractmethod
    def to_serialized(self) -> str:
        """**VOLATILE** Serialize the QueryHandle, so that the query can be continued in another process.

        Treat the serialized form as opaque.  No network calls are made.

        Returns:
            str: The serialized QueryHandle.  Use :meth:`from_serialized` to deserialize it.
        """
        raise NotImplementedError

    @classmethod
    def from_serialized(cls, cluster: AsyncCluster, serialized: str) -> AsyncQueryHandle:
        """**VOLATILE** Deserialize a QueryHandle serialized by :meth:`to_serialized`.

        The returned QueryHandle sends all requests to ``cluster``.  No network calls are made.

        .. note::
            The server ties a query to the user who started it.  If ``cluster`` uses another user's credentials,
            :meth:`fetch_status` raises :class:`~acouchbase_operational_insights.errors.QueryNotFoundError`.

        Args:
            cluster: The cluster to send requests to.  A scope is not accepted.  The handle does not depend on the
                scope that started the query.
            serialized: A QueryHandle serialized by :meth:`to_serialized`.

        Returns:
            AsyncQueryHandle: A QueryHandle for the query.

        Raises:
            ValueError: If ``cluster`` is not an :class:`~acouchbase_operational_insights.cluster.AsyncCluster`.
            ValueError: If ``serialized`` is not a JSON object with non-empty string ``requestID`` and ``handle``
                properties.
        """
        from acouchbase_operational_insights.cluster import AsyncCluster
        from couchbase_operational_insights.common._core.query_handle import deserialize_query_handle

        if not isinstance(cluster, AsyncCluster):
            raise ValueError('An AsyncQueryHandle can only be deserialized with an AsyncCluster.')
        impl: _AsyncCluster = cluster._impl  # type: ignore[attr-defined]
        return impl.create_query_handle(*deserialize_query_handle(serialized))


class BlockingQueryHandle(QueryHandle):
    @abstractmethod
    def cancel(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Cancel the query associated with the QueryHandle.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_status(self, options: Optional[Any] = None, **kwargs: Any) -> BlockingQueryStatus:
        raise NotImplementedError

    @abstractmethod
    def to_serialized(self) -> str:
        """**VOLATILE** Serialize the QueryHandle, so that the query can be continued in another process.

        Treat the serialized form as opaque.  No network calls are made.

        Returns:
            str: The serialized QueryHandle.  Use :meth:`from_serialized` to deserialize it.
        """
        raise NotImplementedError

    @classmethod
    def from_serialized(cls, cluster: Cluster, serialized: str) -> BlockingQueryHandle:
        """**VOLATILE** Deserialize a QueryHandle serialized by :meth:`to_serialized`.

        The returned QueryHandle sends all requests to ``cluster``.  No network calls are made.

        .. note::
            The server ties a query to the user who started it.  If ``cluster`` uses another user's credentials,
            :meth:`fetch_status` raises :class:`~couchbase_operational_insights.errors.QueryNotFoundError`.

        Args:
            cluster: The cluster to send requests to.  A scope is not accepted.  The handle does not depend on the
                scope that started the query.
            serialized: A QueryHandle serialized by :meth:`to_serialized`.

        Returns:
            BlockingQueryHandle: A QueryHandle for the query.

        Raises:
            ValueError: If ``cluster`` is not a :class:`~couchbase_operational_insights.cluster.Cluster`.
            ValueError: If ``serialized`` is not a JSON object with non-empty string ``requestID`` and ``handle``
                properties.
        """
        from couchbase_operational_insights.cluster import Cluster
        from couchbase_operational_insights.common._core.query_handle import deserialize_query_handle

        if not isinstance(cluster, Cluster):
            raise ValueError('A BlockingQueryHandle can only be deserialized with a Cluster.')
        impl: _Cluster = cluster._impl  # type: ignore[attr-defined]
        return impl.create_query_handle(*deserialize_query_handle(serialized))


class AsyncQueryResultHandle(QueryResultHandle):
    """Abstract base class for async query result handle."""

    @abstractmethod
    def fetch_results(
        self, options: Optional[FetchResultsOptions] = None, **kwargs: Any
    ) -> Awaitable[AsyncQueryResult]:
        """
        Get all the results.
        """
        raise NotImplementedError

    @abstractmethod
    def discard_results(self, options: Optional[Any] = None, **kwargs: Any) -> Awaitable[None]:
        """
        Discard the query results associated with the AsyncQueryResultHandle.
        """
        raise NotImplementedError


class BlockingQueryResultHandle(QueryResultHandle):
    """Abstract base class for query result handle."""

    @abstractmethod
    def fetch_results(self, options: Optional[FetchResultsOptions] = None, **kwargs: Any) -> BlockingQueryResult:
        """
        Get all the results.
        """
        raise NotImplementedError

    @abstractmethod
    def discard_results(self, options: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Discard the query results associated with the BlockingQueryResultHandle.
        """
        raise NotImplementedError


class AsyncQueryStatus(QueryStatus):
    @abstractmethod
    def results_ready(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def result_handle(self) -> AsyncQueryResultHandle:
        raise NotImplementedError


class BlockingQueryStatus(QueryStatus):
    @abstractmethod
    def results_ready(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def result_handle(self) -> BlockingQueryResultHandle:
        raise NotImplementedError
