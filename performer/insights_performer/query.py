from __future__ import annotations

import asyncio
import logging
import sys
from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, AsyncIterator, Iterator, Optional, Union
from uuid import uuid4

from google.protobuf import struct_pb2 as struct_pb
from google.protobuf import timestamp_pb2 as timestamp_pb

from acouchbase_operational_insights.cluster import AsyncCluster
from acouchbase_operational_insights.query_handle import AsyncQueryHandle, AsyncQueryResultHandle, AsyncQueryStatus
from acouchbase_operational_insights.scope import AsyncScope
from couchbase_operational_insights.cluster import Cluster
from couchbase_operational_insights.options import FetchResultsOptions, QueryOptions, StartQueryOptions
from couchbase_operational_insights.query import QueryMetadata, QueryScanConsistency
from couchbase_operational_insights.query_handle import (
    BlockingQueryHandle,
    BlockingQueryResultHandle,
    BlockingQueryStatus,
)
from couchbase_operational_insights.result import AsyncQueryResult, BlockingQueryResult
from couchbase_operational_insights.scope import Scope
from insights_performer.clusters import ClusterManager
from insights_performer.exceptions import PerformerException
from insights_performer.options import SharedOptions
from insights_performer.protocol.columnar import metadata_pb2 as metadata_pb
from insights_performer.protocol.columnar import query_pb2 as query_pb
from insights_performer.protocol.columnar import result_pb2 as result_pb
from insights_performer.protocol.columnar import serialization_pb2 as serialization_pb
from insights_performer.result import to_proto_error

IS_PYTHON_3_10 = sys.version_info >= (3, 10)


def _convert_metadata(meta: QueryMetadata) -> query_pb.QueryResultMetadataResponse.QueryMetadata:
    proto_meta = query_pb.QueryResultMetadataResponse.QueryMetadata(request_id=meta.request_id())
    for warn in meta.warnings():
        proto_meta.warnings.append(
            query_pb.QueryResultMetadataResponse.QueryMetadata.Warning(code=warn.code(), message=warn.message())
        )

    proto_meta.metrics.elapsed_time.FromTimedelta(meta.metrics().elapsed_time())
    proto_meta.metrics.execution_time.FromTimedelta(meta.metrics().execution_time())
    proto_meta.metrics.result_count = meta.metrics().result_count()
    proto_meta.metrics.result_size = meta.metrics().result_size()
    proto_meta.metrics.processed_objects = meta.metrics().processed_objects()

    return proto_meta


def _convert_row(row: Any) -> serialization_pb.ContentWas:
    content = serialization_pb.ContentWas()
    if row is None:
        content.content_was_null = struct_pb.NULL_VALUE
    elif isinstance(row, dict):
        content.content_was_map.update(row)
    elif isinstance(row, bytes):
        content.content_was_bytes = row
    elif isinstance(row, int):
        content.content_was_integer = row
    elif isinstance(row, float):
        content.content_was_float = row
    elif isinstance(row, str):
        content.content_was_string = row
    else:
        raise PerformerException(f'Unexpected row type: {str(type(row))}')
    return content


class QueryOperation(ABC):
    def __init__(self, **kwargs: Any):
        self._args = [kwargs.get('statement')]
        if 'options' in kwargs:
            self._args.append(kwargs.get('options'))
        self._handle = str(uuid4())

    @property
    def handle(self) -> str:
        return self._handle

    @abstractmethod
    def start(self, initiated: timestamp_pb.Timestamp) -> query_pb.ExecuteQueryResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def wait_for_result(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def next_row(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def cancel(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def metadata(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryResultMetadataResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError('Concrete subclass must implement this method')

    @classmethod
    def _convert_options(  # noqa: C901
        cls,
        proto_opts: query_pb.ExecuteQueryRequest.Options,
    ) -> QueryOptions:
        kwargs = {}

        if proto_opts.HasField('parameters_positional'):
            kwargs['positional_parameters'] = list(proto_opts.parameters_positional)
        if proto_opts.HasField('parameters_named'):
            kwargs['named_parameters'] = dict(proto_opts.parameters_named)
        if proto_opts.HasField('readonly'):
            kwargs['readonly'] = proto_opts.readonly
        if proto_opts.HasField('scan_consistency'):
            if proto_opts.scan_consistency == query_pb.ExecuteQueryRequest.Options.SCAN_CONSISTENCY_NOT_BOUNDED:
                kwargs['scan_consistency'] = QueryScanConsistency.NOT_BOUNDED
            elif proto_opts.scan_consistency == query_pb.ExecuteQueryRequest.Options.SCAN_CONSISTENCY_REQUEST_PLUS:
                kwargs['scan_consistency'] = QueryScanConsistency.REQUEST_PLUS
            else:
                raise PerformerException(
                    f'Unsupported query scan consistency value: '
                    f'{query_pb.ExecuteQueryRequest.Options.ScanConsistency.Name(proto_opts.scan_consistency)}'
                )
        if proto_opts.HasField('raw'):
            kwargs['raw'] = dict(proto_opts.raw)
        if proto_opts.HasField('timeout'):
            kwargs['timeout'] = proto_opts.timeout.ToTimedelta()
        if proto_opts.HasField('deserializer'):
            kwargs['deserializer'] = SharedOptions.convert_deserializer(proto_opts.deserializer)
        if proto_opts.HasField('max_retries'):
            kwargs['max_retries'] = proto_opts.max_retries

        return QueryOptions(**kwargs)

    @classmethod
    def build(
        cls,
        req: query_pb.ExecuteQueryRequest,
        cluster_or_scope: Union[Cluster, AsyncCluster, Scope, AsyncScope],
    ) -> QueryOperation:
        kwargs = {
            'statement': req.statement,
            'cluster_or_scope': cluster_or_scope,
            'requires_cancellation': req.require_cancellation,
        }
        logging.getLogger().info(f'Executing query on: {type(cluster_or_scope)}')
        if req.HasField('options'):
            kwargs['options'] = cls._convert_options(req.options)

        return cls(**kwargs)


class PollingQueryOperation(ABC):
    def __init__(self, **kwargs: Any):
        self._args = [kwargs.get('statement')]
        if 'options' in kwargs:
            self._args.append(kwargs.get('options'))
        self._handle = str(uuid4())

    @property
    def handle(self) -> str:
        return self._handle

    @abstractmethod
    def start(self, initiated: timestamp_pb.Timestamp) -> query_pb.StartQueryResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def fetch_status(self, initiated: timestamp_pb.Timestamp) -> query_pb.AsyncFetchStatusResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def query_status_result(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def fetch_results(
        self, options: query_pb.AsyncFetchResultsRequest.Options, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def discard_results(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def cancel_handle(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def next_row(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def cancel(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def metadata(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryResultMetadataResponse:
        raise NotImplementedError('Concrete subclass must implement this method')

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError('Concrete subclass must implement this method')

    @classmethod
    def _convert_start_query_options(cls, proto_opts: query_pb.ExecuteQueryRequest.Options) -> StartQueryOptions:
        kwargs = {}

        if proto_opts.HasField('parameters_positional'):
            kwargs['positional_parameters'] = list(proto_opts.parameters_positional)
        if proto_opts.HasField('parameters_named'):
            kwargs['named_parameters'] = dict(proto_opts.parameters_named)
        if proto_opts.HasField('readonly'):
            kwargs['readonly'] = proto_opts.readonly
        if proto_opts.HasField('scan_consistency'):
            if proto_opts.scan_consistency == query_pb.ExecuteQueryRequest.Options.SCAN_CONSISTENCY_NOT_BOUNDED:
                kwargs['scan_consistency'] = QueryScanConsistency.NOT_BOUNDED
            elif proto_opts.scan_consistency == query_pb.ExecuteQueryRequest.Options.SCAN_CONSISTENCY_REQUEST_PLUS:
                kwargs['scan_consistency'] = QueryScanConsistency.REQUEST_PLUS
            else:
                raise PerformerException(
                    f'Unsupported query scan consistency value: '
                    f'{query_pb.ExecuteQueryRequest.Options.ScanConsistency.Name(proto_opts.scan_consistency)}'
                )
        if proto_opts.HasField('raw'):
            kwargs['raw'] = dict(proto_opts.raw)
        if proto_opts.HasField('timeout'):
            kwargs['timeout'] = proto_opts.timeout.ToTimedelta()
        if proto_opts.HasField('max_retries'):
            kwargs['max_retries'] = proto_opts.max_retries

        return StartQueryOptions(**kwargs)

    @classmethod
    def _convert_fetch_results_options(
        cls, proto_opts: query_pb.AsyncFetchResultsRequest.Options
    ) -> FetchResultsOptions:
        kwargs = {}
        if proto_opts.HasField('deserializer'):
            kwargs['deserializer'] = SharedOptions.convert_deserializer(proto_opts.deserializer)

        return FetchResultsOptions(**kwargs)

    @classmethod
    def build(
        cls,
        req: query_pb.StartQueryRequest,
        cluster_or_scope: Union[Cluster, AsyncCluster, Scope, AsyncScope],
    ) -> QueryOperation:
        kwargs = {
            'statement': req.statement,
            'cluster_or_scope': cluster_or_scope,
        }

        logging.getLogger().info(f'Executing start_query on: {type(cluster_or_scope)}')
        if req.HasField('options'):
            kwargs['options'] = cls._convert_start_query_options(req.options)

        return cls(**kwargs)


class BlockingQueryOperation(QueryOperation):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self._cluster_or_scope: Union[Cluster, Scope] = kwargs.get('cluster_or_scope')
        self._enable_cancel: Optional[bool] = None
        if kwargs.get('requires_cancellation'):
            self._enable_cancel = True
            self._args.append(self._enable_cancel)

        # Even if the test does not call for cancellation, we enable cancellation in order
        # to get a future and avoid having to use a thread pool executor.
        self._future: Optional[Future[BlockingQueryResult]] = None
        self._result: Optional[BlockingQueryResult] = None
        self._rows: Optional[Iterator[Any]] = None

    def start(self, initiated: timestamp_pb.Timestamp) -> query_pb.ExecuteQueryResponse:
        if initiated is None:
            raise PerformerException('Query operation must be initiated with a timestamp')
        if self._enable_cancel is None:
            self._future = self._cluster_or_scope.execute_query(*self._args, enable_cancel=True)
        else:
            self._future = self._cluster_or_scope.execute_query(*self._args)
        return query_pb.ExecuteQueryResponse(
            query_handle=self._handle, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def wait_for_result(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._future is None:
            raise PerformerException('Query operation has not been started')

        try:
            self._result = self._future.result()
            self._rows = iter(self._result.rows())
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def next_row(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        if self._rows is None:
            raise PerformerException('Query result is not available')

        try:
            row = next(self._rows)
            content = _convert_row(row)
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(row=query_pb.QueryRowResponse.Row(row_content=content)),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except PerformerException as e:
            raise e
        except StopIteration:
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(end_of_stream=True),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except Exception as e:
            return query_pb.QueryRowResponse(
                row_level_failure=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

    def cancel(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._result is None:
            if self._enable_cancel is None:
                raise PerformerException('Unexpected cancellation - requires_cancellation was not set')
            self._future.cancel()
        else:
            self._result.cancel()
        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def metadata(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryResultMetadataResponse:
        if self._result is None:
            raise PerformerException('The execute query operation has not completed yet')

        try:
            metadata = self._result.metadata()
            resp_metadata = metadata_pb.ResponseMetadata(initiated=initiated)
            return query_pb.QueryResultMetadataResponse(success=_convert_metadata(metadata), metadata=resp_metadata)
        except Exception as e:
            logging.getLogger().error(f'Error fetching query metadata: {str(e)}')
            return query_pb.QueryResultMetadataResponse(
                failure=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

    def close(self) -> None:
        """no-op for blocking operation"""


class BlockingPollingQueryOperation(PollingQueryOperation):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self._cluster_or_scope: Union[Cluster, Scope] = kwargs.get('cluster_or_scope')
        self._query_handle: Optional[BlockingQueryHandle] = None
        self._query_status: Optional[BlockingQueryStatus] = None
        self._query_result_handle: Optional[BlockingQueryResultHandle] = None
        self._result: Optional[BlockingQueryResult] = None
        self._rows: Optional[Iterator[Any]] = None

    def start(self, _: timestamp_pb.Timestamp) -> query_pb.StartQueryResponse:
        try:
            self._query_handle = self._cluster_or_scope.start_query(*self._args)
        except Exception as e:
            return query_pb.StartQueryResponse(
                failure=to_proto_error(e),
            )

        # the driver wants a UUID that represents the query handle
        return query_pb.StartQueryResponse(
            query_handle=self._handle,
        )

    def fetch_status(self, _: timestamp_pb.Timestamp) -> query_pb.AsyncFetchStatusResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        try:
            self._query_status = self._query_handle.fetch_status()
        except Exception as e:
            return query_pb.AsyncFetchStatusResponse(
                failure=to_proto_error(e),
            )

        status_result = query_pb.AsyncFetchStatusResponse.QueryStatusResult(
            results_ready=self._query_status.results_ready(),
            to_string=str(self._query_status),
        )
        return query_pb.AsyncFetchStatusResponse(
            query_status=status_result,
        )

    def query_status_result(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        if self._query_status is None:
            raise PerformerException('Query status does not exist')

        try:
            self._query_result_handle = self._query_status.result_handle()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def fetch_results(
        self, options: query_pb.AsyncFetchResultsRequest.Options, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        if self._query_result_handle is None:
            raise PerformerException('Query result handle does not exist')

        opts = self._convert_fetch_results_options(options)

        try:
            self._result = self._query_result_handle.fetch_results(opts)
            self._rows = iter(self._result.rows())
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def discard_results(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        if self._query_result_handle is None:
            raise PerformerException('Query result handle does not exist')

        try:
            self._query_result_handle.discard_results()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def cancel_handle(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        try:
            self._query_handle.cancel()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def next_row(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        if self._rows is None:
            raise PerformerException('Query result is not available')

        try:
            row = next(self._rows)
            content = _convert_row(row)
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(row=query_pb.QueryRowResponse.Row(row_content=content)),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except PerformerException as e:
            raise e
        except StopIteration:
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(end_of_stream=True),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except Exception as e:
            return query_pb.QueryRowResponse(
                row_level_failure=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

    def cancel(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._result is None:
            self._query_handle.cancel()
        else:
            self._result.cancel()
        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def metadata(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryResultMetadataResponse:
        if self._result is None:
            raise PerformerException('The execute query operation has not completed yet')

        try:
            metadata = self._result.metadata()
            resp_metadata = metadata_pb.ResponseMetadata(initiated=initiated)
            return query_pb.QueryResultMetadataResponse(success=_convert_metadata(metadata), metadata=resp_metadata)
        except Exception as e:
            logging.getLogger().error(f'Error fetching query metadata: {str(e)}')
            return query_pb.QueryResultMetadataResponse(
                failure=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

    def close(self) -> None:
        """no-op for blocking operation"""


class AsyncQueryOperation(QueryOperation):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._cluster_or_scope: Union[AsyncCluster, AsyncScope] = kwargs.get('cluster_or_scope')

        self._task: Optional[asyncio.Task[AsyncQueryResult]] = None
        self._result: Optional[AsyncQueryResult] = None
        self._rows: Optional[AsyncIterator[Any]] = None

    def start(self, initiated: timestamp_pb.Timestamp) -> query_pb.ExecuteQueryResponse:
        self._task = self._cluster_or_scope.execute_query(*self._args)
        return query_pb.ExecuteQueryResponse(
            query_handle=self._handle, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    async def wait_for_result(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._task is None:
            raise PerformerException('Query operation has not been started')

        try:
            self._result = await self._task
            self._rows = self._result.rows().__aiter__()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    async def next_row(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        if self._rows is None:
            raise PerformerException('Query result is not available')

        try:
            row = await self._rows.__anext__()
            content = _convert_row(row)
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(row=query_pb.QueryRowResponse.Row(row_content=content)),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except PerformerException as e:
            raise e
        except StopAsyncIteration:
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(end_of_stream=True),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except Exception as e:
            return query_pb.QueryRowResponse(row_level_failure=to_proto_error(e))

    def cancel(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._result is None:
            self._task.cancel()
        else:
            self._result.cancel()
        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def metadata(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryResultMetadataResponse:
        if self._result is None:
            raise PerformerException('The execute query operation has not completed yet')

        try:
            metadata = self._result.metadata()
            return query_pb.QueryResultMetadataResponse(
                success=_convert_metadata(metadata), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )
        except Exception as e:
            return query_pb.QueryResultMetadataResponse(
                failure=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

    async def close(self) -> None:
        if self._result is not None:
            await self._result.shutdown()
        if isinstance(self._cluster_or_scope, AsyncCluster):
            await self._cluster_or_scope.shutdown()


class AsyncPollingQueryOperation(PollingQueryOperation):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._cluster_or_scope: Union[AsyncCluster, AsyncScope] = kwargs.get('cluster_or_scope')
        self._query_handle: Optional[AsyncQueryHandle] = None
        self._query_status: Optional[AsyncQueryStatus] = None
        self._query_result_handle: Optional[AsyncQueryResultHandle] = None
        self._result: Optional[AsyncQueryResult] = None
        self._rows: Optional[AsyncIterator[Any]] = None

    async def start(self, _: timestamp_pb.Timestamp) -> query_pb.ExecuteQueryResponse:
        try:
            self._query_handle = await self._cluster_or_scope.start_query(*self._args)
        except Exception as e:
            return query_pb.StartQueryResponse(
                failure=to_proto_error(e),
            )

        # the driver wants a UUID that represents the query handle
        return query_pb.StartQueryResponse(
            query_handle=self._handle,
        )

    async def fetch_status(self, _: timestamp_pb.Timestamp) -> query_pb.AsyncFetchStatusResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        try:
            self._query_status = await self._query_handle.fetch_status()
        except Exception as e:
            return query_pb.AsyncFetchStatusResponse(
                failure=to_proto_error(e),
            )

        status_result = query_pb.AsyncFetchStatusResponse.QueryStatusResult(
            results_ready=self._query_status.results_ready(),
            to_string=str(self._query_status),
        )
        return query_pb.AsyncFetchStatusResponse(
            query_status=status_result,
        )

    def query_status_result(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        if self._query_status is None:
            raise PerformerException('Query status does not exist')

        try:
            self._query_result_handle = self._query_status.result_handle()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    async def fetch_results(
        self, options: query_pb.AsyncFetchResultsRequest.Options, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        if self._query_result_handle is None:
            raise PerformerException('Query result handle does not exist')

        opts = self._convert_fetch_results_options(options)

        try:
            self._result = await self._query_result_handle.fetch_results(opts)
            self._rows = self._result.rows().__aiter__()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    async def discard_results(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        if self._query_result_handle is None:
            raise PerformerException('Query result handle does not exist')

        try:
            await self._query_result_handle.discard_results()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    async def cancel_handle(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._query_handle is None:
            raise PerformerException('Query operation has not been started')

        try:
            await self._query_handle.cancel()
        except Exception as e:
            return result_pb.EmptyResultOrFailureResponse(
                error=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    async def next_row(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        if self._rows is None:
            raise PerformerException('Query result is not available')

        try:
            row = await self._rows.__anext__()
            content = _convert_row(row)
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(row=query_pb.QueryRowResponse.Row(row_content=content)),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except PerformerException as e:
            raise e
        except StopAsyncIteration:
            return query_pb.QueryRowResponse(
                success=query_pb.QueryRowResponse.Result(end_of_stream=True),
                metadata=metadata_pb.ResponseMetadata(initiated=initiated),
            )
        except Exception as e:
            return query_pb.QueryRowResponse(row_level_failure=to_proto_error(e))

    async def cancel(self, initiated: timestamp_pb.Timestamp) -> result_pb.EmptyResultOrFailureResponse:
        if self._result is None:
            await self._query_handle.cancel()
        else:
            self._result.cancel()
        return result_pb.EmptyResultOrFailureResponse(
            empty_success=True, metadata=metadata_pb.ResponseMetadata(initiated=initiated)
        )

    def metadata(self, initiated: timestamp_pb.Timestamp) -> query_pb.QueryResultMetadataResponse:
        if self._result is None:
            raise PerformerException('The execute query operation has not completed yet')

        try:
            metadata = self._result.metadata()
            return query_pb.QueryResultMetadataResponse(
                success=_convert_metadata(metadata), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )
        except Exception as e:
            return query_pb.QueryResultMetadataResponse(
                failure=to_proto_error(e), metadata=metadata_pb.ResponseMetadata(initiated=initiated)
            )

    async def close(self) -> None:
        if self._result is not None:
            await self._result.shutdown()
        if isinstance(self._cluster_or_scope, AsyncCluster):
            await self._cluster_or_scope.shutdown()


class QueryExecutor:
    def __init__(self, clusters: ClusterManager) -> None:
        self._logger: logging.Logger = logging.getLogger()
        self._queries: dict[str, BlockingQueryOperation] = {}
        self._polling_queries: dict[str, BlockingPollingQueryOperation] = {}
        self._clusters: ClusterManager = clusters

    def execute(
        self, req: query_pb.ExecuteQueryRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.ExecuteQueryResponse:
        self._logger.debug(f'ExecuteQueryRequest = {req}')

        kwargs = {'req': req}

        level = req.WhichOneof('level')
        if level == 'cluster_level':
            kwargs['cluster_or_scope'] = self._clusters.get(req.cluster_level.cluster_id)
        elif level == 'scope_level':
            kwargs['cluster_or_scope'] = (
                self._clusters.get(req.scope_level.cluster_id)
                .database(req.scope_level.database_name)
                .scope(req.scope_level.scope_name)
            )
        else:
            raise PerformerException(f'Unexpected execute_query level: {level}')

        self._logger.info('Starting blocking query.')
        query = BlockingQueryOperation.build(**kwargs)
        self._queries[query.handle] = query
        return query.start(initiated)

    def execute_async(
        self, req: query_pb.StartQueryRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.StartQueryResponse:
        self._logger.debug(f'StartQueryRequest = {req}')
        kwargs = {'req': req}

        level = req.WhichOneof('level')
        if level == 'cluster_level':
            kwargs['cluster_or_scope'] = self._clusters.get(req.cluster_level.cluster_id)
        elif level == 'scope_level':
            kwargs['cluster_or_scope'] = (
                self._clusters.get(req.scope_level.cluster_id)
                .database(req.scope_level.database_name)
                .scope(req.scope_level.scope_name)
            )
        else:
            raise PerformerException(f'Unexpected execute_query level: {level}')

        self._logger.info('Running blocking start_query.')
        query = BlockingPollingQueryOperation.build(**kwargs)
        self._polling_queries[query.handle] = query
        return query.start(initiated)

    def fetch_status(
        self, req: query_pb.AsyncFetchStatusRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.AsyncFetchStatusResponse:
        return self._polling_queries[req.query_handle].fetch_status(initiated)

    def query_status_result(
        self, req: query_pb.AsyncQueryStatusResultHandleRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return self._polling_queries[req.query_handle].query_status_result(initiated)

    def fetch_results(
        self, req: query_pb.AsyncFetchResultsRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return self._polling_queries[req.query_handle].fetch_results(req.options, initiated)

    def discard_results(
        self, req: query_pb.AsyncDiscardResultsRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return self._polling_queries[req.query_handle].discard_results(initiated)

    def cancel_handle(
        self, req: query_pb.AsyncCancelHandleRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return self._polling_queries[req.query_handle].cancel_handle(initiated)

    def result(
        self, req: query_pb.QueryResultRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return self._queries[req.query_handle].wait_for_result(initiated)

    def row(self, req: query_pb.QueryRowRequest, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        if req.query_handle in self._queries:
            return self._queries[req.query_handle].next_row(initiated)

        return self._polling_queries[req.query_handle].next_row(initiated)

    def cancel(
        self, req: query_pb.QueryCancelRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        if req.query_handle in self._queries:
            return self._queries[req.query_handle].cancel(initiated)

        return self._polling_queries[req.query_handle].cancel(initiated)

    def metadata(
        self, req: query_pb.QueryMetadataRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.QueryResultMetadataResponse:
        if req.query_handle in self._queries:
            return self._queries[req.query_handle].metadata(initiated)

        return self._polling_queries[req.query_handle].metadata(initiated)

    def close(self, req: query_pb.CloseQueryResultRequest) -> result_pb.EmptyResultOrFailureResponse:
        if req.query_handle in self._queries:
            query = self._queries.pop(req.query_handle)
            query.close()
            return result_pb.EmptyResultOrFailureResponse(empty_success=True)

        query = self._polling_queries.pop(req.query_handle)
        query.close()
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    def close_all(self, req: query_pb.CloseAllQueryResultsRequest) -> result_pb.EmptyResultOrFailureResponse:
        for q in self._queries.values():
            q.close()
        self._queries.clear()
        for q in self._polling_queries.values():
            q.close()
        self._polling_queries.clear()
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)


class AsyncQueryExecutor:
    def __init__(self, clusters: ClusterManager) -> None:
        self._logger: logging.Logger = logging.getLogger()
        self._queries: dict[str, AsyncQueryOperation] = {}
        self._polling_queries: dict[str, AsyncPollingQueryOperation] = {}
        self._clusters: ClusterManager = clusters

    def execute(
        self, req: query_pb.ExecuteQueryRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.ExecuteQueryResponse:
        self._logger.debug(f'ExecuteQueryRequest = {req}')

        kwargs = {
            'req': req,
        }

        level = req.WhichOneof('level')
        if level == 'cluster_level':
            kwargs['cluster_or_scope'] = self._clusters.get(req.cluster_level.cluster_id)
        elif level == 'scope_level':
            kwargs['cluster_or_scope'] = (
                self._clusters.get(req.scope_level.cluster_id)
                .database(req.scope_level.database_name)
                .scope(req.scope_level.scope_name)
            )
        else:
            raise PerformerException(f'Unexpected execute_query level: {level}')

        self._logger.info('Starting async query.')
        query = AsyncQueryOperation.build(**kwargs)
        self._queries[query.handle] = query
        return query.start(initiated)

    async def execute_async(
        self, req: query_pb.StartQueryRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.StartQueryResponse:
        self._logger.debug(f'StartQueryRequest = {req}')
        kwargs = {'req': req}

        level = req.WhichOneof('level')
        if level == 'cluster_level':
            kwargs['cluster_or_scope'] = self._clusters.get(req.cluster_level.cluster_id)
        elif level == 'scope_level':
            kwargs['cluster_or_scope'] = (
                self._clusters.get(req.scope_level.cluster_id)
                .database(req.scope_level.database_name)
                .scope(req.scope_level.scope_name)
            )
        else:
            raise PerformerException(f'Unexpected execute_query level: {level}')

        self._logger.info('Running blocking start_query.')
        query = AsyncPollingQueryOperation.build(**kwargs)
        self._polling_queries[query.handle] = query
        return await query.start(initiated)

    async def fetch_status(
        self, req: query_pb.AsyncFetchStatusRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.AsyncFetchStatusResponse:
        return await self._polling_queries[req.query_handle].fetch_status(initiated)

    def query_status_result(
        self, req: query_pb.AsyncQueryStatusResultHandleRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return self._polling_queries[req.query_handle].query_status_result(initiated)

    async def fetch_results(
        self, req: query_pb.AsyncFetchResultsRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return await self._polling_queries[req.query_handle].fetch_results(req.options, initiated)

    async def discard_results(
        self, req: query_pb.AsyncDiscardResultsRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return await self._polling_queries[req.query_handle].discard_results(initiated)

    async def cancel_handle(
        self, req: query_pb.AsyncCancelHandleRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return await self._polling_queries[req.query_handle].cancel_handle(initiated)

    async def result(
        self, req: query_pb.QueryResultRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        return await self._queries[req.query_handle].wait_for_result(initiated)

    async def row(self, req: query_pb.QueryRowRequest, initiated: timestamp_pb.Timestamp) -> query_pb.QueryRowResponse:
        if req.query_handle in self._queries:
            return await self._queries[req.query_handle].next_row(initiated)

        return await self._polling_queries[req.query_handle].next_row(initiated)

    async def cancel(
        self, req: query_pb.QueryCancelRequest, initiated: timestamp_pb.Timestamp
    ) -> result_pb.EmptyResultOrFailureResponse:
        if req.query_handle in self._queries:
            return self._queries[req.query_handle].cancel(initiated)

        return await self._polling_queries[req.query_handle].cancel(initiated)

    def metadata(
        self, req: query_pb.QueryMetadataRequest, initiated: timestamp_pb.Timestamp
    ) -> query_pb.QueryResultMetadataResponse:
        if req.query_handle in self._queries:
            return self._queries[req.query_handle].metadata(initiated)

        return self._polling_queries[req.query_handle].metadata(initiated)

    async def close(self, req: query_pb.CloseQueryResultRequest) -> result_pb.EmptyResultOrFailureResponse:
        if req.query_handle in self._queries:
            query = self._queries.pop(req.query_handle)
            await query.close()
            return result_pb.EmptyResultOrFailureResponse(empty_success=True)

        query = self._polling_queries.pop(req.query_handle)
        await query.close()
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)

    async def close_all(self, req: query_pb.CloseAllQueryResultsRequest) -> result_pb.EmptyResultOrFailureResponse:
        for q in self._queries.values():
            q.close()
        self._queries.clear()
        for q in self._polling_queries.values():
            await q.close()
        self._polling_queries.clear()
        return result_pb.EmptyResultOrFailureResponse(empty_success=True)
