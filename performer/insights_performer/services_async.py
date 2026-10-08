import logging
from typing import Optional

import grpc
from google.protobuf import timestamp_pb2 as timestamp_pb

from insights_performer.caps import build_caps
from insights_performer.clusters import ClusterManager
from insights_performer.exceptions import PerformerException
from insights_performer.protocol.columnar import caps_pb2 as caps_pb
from insights_performer.protocol.columnar import cluster_management_pb2 as cluster_management_pb
from insights_performer.protocol.columnar import query_pb2 as query_pb
from insights_performer.protocol.columnar import result_pb2 as result_pb
from insights_performer.protocol.columnar import services_pb2_grpc as services_pb_grpc
from insights_performer.protocol.shared import echo_pb2 as echo_pb
from insights_performer.query import AsyncQueryExecutor


class AsyncInsightsCrossServiceServicer(services_pb_grpc.ColumnarCrossServiceServicer):
    def __init__(self, clusters: ClusterManager):
        self._logger: logging.Logger = logging.getLogger()
        self._query_executor = AsyncQueryExecutor(clusters)

    async def ExecuteQuery(
        self, request: query_pb.ExecuteQueryRequest, context: grpc.aio.ServicerContext
    ) -> Optional[query_pb.ExecuteQueryResponse]:
        self._logger.info('Executing query')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return self._query_executor.execute(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def QueryResult(
        self, request: query_pb.QueryResultRequest, context: grpc.aio.ServicerContext
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info(f'Requesting query response: query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.result(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def StartQuery(
        self, request: query_pb.StartQueryRequest, context: grpc.ServicerContext
    ) -> Optional[query_pb.StartQueryResponse]:
        self._logger.info('Executing start_query')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.execute_async(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def AsyncFetchStatus(
        self, request: query_pb.AsyncFetchStatusRequest, context: grpc.ServicerContext
    ) -> Optional[query_pb.AsyncFetchStatusResponse]:
        self._logger.info(f'Fetching status query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.fetch_status(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def AsyncQueryStatusResultHandle(
        self, request: query_pb.AsyncQueryStatusResultHandleRequest, context: grpc.ServicerContext
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info(f'Fetching query status result query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return self._query_executor.query_status_result(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def AsyncFetchResults(
        self, request: query_pb.AsyncFetchResultsRequest, context: grpc.ServicerContext
    ) -> Optional[query_pb.AsyncFetchStatusResponse]:
        self._logger.info(f'Fetching results query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.fetch_results(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def AsyncDiscardResults(
        self, request: query_pb.AsyncDiscardResultsRequest, context: grpc.ServicerContext
    ) -> Optional[query_pb.AsyncFetchStatusResponse]:
        self._logger.info(f'Discarding results query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.discard_results(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def AsyncCancelHandle(
        self, request: query_pb.AsyncCancelHandleRequest, context: grpc.ServicerContext
    ) -> Optional[query_pb.AsyncFetchStatusResponse]:
        self._logger.info(f'Canceling handle query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.cancel_handle(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def QueryRow(
        self, request: query_pb.QueryRowRequest, context: grpc.aio.ServicerContext
    ) -> Optional[query_pb.QueryRowResponse]:
        self._logger.info(f'Requesting query row: query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.row(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def QueryCancel(
        self, request: query_pb.QueryCancelRequest, context: grpc.aio.ServicerContext
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info(f'Cancelling query: query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return await self._query_executor.cancel(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def QueryMetadata(
        self, request: query_pb.QueryMetadataRequest, context: grpc.aio.ServicerContext
    ) -> Optional[query_pb.QueryResultMetadataResponse]:
        self._logger.info(f'Fetching query metadata: query_handle={request.query_handle}')
        try:
            initiated = timestamp_pb.Timestamp()
            initiated.GetCurrentTime()
            return self._query_executor.metadata(request, initiated)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def CloseQueryResult(
        self, request: query_pb.CloseQueryResultRequest, context: grpc.aio.ServicerContext
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info(f'Closing query: query_handle={request.query_handle}')
        try:
            return await self._query_executor.close(request)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def CloseAllQueryResults(
        self, request: query_pb.CloseQueryResultRequest, context: grpc.aio.ServicerContext
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info('Closing all queries')
        try:
            return await self._query_executor.close_all(request)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None


class AsyncInsightsServiceServicer(services_pb_grpc.ColumnarServiceServicer):
    def __init__(self):
        self._clusters = ClusterManager()
        self._logger: logging.Logger = logging.getLogger()

    def cross_service(self):
        return AsyncInsightsCrossServiceServicer(self._clusters)

    async def FetchPerformerCaps(
        self, request: caps_pb.FetchPerformerCapsRequest, context: grpc.aio.ServicerContext
    ) -> caps_pb.FetchPerformerCapsResponse:
        return build_caps()

    async def Echo(self, request: echo_pb.EchoRequest, context: grpc.aio.ServicerContext) -> echo_pb.EchoResponse:
        self._logger.info('========= ' + request.testName + ' : ' + request.message + ' =========')
        return echo_pb.EchoResponse()

    async def ClusterNewInstance(
        self,
        request: cluster_management_pb.ClusterNewInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info(
            f'Creating new cluster instance: id={request.cluster_connection_id}, connstr={request.connection_string}'
        )
        try:
            return self._clusters.new_async_instance(request)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def ClusterClose(
        self,
        request: cluster_management_pb.ClusterCloseRequest,
        context: grpc.aio.ServicerContext,
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info(f'Closing cluster instance: id={request.execution_context.cluster_id}')
        try:
            return await self._clusters.close_instance_async(request)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def CloseAllClusters(
        self, request: cluster_management_pb.CloseAllColumnarClustersRequest, context: grpc.aio.ServicerContext
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info('Closing all clusters')
        try:
            return await self._clusters.close_all_async(request)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None

    async def SetCredential(
        self,
        request: cluster_management_pb.SetCredentialRequest,
        context: grpc.ServicerContext,
    ) -> Optional[result_pb.EmptyResultOrFailureResponse]:
        self._logger.info(f'Setting credential on cluster instance: id={request.execution_context.cluster_id}')
        try:
            return await self._clusters.set_credential_async(request)
        except PerformerException as e:
            context.abort(grpc.StatusCode.INTERNAL, str(e))
            return None
