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

import threading
import time
from concurrent.futures import Future
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type

import pytest
from httpx2 import ConnectError, PoolTimeout, ReadTimeout, Response, TimeoutException, WriteTimeout

from couchbase_operational_insights.common._core import JsonStreamConfig
from couchbase_operational_insights.common.backoff_calculator import DefaultBackoffCalculator
from couchbase_operational_insights.errors import (
    InvalidCredentialError,
    OperationalInsightsError,
    QueryError,
    TimeoutError,
)
from couchbase_operational_insights.options import FetchResultsOptions, QueryOptions, StartQueryOptions
from couchbase_operational_insights.protocol._core.json_stream import JsonStream
from couchbase_operational_insights.protocol._core.request import HttpRequest
from couchbase_operational_insights.protocol._core.request_context import RequestContext
from couchbase_operational_insights.protocol.query_handle import BlockingQueryResultHandle
from couchbase_operational_insights.protocol.streaming import HttpStreamingResponse
from couchbase_operational_insights.result import BlockingQueryResult
from tests import SyncQueryType, YieldFixture
from tests.test_server import ErrorType, NonRetriableSpecificationType, ResultType, RetriableGroupType

if TYPE_CHECKING:
    from tests.environments.base_environment import BlockingTestEnvironment


class TestServerTestSuite:
    TEST_MANIFEST = [
        'test_auth_error_unauthorized',
        'test_auth_error_insufficient_permissions',
        'test_error_non_retriable_response',
        'test_error_retriable_response_timeout',
        'test_error_retriable_response_retries_exceeded',
        'test_start_query_error_retriable_response_timeout',
        'test_start_query_retry_limited_to_remaining_deadline',
        'test_start_query_transport_timeout',
        'test_error_retriable_http503',
        'test_error_retriable_http503_with_rows',
        'test_error_timeout',
        'test_fetch_results_idle_timeout_not_reduced_by_deadline',
        'test_fetch_results_iteration_not_bounded_by_timeout',
        'test_fetch_results_late_response_times_out',
        'test_query_timeout_before_first_row_shuts_down',
        'test_fetch_results_resumes_parsing_after_rows_drained',
        'test_fetch_results_stall_before_first_row_times_out',
        'test_fetch_results_stream_stall_times_out',
        'test_fetch_results_time_to_first_byte_timeout',
        'test_fetch_results_timeout_overrides_handle_request_timeout',
        'test_results_object_values',
        'test_results_raw_values',
        'test_stream_cancel_while_waiting_for_row',
    ]

    def test_auth_error_unauthorized(self, test_env: BlockingTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json({'error_type': ErrorType.Unauthorized.value})
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(InvalidCredentialError) as ex:
            test_env.cluster_or_scope.execute_query(statement)
        test_env.assert_error_context_num_attempts(1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    def test_auth_error_insufficient_permissions(self, test_env: BlockingTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json({'error_type': ErrorType.InsufficientPermissions.value})
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(QueryError) as ex:
            test_env.cluster_or_scope.execute_query(statement)
        assert ex.value.code == 20001
        assert 'Insufficient permissions' in ex.value.server_message
        test_env.assert_error_context_num_attempts(1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    @pytest.mark.parametrize(
        'retry_group_type',
        [RetriableGroupType.Zero, RetriableGroupType.First, RetriableGroupType.Middle, RetriableGroupType.Last],
    )
    @pytest.mark.parametrize(
        'non_retriable_spec',
        [
            NonRetriableSpecificationType.AllEmpty,
            NonRetriableSpecificationType.AllFalse,
            NonRetriableSpecificationType.Random,
        ],
    )
    def test_error_non_retriable_response(
        self,
        test_env: BlockingTestEnvironment,
        retry_group_type: RetriableGroupType,
        non_retriable_spec: NonRetriableSpecificationType,
    ) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {
                'error_type': ErrorType.Retriable.value,
                'retry_group_type': retry_group_type.value,
                'non_retriable_spec': non_retriable_spec.value,
            }
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(QueryError) as ex:
            test_env.cluster_or_scope.execute_query(statement)
        test_env.assert_error_context_num_attempts(1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    def test_error_retriable_response_timeout(self, test_env: BlockingTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Retriable.value, 'retry_group_type': RetriableGroupType.All.value}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(TimeoutError) as ex:
            # just-in-case, increase the max_retries to ensure we hit the timeout
            test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(max_retries=10, timeout=timedelta(seconds=1.5))
            )

        test_env.assert_error_context_num_attempts(4, ex.value._context, exact=False)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    def test_error_retriable_response_retries_exceeded(self, test_env: BlockingTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Retriable.value, 'retry_group_type': RetriableGroupType.All.value}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        allowed_retries = 5
        q_opts = QueryOptions(max_retries=allowed_retries, timeout=timedelta(seconds=10))
        with pytest.raises(QueryError) as ex:
            test_env.cluster_or_scope.execute_query(statement, q_opts)

        test_env.assert_error_context_num_attempts(allowed_retries + 1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    def test_start_query_error_retriable_response_timeout(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The client-side deadline for start_query() is the handle_request_timeout (the timeout option is sent to the
        # server). Every backoff is well under it, so the request only times out if the deadline set on the first
        # attempt covers every retry. If each retry reset the deadline, it would exhaust max_retries instead.
        monkeypatch.setattr(
            test_env.cluster_or_scope._impl._request_builder,  # type: ignore[union-attr]
            '_handle_request_timeout',
            1,
        )
        monkeypatch.setattr(DefaultBackoffCalculator, 'calculate_backoff', lambda self, retry_count: 250)
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Retriable.value, 'retry_group_type': RetriableGroupType.All.value}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(TimeoutError) as ex:
            test_env.cluster_or_scope.start_query(statement, StartQueryOptions(max_retries=20))

        test_env.assert_error_context_num_attempts(3, ex.value._context, exact=False)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    def test_start_query_retry_limited_to_remaining_deadline(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The first attempt uses most of the 2s deadline before failing with a retriable error, and the retry then
        # stalls on the server for 3s. The retry must time out at the deadline, not after another full 2s timeout.
        monkeypatch.setattr(
            test_env.cluster_or_scope._impl._request_builder,  # type: ignore[union-attr]
            '_handle_request_timeout',
            2,
        )
        monkeypatch.setattr(DefaultBackoffCalculator, 'calculate_backoff', lambda self, retry_count: 1)
        client_adapter = test_env.cluster_or_scope._impl.client_adapter  # type: ignore[union-attr]
        send_request: Callable[..., Response] = client_adapter.send_request
        num_sent = 0

        def fail_first_attempt(request: HttpRequest, stream: Optional[bool] = True) -> Response:
            nonlocal num_sent
            num_sent += 1
            if num_sent == 1:
                time.sleep(1.5)
                raise ConnectError('Injected first attempt failure')
            return send_request(request, stream=stream)

        monkeypatch.setattr(client_adapter, 'send_request', fail_first_attempt)
        test_env.set_url_path('/test_error')
        test_env.update_request_json({'error_type': ErrorType.Timeout.value, 'timeout': 3})
        statement = 'SELECT "Hello, data!" AS greeting'
        start = time.monotonic()
        with pytest.raises(TimeoutError) as ex:
            test_env.cluster_or_scope.start_query(statement)
        elapsed = time.monotonic() - start

        assert elapsed < 3
        test_env.assert_error_context_num_attempts(2, ex.value._context)

    @pytest.mark.parametrize('timeout_error', [PoolTimeout, ReadTimeout, WriteTimeout])
    def test_start_query_transport_timeout(
        self,
        test_env: BlockingTestEnvironment,
        monkeypatch: pytest.MonkeyPatch,
        timeout_error: Type[TimeoutException],
    ) -> None:
        # Every transport timeout is capped at the time left before the request deadline, so each one is a timeout.
        client_adapter = test_env.cluster_or_scope._impl.client_adapter  # type: ignore[union-attr]

        def raise_timeout(request: HttpRequest, stream: Optional[bool] = True) -> Response:
            raise timeout_error('Injected transport timeout')

        monkeypatch.setattr(client_adapter, 'send_request', raise_timeout)
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(TimeoutError) as ex:
            test_env.cluster_or_scope.start_query(statement)

        test_env.assert_error_context_num_attempts(1, ex.value._context)

    @pytest.mark.parametrize('operational_insights_error', [False, True])
    def test_error_retriable_http503(self, test_env: BlockingTestEnvironment, operational_insights_error: bool) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Http503.value, 'operational_insights_error': operational_insights_error}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        allowed_retries = 5
        q_opts = QueryOptions(max_retries=allowed_retries, timeout=timedelta(seconds=10))
        with pytest.raises(OperationalInsightsError) as ex:
            test_env.cluster_or_scope.execute_query(statement, q_opts)

        test_env.assert_error_context_num_attempts(allowed_retries + 1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    @pytest.mark.parametrize(
        'request_json',
        [
            # the parser is still reading the response when the retry starts
            {'row_count': 5, 'stall_after': 1, 'stall': 2},
            # the parser is waiting on a full row buffer when the retry starts
            {'row_count': 500},
        ],
    )
    def test_error_retriable_http503_with_rows(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch, request_json: Dict[str, Any]
    ) -> None:
        # A retry must not be affected by the previous attempt's parser, which can still be running when it starts
        monkeypatch.setattr(DefaultBackoffCalculator, 'calculate_backoff', lambda self, retry_count: 50)
        test_env.set_url_path('/test_streamed_results')
        test_env.update_request_json({**request_json, 'status': 503})
        allowed_retries = 2
        q_opts = QueryOptions(max_retries=allowed_retries, timeout=timedelta(seconds=10))
        with pytest.raises(OperationalInsightsError) as ex:
            test_env.cluster_or_scope.execute_query('SELECT 1', q_opts)

        test_env.assert_error_context_num_attempts(allowed_retries + 1, ex.value._context)

    @pytest.mark.parametrize('server_side', [False, True])
    def test_error_timeout(self, test_env: BlockingTestEnvironment, server_side: bool) -> None:
        test_env.set_url_path('/test_error')
        if server_side:
            req_json = {'error_type': ErrorType.Timeout.value, 'timeout': 1, 'server_side': True}
        else:
            req_json = {'error_type': ErrorType.Timeout.value, 'timeout': 3}

        test_env.update_request_json(req_json)
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(TimeoutError) as ex:
            test_env.cluster_or_scope.execute_query(statement, QueryOptions(timeout=timedelta(seconds=2)))
        test_env.assert_error_context_num_attempts(1, ex.value._context)
        if server_side:
            test_env.assert_error_context_contains_last_dispatch(ex.value._context)
        else:
            test_env.assert_error_context_missing_last_dispatch(ex.value._context)

    @staticmethod
    def _get_fetch_results_handle(
        test_env: BlockingTestEnvironment,
        monkeypatch: pytest.MonkeyPatch,
        handle_request_timeout: float,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> BlockingQueryResultHandle:
        # The handle path points at the test server's fetch results route, which mimics a results handle.
        impl = test_env.cluster._impl  # type: ignore[attr-defined]
        monkeypatch.setattr(impl._request_builder, '_handle_request_timeout', handle_request_timeout)
        return BlockingQueryResultHandle(
            impl.client_adapter,
            impl._request_builder,
            impl.threadpool_executor,
            'test-fetch-results-request-id',
            '/test_fetch_results',
            stream_config=stream_config,
        )

    @staticmethod
    def _track_streaming_responses(monkeypatch: pytest.MonkeyPatch) -> List[HttpStreamingResponse]:
        responses: List[HttpStreamingResponse] = []
        init = HttpStreamingResponse.__init__

        def init_and_track(resp: HttpStreamingResponse, *args: Any, **kwargs: Any) -> None:
            init(resp, *args, **kwargs)
            responses.append(resp)

        monkeypatch.setattr(HttpStreamingResponse, '__init__', init_and_track)
        return responses

    @staticmethod
    def _assert_responses_shutdown(responses: List[HttpStreamingResponse]) -> None:
        assert len(responses) > 0
        for resp in responses:
            assert resp._request_context.is_shutdown is True
            # close() takes the core response, so it is gone once the response is closed
            assert hasattr(resp, '_core_response') is False

    @pytest.mark.parametrize('set_timeout', [False, True])
    def test_fetch_results_iteration_not_bounded_by_timeout(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch, set_timeout: bool
    ) -> None:
        # The timeout only bounds the time to the first byte, so iterating slowly past it must not time out.
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 1)
        row_count = 8
        test_env.update_request_json({'row_count': row_count})
        opts = FetchResultsOptions(timeout=timedelta(seconds=1)) if set_timeout else None
        start = time.monotonic()
        result = handle.fetch_results(opts)
        count = 0
        for row in result.rows():
            assert row is not None
            count += 1
            time.sleep(0.25)
        elapsed = time.monotonic() - start

        assert count == row_count
        assert elapsed > 1.5
        assert result.metadata() is not None

    def test_fetch_results_late_response_times_out(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Each transport timeout is capped separately, so a slow connect plus a slow response can pass the deadline
        # without either timing out. A longer read timeout stands in for that.
        limit_timeouts = RequestContext._limit_timeouts_to_deadline

        def limit_timeouts_with_long_read(ctx: RequestContext) -> None:
            limit_timeouts(ctx)
            ctx._request.get_request_timeouts()['read'] = 5  # type: ignore[index]

        monkeypatch.setattr(RequestContext, '_limit_timeouts_to_deadline', limit_timeouts_with_long_read)
        responses = self._track_streaming_responses(monkeypatch)
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        test_env.update_request_json({'row_count': 5, 'header_delay': 2})
        with pytest.raises(TimeoutError):
            handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))
        self._assert_responses_shutdown(responses)

    def test_query_timeout_before_first_row_shuts_down(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A longer read timeout keeps the stream read from failing first, so the deadline expires while waiting for
        # the first row. The timeout must still shut down the request and close the response.
        limit_timeouts = RequestContext._limit_timeouts_to_deadline

        def limit_timeouts_with_long_read(ctx: RequestContext) -> None:
            limit_timeouts(ctx)
            ctx._request.get_request_timeouts()['read'] = 5  # type: ignore[index]

        monkeypatch.setattr(RequestContext, '_limit_timeouts_to_deadline', limit_timeouts_with_long_read)
        responses = self._track_streaming_responses(monkeypatch)
        test_env.set_url_path('/test_streamed_results')
        test_env.update_request_json({'row_count': 5, 'stall_after': 0, 'stall': 3})
        with pytest.raises(TimeoutError):
            test_env.cluster_or_scope.execute_query('SELECT 1', QueryOptions(timeout=timedelta(seconds=1)))
        self._assert_responses_shutdown(responses)

    def test_fetch_results_idle_timeout_not_reduced_by_deadline(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The read timeout is capped to the time left before the deadline. A short cap stands in for a slow start, and
        # the stream must still use the configured timeout for gaps between rows.
        limit_timeouts = RequestContext._limit_timeouts_to_deadline

        def limit_timeouts_with_short_read(ctx: RequestContext) -> None:
            limit_timeouts(ctx)
            ctx._request.get_request_timeouts()['read'] = 0.2  # type: ignore[index]

        monkeypatch.setattr(RequestContext, '_limit_timeouts_to_deadline', limit_timeouts_with_short_read)
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        row_count = 10
        test_env.update_request_json({'row_count': row_count, 'stall_after': 3, 'stall': 0.6})
        result = handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=2)))
        assert len(list(result.rows())) == row_count

    def test_fetch_results_time_to_first_byte_timeout(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        test_env.update_request_json({'row_count': 5, 'header_delay': 3})
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))
        elapsed = time.monotonic() - start

        assert elapsed < 2.5

    def test_fetch_results_timeout_overrides_handle_request_timeout(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The response starts after the cluster's handle_request_timeout, but within the fetch results timeout.
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 1)
        row_count = 5
        test_env.update_request_json({'row_count': row_count, 'header_delay': 1.5})
        result = handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=3)))
        assert len(list(result.rows())) == row_count

        with pytest.raises(TimeoutError):
            handle.fetch_results()

    def test_fetch_results_resumes_parsing_after_rows_drained(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The parser pauses once enough rows are buffered. A delay before each parse stage is marked done lets the
        # iteration drain the buffered rows first, and the parser must still resume.
        process_token_stream = JsonStream._process_token_stream

        def process_token_stream_with_delay(stream: JsonStream, *args: Any, **kwargs: Any) -> None:
            process_token_stream(stream, *args, **kwargs)
            time.sleep(0.5)

        monkeypatch.setattr(JsonStream, '_process_token_stream', process_token_stream_with_delay)
        handle = self._get_fetch_results_handle(
            test_env, monkeypatch, 10, stream_config=JsonStreamConfig(buffered_row_max=4)
        )
        row_count = 10
        test_env.update_request_json({'row_count': row_count})
        result = handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=2)))
        rows: List[Any] = []

        def iterate() -> None:
            rows.extend(result.rows())

        iterate_thread = threading.Thread(target=iterate, daemon=True)
        iterate_thread.start()
        iterate_thread.join(timeout=10)
        if iterate_thread.is_alive():
            result.cancel()
        assert iterate_thread.is_alive() is False, 'iteration did not resume after the buffered rows were drained'
        assert len(rows) == row_count

    def test_fetch_results_stream_stall_times_out(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Once rows stream, a server that stops sending is caught by the HTTP client's read (idle) timeout. A small
        # read buffer makes the rows before the stall available to iterate.
        handle = self._get_fetch_results_handle(
            test_env, monkeypatch, 10, stream_config=JsonStreamConfig(http_stream_buffer_size=16)
        )
        test_env.update_request_json({'row_count': 10, 'stall_after': 3, 'stall': 5})
        result = handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))
        count = 0
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            for _ in result.rows():
                count += 1
        elapsed = time.monotonic() - start

        # the reader holds back the last few bytes before the stall until its buffer fills, so the last row before
        # the stall may not be available
        assert 0 < count <= 3
        assert elapsed < 4
        test_env.assert_streaming_response_state(result)

    def test_fetch_results_stall_before_first_row_times_out(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The response has started, so the time to first byte deadline has ended. The HTTP client's read (idle)
        # timeout still stops a server that sends nothing after the headers.
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        test_env.update_request_json({'row_count': 10, 'stall_after': 0, 'stall': 5})
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            result = handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))
            list(result.rows())
        elapsed = time.monotonic() - start

        assert elapsed < 4

    @pytest.mark.parametrize('stream', [False, True])
    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_results_object_values(
        self, test_env: BlockingTestEnvironment, query_type: SyncQueryType, stream: bool
    ) -> None:
        expected_rows = 50
        test_env.set_url_path('/test_results')
        test_env.update_request_json(
            {'result_type': ResultType.Object.value, 'row_count': expected_rows, 'stream': stream}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(statement)
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(statement, QueryOptions(lazy_execute=True))
        else:
            res = test_env.cluster_or_scope.execute_query(statement, enable_cancel=True)
            assert isinstance(res, Future)
            result = res.result()

        assert isinstance(result, BlockingQueryResult)
        test_env.assert_rows(result, expected_rows)

    @pytest.mark.parametrize('stream', [False, True])
    @pytest.mark.parametrize('query_type', [SyncQueryType.NORMAL, SyncQueryType.LAZY, SyncQueryType.CANCELLABLE])
    def test_results_raw_values(
        self, test_env: BlockingTestEnvironment, query_type: SyncQueryType, stream: bool
    ) -> None:
        expected_rows = 50
        test_env.set_url_path('/test_results')
        test_env.update_request_json(
            {'result_type': ResultType.Raw.value, 'row_count': expected_rows, 'stream': stream}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        if query_type == SyncQueryType.NORMAL:
            result = test_env.cluster_or_scope.execute_query(statement)
        elif query_type == SyncQueryType.LAZY:
            result = test_env.cluster_or_scope.execute_query(statement, QueryOptions(lazy_execute=True))
        else:
            res = test_env.cluster_or_scope.execute_query(statement, enable_cancel=True)
            assert isinstance(res, Future)
            result = res.result()

        assert isinstance(result, BlockingQueryResult)
        test_env.assert_rows(result, expected_rows)

    def test_stream_cancel_while_waiting_for_row(
        self, test_env: BlockingTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The server stalls after some rows while the application iterates in another thread. Cancelling the result
        # must end that iteration, rather than raise the error from closing the response. A small read buffer makes
        # the rows before the stall available to iterate.
        handle = self._get_fetch_results_handle(
            test_env, monkeypatch, 10, stream_config=JsonStreamConfig(http_stream_buffer_size=16)
        )
        test_env.update_request_json({'row_count': 10, 'stall_after': 3, 'stall': 30})
        result = handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=10)))
        rows: List[Any] = []
        errors: List[Exception] = []

        def iterate() -> None:
            try:
                for row in result.rows():
                    rows.append(row)
            except Exception as ex:
                errors.append(ex)

        iterate_thread = threading.Thread(target=iterate, daemon=True)
        iterate_thread.start()
        time.sleep(1)
        assert 0 < len(rows) <= 3
        result.cancel()
        iterate_thread.join(timeout=4)

        assert iterate_thread.is_alive() is False, 'iteration did not end after cancel()'
        assert errors == []


class ClusterTestServerTests(TestServerTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ClusterTestServerTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ClusterTestServerTests) if valid_test_method(meth)]
        test_list = set(TestServerTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(
        self, sync_test_env_with_server: BlockingTestEnvironment
    ) -> YieldFixture[BlockingTestEnvironment]:
        test_env = sync_test_env_with_server.enable_test_server()
        yield test_env
        test_env.disable_test_server()


class ScopeTestServerTests(TestServerTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ScopeTestServerTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ScopeTestServerTests) if valid_test_method(meth)]
        test_list = set(TestServerTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(
        self, sync_test_env_with_server: BlockingTestEnvironment
    ) -> YieldFixture[BlockingTestEnvironment]:
        test_env = sync_test_env_with_server.enable_test_server()
        test_env.enable_scope()
        yield test_env
        test_env.disable_scope().disable_test_server()
