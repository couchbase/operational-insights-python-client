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

import asyncio
import sys
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional, Type

import anyio
import pytest
from httpx2 import ConnectError, PoolTimeout, ReadTimeout, Response, TimeoutException, WriteTimeout

from acouchbase_operational_insights.errors import (
    InvalidCredentialError,
    OperationalInsightsError,
    QueryError,
    TimeoutError,
)
from acouchbase_operational_insights.options import FetchResultsOptions, QueryOptions, StartQueryOptions
from acouchbase_operational_insights.protocol._core import request_context
from acouchbase_operational_insights.protocol._core.request_context import AsyncRequestContext
from acouchbase_operational_insights.protocol.query_handle import AsyncQueryResultHandle
from acouchbase_operational_insights.result import AsyncQueryResult
from couchbase_operational_insights.common._core import JsonStreamConfig
from couchbase_operational_insights.common.backoff_calculator import DefaultBackoffCalculator
from couchbase_operational_insights.protocol._core.request import HttpRequest
from tests import AsyncYieldFixture
from tests.test_server import ErrorType, NonRetriableSpecificationType, ResultType, RetriableGroupType

if TYPE_CHECKING:
    from tests.environments.base_environment import AsyncTestEnvironment


class TestServerTestSuite:
    TEST_MANIFEST = [
        'test_auth_error_unauthorized',
        'test_auth_error_insufficient_permissions',
        'test_error_non_retriable_response',
        'test_error_retriable_response_timeout',
        'test_error_retriable_response_retries_exceeded',
        'test_start_query_error_retriable_response_timeout',
        'test_start_query_retry_limited_to_remaining_deadline',
        'test_start_query_retriable_response_exits_task_groups',
        'test_start_query_transport_timeout',
        'test_start_query_cancelled_at_deadline',
        'test_start_query_resolution_cancelled_at_deadline',
        'test_error_retriable_http503',
        'test_error_retriable_http503_with_rows',
        'test_error_timeout',
        'test_fetch_results_idle_timeout_not_reduced_by_deadline',
        'test_fetch_results_iteration_not_bounded_by_timeout',
        'test_fetch_results_late_response_times_out',
        'test_fetch_results_stall_before_first_row_times_out',
        'test_fetch_results_stream_stall_times_out',
        'test_fetch_results_time_to_first_byte_timeout',
        'test_fetch_results_timeout_overrides_handle_request_timeout',
        'test_results_object_values',
        'test_results_raw_values',
        'test_stream_cancel_while_waiting_for_row',
        'test_stream_stall_times_out',
    ]

    async def test_auth_error_unauthorized(self, test_env: AsyncTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json({'error_type': ErrorType.Unauthorized.value})
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(InvalidCredentialError) as ex:
            await test_env.cluster_or_scope.execute_query(statement)
        test_env.assert_error_context_num_attempts(1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    async def test_auth_error_insufficient_permissions(self, test_env: AsyncTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json({'error_type': ErrorType.InsufficientPermissions.value})
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(QueryError) as ex:
            await test_env.cluster_or_scope.execute_query(statement)
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
    async def test_error_non_retriable_response(
        self,
        test_env: AsyncTestEnvironment,
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
            await test_env.cluster_or_scope.execute_query(statement)
        test_env.assert_error_context_num_attempts(1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    async def test_error_retriable_response_timeout(self, test_env: AsyncTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Retriable.value, 'retry_group_type': RetriableGroupType.All.value}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(TimeoutError) as ex:
            # just-in-case, increase the max_retries to ensure we hit the timeout
            await test_env.cluster_or_scope.execute_query(
                statement, QueryOptions(max_retries=10, timeout=timedelta(seconds=1.5))
            )

        test_env.assert_error_context_num_attempts(4, ex.value._context, exact=False)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    async def test_error_retriable_response_retries_exceeded(self, test_env: AsyncTestEnvironment) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Retriable.value, 'retry_group_type': RetriableGroupType.All.value}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        allowed_retries = 5
        q_opts = QueryOptions(max_retries=allowed_retries, timeout=timedelta(seconds=10))
        with pytest.raises(QueryError) as ex:
            await test_env.cluster_or_scope.execute_query(statement, q_opts)

        test_env.assert_error_context_num_attempts(allowed_retries + 1, ex.value._context)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    async def test_start_query_error_retriable_response_timeout(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
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
            await test_env.cluster_or_scope.start_query(statement, StartQueryOptions(max_retries=20))

        test_env.assert_error_context_num_attempts(3, ex.value._context, exact=False)
        test_env.assert_error_context_contains_last_dispatch(ex.value._context)

    async def test_start_query_retry_limited_to_remaining_deadline(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
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
        send_request: Callable[..., Awaitable[Response]] = client_adapter.send_request
        num_sent = 0
        num_task_groups = 0
        enter_request_context = AsyncRequestContext.__aenter__

        async def count_task_groups(self: AsyncRequestContext) -> AsyncRequestContext:
            nonlocal num_task_groups
            num_task_groups += 1
            return await enter_request_context(self)

        monkeypatch.setattr(AsyncRequestContext, '__aenter__', count_task_groups)

        async def fail_first_attempt(request: HttpRequest, stream: Optional[bool] = True) -> Response:
            nonlocal num_sent
            num_sent += 1
            if num_sent == 1:
                await anyio.sleep(1.5)
                raise ConnectError('Injected first attempt failure')
            return await send_request(request, stream=stream)

        monkeypatch.setattr(client_adapter, 'send_request', fail_first_attempt)
        test_env.set_url_path('/test_error')
        test_env.update_request_json({'error_type': ErrorType.Timeout.value, 'timeout': 3})
        statement = 'SELECT "Hello, data!" AS greeting'
        start = time.monotonic()
        with pytest.raises(TimeoutError) as ex:
            await test_env.cluster_or_scope.start_query(statement)
        elapsed = time.monotonic() - start

        assert elapsed < 3
        test_env.assert_error_context_num_attempts(2, ex.value._context)
        # The retry must reuse the task group the first attempt entered, not enter another and leave the first open.
        assert num_task_groups == 1

    async def test_start_query_retriable_response_exits_task_groups(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A retriable error in the response shuts the context down and exits its task group, so each retry enters a
        # new one. Every task group entered must be exited once the request fails.
        monkeypatch.setattr(DefaultBackoffCalculator, 'calculate_backoff', lambda self, retry_count: 1)
        num_entered = 0
        num_exited = 0
        enter_request_context = AsyncRequestContext.__aenter__
        exit_request_context = AsyncRequestContext.__aexit__

        async def count_enter(self: AsyncRequestContext) -> AsyncRequestContext:
            nonlocal num_entered
            num_entered += 1
            return await enter_request_context(self)

        async def count_exit(self: AsyncRequestContext, *args: Any) -> Optional[bool]:
            nonlocal num_exited
            num_exited += 1
            return await exit_request_context(self, *args)

        monkeypatch.setattr(AsyncRequestContext, '__aenter__', count_enter)
        monkeypatch.setattr(AsyncRequestContext, '__aexit__', count_exit)
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Retriable.value, 'retry_group_type': RetriableGroupType.All.value}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(QueryError) as ex:
            await test_env.cluster_or_scope.start_query(statement, StartQueryOptions(max_retries=2))

        test_env.assert_error_context_num_attempts(3, ex.value._context)
        assert num_entered == 3
        assert num_exited == num_entered

    @pytest.mark.parametrize('timeout_error', [PoolTimeout, ReadTimeout, WriteTimeout])
    async def test_start_query_transport_timeout(
        self,
        test_env: AsyncTestEnvironment,
        monkeypatch: pytest.MonkeyPatch,
        timeout_error: Type[TimeoutException],
    ) -> None:
        # Every transport timeout is capped at the time left before the request deadline, so each one is a timeout.
        client_adapter = test_env.cluster_or_scope._impl.client_adapter  # type: ignore[union-attr]

        async def raise_timeout(request: HttpRequest, stream: Optional[bool] = True) -> Response:
            raise timeout_error('Injected transport timeout')

        monkeypatch.setattr(client_adapter, 'send_request', raise_timeout)
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(TimeoutError) as ex:
            await test_env.cluster_or_scope.start_query(statement)

        test_env.assert_error_context_num_attempts(1, ex.value._context)

    async def test_start_query_cancelled_at_deadline(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The transport timeouts apply to each phase of the request, so a response that keeps the request busy (e.g.
        # by trickling in) is not bounded by them. The request must still be cancelled at the deadline.
        monkeypatch.setattr(
            test_env.cluster_or_scope._impl._request_builder,  # type: ignore[union-attr]
            '_handle_request_timeout',
            1,
        )
        client_adapter = test_env.cluster_or_scope._impl.client_adapter  # type: ignore[union-attr]

        async def stall(request: HttpRequest, stream: Optional[bool] = True) -> Response:
            await anyio.sleep(5)
            raise AssertionError('The request was not cancelled at the deadline')

        monkeypatch.setattr(client_adapter, 'send_request', stall)
        statement = 'SELECT "Hello, data!" AS greeting'
        start = time.monotonic()
        with pytest.raises(TimeoutError) as ex:
            await test_env.cluster_or_scope.start_query(statement)
        elapsed = time.monotonic() - start

        assert elapsed < 2
        test_env.assert_error_context_num_attempts(1, ex.value._context)

    async def test_start_query_resolution_cancelled_at_deadline(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Name resolution has no timeout of its own, so a stalled resolver must also be cancelled at the deadline.
        monkeypatch.setattr(
            test_env.cluster_or_scope._impl._request_builder,  # type: ignore[union-attr]
            '_handle_request_timeout',
            1,
        )

        async def stall(*args: Any, **kwargs: Any) -> str:
            await anyio.sleep(5)
            raise AssertionError('Name resolution was not cancelled at the deadline')

        monkeypatch.setattr(request_context, 'get_request_ip_async', stall)
        statement = 'SELECT "Hello, data!" AS greeting'
        start = time.monotonic()
        with pytest.raises(TimeoutError) as ex:
            await test_env.cluster_or_scope.start_query(statement)
        elapsed = time.monotonic() - start

        assert elapsed < 2
        test_env.assert_error_context_num_attempts(1, ex.value._context)

    @pytest.mark.parametrize('operational_insights_error', [False, True])
    async def test_error_retriable_http503(
        self, test_env: AsyncTestEnvironment, operational_insights_error: bool
    ) -> None:
        test_env.set_url_path('/test_error')
        test_env.update_request_json(
            {'error_type': ErrorType.Http503.value, 'operational_insights_error': operational_insights_error}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        allowed_retries = 5
        q_opts = QueryOptions(max_retries=allowed_retries, timeout=timedelta(seconds=10))
        with pytest.raises(OperationalInsightsError) as ex:
            await test_env.cluster_or_scope.execute_query(statement, q_opts)

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
    async def test_error_retriable_http503_with_rows(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch, request_json: Dict[str, Any]
    ) -> None:
        # A retry must not be affected by the previous attempt's parser, which can still be running when it starts
        monkeypatch.setattr(DefaultBackoffCalculator, 'calculate_backoff', lambda self, retry_count: 50)
        test_env.set_url_path('/test_streamed_results')
        test_env.update_request_json({**request_json, 'status': 503})
        allowed_retries = 2
        q_opts = QueryOptions(max_retries=allowed_retries, timeout=timedelta(seconds=10))
        with pytest.raises(OperationalInsightsError) as ex:
            await test_env.cluster_or_scope.execute_query('SELECT 1', q_opts)

        test_env.assert_error_context_num_attempts(allowed_retries + 1, ex.value._context)

    @pytest.mark.parametrize('server_side', [False, True])
    async def test_error_timeout(self, test_env: AsyncTestEnvironment, server_side: bool) -> None:
        test_env.set_url_path('/test_error')
        if server_side:
            req_json = {'error_type': ErrorType.Timeout.value, 'timeout': 1, 'server_side': True}
        else:
            req_json = {'error_type': ErrorType.Timeout.value, 'timeout': 3}

        test_env.update_request_json(req_json)
        statement = 'SELECT "Hello, data!" AS greeting'
        with pytest.raises(TimeoutError) as ex:
            await test_env.cluster_or_scope.execute_query(statement, QueryOptions(timeout=timedelta(seconds=2)))
        test_env.assert_error_context_num_attempts(1, ex.value._context)
        if server_side:
            test_env.assert_error_context_contains_last_dispatch(ex.value._context)
        else:
            test_env.assert_error_context_missing_last_dispatch(ex.value._context)

    @staticmethod
    def _get_fetch_results_handle(
        test_env: AsyncTestEnvironment,
        monkeypatch: pytest.MonkeyPatch,
        handle_request_timeout: float,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> AsyncQueryResultHandle:
        # The handle path points at the test server's fetch results route, which mimics a results handle.
        impl = test_env.cluster._impl  # type: ignore[attr-defined]
        monkeypatch.setattr(impl._request_builder, '_handle_request_timeout', handle_request_timeout)
        return AsyncQueryResultHandle(
            impl.client_adapter,
            impl._request_builder,
            'test-fetch-results-request-id',
            '/test_fetch_results',
            stream_config=stream_config,
        )

    @pytest.mark.parametrize('set_timeout', [False, True])
    async def test_fetch_results_iteration_not_bounded_by_timeout(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch, set_timeout: bool
    ) -> None:
        # The timeout only bounds the time to the first byte, so iterating slowly past it must not time out.
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 1)
        row_count = 8
        test_env.update_request_json({'row_count': row_count})
        opts = FetchResultsOptions(timeout=timedelta(seconds=1)) if set_timeout else None
        start = time.monotonic()
        result = await handle.fetch_results(opts)
        count = 0
        async for row in result.rows():
            assert row is not None
            count += 1
            await anyio.sleep(0.25)
        elapsed = time.monotonic() - start

        assert count == row_count
        assert elapsed > 1.5
        assert result.metadata() is not None

    async def test_fetch_results_late_response_times_out(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Each transport timeout is capped separately, so a slow connect plus a slow response can pass the deadline
        # without either timing out. A longer read timeout stands in for that. The send's cancel scope stops it.
        limit_timeouts = AsyncRequestContext._limit_timeouts_to_deadline

        def limit_timeouts_with_long_read(ctx: AsyncRequestContext) -> None:
            limit_timeouts(ctx)
            ctx._request.get_request_timeouts()['read'] = 5  # type: ignore[index]

        monkeypatch.setattr(AsyncRequestContext, '_limit_timeouts_to_deadline', limit_timeouts_with_long_read)
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        test_env.update_request_json({'row_count': 5, 'header_delay': 2})
        with pytest.raises(TimeoutError):
            await handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))

    async def test_fetch_results_idle_timeout_not_reduced_by_deadline(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The read timeout is capped to the time left before the deadline. A short cap stands in for a slow start, and
        # the stream must still use the configured timeout for gaps between rows.
        limit_timeouts = AsyncRequestContext._limit_timeouts_to_deadline

        def limit_timeouts_with_short_read(ctx: AsyncRequestContext) -> None:
            limit_timeouts(ctx)
            ctx._request.get_request_timeouts()['read'] = 0.2  # type: ignore[index]

        monkeypatch.setattr(AsyncRequestContext, '_limit_timeouts_to_deadline', limit_timeouts_with_short_read)
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        row_count = 10
        test_env.update_request_json({'row_count': row_count, 'stall_after': 3, 'stall': 0.6})
        result = await handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=2)))
        assert len([row async for row in result.rows()]) == row_count

    async def test_fetch_results_time_to_first_byte_timeout(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        test_env.update_request_json({'row_count': 5, 'header_delay': 3})
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            await handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))
        elapsed = time.monotonic() - start

        assert elapsed < 2.5

    async def test_fetch_results_timeout_overrides_handle_request_timeout(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The response starts after the cluster's handle_request_timeout, but within the fetch results timeout.
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 1)
        row_count = 5
        test_env.update_request_json({'row_count': row_count, 'header_delay': 1.5})
        result = await handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=3)))
        assert len([row async for row in result.rows()]) == row_count

        with pytest.raises(TimeoutError):
            await handle.fetch_results()

    async def test_fetch_results_stream_stall_times_out(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Once rows stream, a server that stops sending is caught by the HTTP client's read (idle) timeout. A small
        # read buffer makes the rows before the stall available to iterate.
        handle = self._get_fetch_results_handle(
            test_env, monkeypatch, 10, stream_config=JsonStreamConfig(http_stream_buffer_size=16)
        )
        test_env.update_request_json({'row_count': 10, 'stall_after': 3, 'stall': 5})
        result = await handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))
        count = 0
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            async for _ in result.rows():
                count += 1
        elapsed = time.monotonic() - start

        # the reader holds back the last few bytes before the stall until its buffer fills, so the last row before
        # the stall may not be available
        assert 0 < count <= 3
        assert elapsed < 4
        test_env.assert_streaming_response_state(result)

    async def test_fetch_results_stall_before_first_row_times_out(
        self, test_env: AsyncTestEnvironment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The response has started, so the time to first byte deadline has ended. The HTTP client's read (idle)
        # timeout still stops a server that sends nothing after the headers.
        handle = self._get_fetch_results_handle(test_env, monkeypatch, 10)
        test_env.update_request_json({'row_count': 10, 'stall_after': 0, 'stall': 5})
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            result = await handle.fetch_results(FetchResultsOptions(timeout=timedelta(seconds=1)))
            [row async for row in result.rows()]
        elapsed = time.monotonic() - start

        assert elapsed < 4

    @pytest.mark.parametrize('stream', [False, True])
    async def test_results_object_values(self, test_env: AsyncTestEnvironment, stream: bool) -> None:
        expected_rows = 50
        test_env.set_url_path('/test_results')
        test_env.update_request_json(
            {'result_type': ResultType.Object.value, 'row_count': expected_rows, 'stream': stream}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        result = await test_env.cluster_or_scope.execute_query(statement)
        assert isinstance(result, AsyncQueryResult)
        await test_env.assert_rows(result, expected_rows)

    @pytest.mark.parametrize('stream', [False, True])
    async def test_results_raw_values(self, test_env: AsyncTestEnvironment, stream: bool) -> None:
        expected_rows = 50
        test_env.set_url_path('/test_results')
        test_env.update_request_json(
            {'result_type': ResultType.Raw.value, 'row_count': expected_rows, 'stream': stream}
        )
        statement = 'SELECT "Hello, data!" AS greeting'
        result = await test_env.cluster_or_scope.execute_query(statement)
        assert isinstance(result, AsyncQueryResult)
        await test_env.assert_rows(result, expected_rows)

    @pytest.mark.parametrize('read_timeout', [None, 1])
    @pytest.mark.parametrize('stall_after', [0, 3])
    async def test_stream_stall_times_out(
        self, test_env: AsyncTestEnvironment, stall_after: int, read_timeout: Optional[float]
    ) -> None:
        # The server sends the headers, then stops sending before the first row or after some rows. The HTTP client's
        # read timeout must raise TimeoutError and keep the rows already sent, without cancelling the application's
        # task. The read timeout is the query timeout by default, so the request deadline expires at the same moment;
        # a shorter read timeout isolates the read timeout.
        test_env.set_url_path('/test_streamed_results')
        test_env.update_request_json({'row_count': 10, 'stall_after': stall_after, 'stall': 5})
        # the override is kept on the class scoped client adapter, so reset it for the next test
        test_env.update_request_extensions({} if read_timeout is None else {'timeout': {'read': read_timeout}})
        query_timeout = 2 if read_timeout is None else 3
        statement = 'SELECT "Hello, data!" AS greeting'
        count = 0
        start = time.monotonic()
        try:
            with pytest.raises(TimeoutError):
                result = await test_env.cluster_or_scope.execute_query(
                    statement, QueryOptions(timeout=timedelta(seconds=query_timeout))
                )
                async for _ in result.rows():
                    count += 1
        finally:
            test_env.update_request_extensions({})
        elapsed = time.monotonic() - start

        assert count == stall_after
        assert elapsed < (read_timeout or query_timeout) + 1
        if sys.version_info >= (3, 11):
            task = asyncio.current_task()
            assert task is not None
            assert task.cancelling() == 0
        # the next await must not raise CancelledError
        await anyio.sleep(0.01)

    async def test_stream_cancel_while_waiting_for_row(self, test_env: AsyncTestEnvironment) -> None:
        # The server stalls after some rows while the application iterates in another task. Cancelling the result
        # must end that iteration rather than leave it waiting on a row that never arrives.
        test_env.set_url_path('/test_streamed_results')
        test_env.update_request_json({'row_count': 10, 'stall_after': 3, 'stall': 30})
        statement = 'SELECT "Hello, data!" AS greeting'
        result = await test_env.cluster_or_scope.execute_query(statement, QueryOptions(timeout=timedelta(seconds=30)))
        rows: List[Any] = []
        iteration_done = anyio.Event()

        async def iterate() -> None:
            try:
                async for row in result.rows():
                    rows.append(row)
            except Exception:  # nosec - the iteration only has to end
                pass
            finally:
                iteration_done.set()

        async with anyio.create_task_group() as tg:
            tg.start_soon(iterate)
            await anyio.sleep(1)
            assert len(rows) == 3
            result.cancel()
            with anyio.move_on_after(4) as scope:
                await iteration_done.wait()
            timed_out = scope.cancelled_caught
            if timed_out:
                tg.cancel_scope.cancel()
        assert timed_out is False, 'iteration did not end after cancel()'


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
    async def couchbase_test_environment(
        self, async_test_env_with_server: AsyncTestEnvironment
    ) -> AsyncYieldFixture[AsyncTestEnvironment]:
        test_env = await async_test_env_with_server.enable_test_server()
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
    async def couchbase_test_environment(
        self, async_test_env_with_server: AsyncTestEnvironment
    ) -> AsyncYieldFixture[AsyncTestEnvironment]:
        test_env = await async_test_env_with_server.enable_test_server()
        test_env.enable_scope()
        yield test_env
        test_env.disable_scope().disable_test_server()
