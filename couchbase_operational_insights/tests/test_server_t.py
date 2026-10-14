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

import time
from concurrent.futures import Future
from datetime import timedelta
from typing import TYPE_CHECKING, Callable, Optional, Type

import pytest
from httpx2 import ConnectError, PoolTimeout, ReadTimeout, Response, TimeoutException, WriteTimeout

from couchbase_operational_insights.common.backoff_calculator import DefaultBackoffCalculator
from couchbase_operational_insights.errors import (
    InvalidCredentialError,
    OperationalInsightsError,
    QueryError,
    TimeoutError,
)
from couchbase_operational_insights.options import QueryOptions, StartQueryOptions
from couchbase_operational_insights.protocol._core.request import HttpRequest
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
        'test_error_timeout',
        'test_results_object_values',
        'test_results_raw_values',
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
