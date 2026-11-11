#  Copyright 2016-2026. Couchbase, Inc.
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

import json
from datetime import timedelta
from typing import Any, Dict

import pytest

from acouchbase_operational_insights.deserializer import PassthroughDeserializer
from acouchbase_operational_insights.errors import (
    OperationalInsightsError,
    QueryError,
    QueryNotFoundError,
    TimeoutError,
)
from acouchbase_operational_insights.options import FetchResultsOptions, StartQueryOptions
from acouchbase_operational_insights.protocol.query_handle import AsyncQueryHandle, AsyncQueryStatus
from couchbase_operational_insights.common.request import RequestState
from tests import AsyncYieldFixture
from tests.environments.base_environment import AsyncTestEnvironment


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_cancel_prior_iterating',
        'test_cancel_while_iterating',
        'test_query_handle_serialized',
        'test_query_handle_serialized_cancel',
        'test_query_metadata',
        'test_query_metadata_not_available',
        'test_query_named_parameters',
        'test_query_named_parameters_no_options',
        'test_query_named_parameters_override',
        'test_query_passthrough_deserializer',
        'test_query_positional_params',
        'test_query_positional_params_no_option',
        'test_query_positional_params_override',
        'test_query_raises_exception_prior_to_iterating',
        'test_query_raw_options',
        'test_query_results',
        'test_query_status_not_found',
        'test_query_status_prior_to_results',
        'test_query_timeout',
    ]

    @pytest.fixture(scope='class')
    def query_statement_limit2(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_pos_params_limit2(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $1 LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $1 LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_named_params_limit2(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT 2;'
        else:
            return f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT 2;'

    @pytest.fixture(scope='class')
    def query_statement_limit5(self, test_env: AsyncTestEnvironment) -> str:
        if test_env.use_scope:
            return f'SELECT * FROM {test_env.collection_name} LIMIT 5;'
        else:
            return f'SELECT * FROM {test_env.fqdn} LIMIT 5;'

    async def test_cancel_prior_iterating(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        q_handle = await test_env.cluster_or_scope.start_query(statement)
        assert isinstance(q_handle, AsyncQueryHandle)
        await q_handle.cancel()

        # it takes a moment for the cancellation to propagate, so we'll retry fetching
        # status a few times until we get an exception
        await AsyncTestEnvironment.try_n_times_till_exception(10, 2, q_handle.fetch_status)

        with pytest.raises(QueryError):
            await q_handle.fetch_status()

        await q_handle.cancel()  # should be idempotent and not raise

    async def test_cancel_while_iterating(self, test_env: AsyncTestEnvironment, query_statement_limit5: str) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(query_statement_limit5)
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            rows = []
            count = 0
            async for row in result.rows():
                if count == 2:
                    result.cancel()
                assert row is not None
                rows.append(row)
                count += 1

            assert len(rows) == count
            expected_state = RequestState.Cancelled
            assert result._http_response._request_context.request_state == expected_state
            with pytest.raises(RuntimeError):
                result.metadata()
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_metadata(self, test_env: AsyncTestEnvironment, query_statement_limit5: str) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(query_statement_limit5)
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None

            expected_count = 5
            await test_env.assert_rows(result, expected_count)

            metadata = result.metadata()

            assert len(metadata.warnings()) == 0
            assert len(metadata.request_id()) > 0

            metrics = metadata.metrics()

            assert metrics.result_size() > 0
            assert metrics.result_count() == expected_count
            assert metrics.processed_objects() > 0
            # sometimes we have a negative elapsed time which we set to 0
            assert metrics.elapsed_time() >= timedelta(0)
            assert metrics.execution_time() > timedelta(0)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_metadata_not_available(
        self, test_env: AsyncTestEnvironment, query_statement_limit5: str
    ) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(query_statement_limit5)
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None

            with pytest.raises(RuntimeError):
                result.metadata()

            # Read one row -- NOTE: anext()/aiter() added in Python 3.10
            aiter = result.rows()
            row = await aiter.__anext__()
            assert row is not None
            assert isinstance(row, dict)

            with pytest.raises(RuntimeError):
                result.metadata()

            # Iterate the rest of the rows
            rows = [r async for r in result.rows()]
            assert len(rows) == 4

            metadata = result.metadata()
            assert len(metadata.warnings()) == 0
            assert len(metadata.request_id()) > 0
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_named_parameters(
        self,
        test_env: AsyncTestEnvironment,
        query_statement_named_params_limit2: str,
    ) -> None:
        named_parameters: Dict[str, Any] = {'country': 'United States'}
        q_handle = await test_env.cluster_or_scope.start_query(
            query_statement_named_params_limit2, StartQueryOptions(named_parameters=named_parameters)
        )
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_named_parameters_no_options(
        self, test_env: AsyncTestEnvironment, query_statement_named_params_limit2: str
    ) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(
            query_statement_named_params_limit2, country='United States'
        )
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_named_parameters_override(
        self, test_env: AsyncTestEnvironment, query_statement_named_params_limit2: str
    ) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(
            query_statement_named_params_limit2,
            StartQueryOptions(named_parameters={'country': 'abcdefg'}),
            country='United States',
        )
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_passthrough_deserializer(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'FROM range(0, 10) AS num SELECT *'
        q_handle = await test_env.cluster_or_scope.start_query(statement)
        result_handle, _ = await test_env.wait_for_query_results(q_handle, return_only_result_handle=True)
        result = await result_handle.fetch_results(FetchResultsOptions(deserializer=PassthroughDeserializer()))
        idx = 0
        async for row in result.rows():
            assert isinstance(row, bytes)
            assert json.loads(row) == {'num': idx}
            idx += 1
        test_env.assert_streaming_response_state(result)
        await result_handle.discard_results()

    async def test_query_positional_params(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(
            query_statement_pos_params_limit2, StartQueryOptions(positional_parameters=['United States'])
        )
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_positional_params_no_option(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(query_statement_pos_params_limit2, 'United States')
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_positional_params_override(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(
            query_statement_pos_params_limit2,
            StartQueryOptions(positional_parameters=['abcdefg']),
            'United States',
        )
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_raises_exception_prior_to_iterating(self, test_env: AsyncTestEnvironment) -> None:
        statement = "I'm not N1QL!"
        with pytest.raises(QueryError):
            await test_env.cluster_or_scope.start_query(statement)

    async def test_query_raw_options(
        self, test_env: AsyncTestEnvironment, query_statement_pos_params_limit2: str
    ) -> None:
        if test_env.use_scope:
            statement = f'SELECT * FROM {test_env.collection_name} WHERE country = $country LIMIT $1;'
        else:
            statement = f'SELECT * FROM {test_env.fqdn} WHERE country = $country LIMIT $1;'

        q_handle = await test_env.cluster_or_scope.start_query(
            statement, StartQueryOptions(raw={'$country': 'United States', 'args': [2]})
        )
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
        finally:
            await result_handle.discard_results()

        q_handle = await test_env.cluster_or_scope.start_query(
            query_statement_pos_params_limit2, StartQueryOptions(raw={'args': ['United States']})
        )
        result_handle, result = await test_env.wait_for_query_results(q_handle)
        try:
            assert result is not None
            await test_env.assert_rows(result, 2)
            test_env.assert_streaming_response_state(result)
        finally:
            await result_handle.discard_results()

    async def test_query_handle_serialized(self, test_env: AsyncTestEnvironment, query_statement_limit5: str) -> None:
        # continue the query on another AsyncCluster, as another process would
        q_handle = await test_env.cluster_or_scope.start_query(query_statement_limit5)
        cluster = test_env.create_cluster()
        try:
            handle = AsyncQueryHandle.from_serialized(cluster, q_handle.to_serialized())
            result_handle, result = await test_env.wait_for_query_results(handle)
            assert result is not None
            await test_env.assert_rows(result, 5)
            await result_handle.discard_results()
        finally:
            await cluster.shutdown()

    async def test_query_handle_serialized_cancel(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'FROM range(0, 100000) AS r SELECT *'
        q_handle = await test_env.cluster_or_scope.start_query(statement)
        cluster = test_env.create_cluster()
        try:
            handle = AsyncQueryHandle.from_serialized(cluster, q_handle.to_serialized())
            await handle.cancel()
            await AsyncTestEnvironment.try_n_times_till_exception(10, 2, handle.fetch_status)
            with pytest.raises(QueryError):
                await handle.fetch_status()
        finally:
            await cluster.shutdown()

    async def test_query_results(self, test_env: AsyncTestEnvironment, query_statement_limit5: str) -> None:
        q_handle = await test_env.cluster_or_scope.start_query(query_statement_limit5)
        result_handle, _ = await test_env.wait_for_query_results(q_handle, return_only_result_handle=True)
        result = await result_handle.fetch_results()
        await test_env.assert_rows(result, 5)
        # fetch results again
        result = await result_handle.fetch_results()
        await test_env.assert_rows(result, 5)
        # now discard results
        await result_handle.discard_results()
        # fetching results after discarding should raise
        with pytest.raises(QueryNotFoundError):
            await result_handle.fetch_results()

    async def test_query_status_not_found(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'SELECT sleep("some value", 1000) AS some_field;'
        q_handle = await test_env.cluster_or_scope.start_query(statement)

        result_handle, _ = await test_env.wait_for_query_results(q_handle, return_only_result_handle=True)
        await result_handle.discard_results()

        with pytest.raises(QueryNotFoundError):
            await q_handle.fetch_status()

    async def test_query_status_prior_to_results(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'SELECT sleep("some value", 1000) AS some_field;'
        q_handle = await test_env.cluster_or_scope.start_query(statement)
        assert isinstance(q_handle, AsyncQueryHandle)
        q_status = await q_handle.fetch_status()
        assert isinstance(q_status, AsyncQueryStatus)
        assert q_status.results_ready() is False
        with pytest.raises(OperationalInsightsError):
            q_status.result_handle()

        # clean up
        result_handle, _ = await test_env.wait_for_query_results(q_handle, return_only_result_handle=True)
        await result_handle.discard_results()

    async def test_query_timeout(self, test_env: AsyncTestEnvironment) -> None:
        statement = 'SELECT sleep("some value", 10000) AS some_field;'
        q_handle = await test_env.cluster_or_scope.start_query(
            statement, StartQueryOptions(timeout=timedelta(seconds=2))
        )
        await AsyncTestEnvironment.try_n_times_till_exception(10, 2, q_handle.fetch_status)
        with pytest.raises(TimeoutError):
            await q_handle.fetch_status()


class ClusterStartQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ClusterStartQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ClusterStartQueryTests) if valid_test_method(meth)]
        test_list = set(QueryTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    async def couchbase_test_environment(
        self, async_test_env: AsyncTestEnvironment
    ) -> AsyncYieldFixture[AsyncTestEnvironment]:
        await async_test_env.setup()
        yield async_test_env
        await async_test_env.teardown()


class ScopeStartQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ScopeStartQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(ScopeStartQueryTests) if valid_test_method(meth)]
        test_list = set(QueryTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='test_env')
    async def couchbase_test_environment(
        self, async_test_env: AsyncTestEnvironment
    ) -> AsyncYieldFixture[AsyncTestEnvironment]:
        await async_test_env.setup()
        test_env = async_test_env.enable_scope()
        yield test_env
        test_env.disable_scope()
        await test_env.teardown()
