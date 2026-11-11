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


from typing import List

import pytest

pytest_plugins = [
    'tests.operational_insights_config',
    'tests.environments.base_environment',
    'tests.environments.simple_environment',
]

_UNIT_TESTS = [
    'acouchbase_operational_insights/tests/connection_t.py::ConnectionTests',
    'acouchbase_operational_insights/tests/credential_t.py::CredentialTests',
    'acouchbase_operational_insights/tests/json_parsing_t.py::JsonParsingTests',
    'acouchbase_operational_insights/tests/logging_t.py::LogPrefixTests',
    'acouchbase_operational_insights/tests/options_t.py::ClusterOptionsTests',
    'acouchbase_operational_insights/tests/query_handle_serialization_t.py::QueryHandleSerializationTests',
    'acouchbase_operational_insights/tests/query_options_t.py::ClusterQueryOptionsTests',
    'acouchbase_operational_insights/tests/query_options_t.py::ScopeQueryOptionsTests',
    'acouchbase_operational_insights/tests/test_server_t.py::ClusterTestServerTests',
    'acouchbase_operational_insights/tests/test_server_t.py::ScopeTestServerTests',
    'couchbase_operational_insights/tests/connection_t.py::ConnectionTests',
    'couchbase_operational_insights/tests/credential_t.py::CredentialTests',
    'couchbase_operational_insights/tests/duration_parsing_t.py::DurationParsingTests',
    'couchbase_operational_insights/tests/json_parsing_t.py::JsonParsingTests',
    'couchbase_operational_insights/tests/logging_t.py::LogPrefixTests',
    'couchbase_operational_insights/tests/logging_t.py::LoggingTests',
    'couchbase_operational_insights/tests/options_t.py::ClusterOptionsTests',
    'couchbase_operational_insights/tests/query_handle_serialization_t.py::QueryHandleSerializationTests',
    'couchbase_operational_insights/tests/query_options_t.py::ClusterQueryOptionsTests',
    'couchbase_operational_insights/tests/query_options_t.py::ScopeQueryOptionsTests',
    'couchbase_operational_insights/tests/start_query_options_t.py::ClusterStartQueryOptionsTests',
    'couchbase_operational_insights/tests/start_query_options_t.py::ScopeStartQueryOptionsTests',
    'couchbase_operational_insights/tests/test_server_t.py::ClusterTestServerTests',
    'couchbase_operational_insights/tests/test_server_t.py::ScopeTestServerTests',
]

_INTEGRATRION_TESTS = [
    'acouchbase_operational_insights/tests/connect_integration_t.py::ConnectTests',
    'acouchbase_operational_insights/tests/query_integration_t.py::ClusterQueryTests',
    'acouchbase_operational_insights/tests/query_integration_t.py::ScopeQueryTests',
    'acouchbase_operational_insights/tests/start_query_integration_t.py::ClusterStartQueryTests',
    'acouchbase_operational_insights/tests/start_query_integration_t.py::ScopeStartQueryTests',
    'couchbase_operational_insights/tests/connect_integration_t.py::ConnectTests',
    'couchbase_operational_insights/tests/query_integration_t.py::ClusterQueryTests',
    'couchbase_operational_insights/tests/query_integration_t.py::ScopeQueryTests',
    'couchbase_operational_insights/tests/start_query_integration_t.py::ClusterStartQueryTests',
    'couchbase_operational_insights/tests/start_query_integration_t.py::ScopeStartQueryTests',
]


@pytest.fixture(scope='class')
def anyio_backend() -> str:
    return 'asyncio'


# https://docs.pytest.org/en/stable/reference/reference.html#std-hook-pytest_collection_modifyitems
def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: List[pytest.Item]) -> None:  # noqa: C901
    for item in items:
        item_details = item.nodeid.split('::')

        item_api = item_details[0].split('/')
        if item_api[0] == 'couchbase_operational_insights':
            item.add_marker(pytest.mark.pycbac_couchbase)
        elif item_api[0] == 'acouchbase_operational_insights':
            item.add_marker(pytest.mark.pycbac_acouchbase)

        test_class_path = '::'.join(item_details[:-1])
        if test_class_path in _UNIT_TESTS:
            item.add_marker(pytest.mark.pycbac_unit)
        elif test_class_path in _INTEGRATRION_TESTS:
            item.add_marker(pytest.mark.pycbac_integration)
