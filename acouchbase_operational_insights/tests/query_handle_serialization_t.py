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
from typing import Dict, List, NamedTuple, Optional, Union

import pytest
from httpx2 import Response

from acouchbase_operational_insights.cluster import AsyncCluster
from acouchbase_operational_insights.credential import Credential
from acouchbase_operational_insights.query_handle import AsyncQueryHandle
from couchbase_operational_insights.protocol._core.request import HttpRequest
from tests import AsyncYieldFixture

REQUEST_ID = '8fb94489-dcd7-466d-8639-cb02144c3f79'
HANDLE = f'/api/v1/request/status/{REQUEST_ID}/12-0'


class SentRequest(NamedTuple):
    method: str
    ip: Optional[str]
    port: int
    path: Optional[str]
    data: Optional[Dict[str, Union[str, object]]]


class QueryHandleSerializationTestSuite:
    TEST_MANIFEST = [
        'test_fetch_status_creates_client',
        'test_from_serialized_ignores_extra_properties',
        'test_from_serialized_invalid',
        'test_from_serialized_rejects_non_cluster',
        'test_handle_url_only_sends_path',
        'test_round_trip',
        'test_round_trip_requests',
        'test_to_serialized',
    ]

    @pytest.fixture
    async def cluster(self) -> AsyncYieldFixture[AsyncCluster]:
        cred = Credential.from_username_and_password('Administrator', 'password')
        cluster = AsyncCluster.create_instance('http://127.0.0.1:8095', cred)
        yield cluster
        await cluster.shutdown()

    @pytest.fixture
    def sent_requests(self, cluster: AsyncCluster, monkeypatch: pytest.MonkeyPatch) -> List[SentRequest]:
        sent: List[SentRequest] = []

        async def send_request(request: HttpRequest, stream: Optional[bool] = True) -> Response:
            # the request URL is shared by every request, so copy what was sent
            url = request.url
            sent.append(SentRequest(request.method, url.ip, url.port, url.path, request.data))
            if request.method == 'DELETE':
                return Response(200)
            return Response(200, json={'status': 'running'})

        monkeypatch.setattr(cluster._impl.client_adapter, 'send_request', send_request)  # type: ignore[attr-defined]
        return sent

    @pytest.mark.anyio
    async def test_from_serialized_ignores_extra_properties(self, cluster: AsyncCluster) -> None:
        serialized = json.dumps({'requestID': REQUEST_ID, 'handle': HANDLE, 'other': {'nested': True}})
        handle = AsyncQueryHandle.from_serialized(cluster, serialized)
        assert json.loads(handle.to_serialized()) == {'requestID': REQUEST_ID, 'handle': HANDLE}

    @pytest.mark.parametrize(
        'serialized',
        [
            'not json',
            '',
            '[]',
            '"a string"',
            'null',
            json.dumps({'handle': HANDLE}),
            json.dumps({'requestID': REQUEST_ID}),
            json.dumps({'requestID': '', 'handle': HANDLE}),
            json.dumps({'requestID': REQUEST_ID, 'handle': ''}),
            json.dumps({'requestID': 1, 'handle': HANDLE}),
            json.dumps({'requestID': REQUEST_ID, 'handle': None}),
            None,
        ],
    )
    @pytest.mark.anyio
    async def test_from_serialized_invalid(self, cluster: AsyncCluster, serialized: str) -> None:
        with pytest.raises(ValueError):
            AsyncQueryHandle.from_serialized(cluster, serialized)

    @pytest.mark.anyio
    async def test_fetch_status_creates_client(self, cluster: AsyncCluster, sent_requests: List[SentRequest]) -> None:
        # the cluster creates its client on the first query, a deserialized handle can come before that
        serialized = json.dumps({'requestID': REQUEST_ID, 'handle': HANDLE})
        handle = AsyncQueryHandle.from_serialized(cluster, serialized)
        assert cluster._impl.has_client is False  # type: ignore[attr-defined]
        await handle.fetch_status()
        assert cluster._impl.has_client is True  # type: ignore[attr-defined]
        assert len(sent_requests) == 1

    @pytest.mark.anyio
    async def test_from_serialized_rejects_non_cluster(self, cluster: AsyncCluster) -> None:
        serialized = json.dumps({'requestID': REQUEST_ID, 'handle': HANDLE})
        scope = cluster.database('test-database').scope('test-scope')
        with pytest.raises(ValueError, match='An AsyncQueryHandle can only be deserialized with an AsyncCluster.'):
            AsyncQueryHandle.from_serialized(scope, serialized)  # type: ignore[arg-type]

    @pytest.mark.anyio
    async def test_handle_url_only_sends_path(self, cluster: AsyncCluster, sent_requests: List[SentRequest]) -> None:
        # The handle is only ever used as a path, so the cluster's credentials are only sent to its own endpoint
        serialized = json.dumps({'requestID': REQUEST_ID, 'handle': f'https://other-host:9999{HANDLE}'})
        handle = AsyncQueryHandle.from_serialized(cluster, serialized)
        await handle.fetch_status()

        assert len(sent_requests) == 1
        assert sent_requests[0].ip == '127.0.0.1'
        assert sent_requests[0].port == 8095
        assert sent_requests[0].path == HANDLE

    @pytest.mark.anyio
    async def test_round_trip(self, cluster: AsyncCluster) -> None:
        serialized = json.dumps({'requestID': REQUEST_ID, 'handle': HANDLE})
        handle = AsyncQueryHandle.from_serialized(cluster, serialized)
        assert isinstance(handle, AsyncQueryHandle)
        assert AsyncQueryHandle.from_serialized(cluster, handle.to_serialized()).to_serialized() == serialized

    @pytest.mark.anyio
    async def test_round_trip_requests(self, cluster: AsyncCluster, sent_requests: List[SentRequest]) -> None:
        serialized = json.dumps({'requestID': REQUEST_ID, 'handle': HANDLE})
        handle = AsyncQueryHandle.from_serialized(cluster, serialized)
        status = await handle.fetch_status()
        assert status.results_ready() is False
        await handle.cancel()

        assert len(sent_requests) == 2
        assert sent_requests[0].method == 'GET'
        assert sent_requests[0].path == HANDLE
        assert sent_requests[1].method == 'DELETE'
        assert sent_requests[1].data == {'request_id': REQUEST_ID}

    @pytest.mark.anyio
    async def test_to_serialized(self, cluster: AsyncCluster) -> None:
        serialized = json.dumps({'requestID': REQUEST_ID, 'handle': HANDLE})
        handle = AsyncQueryHandle.from_serialized(cluster, serialized)
        assert json.loads(handle.to_serialized()) == {'requestID': REQUEST_ID, 'handle': HANDLE}


class QueryHandleSerializationTests(QueryHandleSerializationTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(QueryHandleSerializationTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(QueryHandleSerializationTests) if valid_test_method(meth)]
        test_list = set(QueryHandleSerializationTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
