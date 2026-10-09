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


from typing import Dict, Optional, Union

from httpx2 import URL, Response

from acouchbase_operational_insights.protocol._core.client_adapter import _AsyncClientAdapter
from couchbase_operational_insights.protocol._core.request import (
    CancelRequest,
    HttpRequest,
    QueryRequest,
    StartQueryRequest,
)


def client_adapter_init_override(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
    if not hasattr(self, 'PYCBOI_TESTING'):
        raise RuntimeError('This is a testing only adapter')
    self._http_transport_cls = kwargs.pop('http_transport_cls', None)
    if self._http_transport_cls is not None and not hasattr(self._http_transport_cls, 'PYCBOI_TESTING'):
        raise RuntimeError('http_transport_cls must be a test transport')
    adapter: _AsyncClientAdapter = kwargs.pop('adapter', None)
    self._client_id = adapter._client_id
    self._prefix = adapter._prefix
    self._cluster_id = adapter._cluster_id
    self._opts_builder = adapter._opts_builder
    self._conn_details = adapter._conn_details
    self._credential_holder = adapter._credential_holder
    if self._http_transport_cls is None:
        self._http_transport_cls = adapter._http_transport_cls


async def send_request_override(
    self: _AsyncClientAdapter,
    request: Union[CancelRequest, HttpRequest, QueryRequest, StartQueryRequest],
    stream: Optional[bool] = True,
) -> Response:
    if not hasattr(self, '_client'):
        raise RuntimeError('Client not created yet')

    request_json = request.body
    if hasattr(self, '_request_json') and self._request_json is not None:
        request_json.update(self._request_json)

    request_extensions = request.extensions
    if hasattr(self, '_request_extensions') and self._request_extensions is not None:
        if request_extensions is None:
            request_extensions = self._request_extensions
        else:
            if 'timeout' in self._request_extensions:
                request_extensions['timeout'].update(self._request_extensions['timeout'])

    url = URL(scheme=request.url.scheme, host=request.url.host, port=request.url.port, path=request.url.path)
    req = self._client.build_request(request.method, url, json=request_json, extensions=request_extensions)
    if stream is None:
        stream = True
    return await self._client.send(req, stream=stream)


def set_request_path(self: _AsyncClientAdapter, path: str) -> None:
    self.OPERATIONAL_INSIGHTS_PATH = path


def update_request_json(self: _AsyncClientAdapter, json: Dict[str, object]) -> None:
    self._request_json = json  # type: ignore[attr-defined]


def update_request_extensions(self: _AsyncClientAdapter, extensions: Dict[str, str]) -> None:
    self._request_extensions = extensions  # type: ignore[attr-defined]


class _TestAsyncClientAdapter(_AsyncClientAdapter):
    pass


_TestAsyncClientAdapter.__init__ = client_adapter_init_override  # type: ignore[method-assign]
_TestAsyncClientAdapter.send_request = send_request_override  # type: ignore[method-assign]
_TestAsyncClientAdapter.set_request_path = set_request_path
_TestAsyncClientAdapter.update_request_json = update_request_json
_TestAsyncClientAdapter.update_request_extensions = update_request_extensions
_TestAsyncClientAdapter.PYCBOI_TESTING = True

__all__ = ['_TestAsyncClientAdapter']
