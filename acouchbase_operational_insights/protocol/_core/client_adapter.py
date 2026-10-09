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

import logging
from typing import Optional, cast
from uuid import uuid4

from httpx2 import URL, AsyncClient, Response

from couchbase_operational_insights.common.credential import Credential, _CredentialHolder
from couchbase_operational_insights.common.deserializer import Deserializer
from couchbase_operational_insights.common.logging import (
    ASYNC_LOGGER_NAME,
    LogLevel,
    log_client_version,
    log_message,
)
from couchbase_operational_insights.protocol import PYCBOI_VERSION
from couchbase_operational_insights.protocol._core.auth import DynamicCredentialAuth
from couchbase_operational_insights.protocol._core.request import HttpRequest
from couchbase_operational_insights.protocol.connection import _ConnectionDetails
from couchbase_operational_insights.protocol.options import OptionsBuilder


class _AsyncClientAdapter:
    """
    **INTERNAL**
    """

    OPERATIONAL_INSIGHTS_PATH = '/api/v1/request'
    LOGGER_NAME = ASYNC_LOGGER_NAME

    def __init__(
        self, http_endpoint: str, credential: Credential, options: Optional[object] = None, **kwargs: object
    ) -> None:
        self._client_id = str(uuid4())
        self._prefix = ''
        self._cluster_id = cast(str, kwargs.pop('cluster_id', ''))
        self._opts_builder = OptionsBuilder()
        kwargs['logger_name'] = self.logger_name
        self._conn_details = _ConnectionDetails.create(self._opts_builder, http_endpoint, credential, options, **kwargs)
        self._credential_holder = _CredentialHolder(credential)
        # PYCO-67:  Do we want to allow supporting custom HTTP transports?
        self._http_transport_cls = None

    @property
    def operational_insights_path(self) -> str:
        """
        **INTERNAL**
        """
        return self.OPERATIONAL_INSIGHTS_PATH

    @property
    def client(self) -> AsyncClient:
        """
        **INTERNAL**
        """
        return self._client

    @property
    def client_id(self) -> str:
        """
        **INTERNAL**
        """
        return self._client_id

    @property
    def connection_details(self) -> _ConnectionDetails:
        """
        **INTERNAL**
        """
        return self._conn_details

    @property
    def credential_holder(self) -> _CredentialHolder:
        """
        **INTERNAL**
        """
        return self._credential_holder

    @property
    def default_deserializer(self) -> Deserializer:
        """
        **INTERNAL**
        """
        return self._conn_details.default_deserializer

    @property
    def has_client(self) -> bool:
        """
        **INTERNAL**
        """
        return hasattr(self, '_client')

    @property
    def log_prefix(self) -> str:
        """
        **INTERNAL**
        """
        if self._prefix:
            return self._prefix
        if not self.has_client:
            # The client id and scheme are part of the prefix, so there is nothing worth
            # caching yet; caching here would pin the truncated form for the cluster's life.
            return f'[{self._cluster_id}]'
        self._prefix = f'[{self._cluster_id}/{self._client_id}'
        if self.connection_details.is_secure():
            self._prefix += '/https]'
        else:
            self._prefix += '/http]'

        return self._prefix

    @property
    def logger_name(self) -> str:
        """
        **INTERNAL**
        """
        return self.LOGGER_NAME

    @property
    def options_builder(self) -> OptionsBuilder:
        """
        **INTERNAL**
        """
        return self._opts_builder

    async def close_client(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_client'):
            await self._client.aclose()
            self.log_message('Cluster HTTP client closed', LogLevel.INFO)

    def _build_client(self) -> AsyncClient:
        auth = DynamicCredentialAuth(self._credential_holder)
        if self._conn_details.is_secure():
            if self._conn_details.ssl_context is None:
                raise ValueError('SSL context is required for secure connections.')
            transport = None
            if self._http_transport_cls is not None:
                transport = self._http_transport_cls(verify=self._conn_details.ssl_context)
            return AsyncClient(verify=self._conn_details.ssl_context, auth=auth, transport=transport)
        transport = None
        if self._http_transport_cls is not None:
            transport = self._http_transport_cls()
        return AsyncClient(auth=auth, transport=transport)

    async def create_client(self) -> None:
        """
        **INTERNAL**
        """
        if not hasattr(self, '_client'):
            self._client = self._build_client()
            log_client_version(logger, PYCBOI_VERSION, self.log_prefix)
            self.log_message(
                (f'Cluster HTTP client created: connection_details={self._conn_details.get_init_details()}'),
                LogLevel.INFO,
            )
        else:
            self.log_message('Cluster HTTP client already exists, skipping creation.', LogLevel.INFO)

    def log_message(self, message: str, log_level: LogLevel) -> None:
        log_message(logger, f'{self.log_prefix} {message}', log_level)

    async def send_request(self, request: HttpRequest, stream: Optional[bool] = True) -> Response:
        """
        **INTERNAL**
        """
        if not hasattr(self, '_client'):
            raise RuntimeError('Client not created yet')

        url = URL(scheme=request.url.scheme, host=request.url.ip, port=request.url.port, path=request.url.path)
        req = self._client.build_request(
            request.method,
            url,
            data=request.data,
            json=request.body,
            headers=request.headers,
            extensions=request.extensions,
        )

        if stream is None:
            stream = True

        return await self._client.send(req, stream=stream)

    def reset_client(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_client'):
            del self._client

    async def update_credential(self, new_credential: Credential) -> None:
        old_client = None
        self._credential_holder.credential._check_replaceable_with(new_credential)
        if new_credential._kind == 'cert':
            # httpx pins the SSL context to the AsyncClient at construction,
            # and the cert chain is part of that context.  So a cert rotation
            # needs a fresh Client.  Build it before aclosing the old one,
            # otherwise a concurrent send_request can see self._client gone.
            old_client = getattr(self, '_client', None)
            old_ssl_context = self._conn_details.ssl_context
            old_sni_hostname = self._conn_details.sni_hostname
            try:
                self._conn_details.validate_security_options(new_credential)
                # If the cluster hasn't issued a request yet there's no Client
                # to swap; we still refreshed the SSL context above.
                if old_client is not None:
                    self._client = self._build_client()
            except Exception:
                self._conn_details.ssl_context = old_ssl_context
                self._conn_details.sni_hostname = old_sni_hostname
                raise
        self._credential_holder.replace(new_credential)
        if old_client is not None:
            await old_client.aclose()
        self.log_message('Cluster HTTP credential updated', LogLevel.INFO)


logger = logging.getLogger(_AsyncClientAdapter.LOGGER_NAME)
