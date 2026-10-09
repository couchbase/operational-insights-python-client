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

from typing import TYPE_CHECKING, AsyncGenerator, Generator

from httpx2 import Auth, Request, Response

if TYPE_CHECKING:
    from couchbase_operational_insights.common.credential import _CredentialHolder


class DynamicCredentialAuth(Auth):
    """httpx ``Auth`` that reads the current credential from a ``_CredentialHolder`` at
    request time, so rotating a credential via ``Cluster.set_credential`` takes effect
    immediately without rebuilding the HTTP client.
    """

    def __init__(self, credential_holder: _CredentialHolder) -> None:
        self._credential_holder = credential_holder

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        self._credential_holder.apply_to_request(request)
        yield request

    async def async_auth_flow(self, request: Request) -> AsyncGenerator[Request, Response]:
        # Mirror of auth_flow — applying a credential is non-blocking.
        self._credential_holder.apply_to_request(request)
        yield request
