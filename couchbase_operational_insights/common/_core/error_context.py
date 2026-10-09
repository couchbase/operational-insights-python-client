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

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from httpx2 import Response as HttpCoreResponse

from couchbase_operational_insights.protocol._core.request import FetchResultsRequest, HttpRequest, QueryRequest


@dataclass
class ErrorContext:
    num_attempts: int = 0
    path: Optional[str] = None
    method: Optional[str] = None
    status_code: Optional[int] = None
    statement: Optional[str] = None
    last_dispatched_to: Optional[str] = None
    last_dispatched_from: Optional[str] = None
    errors: Optional[List[Dict[str, Any]]] = None
    first_error: Optional[Dict[str, Any]] = None

    def set_errors(self, errors: List[Dict[str, Any]]) -> None:
        self.errors: List[Dict[str, Any]] = errors

    def set_first_error(self, error: Dict[str, Any]) -> None:
        self.first_error = error

    def set_statement(self, statement: Optional[str]) -> None:
        self.statement = statement

    def maybe_update_errors(self) -> None:
        if self.errors is not None and len(self.errors) > 0:
            return
        if self.first_error is not None:
            self.errors = [self.first_error]

    def update_num_attempts(self) -> None:
        self.num_attempts += 1

    def update_request_context(
        self, request: Union[HttpRequest, FetchResultsRequest, QueryRequest], path: Optional[str] = None
    ) -> None:
        self.path = path or request.url.path

    def update_response_context(self, response: HttpCoreResponse) -> None:
        network_stream = response.extensions.get('network_stream', None)
        if network_stream is not None:
            addr, port, *_ = network_stream.get_extra_info('client_addr')
            self.last_dispatched_from = f'{addr}:{port}'
            addr, port, *_ = network_stream.get_extra_info('server_addr')
            self.last_dispatched_to = f'{addr}:{port}'
        self.status_code = response.status_code

    def _ctx_details(self) -> Dict[str, str]:
        details: Dict[str, str] = {
            'num_attempts': str(self.num_attempts),
        }
        if self.path is not None:
            details['path'] = self.path
        if self.method is not None:
            details['method'] = self.method
        if self.status_code is not None:
            details['status_code'] = str(self.status_code)
        if self.statement is not None:
            details['statement'] = self.statement
        if self.last_dispatched_to is not None:
            details['last_dispatched_to'] = self.last_dispatched_to
        if self.last_dispatched_from is not None:
            details['last_dispatched_from'] = self.last_dispatched_from
        if self.errors is not None:
            errors = ', '.join(str(e) for e in self.errors)
            details['errors'] = f'[{errors}]'
        return details

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._ctx_details()})'

    def __str__(self) -> str:
        return str(self._ctx_details())
