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

from copy import deepcopy
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Mapping,
    Optional,
    TypedDict,
    Union,
    cast,
    overload,
)
from urllib.parse import urlparse
from uuid import uuid4

from couchbase_operational_insights.common.deserializer import Deserializer
from couchbase_operational_insights.common.options import FetchResultsOptions, QueryOptions, StartQueryOptions
from couchbase_operational_insights.common.request import RequestURL
from couchbase_operational_insights.protocol.options import (
    QueryOptionsTransformedKwargs,
    StartQueryOptionsTransformedKwargs,
)
from couchbase_operational_insights.query import QueryScanConsistency

if TYPE_CHECKING:
    from acouchbase_operational_insights.protocol._core.client_adapter import _AsyncClientAdapter as AsyncClientAdapter
    from couchbase_operational_insights.protocol._core.client_adapter import _ClientAdapter as BlockingClientAdapter


class RequestTimeoutExtensions(TypedDict, total=False):
    pool: Optional[float]  # Timeout for acquiring a connection from the pool
    connect: Optional[float]  # Timeout for establishing a socket connection
    read: Optional[float]  # Timeout for reading data from the socket connection
    write: Optional[float]  # Timeout for writing data to the socket connection


class RequestExtensions(TypedDict, total=False):
    timeout: RequestTimeoutExtensions
    sni_hostname: Optional[str]
    trace: Optional[Callable[[str, str], Union[None, Coroutine[Any, Any, None]]]]


@dataclass(kw_only=True)
class HttpRequest:
    url: RequestURL
    extensions: RequestExtensions
    path: str
    method: str
    headers: Mapping[str, str]
    max_retries: int
    data: Optional[Dict[str, Union[str, object]]] = None
    body: Optional[Dict[str, Union[str, object]]] = None
    should_stream: Optional[bool] = False

    def add_trace_to_extensions(
        self, handler: Callable[[str, str], Union[None, Coroutine[Any, Any, None]]]
    ) -> HttpRequest:
        """
        **INTERNAL**
        """
        if self.extensions is None:
            self.extensions = {}
        self.extensions['trace'] = handler
        return self

    def get_request_timeouts(self) -> Optional[RequestTimeoutExtensions]:
        """
        **INTERNAL**
        """
        if self.extensions is None or 'timeout' not in self.extensions:
            return {}
        return self.extensions['timeout']

    def update_url(self, ip: str, path: str) -> HttpRequest:
        """
        **INTERNAL**
        """
        self.url.ip = ip
        self.url.path = path
        return self


@dataclass(kw_only=True)
class CancelRequest(HttpRequest):
    pass


@dataclass(kw_only=True)
class FetchResultsRequest(HttpRequest):
    deserializer: Deserializer
    should_stream: bool = True


@dataclass(kw_only=True)
class QueryRequest(HttpRequest):
    deserializer: Deserializer
    body: Dict[str, Union[str, object]]
    options: Optional[QueryOptionsTransformedKwargs] = None
    enable_cancel: Optional[bool] = None
    should_stream: bool = True

    def get_request_statement(self) -> Optional[str]:
        """
        **INTERNAL**
        """
        if 'statement' in self.body:
            return cast(str, self.body['statement'])
        return None


@dataclass(kw_only=True)
class StartQueryRequest(HttpRequest):
    body: Dict[str, Union[str, object]]
    options: Optional[StartQueryOptionsTransformedKwargs] = None

    def get_request_statement(self) -> Optional[str]:
        """
        **INTERNAL**
        """
        if 'statement' in self.body:
            return cast(str, self.body['statement'])
        return None


class _RequestBuilder:
    def __init__(
        self,
        client: Union[AsyncClientAdapter, BlockingClientAdapter],
        database_name: Optional[str] = None,
        scope_name: Optional[str] = None,
    ) -> None:
        self._conn_details = client.connection_details
        self._opts_builder = client.options_builder
        self._database_name = database_name
        self._scope_name = scope_name

        connect_timeout = self._conn_details.get_connect_timeout()
        self._handle_request_timeout = self._conn_details.get_handle_request_timeout()
        self._default_query_timeout = self._conn_details.get_query_timeout()
        self._extensions: RequestExtensions = {
            'timeout': {'pool': connect_timeout, 'connect': connect_timeout, 'read': self._default_query_timeout}
        }
        if self._conn_details.is_secure() and self._conn_details.sni_hostname is not None:
            self._extensions['sni_hostname'] = self._conn_details.sni_hostname

    def build_request_from_handle(self, handle: str, method: Optional[str] = None) -> HttpRequest:
        method = method or 'GET'
        extensions = deepcopy(self._extensions)
        extensions['timeout']['read'] = self._handle_request_timeout
        max_retries = self._conn_details.get_max_retries()
        parsed = urlparse(handle)
        path = parsed.path if parsed.scheme else handle
        return HttpRequest(
            url=self._conn_details.url,
            extensions=extensions,
            path=path,
            method=method,
            headers={},
            max_retries=max_retries,
        )

    def build_cancel_request(self, request_id: str) -> CancelRequest:
        extensions = deepcopy(self._extensions)
        extensions['timeout']['read'] = self._handle_request_timeout
        max_retries = self._conn_details.get_max_retries()
        return CancelRequest(
            url=self._conn_details.url,
            extensions=extensions,
            path='/api/v1/active_requests',
            method='DELETE',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            max_retries=max_retries,
            data={'request_id': request_id},
        )

    def build_discard_results_request(self, handle: str) -> HttpRequest:
        return self.build_request_from_handle(handle, method='DELETE')

    def build_fetch_results_request(
        self, handle: str, options: Optional[FetchResultsOptions] = None, **kwargs: object
    ) -> FetchResultsRequest:
        q_opts = self._opts_builder.build_options(FetchResultsOptions, kwargs, options)
        base_request = self.build_request_from_handle(handle)
        deserializer = q_opts.pop('deserializer', None) or self._conn_details.default_deserializer
        # time to first byte, then stays as the HTTP client's idle timeout
        timeout = q_opts.pop('timeout', None)
        if timeout:
            base_request.extensions['timeout']['read'] = timeout
        max_retries = self._conn_details.get_max_retries()
        return FetchResultsRequest(
            url=base_request.url,
            extensions=base_request.extensions,
            path=base_request.path,
            method=base_request.method,
            headers={},
            max_retries=max_retries,
            deserializer=deserializer,
        )

    def build_query_request(
        self,
        statement: str,
        *args: object,
        **kwargs: object,
    ) -> QueryRequest:
        enable_cancel: Optional[bool] = None
        cancel_kwarg_token = kwargs.pop('enable_cancel', None)
        if isinstance(cancel_kwarg_token, bool):
            enable_cancel = cancel_kwarg_token

        # default if no options provided
        opts = QueryOptions()
        args_list = list(args)
        parsed_args_list = []
        for arg in args_list:
            if isinstance(arg, QueryOptions):
                # we have options passed in
                opts = arg
            elif enable_cancel is None and isinstance(arg, bool):
                enable_cancel = arg
            else:
                parsed_args_list.append(arg)

        extensions, body, q_opts = self._get_query_request_details(
            QueryOptions, opts, statement, parsed_args_list=parsed_args_list, **kwargs
        )

        # handle deserializer and max_retries
        deserializer = q_opts.pop('deserializer', None) or self._conn_details.default_deserializer
        retries = q_opts.pop('max_retries', None)
        max_retries = retries if retries is not None else self._conn_details.get_max_retries()

        return QueryRequest(
            url=self._conn_details.url,
            extensions=extensions,
            path='',
            method='POST',
            headers={},
            max_retries=max_retries,
            deserializer=deserializer,
            body=body,
            options=q_opts,
            enable_cancel=enable_cancel,
        )

    def build_start_query_request(  # noqa: C901
        self,
        statement: str,
        *args: object,
        **kwargs: object,
    ) -> StartQueryRequest:  # noqa: C901
        # default if no options provided
        opts = StartQueryOptions()
        args_list = list(args)
        parsed_args_list = []
        for arg in args_list:
            if isinstance(arg, StartQueryOptions):
                # we have options passed in
                opts = arg
            else:
                parsed_args_list.append(arg)

        extensions, body, q_opts = self._get_query_request_details(
            StartQueryOptions, opts, statement, parsed_args_list=parsed_args_list, **kwargs
        )

        body['mode'] = 'async'
        retries = q_opts.pop('max_retries', None)
        max_retries = retries if retries is not None else self._conn_details.get_max_retries()

        return StartQueryRequest(
            url=self._conn_details.url,
            extensions=extensions,
            path='',
            method='POST',
            headers={},
            max_retries=max_retries,
            body=body,
            options=q_opts,
        )

    @overload
    def _get_query_request_details(
        self,
        option_type: type[QueryOptions],
        query_opts: QueryOptions,
        statement: str,
        parsed_args_list: Optional[List[object]] = None,
        **kwargs: object,
    ) -> tuple[RequestExtensions, Dict[str, Union[str, object]], QueryOptionsTransformedKwargs]: ...

    @overload
    def _get_query_request_details(
        self,
        option_type: type[StartQueryOptions],
        query_opts: StartQueryOptions,
        statement: str,
        parsed_args_list: Optional[List[object]] = None,
        **kwargs: object,
    ) -> tuple[RequestExtensions, Dict[str, Union[str, object]], StartQueryOptionsTransformedKwargs]: ...

    def _get_query_request_details(  # noqa: C901
        self,
        option_type: Union[type[QueryOptions], type[StartQueryOptions]],
        query_opts: Union[QueryOptions, StartQueryOptions],
        statement: str,
        parsed_args_list: Optional[List[object]] = None,
        **kwargs: object,
    ) -> Any:  # noqa: C901
        # need to pop out named params prior to sending options to the builder
        named_param_keys = list(filter(lambda k: k not in option_type.VALID_OPTION_KEYS, kwargs.keys()))
        named_params = {}
        for key in named_param_keys:
            named_params[key] = kwargs.pop(key)

        q_opts = self._opts_builder.build_options(option_type, kwargs, query_opts)
        # positional params and named params passed in outside of QueryOptions serve as overrides
        if parsed_args_list and len(parsed_args_list) > 0:
            q_opts['positional_parameters'] = parsed_args_list
        if named_params and len(named_params) > 0:
            q_opts['named_parameters'] = named_params

        body: Dict[str, Union[str, object]] = {
            'statement': statement,
            'client_context_id': q_opts.get('client_context_id', None) or str(uuid4()),
        }

        if self._database_name is not None and self._scope_name is not None:
            body['query_context'] = f'default:`{self._database_name}`.`{self._scope_name}`'

        # handle timeouts
        timeout = q_opts.get('timeout', None) or self._default_query_timeout
        extensions = deepcopy(self._extensions)
        if option_type == QueryOptions:
            if timeout is not None and timeout != self._default_query_timeout:
                extensions['timeout']['read'] = timeout
        else:
            extensions['timeout']['read'] = self._handle_request_timeout
        # we add 5 seconds to the server timeout to ensure we always trigger a client side timeout
        timeout_ms = (timeout + 5) * 1e3  # convert to milliseconds
        body['timeout'] = f'{timeout_ms}ms'

        for opt_key, opt_val in q_opts.items():
            if opt_key == 'deserializer':
                continue
            elif opt_key == 'raw':
                for k, v in opt_val.items():  # type: ignore[attr-defined]
                    body[k] = v
            elif opt_key == 'positional_parameters':
                body['args'] = list(opt_val)  # type: ignore[call-overload]
            elif opt_key == 'named_parameters':
                for k, v in opt_val.items():  # type: ignore[attr-defined]
                    key = f'${k}' if not k.startswith('$') else k
                    body[key] = v
            elif opt_key == 'readonly':
                body['readonly'] = opt_val
            elif opt_key == 'scan_consistency':
                if isinstance(opt_val, QueryScanConsistency):
                    body['scan_consistency'] = opt_val.value
                else:
                    body['scan_consistency'] = opt_val

        return extensions, body, q_opts
