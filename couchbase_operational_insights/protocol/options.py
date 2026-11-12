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

from copy import copy
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, TypedDict, Union, overload

from couchbase_operational_insights.common._core import JsonStreamConfig
from couchbase_operational_insights.common._core.utils import (
    VALIDATE_BOOL,
    VALIDATE_DESERIALIZER,
    VALIDATE_INT,
    VALIDATE_STR,
    VALIDATE_STR_LIST,
    EnumToStr,
    to_seconds,
    validate_path,
    validate_raw_dict,
)
from couchbase_operational_insights.common.deserializer import Deserializer
from couchbase_operational_insights.common.enums import QueryScanConsistency
from couchbase_operational_insights.common.options import (
    ClusterOptions,
    FetchResultsOptions,
    OptionsClass,
    QueryOptions,
    SecurityOptions,
    StartQueryOptions,
    TimeoutOptions,
)
from couchbase_operational_insights.common.options_base import (
    ClusterOptionsValidKeys,
    FetchResultsOptionsValidKeys,
    QueryOptionsValidKeys,
    SecurityOptionsValidKeys,
    StartQueryOptionsValidKeys,
    TimeoutOptionsValidKeys,
)

QUERY_CONSISTENCY_TO_STR = EnumToStr[QueryScanConsistency]()

QueryStrVal = Union[List[str], str, bool, int, float]


class ClusterOptionsTransforms(TypedDict):
    deserializer: Dict[Literal['deserializer'], Callable[[Any], Deserializer]]
    max_retries: Dict[Literal['max_retries'], Callable[[Any], int]]
    security_options: Dict[Literal['security_options'], Callable[[Any], Any]]
    timeout_options: Dict[Literal['timeout_options'], Callable[[Any], Any]]


CLUSTER_OPTIONS_TRANSFORMS: ClusterOptionsTransforms = {
    'deserializer': {'deserializer': VALIDATE_DESERIALIZER},
    'max_retries': {'max_retries': VALIDATE_INT},
    'security_options': {'security_options': lambda x: x},
    'timeout_options': {'timeout_options': lambda x: x},
}


class ClusterOptionsTransformedKwargs(TypedDict, total=False):
    deserializer: Optional[Deserializer]
    max_retries: Optional[int]
    security_options: Optional[SecurityOptionsTransformedKwargs]
    timeout_options: Optional[TimeoutOptionsTransformedKwargs]


class SecurityOptionsTransforms(TypedDict):
    trust_only_capella: Dict[Literal['trust_only_capella'], Callable[[Any], bool]]
    trust_only_pem_file: Dict[Literal['trust_only_pem_file'], Callable[[Any], str]]
    trust_only_pem_str: Dict[Literal['trust_only_pem_str'], Callable[[Any], str]]
    trust_only_certificates: Dict[Literal['trust_only_certificates'], Callable[[Any], List[str]]]
    disable_server_certificate_verification: Dict[
        Literal['disable_server_certificate_verification'], Callable[[Any], bool]
    ]


SECURITY_OPTIONS_TRANSFORMS: SecurityOptionsTransforms = {
    'trust_only_capella': {'trust_only_capella': VALIDATE_BOOL},
    'trust_only_pem_file': {'trust_only_pem_file': validate_path},
    'trust_only_pem_str': {'trust_only_pem_str': VALIDATE_STR},
    'trust_only_certificates': {'trust_only_certificates': VALIDATE_STR_LIST},
    'disable_server_certificate_verification': {'disable_server_certificate_verification': VALIDATE_BOOL},
}


class SecurityOptionsTransformedKwargs(TypedDict, total=False):
    trust_only_capella: Optional[bool]
    trust_only_pem_file: Optional[str]
    trust_only_pem_str: Optional[str]
    trust_only_certificates: Optional[List[str]]
    disable_server_certificate_verification: Optional[bool]


class TimeoutOptionsTransforms(TypedDict):
    connect_timeout: Dict[Literal['connect_timeout'], Callable[[Any], float]]
    handle_request_timeout: Dict[Literal['handle_request_timeout'], Callable[[Any], float]]
    query_timeout: Dict[Literal['query_timeout'], Callable[[Any], float]]


TIMEOUT_OPTIONS_TRANSFORMS: TimeoutOptionsTransforms = {
    'connect_timeout': {'connect_timeout': to_seconds},
    'handle_request_timeout': {'handle_request_timeout': to_seconds},
    'query_timeout': {'query_timeout': to_seconds},
}


class TimeoutOptionsTransformedKwargs(TypedDict, total=False):
    connect_timeout: Optional[int]
    handle_request_timeout: Optional[int]
    query_timeout: Optional[int]


class QueryOptionsTransforms(TypedDict):
    client_context_id: Dict[Literal['client_context_id'], Callable[[Any], str]]
    deserializer: Dict[Literal['deserializer'], Callable[[Any], Deserializer]]
    lazy_execute: Dict[Literal['lazy_execute'], Callable[[Any], bool]]
    max_retries: Dict[Literal['max_retries'], Callable[[Any], int]]
    named_parameters: Dict[Literal['named_parameters'], Callable[[Any], Any]]
    positional_parameters: Dict[Literal['positional_parameters'], Callable[[Any], Any]]
    query_context: Dict[Literal['query_context'], Callable[[Any], str]]
    raw: Dict[Literal['raw'], Callable[[Any], Dict[str, Any]]]
    readonly: Dict[Literal['readonly'], Callable[[Any], bool]]
    scan_consistency: Dict[Literal['scan_consistency'], Callable[[Any], str]]
    stream_config: Dict[Literal['stream_config'], Callable[[Any], JsonStreamConfig]]
    timeout: Dict[Literal['timeout'], Callable[[Any], float]]


QUERY_OPTIONS_TRANSFORMS: QueryOptionsTransforms = {
    'client_context_id': {'client_context_id': VALIDATE_STR},
    'deserializer': {'deserializer': VALIDATE_DESERIALIZER},
    'lazy_execute': {'lazy_execute': VALIDATE_BOOL},
    'max_retries': {'max_retries': VALIDATE_INT},
    'named_parameters': {'named_parameters': lambda x: x},
    'positional_parameters': {'positional_parameters': lambda x: x},
    'query_context': {'query_context': VALIDATE_STR},
    'raw': {'raw': validate_raw_dict},
    'readonly': {'readonly': VALIDATE_BOOL},
    'scan_consistency': {'scan_consistency': QUERY_CONSISTENCY_TO_STR},
    'stream_config': {'stream_config': lambda x: x},
    'timeout': {'timeout': to_seconds},
}


class QueryOptionsTransformedKwargs(TypedDict, total=False):
    client_context_id: Optional[str]
    deserializer: Optional[Deserializer]
    lazy_execute: Optional[bool]
    max_retries: Optional[int]
    named_parameters: Optional[Any]
    positional_parameters: Optional[Any]
    query_context: Optional[str]
    raw: Optional[Dict[str, Any]]
    readonly: Optional[bool]
    scan_consistency: Optional[str]
    stream_config: Optional[JsonStreamConfig]
    timeout: Optional[float]


class StartQueryOptionsTransforms(TypedDict):
    client_context_id: Dict[Literal['client_context_id'], Callable[[Any], str]]
    max_retries: Dict[Literal['max_retries'], Callable[[Any], int]]
    named_parameters: Dict[Literal['named_parameters'], Callable[[Any], Any]]
    positional_parameters: Dict[Literal['positional_parameters'], Callable[[Any], Any]]
    query_context: Dict[Literal['query_context'], Callable[[Any], str]]
    raw: Dict[Literal['raw'], Callable[[Any], Dict[str, Any]]]
    readonly: Dict[Literal['readonly'], Callable[[Any], bool]]
    scan_consistency: Dict[Literal['scan_consistency'], Callable[[Any], str]]
    stream_config: Dict[Literal['stream_config'], Callable[[Any], JsonStreamConfig]]
    timeout: Dict[Literal['timeout'], Callable[[Any], float]]


START_QUERY_OPTIONS_TRANSFORMS: StartQueryOptionsTransforms = {
    'client_context_id': {'client_context_id': VALIDATE_STR},
    'max_retries': {'max_retries': VALIDATE_INT},
    'named_parameters': {'named_parameters': lambda x: x},
    'positional_parameters': {'positional_parameters': lambda x: x},
    'query_context': {'query_context': VALIDATE_STR},
    'raw': {'raw': validate_raw_dict},
    'readonly': {'readonly': VALIDATE_BOOL},
    'scan_consistency': {'scan_consistency': QUERY_CONSISTENCY_TO_STR},
    'stream_config': {'stream_config': lambda x: x},
    'timeout': {'timeout': to_seconds},
}


class StartQueryOptionsTransformedKwargs(TypedDict, total=False):
    client_context_id: Optional[str]
    max_retries: Optional[int]
    named_parameters: Optional[Any]
    positional_parameters: Optional[Any]
    query_context: Optional[str]
    raw: Optional[Dict[str, Any]]
    readonly: Optional[bool]
    scan_consistency: Optional[str]
    stream_config: Optional[JsonStreamConfig]
    timeout: Optional[float]


class FetchResultsOptionsTransforms(TypedDict):
    deserializer: Dict[Literal['deserializer'], Callable[[Any], Deserializer]]
    timeout: Dict[Literal['timeout'], Callable[[Any], float]]


FETCH_RESULTS_OPTIONS_TRANSFORMS: FetchResultsOptionsTransforms = {
    'deserializer': {'deserializer': VALIDATE_DESERIALIZER},
    'timeout': {'timeout': to_seconds},
}


class FetchResultsOptionsTransformedKwargs(TypedDict, total=False):
    deserializer: Optional[Deserializer]
    timeout: Optional[float]


TransformDetailsPair = Union[
    Tuple[List[QueryOptionsValidKeys], QueryOptionsTransforms],
    Tuple[List[ClusterOptionsValidKeys], ClusterOptionsTransforms],
    Tuple[List[FetchResultsOptionsValidKeys], FetchResultsOptionsTransforms],
    Tuple[List[SecurityOptionsValidKeys], SecurityOptionsTransforms],
    Tuple[List[StartQueryOptionsValidKeys], StartQueryOptionsTransforms],
    Tuple[List[TimeoutOptionsValidKeys], TimeoutOptionsTransforms],
]


class OptionsBuilder:
    """
    **INTERNAL**
    """

    def _get_options_copy(
        self, options_class: type[OptionsClass], orig_kwargs: Dict[str, object], options: Optional[object] = None
    ) -> Dict[str, object]:
        orig_kwargs = copy(orig_kwargs) if orig_kwargs else {}
        # set our options base dict()
        temp_options: Dict[str, object] = {}
        if options and isinstance(options, (options_class, dict)):
            # mypy cannot recognize that all our options classes are dicts
            temp_options = options_class(**options)  # type: ignore[arg-type]
        else:
            temp_options = {}
        temp_options.update(orig_kwargs)

        return temp_options

    def _get_transform_details(self, option_type: str) -> TransformDetailsPair:  # noqa: C901
        if option_type == 'ClusterOptions':
            return ClusterOptions.VALID_OPTION_KEYS, CLUSTER_OPTIONS_TRANSFORMS
        elif option_type == 'SecurityOptions':
            return SecurityOptions.VALID_OPTION_KEYS, SECURITY_OPTIONS_TRANSFORMS
        elif option_type == 'TimeoutOptions':
            return TimeoutOptions.VALID_OPTION_KEYS, TIMEOUT_OPTIONS_TRANSFORMS
        elif option_type == 'QueryOptions':
            return QueryOptions.VALID_OPTION_KEYS, QUERY_OPTIONS_TRANSFORMS
        elif option_type == 'StartQueryOptions':
            return StartQueryOptions.VALID_OPTION_KEYS, START_QUERY_OPTIONS_TRANSFORMS
        elif option_type == 'FetchResultsOptions':
            return FetchResultsOptions.VALID_OPTION_KEYS, FETCH_RESULTS_OPTIONS_TRANSFORMS
        else:
            raise ValueError('Invalid OptionType.')

    def build_cluster_options(  # noqa: C901
        self,
        orig_kwargs: Dict[str, object],
        options: Optional[object] = None,
        query_str_opts: Optional[Dict[str, QueryStrVal]] = None,
    ) -> ClusterOptionsTransformedKwargs:
        temp_options = self._get_options_copy(ClusterOptions, orig_kwargs, options)

        # we flatten all the nested options (timeout_options & security_options)
        # so that we can combine the nested options w/ potential query string options
        # when parsing the various nested options we pass in keys that are okay to be ignored as
        # we know they are included in the overall "cluster options" umbrella (mainly due to handling
        # the query string options).

        security_opts = temp_options.pop('security_options', {})
        if security_opts and isinstance(security_opts, dict):
            for k, v in security_opts.items():
                if k not in temp_options:
                    temp_options[k] = v

        timeout_opts = temp_options.pop('timeout_options', {})
        if timeout_opts and isinstance(timeout_opts, dict):
            for k, v in timeout_opts.items():
                if k not in temp_options:
                    temp_options[k] = v

        if query_str_opts:
            # query string options override the options passed in via ClusterOptions
            for k, v in query_str_opts.items():
                temp_options[k] = v

        keys_to_ignore: List[str] = [*ClusterOptions.VALID_OPTION_KEYS, *TimeoutOptions.VALID_OPTION_KEYS]

        transformed_security_opts = self.build_options(SecurityOptions, temp_options, keys_to_ignore=keys_to_ignore)
        if transformed_security_opts:
            temp_options['security_options'] = transformed_security_opts

        keys_to_ignore = [*ClusterOptions.VALID_OPTION_KEYS, *SecurityOptions.VALID_OPTION_KEYS]

        transformed_timeout_opts = self.build_options(TimeoutOptions, temp_options, keys_to_ignore=keys_to_ignore)
        if transformed_timeout_opts:
            temp_options['timeout_options'] = transformed_timeout_opts

        # transform final ClusterOptions
        transformed_opts = self.build_options(ClusterOptions, temp_options)

        return transformed_opts

    @overload
    def build_options(
        self,
        option_type: type[ClusterOptions],
        orig_kwargs: Dict[str, object],
        options: Optional[object] = ...,
        keys_to_ignore: Optional[List[str]] = ...,
    ) -> ClusterOptionsTransformedKwargs: ...

    @overload
    def build_options(
        self,
        option_type: type[SecurityOptions],
        orig_kwargs: Dict[str, object],
        options: Optional[object] = ...,
        keys_to_ignore: Optional[List[str]] = ...,
    ) -> SecurityOptionsTransformedKwargs: ...

    @overload
    def build_options(
        self,
        option_type: type[TimeoutOptions],
        orig_kwargs: Dict[str, object],
        options: Optional[object] = ...,
        keys_to_ignore: Optional[List[str]] = ...,
    ) -> TimeoutOptionsTransformedKwargs: ...

    @overload
    def build_options(
        self,
        option_type: type[QueryOptions],
        orig_kwargs: Dict[str, object],
        options: Optional[object] = ...,
        keys_to_ignore: Optional[List[str]] = ...,
    ) -> QueryOptionsTransformedKwargs: ...

    @overload
    def build_options(
        self,
        option_type: type[StartQueryOptions],
        orig_kwargs: Dict[str, object],
        options: Optional[object] = ...,
        keys_to_ignore: Optional[List[str]] = ...,
    ) -> StartQueryOptionsTransformedKwargs: ...

    @overload
    def build_options(
        self,
        option_type: type[FetchResultsOptions],
        orig_kwargs: Dict[str, object],
        options: Optional[object] = ...,
        keys_to_ignore: Optional[List[str]] = ...,
    ) -> FetchResultsOptionsTransformedKwargs: ...

    def build_options(
        self,
        option_type: type[OptionsClass],
        orig_kwargs: Dict[str, object],
        options: Optional[object] = None,
        keys_to_ignore: Optional[List[str]] = None,
    ) -> Any:
        temp_options = self._get_options_copy(option_type, orig_kwargs, options)
        transformed_opts: Any = {}
        # Option 1 satisfies mypy, but we want temp_options to be the limiting factor for the loop.
        # Option 2. Also makes providing warnings/exceptions for users not using static type checking easier,
        # but unfortunately we need to use some type: ignore comments

        # Option 1:
        # for k in option_type.VALID_OPTION_KEYS:
        #     if k in ALLOWED_TRANSFORM_KEYS and k in temp_options:
        #         for nk, cfn in tf_dict[k].items():
        #             conv = cfn(temp_options[k])
        #             transformed_opts[nk] = conv # type: ignore

        # Option 2:
        allowed_keys, option_transforms = self._get_transform_details(option_type.__name__)
        for k, v in temp_options.items():
            if k in allowed_keys:
                transforms = option_transforms[k]  # type: ignore[literal-required]
                for nk, cfn in transforms.items():
                    conv = cfn(v)
                    if conv is not None:
                        transformed_opts[nk] = conv
            elif keys_to_ignore and k not in keys_to_ignore:
                raise ValueError(f'Invalid key provided (key={k}).')

        return transformed_opts
