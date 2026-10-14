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

import sys
from typing import List, Union

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_operational_insights.common.options_base import (
    ClusterOptionsBase,
    FetchResultsOptionsBase,
    QueryOptionsBase,
    SecurityOptionsBase,
    StartQueryOptionsBase,
    TimeoutOptionsBase,
)
from couchbase_operational_insights.common.options_base import (
    ClusterOptionsKwargs as ClusterOptionsKwargs,  # noqa: F401
)
from couchbase_operational_insights.common.options_base import (
    FetchResultsOptionsKwargs as FetchResultsOptionsKwargs,  # noqa: F401
)
from couchbase_operational_insights.common.options_base import QueryOptionsKwargs as QueryOptionsKwargs  # noqa: F401
from couchbase_operational_insights.common.options_base import (
    SecurityOptionsKwargs as SecurityOptionsKwargs,  # noqa: F401
)
from couchbase_operational_insights.common.options_base import (
    StartQueryOptionsKwargs as StartQueryOptionsKwargs,  # noqa: F401
)
from couchbase_operational_insights.common.options_base import (
    TimeoutOptionsKwargs as TimeoutOptionsKwargs,  # noqa: F401
)

"""
    Python SDK Cluster Options Classes
"""


class ClusterOptions(ClusterOptionsBase):
    """Available options to set when creating a cluster.

    Cluster options enable the configuration of various global cluster settings.
    Some options can be set globally for the cluster, but overridden for a specific request (i.e. :class:`.TimeoutOptions`).

    .. note::
        Options and methods marked **VOLATILE** are subject to change at any time.

    Args:
        deserializer (Optional[Deserializer]): Set to configure global serializer to translate JSON to Python objects. Defaults to `None` (:class:`~couchbase_operational_insights.deserializer.DefaultJsonDeserializer`).
        max_retries (Optional[int]): **VOLATILE** Set to configure the maximum number of retries for a request. Defaults to 7.
        security_options (Optional[:class:`.SecurityOptions`]): Security options for SDK connection.
        timeout_options (Optional[:class:`.TimeoutOptions`]): Timeout options for the various stages of a request. See :class:`.TimeoutOptions` for details.
    """  # noqa: E501


class FetchResultsOptions(FetchResultsOptionsBase):
    """Available options for Operational Insights asynchronous server query fetch results operation.

    Args:
        deserializer (Optional[Deserializer]): Specifies a :class:`~couchbase_operational_insights.deserializer.Deserializer` to apply to results.  Defaults to `None` (:class:`~couchbase_operational_insights.deserializer.DefaultJsonDeserializer`).
    """  # noqa: E501


class SecurityOptions(SecurityOptionsBase):
    """Available security options to set when creating a cluster.

    All options are optional and not required to be specified.  By default the SDK will trust only the Capella CA certificate(s).
    Only a single option related to which certificate(s) the SDK should trust can be used.
    The `disable_server_certificate_verification` option can either be enabled or disabled for any of the specified trust settings.

    Args:
        trust_only_capella (Optional[bool]): If enabled, SDK will trust only the Capella CA certificate(s). Defaults to `True` (enabled).
        trust_only_pem_file (Optional[str]): If set, SDK will trust only the PEM-encoded certificate(s) at the specified file path. Defaults to `None`.
        trust_only_pem_str (Optional[str]): If set, SDK will trust only the PEM-encoded certificate(s) in the specified str. Defaults to `None`.
        trust_only_certificates (Optional[List[str]]): If set, SDK will trust only the PEM-encoded certificate(s) specified. Defaults to `None`.
        disable_server_certificate_verification (Optional[bool]): If disabled, SDK will trust any certificate regardless of validity.
            Should not be disabled in production environments. Defaults to `True` (enabled).
    """  # noqa: E501

    @classmethod
    def trust_only_capella(cls) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_capella=True`.

        Returns:
            :class:`~couchbase_operational_insights.common.options.SecurityOptions`
        """
        return cls(trust_only_capella=True)

    @classmethod
    def trust_only_pem_file(cls, pem_file: str) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_pem_file` set to provided certificate(s) path.

        Args:
            pem_file (str): Path to PEM-encoded certificate(s) the SDK should trust.

        Returns:
            :class:`~couchbase_operational_insights.common.options.SecurityOptions`
        """  # noqa: E501
        return cls(trust_only_capella=False, trust_only_pem_file=pem_file)

    @classmethod
    def trust_only_pem_str(cls, pem_str: str) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_pem_str` set to provided certificate(s) str.

        Args:
            pem_str (str): PEM-encoded certificate(s) the SDK should trust.

        Returns:
            :class:`~couchbase_operational_insights.common.options.SecurityOptions`
        """  # noqa: E501
        return cls(trust_only_capella=False, trust_only_pem_str=pem_str)

    @classmethod
    def trust_only_certificates(cls, certificates: List[str]) -> SecurityOptions:
        """
        Convenience method that returns `SecurityOptions` instance with `trust_only_certificates` set to provided certificates.

        Args:
            trust_only_certificates (List[str]): List of PEM-encoded certificate(s) the SDK should trust.

        Returns:
            :class:`~couchbase_operational_insights.common.options.SecurityOptions`
        """  # noqa: E501
        return cls(trust_only_capella=False, trust_only_certificates=certificates)


class TimeoutOptions(TimeoutOptionsBase):
    """Available timeout options to set when creating a cluster.

    These options set the default timeouts for operations for the cluster.  Some operations allow the timeout to be overridden on a per operation basis.
    All options are optional and default to `None`.

    .. note::
        Options marked **VOLATILE** are subject to change at any time.

    Args:
        connect_timeout (Optional[timedelta]): Set to configure the period of time allowed to make a connection. Defaults to `None` (10s).
        handle_request_timeout (Optional[timedelta]): Set to configure the period of time allowed for HTTP requests when using the server asynchronous requests API. Defaults to `None` (10s).
        query_timeout (Optional[timedelta]): Set to configure the period of time allowed for query operations. Defaults to `None` (10m).
    """  # noqa: E501


class QueryOptions(QueryOptionsBase):
    """Available options for Operational Insights query operation.

    Timeout will default to cluster setting if not set for the operation.

    .. note::
        Options marked **VOLATILE** are subject to change at any time.

    Args:
        client_context_id (Optional[str]): Set to configure a unique identifier for this query request.  Defaults to `None` (autogenerated by client).
        deserializer (Optional[Deserializer]): Specifies a :class:`~couchbase_operational_insights.deserializer.Deserializer` to apply to results.  Defaults to `None` (:class:`~couchbase_operational_insights.deserializer.DefaultJsonDeserializer`).
        lazy_execute (Optional[bool]): **VOLATILE** If enabled, the query will not execute until the application begins to iterate over results.  Defaults to `None` (disabled).
        max_retries (Optional[int]): **VOLATILE** Set to configure the maximum number of retries for a request.
        named_parameters (Optional[Dict[str, :py:type:`~couchbase_operational_insights.JSONType`]]): Values to use for named placeholders in query.
        positional_parameters (Optional[List[:py:type:`~couchbase_operational_insights.JSONType`]]): Values to use for positional placeholders in query.
        query_context (Optional[str]): Specifies the context within which this query should be executed.
        raw (Optional[Dict[str, Any]]): Specifies any additional parameters which should be passed to the Operational Insights engine when executing the query.
        readonly (Optional[bool]): Specifies that this query should be executed in read-only mode, disabling the ability for the query to make any changes to the data.
        scan_consistency (Optional[QueryScanConsistency]): Specifies the consistency requirements when executing the query.
        timeout (Optional[timedelta]): Set to configure allowed time for operation to complete. Defaults to `None` (the cluster's ``query_timeout``, 10m).
        stream_config (Optional[JsonStreamConfig]): **VOLATILE** Configuration for JSON stream processing. Defaults to `None` (default configuration).  See :class:`~couchbase_operational_insights.common.json_parsing.JsonStreamConfig` for details.
    """  # noqa: E501


class StartQueryOptions(StartQueryOptionsBase):
    """Available options for Operational Insights asynchronous server query operation.

    Timeout will default to cluster setting if not set for the operation.

    .. note::
        Options marked **VOLATILE** are subject to change at any time.

    Args:
        client_context_id (Optional[str]): Set to configure a unique identifier for this query request.  Defaults to `None` (autogenerated by client).
        max_retries (Optional[int]): **VOLATILE** Set to configure the maximum number of retries for a request.
        named_parameters (Optional[Dict[str, :py:type:`~couchbase_operational_insights.JSONType`]]): Values to use for named placeholders in query.
        positional_parameters (Optional[List[:py:type:`~couchbase_operational_insights.JSONType`]]): Values to use for positional placeholders in query.
        query_context (Optional[str]): Specifies the context within which this query should be executed.
        raw (Optional[Dict[str, Any]]): Specifies any additional parameters which should be passed to the Operational Insights engine when executing the query.
        readonly (Optional[bool]): Specifies that this query should be executed in read-only mode, disabling the ability for the query to make any changes to the data.
        scan_consistency (Optional[QueryScanConsistency]): Specifies the consistency requirements when executing the query.
        timeout (Optional[timedelta]): Set to configure allowed time for the query to run on the server. The request that starts the query is bounded by the cluster's ``handle_request_timeout`` instead. Defaults to `None` (the cluster's ``query_timeout``, 10m).
        stream_config (Optional[JsonStreamConfig]): **VOLATILE** Configuration for JSON stream processing. Defaults to `None` (default configuration).  See :class:`~couchbase_operational_insights.common.json_parsing.JsonStreamConfig` for details.
    """  # noqa: E501


OptionsClass: TypeAlias = Union[
    ClusterOptions,
    FetchResultsOptions,
    SecurityOptions,
    TimeoutOptions,
    QueryOptions,
    StartQueryOptions,
]
