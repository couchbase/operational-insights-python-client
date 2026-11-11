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

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, List, Mapping, Optional, Tuple, TypedDict, Union

from couchbase_operational_insights.common._core.result import QueryResult
from couchbase_operational_insights.common.errors import OperationalInsightsError


class QueryHandle(ABC):
    @abstractmethod
    def cancel(self) -> Union[Awaitable[None], None]:
        """
        Cancel the query associated with the QueryHandle.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_status(self) -> Union[Awaitable[QueryStatus], QueryStatus]:
        raise NotImplementedError


class QueryResultHandle(ABC):
    """Abstract base class for query result handle."""

    @abstractmethod
    def fetch_results(self) -> Union[Awaitable[QueryResult], QueryResult]:
        """
        Get all the results.
        """
        raise NotImplementedError

    @abstractmethod
    def discard_results(self) -> Union[Awaitable[None], None]:
        """
        Discard the query results associated with the QueryResultHandle.
        """
        raise NotImplementedError


class QueryStatus(ABC):
    @abstractmethod
    def results_ready(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def result_handle(self) -> QueryResultHandle:
        raise NotImplementedError


class ResultPartition(TypedDict):
    handle: str
    result_count: Optional[int]


@dataclass
class QueryHandleStatusResponse:
    """**INTERNAL**"""

    request_id: str
    status: str
    handle: Optional[str] = None
    result_count: Optional[int] = None
    partitions: Optional[List[ResultPartition]] = None
    result_set_ordered: Optional[bool] = None
    metrics: Optional[Mapping[str, Union[str, int]]] = None
    created_at: Optional[str] = None

    def get_details(self) -> Mapping[str, Any]:
        """**INTERNAL**"""
        return {
            'request_id': self.request_id,
            'status': self.status,
            'handle': self.handle,
            'metrics': self.metrics,
        }

    @classmethod
    def from_server(cls, request_id: str, raw_json: Any) -> QueryHandleStatusResponse:
        raw_partitions = raw_json.get('partitions', [])
        partitions: list[ResultPartition] = []
        for partition in raw_partitions:
            partitions.append(
                {'handle': partition.get('handle', None), 'result_count': partition.get('resultCount', None)}
            )
        return cls(
            request_id,
            raw_json.get('status', None),
            raw_json.get('handle', None),
            result_count=raw_json.get('resultCount', None),
            partitions=partitions,
            result_set_ordered=raw_json.get('resultSetOrdered', None),
            metrics=raw_json.get('metrics', None),
            created_at=raw_json.get('createdAt', None),
        )


def get_query_handle_fields(json_response: Optional[Any]) -> Tuple[str, str]:
    """
    **INTERNAL**

    Returns the ``requestID`` and ``handle`` from a start query response.
    """
    if json_response is None:
        raise OperationalInsightsError(message='HTTP response does not contain JSON data.')

    request_id = json_response.get('requestID', None)
    handle = json_response.get('handle', None)
    required_fields_missing = []
    if request_id is None:
        required_fields_missing.append('requestID')

    if handle is None:
        required_fields_missing.append('handle')

    if len(required_fields_missing) > 0:
        msg = f'Server response is missing required field(s): {", ".join(required_fields_missing)}.'
        raise OperationalInsightsError(message=msg)

    return request_id, handle


def serialize_query_handle(request_id: str, handle: str) -> str:
    """
    **INTERNAL**

    The property names match the start query response and must not change, so that later SDK versions (and other
    SDKs) can deserialize the handle.
    """
    return json.dumps({'requestID': request_id, 'handle': handle})


def deserialize_query_handle(serialized: str) -> Tuple[str, str]:
    """
    **INTERNAL**

    Returns the ``requestID`` and ``handle`` from a serialized query handle. Other properties are ignored.
    """
    try:
        parsed = json.loads(serialized)
    except (TypeError, ValueError):
        raise ValueError('The serialized query handle is not valid JSON.') from None

    if not isinstance(parsed, dict):
        raise ValueError('The serialized query handle must be a JSON object.')

    request_id = parsed.get('requestID', None)
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('The serialized query handle must have a non-empty string requestID.')
    handle = parsed.get('handle', None)
    if not isinstance(handle, str) or not handle:
        raise ValueError('The serialized query handle must have a non-empty string handle.')

    return request_id, handle
