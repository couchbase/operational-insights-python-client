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
from typing import Any, Dict, Optional

from tests.test_server import ErrorType, NonRetriableSpecificationType, ResultType, RetriableGroupType


@dataclass
class ServerErrorRequest:
    error_type: ErrorType
    retry_group_type: Optional[RetriableGroupType] = None
    non_retriable_spec: Optional[NonRetriableSpecificationType] = None
    error_count: Optional[int] = None

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> ServerErrorRequest:
        error_type = json_data.get('error_type', None)
        if error_type is None:
            raise ValueError('Missing "error_type" in JSON data.')
        err_type = ErrorType.from_str(error_type)
        retry_grp = json_data.get('retry_group_type', None)
        rgt = None
        if retry_grp is not None:
            rgt = RetriableGroupType.from_str(retry_grp)
        non_retry_spec = json_data.get('non_retriable_spec', None)
        nrst = None
        if non_retry_spec is not None:
            nrst = NonRetriableSpecificationType.from_str(non_retry_spec)
        return cls(
            error_type=err_type,
            retry_group_type=rgt,
            non_retriable_spec=nrst,
            error_count=json_data.get('error_count', None),
        )


@dataclass
class ServerResultsRequest:
    result_type: ResultType
    row_count: Optional[int] = None
    chunk_size: Optional[int] = None
    stream: Optional[bool] = False
    until: Optional[float] = None

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> ServerResultsRequest:
        until_raw = json_data.get('until', None)
        if until_raw is not None and not isinstance(until_raw, (float, int)):
            raise ValueError(f'Invalid "until" value: {until_raw}. Must be a number.')
        until = float(until_raw) if until_raw is not None else None

        row_count = json_data.get('row_count', None)
        if row_count is None and until is None:
            raise ValueError('Missing "row_count" in JSON data.')
        if until is None and not isinstance(row_count, int):
            raise ValueError(f'Invalid "row_count" value: {row_count}. Must be an integer.')

        rtype = json_data.get('result_type', None)
        result_type = ResultType.from_str(rtype) if rtype is not None else ResultType.Object

        chunk_raw = json_data.get('chunk_size', None)
        if chunk_raw is not None and not isinstance(chunk_raw, int):
            raise ValueError(f'Invalid "chunk_size" value: {chunk_raw}. Must be an integer.')
        chunk_size = int(chunk_raw) if chunk_raw is not None else None

        return cls(
            result_type=result_type,
            row_count=row_count,
            chunk_size=chunk_size,
            stream=json_data.get('stream', False),
            until=until,
        )


@dataclass
class ServerSlowResultsRequest:
    row_count: int
    result_type: Optional[ResultType] = ResultType.Object
    chunk_size: Optional[int] = None
    stream: Optional[bool] = False
    until: Optional[float] = None


@dataclass
class ServerTimeoutRequest:
    error_type: ErrorType
    timeout: float
    server_side: Optional[bool] = False

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> ServerTimeoutRequest:
        timeout = json_data.get('timeout', None)
        if timeout is None:
            raise ValueError('Missing "timeout" in JSON data.')
        if not isinstance(timeout, (int, float)):
            raise ValueError(f'Invalid "timeout" value: {timeout}. Must be a number.')
        return cls(
            error_type=ErrorType.Timeout, timeout=float(timeout), server_side=json_data.get('server_side', False)
        )


@dataclass
class ServerHttp503Request:
    error_type: ErrorType
    operational_insights_error: Optional[bool] = False

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> ServerHttp503Request:
        return cls(
            error_type=ErrorType.Http503, operational_insights_error=json_data.get('operational_insights_error', False)
        )


@dataclass
class ServerStreamedResultsRequest:
    """Instructions for the test streamed results route, which streams rows with optional delays.

    Args:
        row_count: Number of rows to send.
        header_delay: Seconds to wait before sending the response headers.
        row_delay: Seconds to wait before sending each row.
        stall_after: Number of rows after which the server stops sending, for ``stall`` seconds.
        stall: Seconds to stop sending after ``stall_after`` rows.
    """

    row_count: int
    header_delay: float = 0.0
    row_delay: float = 0.0
    stall_after: Optional[int] = None
    stall: float = 0.0

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> ServerStreamedResultsRequest:
        row_count = json_data.get('row_count', None)
        if not isinstance(row_count, int):
            raise ValueError(f'Invalid "row_count" value: {row_count}. Must be an integer.')
        stall_after = json_data.get('stall_after', None)
        if stall_after is not None and not isinstance(stall_after, int):
            raise ValueError(f'Invalid "stall_after" value: {stall_after}. Must be an integer.')
        delays = {}
        for key in ('header_delay', 'row_delay', 'stall'):
            value = json_data.get(key, 0.0)
            if not isinstance(value, (int, float)):
                raise ValueError(f'Invalid "{key}" value: {value}. Must be a number.')
            delays[key] = float(value)
        return cls(row_count=row_count, stall_after=stall_after, **delays)
