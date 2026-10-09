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

from typing import Any, Optional

from httpx2 import Response as HttpCoreResponse

from couchbase_operational_insights.common._core.query import build_query_metadata
from couchbase_operational_insights.common.errors import InternalSDKError, OperationalInsightsError
from couchbase_operational_insights.common.logging import LogLevel
from couchbase_operational_insights.common.query import QueryMetadata
from couchbase_operational_insights.protocol._core.request_context import RequestContext
from couchbase_operational_insights.protocol._core.retries import RetryHandler
from couchbase_operational_insights.protocol.errors import WrappedError


class HttpResponse:
    def __init__(
        self,
        request_context: RequestContext,
        has_no_body_response: Optional[bool] = None,
        request_id: Optional[str] = None,
    ) -> None:
        self._request_context = request_context
        self._metadata: Optional[QueryMetadata] = None
        self._core_response: HttpCoreResponse
        self._json_response: Optional[Any] = None
        self._has_no_body_response = has_no_body_response
        self._request_id = request_id

    @property
    def json_response(self) -> Optional[Any]:
        """
        **INTERNAL**
        """
        return self._json_response

    def close(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_core_response'):
            self._core_response.close()
            self._request_context.log_message('HTTP core response closed', LogLevel.INFO)
            del self._core_response

    def get_metadata(self) -> QueryMetadata:
        """
        **INTERNAL**
        """
        if self._metadata is None:
            raise RuntimeError('Query metadata is only available after response has been processed.')
        return self._metadata

    def set_metadata(self, json_data: Optional[Any] = None, raw_metadata: Optional[bytes] = None) -> None:
        """
        **INTERNAL**
        """
        try:
            self._metadata = QueryMetadata(
                build_query_metadata(
                    json_data=json_data,
                    raw_metadata=raw_metadata,
                    request_id=self._request_id,
                    log_fn=self._request_context.log_message,
                )
            )
            self._request_context.shutdown()
        except (OperationalInsightsError, ValueError) as err:
            self._request_context.shutdown(err)
            raise err
        except Exception as ex:
            internal_err = InternalSDKError(cause=ex, message=str(ex), context=str(self._request_context.error_context))
            self._request_context.shutdown(internal_err)
        finally:
            self.close()

    @RetryHandler.with_retries
    def send_request(self) -> None:
        """
        **INTERNAL**
        """
        self._request_context.initialize()
        self._core_response = self._request_context.send_request()
        if self._has_no_body_response is True:
            self._process_no_body_response()
            return
        self._process_response()

    def _process_no_body_response(self) -> None:
        status_code = self._core_response.status_code
        self.close()
        if 200 <= status_code < 300 or status_code == 404:
            self._request_context.shutdown()
            return
        self._request_context.check_for_http_status_error(status_code, ignore_not_found_status=True)
        ctx = str(self._request_context.error_context)
        raise WrappedError(OperationalInsightsError(context=ctx, message=f'Request failed with status {status_code}.'))

    def _process_response(self) -> None:
        self._json_response = self._request_context.process_response(
            self._core_response,
            self.close,
            handle_context_shutdown=True,
            ignore_not_found_status=self._has_no_body_response,
        )
        self.set_metadata(json_data=self._json_response)
