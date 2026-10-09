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


import logging
import time
import typing

from httpcore2 import ConnectionPool, Origin, Request, Response
from httpcore2._backends.base import NetworkStream
from httpcore2._exceptions import ConnectError, ConnectionNotAvailable, ConnectTimeout, PoolTimeout, UnsupportedProtocol
from httpcore2._ssl import default_ssl_context
from httpcore2._sync.connection import RETRIES_BACKOFF_FACTOR, HTTPConnection, exponential_backoff, logger
from httpcore2._sync.connection_pool import PoolByteStream, PoolRequest
from httpcore2._sync.interfaces import ConnectionInterface
from httpcore2._trace import Trace
from httpx2 import HTTPTransport, Limits, create_ssl_context

from tests import TEST_LOGGER_NAME

cb_logger = logging.getLogger(TEST_LOGGER_NAME)


class TestHTTPConnection(HTTPConnection):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)

    def _connect(self, request: Request) -> NetworkStream:
        timeouts = request.extensions.get('timeout', {})
        sni_hostname = request.extensions.get('sni_hostname', None)
        timeout = timeouts.get('connect', None)
        # -- START PYCBOI TESTING --
        test_connect_timeout = timeouts.get('test_connect_timeout', None)
        cb_logger.debug(f'PYCBOI OVERRIDE: connect timeout: {timeout}, test_connect_timeout: {test_connect_timeout}')
        # -- END PYCBOI TESTING --

        retries_left = self._retries
        delays = exponential_backoff(factor=RETRIES_BACKOFF_FACTOR)

        # -- START PYCBOI TESTING --
        deadline = time.monotonic() + timeout
        # -- END PYCBOI TESTING --
        while True:
            try:
                if self._uds is None:
                    kwargs = {
                        'host': self._origin.host.decode('ascii'),
                        'port': self._origin.port,
                        'local_address': self._local_address,
                        'timeout': timeout,
                        'socket_options': self._socket_options,
                    }
                    with Trace('connect_tcp', logger, request, kwargs) as trace:
                        # -- START PYCBOI TESTING --
                        if test_connect_timeout is not None:
                            time.sleep(test_connect_timeout)
                        current_time = time.monotonic()
                        if current_time > deadline:
                            raise ConnectTimeout(f'Connection timed out after {timeout} seconds')
                        # -- END PYCBOI TESTING --
                        stream = self._network_backend.connect_tcp(**kwargs)
                        trace.return_value = stream
                else:
                    kwargs = {
                        'path': self._uds,
                        'timeout': timeout,
                        'socket_options': self._socket_options,
                    }
                    with Trace('connect_unix_socket', logger, request, kwargs) as trace:
                        stream = self._network_backend.connect_unix_socket(**kwargs)
                        trace.return_value = stream

                if self._origin.scheme in (b'https', b'wss'):
                    ssl_context = default_ssl_context() if self._ssl_context is None else self._ssl_context
                    alpn_protocols = ['http/1.1', 'h2'] if self._http2 else ['http/1.1']
                    ssl_context.set_alpn_protocols(alpn_protocols)

                    kwargs = {
                        'ssl_context': ssl_context,
                        'server_hostname': sni_hostname or self._origin.host.decode('ascii'),
                        'timeout': timeout,
                    }
                    with Trace('start_tls', logger, request, kwargs) as trace:
                        stream = stream.start_tls(**kwargs)
                        trace.return_value = stream
                return stream
            except (ConnectError, ConnectTimeout):
                if retries_left <= 0:
                    raise
                retries_left -= 1
                delay = next(delays)
                with Trace('retry', logger, request, kwargs) as trace:
                    self._network_backend.sleep(delay)


class TestConnectionPool(ConnectionPool):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)

    def create_connection(self, origin: Origin) -> ConnectionInterface:
        if self._proxy is not None:
            if self._proxy.url.scheme in (b'socks5', b'socks5h'):
                from httpcore2._sync.socks_proxy import Socks5Connection

                return Socks5Connection(
                    proxy_origin=self._proxy.url.origin,
                    proxy_auth=self._proxy.auth,
                    remote_origin=origin,
                    ssl_context=self._ssl_context,
                    keepalive_expiry=self._keepalive_expiry,
                    http1=self._http1,
                    http2=self._http2,
                    network_backend=self._network_backend,
                )
            elif origin.scheme == b'http':
                from httpcore2._sync.http_proxy import ForwardHTTPConnection

                return ForwardHTTPConnection(
                    proxy_origin=self._proxy.url.origin,
                    proxy_headers=self._proxy.headers,
                    proxy_ssl_context=self._proxy.ssl_context,
                    remote_origin=origin,
                    keepalive_expiry=self._keepalive_expiry,
                    network_backend=self._network_backend,
                )
            from httpcore2._sync.http_proxy import TunnelHTTPConnection

            return TunnelHTTPConnection(
                proxy_origin=self._proxy.url.origin,
                proxy_headers=self._proxy.headers,
                proxy_ssl_context=self._proxy.ssl_context,
                remote_origin=origin,
                ssl_context=self._ssl_context,
                keepalive_expiry=self._keepalive_expiry,
                http1=self._http1,
                http2=self._http2,
                network_backend=self._network_backend,
            )

        # TESTING_OVERRIDE
        return TestHTTPConnection(
            origin=origin,
            ssl_context=self._ssl_context,
            keepalive_expiry=self._keepalive_expiry,
            http1=self._http1,
            http2=self._http2,
            retries=self._retries,
            local_address=self._local_address,
            uds=self._uds,
            network_backend=self._network_backend,
            socket_options=self._socket_options,
        )

    def handle_request(self, request: Request) -> Response:
        """
        Send an HTTP request, and return an HTTP response.

        This is the core implementation that is called into by `.request()` or `.stream()`.
        """
        scheme = request.url.scheme.decode()
        if scheme == '':
            raise UnsupportedProtocol("Request URL is missing an 'http://' or 'https://' protocol.")
        if scheme not in ('http', 'https', 'ws', 'wss'):
            raise UnsupportedProtocol(f"Request URL has an unsupported protocol '{scheme}://'.")

        timeouts = request.extensions.get('timeout', {})
        timeout = timeouts.get('pool', None)
        # -- START PYCBOI TESTING --
        test_pool_timeout = timeouts.get('test_pool_timeout', None)
        cb_logger.debug(f'PYCBOI OVERRIDE: pool timeout: {timeout}, test_pool_timeout: {test_pool_timeout}')
        # -- END PYCBOI TESTING --

        with self._optional_thread_lock:
            # Add the incoming request to our request queue.
            pool_request = PoolRequest(request)
            self._requests.append(pool_request)

        # PYCBOI Addition: track the deadline
        deadline = time.monotonic() + timeout
        try:
            while True:
                with self._optional_thread_lock:
                    # Assign incoming requests to available connections,
                    # closing or creating new connections as required.
                    closing = self._assign_requests_to_connections()
                self._close_connections(closing)

                # -- START PYCBOI TESTING --
                if test_pool_timeout is not None:
                    time.sleep(test_pool_timeout)
                current_time = time.monotonic()
                if current_time > deadline:
                    raise PoolTimeout(f'Connection timed out after {timeout} seconds')
                # -- END PYCBOI TESTING --
                # Wait until this request has an assigned connection.
                connection = pool_request.wait_for_connection(timeout=timeout)
                # PYCBOI Addition: We _always_ set the request timeouts, so no need to validate keys
                connect_timeout = round(deadline - time.monotonic(), 6)  # round to microseconds
                pool_request.request.extensions['timeout']['connect'] = connect_timeout

                try:
                    # Send the request on the assigned connection.
                    response = connection.handle_request(pool_request.request)
                except ConnectionNotAvailable:
                    # In some cases a connection may initially be available to
                    # handle a request, but then become unavailable.
                    #
                    # In this case we clear the connection and try again.
                    pool_request.clear_connection()
                    # PYCBOI Addition: We update the timeout for the next attempt
                    timeout = round(deadline - time.monotonic(), 6)  # round to microseconds
                else:
                    break  # pragma: nocover

        except BaseException as exc:
            with self._optional_thread_lock:
                # For any exception or cancellation we remove the request from
                # the queue, and then re-assign requests to connections.
                self._requests.remove(pool_request)
                closing = self._assign_requests_to_connections()

            self._close_connections(closing)
            raise exc from None

        # Return the response. Note that in this case we still have to manage
        # the point at which the response is closed.
        assert isinstance(response.stream, typing.Iterable)
        return Response(
            status=response.status,
            headers=response.headers,
            content=PoolByteStream(stream=response.stream, pool_request=pool_request, pool=self),
            extensions=response.extensions,
        )


def http_transport_init_override(self, *args, **kwargs) -> None:  # type: ignore
    verify = kwargs.get('verify')
    cert = kwargs.get('cert')
    trust_env = kwargs.get('trust_env')
    ssl_context = create_ssl_context(verify=verify, cert=cert, trust_env=trust_env)  # type: ignore

    # See https://github.com/pydantic/httpx2/blob/main/src/httpx2/httpx2/_config.py for defaults
    # default keepalive_expiry is 5 seconds
    limits = kwargs.get('limits', Limits(max_connections=100, max_keepalive_connections=20))
    http1 = kwargs.get('http1')
    http2 = kwargs.get('http2')
    uds = kwargs.get('uds')
    local_address = kwargs.get('local_address')
    retries = kwargs.get('retries', 0)
    socket_options = kwargs.get('socket_options')
    self._pool = TestConnectionPool(
        ssl_context=ssl_context,
        max_connections=limits.max_connections,
        max_keepalive_connections=limits.max_keepalive_connections,
        keepalive_expiry=limits.keepalive_expiry,
        http1=http1,
        http2=http2,
        uds=uds,
        local_address=local_address,
        retries=retries,
        socket_options=socket_options,
    )


HTTPTransport.__init__ = http_transport_init_override  # type: ignore
HTTPTransport.PYCBOI_TESTING = True

TestHTTPTransport = HTTPTransport

__all__ = ['TestHTTPTransport']
