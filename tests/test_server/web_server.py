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


# ruff: noqa: E402

from __future__ import annotations

import asyncio
import json
import logging
import pathlib
import sys
from time import perf_counter
from typing import Optional, Union
from uuid import uuid4

from aiohttp import web

CLIENT_ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.append(str(CLIENT_ROOT))

from tests.test_server import ErrorType
from tests.test_server.request import (
    ServerErrorRequest,
    ServerHttp503Request,
    ServerResultsRequest,
    ServerStreamedResultsRequest,
    ServerTimeoutRequest,
)
from tests.test_server.response import ServerResponse, ServerResponseError, ServerResponseResults
from tests.utils import AsyncBytesIterator, AsyncInfiniteBytesIterator

logging.basicConfig(
    level=logging.INFO, stream=sys.stderr, format='%(asctime)s - %(levelname)s - (PID:%(process)d) - %(message)s'
)
logger = logging.getLogger(__name__)


class AsyncWebServer:
    def __init__(self, host: Optional[str] = '0.0.0.0', port: Optional[int] = 8080) -> None:
        self._app = web.Application()
        self._host = host
        self._port = port
        self._app.add_routes(
            [
                web.post('/test_error', self.handle_error_request),
                web.post('/test_results', self.handle_results_request),
                web.post('/test_slow_results', self.handle_slow_results_request),
                web.post('/test_streamed_results', self.handle_streamed_results_request),
                web.get('/test_fetch_results', self.handle_streamed_results_request),
            ]
        )

    async def _handle_timeout_error_request(self, request: ServerTimeoutRequest) -> web.Response:
        timeout = request.timeout
        start = perf_counter()
        await asyncio.sleep(timeout)
        end = perf_counter()
        elapsed = end - start
        request_id = str(uuid4())
        resp = ServerResponse.create()
        if request.server_side:
            ServerResponseError.build_errors(resp, ErrorType.Timeout)
            resp.update_elapsed_time(elapsed)
            return web.json_response(resp.to_json_repr())

        return web.json_response(
            {
                'requestID': request_id,
                'status': 'timeout',
                'elapsedTime': f'{elapsed}s',
                'message': f'Request timed out after {timeout} seconds.',
            }
        )

    def _handle_auth_error_request(self, error_type: ErrorType) -> web.Response:
        start = perf_counter()
        resp = ServerResponse.create()
        ServerResponseError.build_errors(resp, error_type)
        end = perf_counter()
        elapsed = end - start
        resp.update_elapsed_time(elapsed)
        return web.json_response(resp.to_json_repr())

    def _handle_http503_error_request(self, request: ServerHttp503Request) -> web.Response:
        if request.operational_insights_error is False:
            return web.Response(status=503, text='Service Unavailable')
        start = perf_counter()
        resp = ServerResponse.create()
        ServerResponseError.build_errors(resp, request.error_type)
        end = perf_counter()
        elapsed = end - start
        resp.update_elapsed_time(elapsed)
        return web.json_response(resp.to_json_repr(), status=resp.http_status)

    async def _handle_retry_error_request(self, request: ServerErrorRequest) -> web.Response:
        start = perf_counter()
        resp = ServerResponse.create()
        ServerResponseError.build_errors(
            resp,
            request.error_type,
            group_type=request.retry_group_type,
            retry_specification=request.non_retriable_spec,
            err_count=request.error_count,
        )
        end = perf_counter()
        elapsed = end - start
        resp.update_elapsed_time(elapsed)
        return web.json_response(resp.to_json_repr())

    async def _handle_timed_streaming_request(
        self, request: ServerResultsRequest, web_request: web.Request
    ) -> web.StreamResponse:
        if request.until is None:
            raise ValueError('Missing "until" in JSON data.')
        resp = ServerResponse.create()
        start = perf_counter()
        response = web.StreamResponse(
            status=200,
            headers={
                'Content-Type': 'application/json',
                'Transfer-Encoding': 'chunked',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
            },
        )
        await response.prepare(web_request)
        now = asyncio.get_running_loop().time()
        deadline = now + request.until
        chunk_size = request.chunk_size or 100
        bytes_generator = ServerResponseResults.get_result_generator(request.result_type)
        initial_data = json.dumps({'requestID': resp.request_id, 'status': resp.status}).encode('utf-8')
        async_inf_iterator = AsyncInfiniteBytesIterator(
            bytes_generator(), initial_data=initial_data, chunk_size=chunk_size
        )
        num_bytes_sent = 0
        while deadline > now:
            chunk = await async_inf_iterator.__anext__()
            num_bytes_sent += len(chunk)
            logger.info(f'Writing chunk of size {len(chunk)}; {chunk=}; {num_bytes_sent=}')
            await response.write(chunk)
            now = asyncio.get_running_loop().time()
        end = perf_counter()
        elapsed = end - start
        resp.update_elapsed_time(elapsed)
        metrics = resp.metrics
        metrics.result_count = async_inf_iterator.get_data_count()
        meta = json.dumps({'metrics': metrics.to_json_repr()}).encode('utf-8')
        async_inf_iterator.stop_iterating(end_data=meta)
        async for chunk in async_inf_iterator:
            num_bytes_sent += len(chunk)
            logger.info(f'Writing chunk of size {len(chunk)}; {chunk=}; {num_bytes_sent=}')
            await response.write(chunk)
        logger.info(f'Writing EOF; {num_bytes_sent=}')
        await response.write_eof()
        logger.info('returning response')
        return response

    async def _handle_count_streaming_request(
        self, request: ServerResultsRequest, web_request: web.Request
    ) -> web.StreamResponse:
        if request.row_count is None:
            raise ValueError('Missing "row_count" in JSON data.')

        response = web.StreamResponse(
            status=200,
            headers={
                'Content-Type': 'application/json',
                'Transfer-Encoding': 'chunked',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
            },
        )
        await response.prepare(web_request)
        resp = ServerResponse.create()
        start = perf_counter()
        ServerResponseResults.build_results(resp, request.row_count, request.result_type)
        end = perf_counter()
        elapsed = end - start
        resp.update_elapsed_time(elapsed)

        chunk_size = request.chunk_size or 100
        async_iterator = AsyncBytesIterator(bytes(json.dumps(resp.to_json_repr()), 'utf-8'), chunk_size=chunk_size)
        async for chunk in async_iterator:
            logger.info(f'Writing chunk of size {len(chunk)}; {chunk=}')
            await response.write(chunk)
        logger.info('Writing EOF')
        await response.write_eof()
        logger.info('returning response')
        return response

    def _handle_basic_results_request(
        self, request: ServerResultsRequest, web_request: web.Request
    ) -> Union[web.Response, web.StreamResponse]:
        if request.row_count is None:
            raise ValueError('Missing "row_count" in JSON data.')
        resp = ServerResponse.create()
        start = perf_counter()
        ServerResponseResults.build_results(resp, request.row_count, request.result_type)
        end = perf_counter()
        elapsed = end - start
        resp.update_elapsed_time(elapsed)
        res = resp.to_json_repr()
        return web.json_response(res)

    async def handle_error_request(self, request: web.Request) -> web.Response:
        try:
            received_json = await request.json()
            if 'error_type' not in received_json:
                raise ValueError('Missing "error_type" in JSON data.')

            error_req = ServerErrorRequest.from_json(received_json)
            if error_req.error_type == ErrorType.Timeout:
                timeout_req = ServerTimeoutRequest.from_json(received_json)
                return await self._handle_timeout_error_request(timeout_req)
            elif error_req.error_type in [ErrorType.InsufficientPermissions, ErrorType.Unauthorized]:
                return self._handle_auth_error_request(error_req.error_type)
            elif error_req.error_type == ErrorType.Retriable:
                return await self._handle_retry_error_request(error_req)
            elif error_req.error_type == ErrorType.Http503:
                http503_req = ServerHttp503Request.from_json(received_json)
                return self._handle_http503_error_request(http503_req)
            logger.info(f'Received JSON: {received_json}')
            return web.json_response({'status': 'success', 'data': received_json})
        except json.JSONDecodeError:
            received_text = await request.text()
            msg = 'POST request received, but data is not valid JSON. Showing as plain text.'
            logger.error(msg)
            logger.error(f'Received text: {received_text}')
            return web.Response(status=400, text='Bad Request')
        except Exception as e:
            logger.error(f'An error occurred: {e}', exc_info=True)
            return web.Response(status=400, text='Bad Request')

    async def handle_results_request(self, request: web.Request) -> Union[web.Response, web.StreamResponse]:
        try:
            received_json = await request.json()
            result_req = ServerResultsRequest.from_json(received_json)
            if result_req.stream:
                if result_req.until is not None:
                    return await self._handle_timed_streaming_request(result_req, request)
                return await self._handle_count_streaming_request(result_req, request)
            return self._handle_basic_results_request(result_req, request)
        except json.JSONDecodeError:
            received_text = await request.text()
            msg = 'POST request received, but data is not valid JSON. Showing as plain text.'
            logger.error(msg)
            logger.error(f'Received text: {received_text}')
            return web.Response(status=400, text='Bad Request')
        except Exception as e:
            logger.error(f'An error occurred: {e}', exc_info=True)
            return web.Response(status=400, text='Bad Request')

    async def handle_streamed_results_request(self, request: web.Request) -> web.StreamResponse:
        """Streams a results response row by row, with optional delays.

        The delays let tests hold back the start of the response, slow the rows down, or stall partway through the
        stream.  Each row is written as its own chunk, so the client can parse the rows sent before a stall.
        """
        try:
            streamed_req = ServerStreamedResultsRequest.from_json(await request.json())
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f'Invalid streamed results request: {e}')
            return web.Response(status=400, text='Bad Request')

        if streamed_req.header_delay > 0:
            await asyncio.sleep(streamed_req.header_delay)
        response = web.StreamResponse(
            status=streamed_req.status,
            headers={'Content-Type': 'application/json', 'Transfer-Encoding': 'chunked', 'Cache-Control': 'no-cache'},
        )
        await response.prepare(request)
        resp = ServerResponse.create()
        start = perf_counter()
        await response.write(json.dumps({'requestID': resp.request_id})[:-1].encode('utf-8') + b', "results": [')
        for idx in range(streamed_req.row_count):
            if streamed_req.stall_after is not None and idx == streamed_req.stall_after and streamed_req.stall > 0:
                logger.info(f'Stalling the streamed results for {streamed_req.stall}s after {idx} rows')
                await asyncio.sleep(streamed_req.stall)
            if streamed_req.row_delay > 0:
                await asyncio.sleep(streamed_req.row_delay)
            row = json.dumps({'id': idx + 1, 'name': f'name-{idx + 1}'}).encode('utf-8')
            await response.write(row if idx == 0 else b',' + row)
        resp.update_elapsed_time(perf_counter() - start)
        resp.metrics.result_count = streamed_req.row_count
        meta = json.dumps({'status': resp.status, 'metrics': resp.metrics.to_json_repr()})
        await response.write(b'], ' + meta[1:].encode('utf-8'))
        await response.write_eof()
        return response

    async def handle_slow_results_request(self, request: web.Request) -> web.StreamResponse:
        try:
            received_json = await request.json()
            if 'request_type' not in received_json:
                raise ValueError('Missing "request_type" in JSON data.')

            logger.info(f'Received JSON: {received_json}')
            return web.json_response({'status': 'success', 'data': received_json})
        except json.JSONDecodeError:
            received_text = await request.text()
            msg = 'POST request received, but data is not valid JSON. Showing as plain text.'
            logger.error(msg)
            logger.error(f'Received text: {received_text}')
            return web.Response(status=400, text='Bad Request')
        except Exception as e:
            logger.error(f'An error occurred: {e}', exc_info=True)
            return web.Response(status=400, text='Bad Request')

    async def start(self) -> None:
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, self._host, self._port)
        await site.start()
        logger.info(f'Server running on http://{self._host}:{self._port}')

    async def stop(self) -> None:
        await self._app.shutdown()
        await self._app.cleanup()


async def run_server(host: str, port: int) -> None:
    server = AsyncWebServer(host=host, port=port)
    logger.info(f'Attempting to start server on {host}:{port}...')
    await server.start()
    logger.info('Server started. Listening for requests...')
    try:
        while True:
            await asyncio.sleep(300)
    except asyncio.CancelledError:
        logger.info('asyncio task cancelled (e.g., from SIGTERM). Shutting down.')
    except Exception as e:
        logger.error(f'Unexpected error: {e}', exc_info=True)
    finally:
        logger.info('Stopping server...')
        await server.stop()
        logger.info('Server stopped.')


if __name__ == '__main__':
    from argparse import ArgumentParser

    ap = ArgumentParser(description='Run Async Web Server')
    ap.add_argument(
        '--host', type=str, default='127.0.0.1', help='Host address to bind to (e.g., 127.0.0.1 for localhost only)'
    )
    ap.add_argument('--port', type=int, default=8000, help='Port number to listen on')
    options = ap.parse_args()
    try:
        asyncio.run(run_server(host=options.host, port=options.port))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f'Critical error: {e}', exc_info=True)
