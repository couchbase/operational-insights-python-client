import argparse
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

import grpc
from anyio import create_task_group, open_signal_receiver, run
from anyio.abc import CancelScope
from insights_performer.protocol.columnar import services_pb2_grpc as services_pb_grpc


def get_logging_level_name_mapping() -> dict[str, int]:
    if sys.version_info >= (3, 11):
        return logging.getLevelNamesMapping()
    return logging._nameToLevel.copy()


def setup_logger(level: int) -> None:
    log_format_array = [
        '[%(asctime)s.%(msecs)03d]',
        '%(relativeCreated)dms',
        '[%(levelname)s]',
        '[%(process)d, %(threadName)s (%(thread)d)] %(name)s',
        '- %(message)s',
    ]
    log_format = ' '.join(log_format_array)
    log_date_format = '%Y-%m-%d %H:%M:%S'
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt=log_format, datefmt=log_date_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)


def serve(port: int) -> None:
    from insights_performer.services import InsightsServiceServicer

    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    servicer = InsightsServiceServicer()
    services_pb_grpc.add_ColumnarServiceServicer_to_server(servicer, server)
    services_pb_grpc.add_ColumnarCrossServiceServicer_to_server(servicer.cross_service(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.getLogger().info(f'Server started, listening on {port}, BLOCKING mode')
    signal.signal(signal.SIGINT, lambda sig, frame: server.stop(None))
    server.wait_for_termination()


async def signal_handler(server: grpc.Server, scope: CancelScope):
    with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                logging.getLogger().info('Stopping the server (CTRL+C received).')
            else:
                logging.getLogger().info(f'Stopping the server (signal {signum} received).')

            await server.stop(None)
            scope.cancel()
            return


async def serve_async(port: int) -> None:
    from insights_performer.services_async import AsyncInsightsServiceServicer

    async with create_task_group() as tg:
        server = grpc.aio.server()
        servicer = AsyncInsightsServiceServicer()
        services_pb_grpc.add_ColumnarServiceServicer_to_server(servicer, server)
        services_pb_grpc.add_ColumnarCrossServiceServicer_to_server(servicer.cross_service(), server)
        server.add_insecure_port(f'[::]:{port}')
        await server.start()
        tg.start_soon(signal_handler, server, tg.cancel_scope)
        logging.getLogger().info(f'Server started, listening on {port}, ASYNC mode')
        await server.wait_for_termination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, nargs='?', default=8060)
    parser.add_argument('--mode', '-m', type=str, choices=['BLOCKING', 'ASYNC'])
    parser.add_argument('--log-level', '-l', type=str)
    args = parser.parse_args()

    log_level = args.log_level or os.getenv('LOG_LEVEL', 'INFO')
    setup_logger(get_logging_level_name_mapping()[log_level.upper()])

    if args.mode == 'BLOCKING':
        serve(args.port)
    elif args.mode == 'ASYNC':
        run(serve_async, args.port)
    else:
        mode = os.getenv('SERVER_MODE', None)
        if mode is not None:
            mode = mode.upper()
            logging.getLogger().info(f'Using mode from environment variable: {mode}')
            if mode == 'BLOCKING':
                serve(args.port)
            elif mode == 'ASYNC':
                run(serve_async, args.port)
            else:
                msg = f'Unknown mode: {mode}. Using default mode: BLOCKING.'
                logging.getLogger().warning(msg)
                serve(args.port)
        else:
            logging.getLogger().info('No mode specified, using default mode: BLOCKING')
            serve(args.port)


if __name__ == '__main__':
    main()
