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


import atexit
import logging
import pathlib
import subprocess
import sys
import time
from os import path
from typing import Optional

WEB_SERVER_PATH = path.join(pathlib.Path(__file__).parent.parent, 'test_server', 'web_server.py')

logging.basicConfig(
    level=logging.INFO, stream=sys.stderr, format='%(asctime)s - %(levelname)s - (PID:%(process)d) - %(message)s'
)
logger = logging.getLogger(__name__)


class WebServerHandler:
    def __init__(self, host: Optional[str] = '0.0.0.0', port: Optional[int] = 8080) -> None:
        self._host = host or '0.0.0.0'
        self._port = port or 8080
        self._server_process: Optional[subprocess.Popen[bytes]] = None
        atexit.register(self.stop_server)

    @property
    def connstr(self) -> str:
        # the server only listens on IPv4, and on Windows localhost tries ::1 first, which takes ~1s to fail
        host = self._host if self._host != '0.0.0.0' else '127.0.0.1'
        return f'http://{host}:{self._port}'

    def start_server(self) -> None:
        if self._server_process and self._server_process.poll() is None:
            logger.info(f'Web server is already running (PID: {self._server_process.pid}).')
            return

        if not path.exists(WEB_SERVER_PATH):
            msg = f'Web server script not found at {WEB_SERVER_PATH}.'
            logger.error(msg)
            raise FileNotFoundError(msg)

        try:
            cmd = [sys.executable, WEB_SERVER_PATH, '--host', self._host, '--port', str(self._port)]
            self._server_process = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
            time.sleep(1)

            # Check if the server process unexpectedly exited during startup
            if self._server_process.poll() is not None:
                logger.error(
                    (
                        f'Server process (PID: {self._server_process.pid}) exited immediately after launch. '
                        f'Exit code: {self._server_process.returncode}.'
                    )
                )
                self._server_process = None
            else:
                logger.info('Server should be running at http://%s:%d/', self._host, self._port)
        except Exception as e:
            logger.error(f'Failed to start web server: {e}', exc_info=True)
            self._server_process = None
            raise

    def stop_server(self) -> None:
        if self._server_process is None:
            self._server_process = None
            return

        if self._server_process.poll() is not None:
            self._server_process = None
            return

        try:
            self._server_process.terminate()
            try:
                self._server_process.wait(timeout=5)
                logger.info(f'Web server stopped (PID: {self._server_process.pid}).')
            except subprocess.TimeoutExpired:
                logger.warning(f'Web server (PID: {self._server_process.pid}) did not terminate in time, killing it.')
                self._server_process.kill()
                self._server_process.wait()
        except Exception as e:
            logger.error(f'Error stopping web server: {e}', exc_info=True)
            raise
        finally:
            self._server_process = None
