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

import io
import logging
import os
import subprocess  # nosec
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List

import ijson
import pytest

import couchbase_operational_insights.common.logging as cb_logging
from couchbase_operational_insights.common.logging import (
    ASYNC_LOGGER_NAME,
    LOG_LEVEL_ENV_VAR,
    SYNC_LOGGER_NAME,
    configure_logging_from_env,
    log_client_version,
)
from couchbase_operational_insights.credential import Credential
from couchbase_operational_insights.protocol._core.client_adapter import _ClientAdapter

SDK_LOGGER_NAMES = (SYNC_LOGGER_NAME, ASYNC_LOGGER_NAME)
TEST_CLUSTER_ID = 'test-cluster'


class _RecordingHandler(logging.Handler):
    """A handler that keeps the records it is given, standing in for a host application's handler."""

    def __init__(self) -> None:
        super().__init__()
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@contextmanager
def pristine_root_logger() -> Iterator[logging.Logger]:
    """Detach pytest's own root handlers for the duration of a test.

    The SDK decides whether to attach a handler of its own by looking at the root logger, so a test
    cannot see that decision while pytest's log capturing is installed there.
    """
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    root.setLevel(logging.WARNING)
    try:
        yield root
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)


class LoggingTestSuite:
    TEST_MANIFEST = [
        'test_banner_json_parser_is_info_with_c_backend',
        'test_banner_logged_once_per_logger',
        'test_banner_not_logged_when_handler_stream_closed',
        'test_banner_not_logged_without_handlers',
        'test_banner_warns_on_pure_python_json_parser',
        'test_configure_is_idempotent',
        'test_env_set_does_not_replace_sdk_logger_handlers',
        'test_env_set_propagates_when_host_configured_logging',
        'test_env_unset_configures_nothing',
        'test_import_leaves_root_logger_alone',
        'test_root_logger_untouched_when_nothing_configured',
        'test_unrecognized_level_defaults_to_info',
    ]

    @pytest.fixture(autouse=True)
    def reset_sdk_logging(self) -> Iterator[None]:
        loggers = [logging.getLogger(name) for name in SDK_LOGGER_NAMES]
        saved = [(lg, lg.handlers[:], lg.level, lg.propagate) for lg in loggers]
        for lg in loggers:
            lg.handlers = []
            lg.setLevel(logging.NOTSET)
            lg.propagate = True

        cb_logging._logging_configured = False
        cb_logging._version_logged = set()
        try:
            yield
        finally:
            for lg, handlers, level, propagate in saved:
                lg.handlers = handlers
                lg.setLevel(level)
                lg.propagate = propagate
            cb_logging._logging_configured = False
            cb_logging._version_logged = set()

    def test_env_unset_configures_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(LOG_LEVEL_ENV_VAR, raising=False)
        with pristine_root_logger() as root:
            configure_logging_from_env()

            assert root.handlers == []
            assert root.level == logging.WARNING
            for name in SDK_LOGGER_NAMES:
                logger = logging.getLogger(name)
                assert logger.handlers == []
                assert logger.level == logging.NOTSET
                assert logger.propagate is True

    def test_root_logger_untouched_when_nothing_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(LOG_LEVEL_ENV_VAR, 'debug')
        with pristine_root_logger() as root:
            configure_logging_from_env()

            # The regression: the SDK used to basicConfig() the root logger, which both attached a
            # handler to it and dropped the host application to DEBUG.
            assert root.handlers == []
            assert root.level == logging.WARNING

            for name in SDK_LOGGER_NAMES:
                logger = logging.getLogger(name)
                assert logger.level == logging.DEBUG
                assert len(logger.handlers) == 1
                assert logger.propagate is False

    def test_env_set_propagates_when_host_configured_logging(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(LOG_LEVEL_ENV_VAR, 'debug')
        host_handler = _RecordingHandler()
        with pristine_root_logger() as root:
            root.addHandler(host_handler)
            root.setLevel(logging.DEBUG)

            configure_logging_from_env()

            assert root.handlers == [host_handler]
            for name in SDK_LOGGER_NAMES:
                logger = logging.getLogger(name)
                assert logger.level == logging.DEBUG
                assert logger.handlers == []
                assert logger.propagate is True

            logging.getLogger(SYNC_LOGGER_NAME).debug('sync message')
            logging.getLogger(ASYNC_LOGGER_NAME).debug('async message')
            assert [r.getMessage() for r in host_handler.records] == ['sync message', 'async message']

    def test_env_set_does_not_replace_sdk_logger_handlers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(LOG_LEVEL_ENV_VAR, 'error')
        host_handler = _RecordingHandler()
        logging.getLogger(SYNC_LOGGER_NAME).addHandler(host_handler)

        with pristine_root_logger():
            configure_logging_from_env()

        sync_logger = logging.getLogger(SYNC_LOGGER_NAME)
        assert sync_logger.handlers == [host_handler]
        assert sync_logger.level == logging.ERROR
        assert sync_logger.propagate is True

    def test_unrecognized_level_defaults_to_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(LOG_LEVEL_ENV_VAR, 'not-a-level')
        host_handler = _RecordingHandler()
        with pristine_root_logger() as root:
            root.addHandler(host_handler)

            configure_logging_from_env()

            assert logging.getLogger(SYNC_LOGGER_NAME).level == logging.INFO
            assert len(host_handler.records) == 1
            record = host_handler.records[0]
            assert record.levelno == logging.WARNING
            assert 'not-a-level' in record.getMessage()

    def test_configure_is_idempotent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(LOG_LEVEL_ENV_VAR, 'debug')
        with pristine_root_logger():
            configure_logging_from_env()
            configure_logging_from_env()

            for name in SDK_LOGGER_NAMES:
                assert len(logging.getLogger(name).handlers) == 1

    def test_banner_logged_once_per_logger(self) -> None:
        handlers = {}
        for name in SDK_LOGGER_NAMES:
            handler = _RecordingHandler()
            handlers[name] = handler
            logger = logging.getLogger(name)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

        for name in SDK_LOGGER_NAMES:
            logger = logging.getLogger(name)
            log_client_version(logger, 'pycboi/1.2.3', '[cluster-id]')
            log_client_version(logger, 'pycboi/1.2.3', '[cluster-id]')

        for name in SDK_LOGGER_NAMES:
            records = handlers[name].records
            assert len(records) == 2
            assert records[0].getMessage() == '[cluster-id] Python Couchbase Operational Insights Client (pycboi/1.2.3)'
            parser_message = f'[cluster-id] JSON parser: ijson {ijson.__version__} ({ijson.backend_name} backend)'
            assert records[1].getMessage().startswith(parser_message)

    def test_banner_warns_on_pure_python_json_parser(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ijson, 'backend_name', 'python')
        handler = _RecordingHandler()
        logger = logging.getLogger(SYNC_LOGGER_NAME)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        log_client_version(logger, 'pycboi/1.2.3')

        assert len(handler.records) == 2
        record = handler.records[1]
        assert record.levelno == logging.WARNING
        assert record.getMessage().startswith(f'JSON parser: ijson {ijson.__version__} (python backend).')
        assert '--no-binary ijson' in record.getMessage()

    def test_banner_json_parser_is_info_with_c_backend(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ijson, 'backend_name', 'yajl2_c')
        handler = _RecordingHandler()
        logger = logging.getLogger(SYNC_LOGGER_NAME)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        log_client_version(logger, 'pycboi/1.2.3')

        assert len(handler.records) == 2
        record = handler.records[1]
        assert record.levelno == logging.INFO
        assert record.getMessage() == f'JSON parser: ijson {ijson.__version__} (yajl2_c backend)'

    def test_banner_not_logged_without_handlers(self) -> None:
        logger = logging.getLogger(SYNC_LOGGER_NAME)
        logger.setLevel(logging.INFO)
        with pristine_root_logger():
            log_client_version(logger, 'pycboi/1.2.3')
            assert SYNC_LOGGER_NAME not in cb_logging._version_logged

            # An application that sets up logging later still gets the banner.
            handler = _RecordingHandler()
            logger.addHandler(handler)
            log_client_version(logger, 'pycboi/1.2.3')
            assert len(handler.records) == 2

    def test_banner_not_logged_when_handler_stream_closed(self) -> None:
        logger = logging.getLogger(SYNC_LOGGER_NAME)
        logger.setLevel(logging.INFO)
        stream = io.StringIO()
        logger.addHandler(logging.StreamHandler(stream))
        stream.close()
        with pristine_root_logger():
            # A closed stream still counts for hasHandlers(), but log_message() drops the record,
            # so the banner must not be marked as emitted here.
            log_client_version(logger, 'pycboi/1.2.3')
            assert SYNC_LOGGER_NAME not in cb_logging._version_logged

            handler = _RecordingHandler()
            logger.handlers = [handler]
            log_client_version(logger, 'pycboi/1.2.3')
            assert len(handler.records) == 2

    def test_import_leaves_root_logger_alone(self) -> None:
        """Importing the SDK in a fresh interpreter must not configure the root logger."""
        script = (
            'import logging\n'
            'import couchbase_operational_insights.protocol\n'
            'root = logging.getLogger()\n'
            'print(len(root.handlers), root.level)\n'
        )
        repo_root = Path(__file__).resolve().parents[2]
        # Inherit the environment; a stripped one has no SYSTEMROOT, which CPython <= 3.10
        # needs on Windows to seed hash randomization (it aborts during pre-init without it).
        env = os.environ.copy()
        env[LOG_LEVEL_ENV_VAR] = 'debug'
        result = subprocess.run(  # nosec
            [sys.executable, '-c', script],
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip() == f'0 {logging.WARNING}'


class LogPrefixTestSuite:
    TEST_MANIFEST = [
        'test_prefix_before_client_created_is_not_cached',
        'test_prefix_reflects_secure_connection',
    ]

    @staticmethod
    def _adapter(http_endpoint: str = 'http://localhost:8095') -> _ClientAdapter:
        cred = Credential.from_username_and_password('Administrator', 'password')
        return _ClientAdapter(http_endpoint, cred, cluster_id=TEST_CLUSTER_ID)

    def test_prefix_before_client_created_is_not_cached(self) -> None:
        adapter = self._adapter()
        # The cluster logs before it creates the client (e.g. 'Creating the client'), and caching
        # the prefix there would pin a truncated form for the life of the cluster.
        assert adapter.log_prefix == f'[{TEST_CLUSTER_ID}]'
        adapter.create_client()
        try:
            assert adapter.log_prefix == f'[{TEST_CLUSTER_ID}/{adapter.client_id}/http]'
        finally:
            adapter.close_client()

    def test_prefix_reflects_secure_connection(self) -> None:
        adapter = self._adapter('https://localhost:8095')
        adapter.create_client()
        try:
            assert adapter.log_prefix == f'[{TEST_CLUSTER_ID}/{adapter.client_id}/https]'
        finally:
            adapter.close_client()


class LoggingTests(LoggingTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(LoggingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(LoggingTests) if valid_test_method(meth)]
        test_list = set(LoggingTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')


class LogPrefixTests(LogPrefixTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(LogPrefixTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(LogPrefixTests) if valid_test_method(meth)]
        test_list = set(LogPrefixTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
