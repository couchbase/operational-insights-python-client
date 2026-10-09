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
import os
from enum import Enum
from typing import Dict, Optional, Set, Tuple

import ijson

LOG_FORMAT_ARR = [
    '[%(asctime)s.%(msecs)03d]',
    '%(relativeCreated)dms',
    '[%(levelname)s]',
    '[%(process)d, %(threadName)s (%(thread)d)] %(name)s',
    '- %(message)s',
]
LOG_FORMAT = ' '.join(LOG_FORMAT_ARR)
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# The SDK's two APIs are peer top-level packages, so they get peer logger names that match.  An
# application configures whichever one it imported; anything the SDK does on its own covers both.
SYNC_LOGGER_NAME = 'couchbase_operational_insights'
ASYNC_LOGGER_NAME = 'acouchbase_operational_insights'
LOG_LEVEL_ENV_VAR = 'PYCBOI_LOG_LEVEL'


class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


_LOG_LEVEL_NAMES: Dict[str, int] = {
    'trace': logging.DEBUG,
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warn': logging.WARNING,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
    'off': logging.CRITICAL + 10,
}

_logging_configured = False
_version_logged: Set[str] = set()


def _has_open_handlers(logger: logging.Logger) -> bool:
    current: Optional[logging.Logger] = logger
    while current is not None:
        for handler in current.handlers:
            if isinstance(handler, logging.StreamHandler):
                if hasattr(handler.stream, 'closed') and handler.stream.closed:
                    continue
            return True
        if not current.propagate:
            break
        current = current.parent
    return False


def log_message(logger: logging.Logger, message: str, log_level: LogLevel) -> None:
    if not logger or not logger.hasHandlers():
        return

    if not _has_open_handlers(logger):
        return

    if log_level == LogLevel.DEBUG:
        logger.debug(message)
    elif log_level == LogLevel.INFO:
        logger.info(message)
    elif log_level == LogLevel.WARNING:
        logger.warning(message)
    elif log_level == LogLevel.ERROR:
        logger.error(message)
    elif log_level == LogLevel.CRITICAL:
        logger.critical(message)


def configure_logging_from_env() -> None:
    """
    **INTERNAL** Apply the PYCBOI_LOG_LEVEL environment variable to the SDK's own loggers.

    The root logger belongs to the host application, so it is never configured here.  When the
    application has already set up logging, the SDK only adjusts the level of its own loggers and
    lets the records propagate to the handlers the application installed.  Only when nothing else
    would handle them does the SDK attach a handler of its own.
    """
    global _logging_configured
    if _logging_configured:
        return
    _logging_configured = True

    log_level = os.getenv(LOG_LEVEL_ENV_VAR, None)
    if log_level is None:
        return

    level = _LOG_LEVEL_NAMES.get(log_level.strip().lower())
    unrecognized_level = level is None
    if level is None:
        level = logging.INFO

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    root_has_handlers = logging.getLogger().hasHandlers()
    for logger_name in (SYNC_LOGGER_NAME, ASYNC_LOGGER_NAME):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        if logger.handlers or root_has_handlers:
            continue
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    if unrecognized_level:
        logging.getLogger(SYNC_LOGGER_NAME).warning(
            f'Unrecognized {LOG_LEVEL_ENV_VAR} value {log_level!r}, defaulting to INFO. '
            f'Allowed values: {", ".join(sorted(_LOG_LEVEL_NAMES))}.'
        )


def log_client_version(logger: logging.Logger, version: str, prefix: str = '') -> None:
    """
    **INTERNAL** Emit the client version banner, and the JSON parser in use, at most once per logger.

    Nothing is emitted, and nothing is recorded as emitted, while the logger has no handler that
    could take the record; an application that configures logging later still gets the banner from
    its next cluster.
    """
    if logger.name in _version_logged or not _has_open_handlers(logger):
        return
    _version_logged.add(logger.name)
    message = f'Python Couchbase Operational Insights Client ({version})'
    log_message(logger, f'{prefix} {message}'.strip(), LogLevel.INFO)
    message, log_level = _json_parser_message(ijson.__version__, ijson.backend_name)
    log_message(logger, f'{prefix} {message}'.strip(), log_level)


def _json_parser_message(ijson_version: str, backend_name: str) -> Tuple[str, LogLevel]:
    # ijson falls back to its pure-Python backend when no prebuilt wheel exists for the running
    # Python (for example, a release newer than ijson's wheels) and yajl was not found at build time.
    message = f'JSON parser: ijson {ijson_version} ({backend_name} backend)'
    if backend_name != 'python':
        return message, LogLevel.INFO
    return (
        f'{message}.  Streaming JSON parsing is slower without the C backend; to build it, install the '
        f'yajl 2 library and reinstall with ijson built from source '
        f'(pip install --force-reinstall --no-binary ijson couchbase-operational-insights).'
    ), LogLevel.WARNING
