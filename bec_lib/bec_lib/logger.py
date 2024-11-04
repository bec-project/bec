"""
This module contains the BECLogger class, which is a wrapper around the loguru logger. It is used to
configure and manage the logging of the BEC.
"""

from __future__ import annotations

import enum
import json
import os
import sys
import traceback
from itertools import takewhile
from typing import TYPE_CHECKING

# TODO: Importing bec_lib, instead of `from bec_lib.messages import LogMessage`, avoids potential
# logger <-> messages circular import. But there could be a better solution.
import bec_lib
from bec_lib.bec_errors import ServiceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.utils.import_utils import lazy_import_from

if TYPE_CHECKING:
    from loguru import logger as loguru_logger

    from bec_lib.connector import ConnectorBase
    from bec_lib.file_utils import LogWriter
else:
    loguru_logger = lazy_import_from("loguru", ("logger",))
    LogWriter = lazy_import_from("bec_lib.file_utils", ("LogWriter",))


class LogLevel(int, enum.Enum):
    """Mapping of Loguru log levels to BEC log levels."""

    TRACE = 5
    DEBUG = 10
    INFO = 20
    SUCCESS = 25
    WARNING = 30
    ERROR = 40
    CRITICAL = 50
    CONSOLE_LOG = 21


class BECLogger:
    """Logger for BEC."""

    LOG_FORMAT_STDERR = (
        "<green>{service_name} | {{time:YYYY-MM-DD HH:mm:ss}}</green> | {{name}} | <level>[{{level}}]</level> |"
        " <level>{{message}}</level>\n"
    )
    LOG_FORMAT = (
        "<green>{{time:YYYY-MM-DD HH:mm:ss}}</green> | {{name}} | <level>[{{level}}]</level> |"
        " <level>{{message}}</level>\n"
    )
    DEBUG_FORMAT = (
        "<green>{service_name} | {{time:YYYY-MM-DD HH:mm:ss.SSS}}</green> | <level>{{level}}</level> |"
        "  <level>{{thread.name}} ({{thread.id}})</level> | <cyan>{{name}}</cyan>:<cyan>{{function}}</cyan>:<cyan>{{line}}</cyan> -"
        " <level>{{message}}</level>\n"
    )
    TRACE_FORMAT = (
        "<green>{service_name} | {{time:YYYY-MM-DD HH:mm:ss.SSS}}</green> | <level>{{level}}</level> |"
        " <level>{{thread.name}} ({{thread.id}})</level> | <cyan>{{extra[stack]}}</cyan> - <level>{{message}}</level>\n"
    )
    LOGLEVEL = LogLevel

    _logger = None

    def __init__(self) -> None:
        if hasattr(self, "_configured"):
            return
        self.bootstrap_server = None
        self.connector = None
        self.service_name = None
        self.writer_mixin = None
        self._base_path = None
        self.logger = loguru_logger
        self._log_level = LogLevel.INFO
        self._redis_log_level = self._log_level
        self._stderr_log_level = self._log_level
        self._file_log_level = self._log_level
        self._console_log = False
        self._configured = False
        self._disabled_modules = set()

    def __new__(cls):
        if not hasattr(cls, "_logger") or cls._logger is None:
            cls._logger = super(BECLogger, cls).__new__(cls)
        return cls._logger

    @classmethod
    def _reset_singleton(cls):
        if cls._logger is not None:
            cls._logger.logger.remove()
        cls._logger = None

    def configure(
        self,
        bootstrap_server: list,
        connector_cls: ConnectorBase,
        service_name: str,
        service_config: dict = None,
    ) -> None:
        """
        Configure the logger.

        Args:
            bootstrap_server (list): List of bootstrap servers.
            connector_cls (ConnectorBase): Connector class.
            service_name (str): Name of the service to which the logger belongs.
        """
        if self._configured:
            # already configured, nothing to do - this can happen
            # if running another BECClient (or BECService) in addition
            # to a main one
            return
        if not self._base_path:
            self._update_base_path(service_config)
        if os.path.exists(self._base_path) is False:
            self.writer_mixin.create_directory(self._base_path)
        self.bootstrap_server = bootstrap_server
        self.connector = connector_cls(bootstrap_server)
        self.service_name = service_name
        self._configured = True
        self._update_sinks()

    def _update_base_path(self, service_config: dict = None):
        """
        Compile the log base path.
        """
        # pylint: disable=import-outside-toplevel
        if service_config:
            service_cfg = service_config.get("log_writer", None)
            if not service_cfg:
                raise ServiceConfigError(
                    f"ServiceConfig {service_config} must at least contain key with 'log_writer'"
                )
        else:
            service_cfg = {"base_path": "./"}
        self.writer_mixin = LogWriter(service_cfg)
        self._base_path = self.writer_mixin.directory
        self.writer_mixin.create_directory(self._base_path)

    def _logger_callback(self, msg):
        if not self._configured:
            return
        msg = json.loads(msg)
        msg["service_name"] = self.service_name
        try:
            self.connector.xadd(
                topic=MessageEndpoints.log(),
                msg_dict={
                    "data": bec_lib.messages.LogMessage(
                        log_type=msg["record"]["level"]["name"].lower(), log_msg=msg
                    )
                },
                max_size=10000,
            )
        except Exception:
            # connector disconnected?
            # just ignore the error here...
            # Exception is not explicitely specified,
            # because it depends on the connector
            pass

    def get_format(self, level: LogLevel = None, is_stderr=False) -> str:
        """
        Get the format for a specific log level.

        Args:
            level (LogLevel, optional): Log level. Defaults to None. If None, the current log level will be used.
            is_stderr (bool, optional): Whether the log is for stderr. Defaults to False.

        Returns:
            str: Log format.
        """
        service_name = self.service_name if self.service_name else ""
        if level is None:
            level = self.level
        if level > self.LOGLEVEL.DEBUG:
            if is_stderr:
                return self.LOG_FORMAT_STDERR.format(service_name=service_name)
            return self.LOG_FORMAT.format(service_name=service_name)
        if level > self.LOGLEVEL.TRACE:
            return self.DEBUG_FORMAT.format(service_name=service_name)
        return self.TRACE_FORMAT.format(service_name=service_name)

    def formatting(self, is_stderr=False):
        """
        Format the log message.

        Args:
            record (dict): Log record.

        Returns:
            dict: Formatted log record.
        """

        def _update_record(record):
            level = record["level"].no
            if level <= self.LOGLEVEL.TRACE:
                frames = takewhile(
                    lambda f: "/loguru/" not in f.filename, traceback.extract_stack()
                )
                stack = " > ".join("{}:{}:{}".format(f.filename, f.name, f.lineno) for f in frames)
                record["extra"]["stack"] = stack
            return level

        def _format(record):
            level = _update_record(record)
            return self.get_format(level)

        def _format_stderr(record):
            level = _update_record(record)
            return self.get_format(level, is_stderr=True)

        if is_stderr:
            return _format_stderr
        return _format

    def _update_sinks(self):
        self.logger.remove()
        self.add_redis_log(self._redis_log_level)
        self.add_sys_stderr(self._stderr_log_level)
        self.add_file_log(self._file_log_level)
        if self._console_log:
            self.add_console_log()

    def filter(self, is_console: bool = False):
        """
        Filter factory function for log messages.

        Args:
            is_console (bool, optional): Whether the log is for the console. Defaults to False.

        Returns:
            function: Filter function.
        """

        def _filter(record):
            if record["name"] in self._disabled_modules:
                return False
            for module in self._disabled_modules:
                if record["name"].startswith(module):
                    return False
            if not is_console and record["level"].no == LogLevel.CONSOLE_LOG:
                return False
            return True

        return _filter

    def add_sys_stderr(self, level: LogLevel):
        """
        Add a sink to stderr.

        Args:
            level (LogLevel): Log level.
        """
        self.logger.add(
            sys.__stderr__,
            level=level,
            format=self.formatting(is_stderr=True),
            filter=self.filter(),
        )

    def add_file_log(self, level: LogLevel):
        """
        Add a sink to the service log file.

        Args:
            level (LogLevel): Log level.
        """
        if not self.service_name:
            return
        filename = os.path.join(self._base_path, f"{self.service_name}.log")
        self.logger.add(
            filename,
            level=level,
            format=self.formatting(),
            filter=self.filter(),
            retention="3 days",
            rotation="3 days",
            opener=self._file_opener,
        )

    def add_console_log(self):
        """
        Add a sink to the console log.
        """
        try:
            self.logger.level("CONSOLE_LOG", no=21, color="<yellow>", icon="📣")
        except TypeError:
            # level with same severity already exists: already configured
            pass

        if not self.service_name:
            return
        filename = os.path.join(self._base_path, f"{self.service_name}_CONSOLE.log")

        # define a level corresponding to console log - this is to be able to filter messages
        # (only those with this particular level will be recorded by the console logger,
        # while other loggers will ignore them)

        self.logger.add(
            filename,
            level=LogLevel.CONSOLE_LOG,
            format=self.get_format(LogLevel.CONSOLE_LOG).rstrip(),
            filter=self.filter(is_console=True),
            retention="3 days",
            rotation="3 days",
            opener=self._file_opener,
        )
        self._console_log = True

    def add_redis_log(self, level: LogLevel):
        """
        Add a sink to the redis log.

        Args:
            level (LogLevel): Log level.
        """
        self.logger.add(
            self._logger_callback,
            serialize=True,
            level=level,
            format=self.formatting(),
            filter=self.filter(),
        )

    @property
    def disabled_modules(self) -> set[str]:
        """
        Get the disabled modules.
        """
        return self._disabled_modules

    @disabled_modules.setter
    def disabled_modules(self, module_names: str | list[str]) -> None:
        """
        Disable log messages from specific modules.

        Args:
            module_names (str | list[str]): Module name(s).
        """
        if isinstance(module_names, str):
            module_names = [module_names]
        self._disabled_modules.update(module_names)

    @property
    def level(self):
        """
        Get the current log level.
        """
        return self._log_level

    @level.setter
    def level(self, val: LogLevel):
        self._log_level = val
        self._redis_log_level = val
        self._file_log_level = val
        self._stderr_log_level = val
        self._update_sinks()

    def _file_opener(self, path: str, mode: str, **kwargs):
        """
        Open the log file.

        Args:
            path (str): Path to the log file.
            mode (str): File mode.

        Returns:
            file: File object.
        """
        # pylint: disable=consider-using-with
        # pylint: disable=unspecified-encoding
        file_existed = os.path.exists(path)
        textio = os.open(path, mode)
        if file_existed is False:
            os.chmod(path, 0o664)
        return textio


bec_logger = BECLogger()
