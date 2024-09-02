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
    from bec_lib.connector import ConnectorBase

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

    LOG_FORMAT = (
        "<green>{service_name} | {{time:YYYY-MM-DD HH:mm:ss}}</green> | <level>[{{level}}]</level> |"
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
            self.connector.send(
                topic=MessageEndpoints.log(),
                msg=bec_lib.messages.LogMessage(
                    log_type=msg["record"]["level"]["name"].lower(), log_msg=msg
                ),
            )
        except Exception:
            # connector disconnected?
            # just ignore the error here...
            # Exception is not explicitely specified,
            # because it depends on the connector
            pass

    def get_format(self, level: LogLevel = None) -> str:
        """
        Get the format for a specific log level.

        Args:
            level (LogLevel, optional): Log level. Defaults to None. If None, the current log level will be used.

        Returns:
            str: Log format.
        """
        service_name = self.service_name if self.service_name else ""
        if level is None:
            level = self.level
        if level > self.LOGLEVEL.DEBUG:
            return self.LOG_FORMAT.format(service_name=service_name)
        if level > self.LOGLEVEL.TRACE:
            return self.DEBUG_FORMAT.format(service_name=service_name)
        return self.TRACE_FORMAT.format(service_name=service_name)

    def formatting(self, record):
        """
        Format the log message.

        Args:
            record (dict): Log record.

        Returns:
            dict: Formatted log record.
        """
        level = record["level"].no
        if level <= self.LOGLEVEL.TRACE:
            frames = takewhile(lambda f: "/loguru/" not in f.filename, traceback.extract_stack())
            stack = " > ".join("{}:{}:{}".format(f.filename, f.name, f.lineno) for f in frames)
            record["extra"]["stack"] = stack
        return self.get_format(level)

    def _update_sinks(self):
        self.logger.remove()
        self.add_redis_log(self._redis_log_level)
        self.add_sys_stderr(self._stderr_log_level)
        self.add_file_log(self._file_log_level)
        if self._console_log:
            self.add_console_log()

    def add_sys_stderr(self, level: LogLevel):
        """
        Add a sink to stderr.

        Args:
            level (LogLevel): Log level.
        """
        self.logger.add(
            sys.__stderr__,
            level=level,
            format=self.formatting,
            filter=lambda record: record["level"].no != LogLevel.CONSOLE_LOG,
        )

    def add_file_log(self, level: LogLevel):
        """
        Add a sink to the service log file.

        Args:
            level (LogLevel): Log level.
        """
        if not self.service_name:
            return
        try:
            filename = os.path.join(self._base_path, f"{self.service_name}.log")
            self.logger.add(
                filename,
                level=level,
                format=self.formatting,
                filter=lambda record: record["level"].no != LogLevel.CONSOLE_LOG,
            )
        except PermissionError:
            import tempfile
            filename = tempfile.NamedTemporaryFile(prefix=self.service_name, delete=False, mode="a+")
            self.logger.add(
                filename,
                level=level,
                format=self.formatting,
                filter=lambda record: record["level"].no != LogLevel.CONSOLE_LOG,
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

        # define a level corresponding to console log - this is to be able to filter messages
        # (only those with this particular level will be recorded by the console logger,
        # while other loggers will ignore them)

        try:
            filename = os.path.join(self._base_path, f"{self.service_name}_CONSOLE.log")
            self.logger.add(
                filename,
                level=LogLevel.CONSOLE_LOG,
                format=self.get_format(LogLevel.CONSOLE_LOG).rstrip(),
                filter=lambda record: record["level"].no == LogLevel.CONSOLE_LOG,
            )
        except PermissionError:
            import tempfile
            filename = tempfile.NamedTemporaryFile(suffix=f"{self.service_name}_CONSOLE.log", delete=False, mode="a+")
            self.logger.add(
                filename,
                level=LogLevel.CONSOLE_LOG,
                format=self.get_format(LogLevel.CONSOLE_LOG).rstrip(),
                filter=lambda record: record["level"].no == LogLevel.CONSOLE_LOG,
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
            format=self.formatting,
            filter=lambda record: record["level"].no != LogLevel.CONSOLE_LOG,
        )

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


bec_logger = BECLogger()
