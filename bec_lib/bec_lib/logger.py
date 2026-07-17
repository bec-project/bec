"""
This module contains the BECLogger class, which is a wrapper around the loguru logger. It is used to
configure and manage the logging of the BEC.
"""

from __future__ import annotations

import enum
import json
import os
import sys
import time
import traceback
from itertools import takewhile
from typing import TYPE_CHECKING, Literal

# TODO: Importing bec_lib, instead of `from bec_lib.messages import LogMessage`, avoids potential
# logger <-> messages circular import. But there could be a better solution.
import bec_lib
from bec_lib.bec_errors import ServiceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.utils.import_utils import lazy_import_from

if TYPE_CHECKING:  # pragma: no cover
    from loguru import logger as loguru_logger

    from bec_lib.file_utils import LogWriter
    from bec_lib.redis_connector import RedisConnector
else:
    loguru_logger = lazy_import_from("loguru", ("logger",))
    LogWriter = lazy_import_from("bec_lib.file_utils", ("LogWriter",))
    RedisConnector = lazy_import_from("bec_lib.redis_connector", ("RedisConnector",))


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
    CONSOLE_LOG_ERROR = 22


class BECLogger:
    """Logger for BEC."""

    DEFAULT_MAX_FILE_SIZE_MB = 50
    DEFAULT_MAX_FILES = 3
    DEFAULT_MAX_FILE_AGE_DAYS = 14

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
    CONTAINER_FORMAT = "{{time:YYYY-MM-DD HH:mm:ss.SSS}} | {{level}} | {{message}}\n"
    LOGLEVEL = LogLevel

    _logger = None

    def __init__(self) -> None:
        if hasattr(self, "_configured"):
            return
        self.bootstrap_server = None
        self.connector: RedisConnector | None = None
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
        self._file_max_size_mb = self.DEFAULT_MAX_FILE_SIZE_MB
        self._file_max_files = self.DEFAULT_MAX_FILES
        self._file_max_age_days = self.DEFAULT_MAX_FILE_AGE_DAYS

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
        service_name: str,
        connector: RedisConnector | None = None,
        connector_cls: type[RedisConnector] | None = None,
        service_config: dict | None = None,
    ) -> None:
        """
        Configure the logger.

        Args:
            bootstrap_server (list): List of bootstrap servers.
            service_name (str): Name of the service to which the logger belongs.
            connector (RedisConnector, optional): Connector instance. Defaults to None.
            connector_cls (type[RedisConnector], optional): Connector class. Defaults to None.
            service_config (dict, optional): Service configuration dictionary. Defaults to None.
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

        self.connector = self._get_connector(
            connector=connector, connector_cls=connector_cls, bootstrap_server=bootstrap_server
        )

        self.bootstrap_server = bootstrap_server
        self.service_name = service_name
        self._update_file_log_policy(service_config)
        self._configured = True
        self._update_sinks()

    def _get_connector(
        self,
        connector: RedisConnector | None = None,
        connector_cls: type[RedisConnector] | None = None,
        bootstrap_server: list | None = None,
    ) -> RedisConnector:
        """
        Validate and return a RedisConnector instance.
        This method checks if either a connector instance or a connector class is provided,
        and if so, it initializes the connector with the provided bootstrap server.

        Args:
            connector (RedisConnector, optional): Connector instance. Defaults to None.
            connector_cls (type[RedisConnector], optional): Connector class. Defaults to None.

        Returns:
            RedisConnector: Connector instance.

        Raises:
            ValueError: If neither connector nor connector_cls is provided, or if both are provided.
            TypeError: If the provided connector is not an instance of RedisConnector,
                       or if the connector_cls is not a subclass of RedisConnector.
            ValueError: If bootstrap_server is not provided when using connector_cls.
        """
        if connector is None and connector_cls is None:
            raise ValueError(
                "Either connector or connector_cls must be provided to configure the logger."
            )
        if connector is not None and connector_cls is not None:
            raise ValueError(
                "Only one of connector or connector_cls should be provided to configure the logger."
            )

        # connector is already provided
        if connector is not None:
            return connector

        # connector_cls is provided

        # disabled for now, cf issue #522
        # if connector_cls is None:
        #     raise ValueError("connector_cls must be provided when using connector_cls")
        # if not issubclass(connector_cls, RedisConnector):
        #     raise TypeError(
        #         f"connector_cls must be a subclass of RedisConnector, got {connector_cls}"
        #     )
        if not bootstrap_server:
            raise ValueError("bootstrap_server must be provided when using connector_cls")
        return connector_cls(bootstrap=bootstrap_server)

    def _update_base_path(self, service_config: dict | None = None):
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

    def _update_file_log_policy(self, service_config: dict | None = None) -> None:
        """Resolve the file rotation policy for the configured service."""
        service_cfg = service_config.get("log_writer", {}) if service_config else {}
        max_file_size_mb = service_cfg.get("max_file_size_mb", self.DEFAULT_MAX_FILE_SIZE_MB)
        max_files = service_cfg.get("max_files", self.DEFAULT_MAX_FILES)
        max_file_age_days = service_cfg.get("max_file_age_days", self.DEFAULT_MAX_FILE_AGE_DAYS)

        service_overrides = service_cfg.get("service_overrides", {})
        service_override = service_overrides.get(self.service_name, {})
        override_max_file_size_mb = service_override.get("max_file_size_mb")
        override_max_files = service_override.get("max_files")
        override_max_file_age_days = service_override.get("max_file_age_days")

        self._file_max_size_mb = (
            override_max_file_size_mb if override_max_file_size_mb is not None else max_file_size_mb
        )
        self._file_max_files = override_max_files if override_max_files is not None else max_files
        self._file_max_age_days = (
            override_max_file_age_days
            if override_max_file_age_days is not None
            else max_file_age_days
        )

    def _file_retention(self, files: list[str]) -> None:
        """Delete archives exceeding the configured count or maximum age."""
        max_archives = self._file_max_files - 1
        cutoff = time.time() - self._file_max_age_days * 24 * 60 * 60
        files_by_age = sorted(files, key=lambda path: (-os.stat(path).st_mtime, path))

        for index, path in enumerate(files_by_age):
            if index >= max_archives or os.stat(path).st_mtime <= cutoff:
                os.remove(path)

    def get_format(self, level: LogLevel = None, is_stderr=False, is_container=False) -> str:
        """
        Get the format for a specific log level.

        Args:
            level (LogLevel, optional): Log level. Defaults to None. If None, the current log level will be used.
            is_stderr (bool, optional): Whether the log is for stderr. Defaults to False.
            is_container (bool, optional): Simple logging for procedure container. Defaults to False.

        Returns:
            str: Log format.
        """
        service_name = self.service_name if self.service_name else ""
        if is_container:
            return self.CONTAINER_FORMAT.format()
        if level is None:
            level = self.level
        if level > self.LOGLEVEL.DEBUG:
            if is_stderr:
                return self.LOG_FORMAT_STDERR.format(service_name=service_name)
            return self.LOG_FORMAT.format(service_name=service_name)
        if level > self.LOGLEVEL.TRACE:
            return self.DEBUG_FORMAT.format(service_name=service_name)
        return self.TRACE_FORMAT.format(service_name=service_name)

    def formatting(self, is_stderr=False, is_container=False):
        """
        Format the log message.

        Args:
            record (dict): Log record.
            is_container (bool, optional): Simple logging for procedure container. Defaults to False.

        Returns:
            str: Log format.
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
            return self.get_format(level, is_container=is_container)

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
            if self._is_disabled_record(record):
                return False
            if not is_console and self._is_console_level(record["level"].no):
                return False
            return True

        return _filter

    def _is_disabled_record(self, record) -> bool:
        if record["name"] in self._disabled_modules:
            return True
        return any(record["name"].startswith(module) for module in self._disabled_modules)

    def _is_console_level(self, level_no: int) -> bool:
        return level_no in (LogLevel.CONSOLE_LOG, LogLevel.CONSOLE_LOG_ERROR)

    def filter_console_redis_log(self, record) -> bool:
        """
        Filter function for console redis log messages, which are used to send log messages to redis.

        Args:
            record (dict): Log record.
        Returns:
            bool: True if the log message should be sent to the console via redis, False otherwise
        """
        return not self._is_disabled_record(record) and self._is_console_level(record["level"].no)

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
            retention=self._file_retention,
            rotation=f"{self._file_max_size_mb} MB",
            catch=True,
            opener=self._file_opener,
        )

    def add_console_log(self):
        """
        Add a sink to the console log.
        """
        try:
            self.logger.level("CONSOLE_LOG", no=LogLevel.CONSOLE_LOG, color="<yellow>", icon="📣")
            self.logger.level(
                "CONSOLE_LOG_ERROR", no=LogLevel.CONSOLE_LOG_ERROR, color="<red>", icon="📣"
            )
        except (TypeError, ValueError):
            # level with same severity already exists: already configured
            pass

        if not self.service_name:
            return
        if not self._base_path:
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
            retention=self._file_retention,
            rotation=f"{self._file_max_size_mb} MB",
            catch=True,
            opener=self._file_opener,
        )
        self._console_log = True
        self.add_console_redis_log()

    def add_redis_log(self, level: LogLevel):
        """
        Add a sink to the redis log.

        Args:
            level (LogLevel): Log level.
        """
        self.logger.add(
            self._publish_log_message,
            serialize=True,
            level=level,
            format=self.formatting(),
            filter=self.filter(),
        )

    def add_console_redis_log(self):
        """
        Add a sink to the console redis log.
        It deviates from the regular redis log in that it only includes messages with
        level CONSOLE_LOG and CONSOLE_LOG_ERROR.
        """
        self.logger.add(
            self._console_redis_logger_callback,
            serialize=True,
            level=LogLevel.CONSOLE_LOG,
            format=self.formatting(is_stderr=True),
            filter=self.filter_console_redis_log,
        )

    def _console_redis_logger_callback(self, msg):
        if not self._configured or self.connector is None:
            return
        self._publish_log_message(msg, service_name=f"{self.service_name}_CONSOLE")

    def _decode_log_payload(self, msg: str | dict) -> dict:
        return json.loads(msg) if isinstance(msg, str) else dict(msg)

    def _publish_log_message(self, msg: str | dict, service_name: str | None = None) -> bool:
        if not self._configured or self.connector is None:
            return False
        payload = self._decode_log_payload(msg)
        payload["service_name"] = self.service_name if service_name is None else service_name
        try:
            self.connector.xadd(
                topic=MessageEndpoints.log(),
                msg_dict={
                    "data": bec_lib.messages.LogMessage(
                        log_type=payload["record"]["level"]["name"].lower(), log_msg=payload
                    )
                },
                max_size=10000,
            )
            return True
        except Exception:
            # connector disconnected?
            # just ignore the error here...
            # Exception is not explicitly specified,
            # because it depends on the connector
            return False

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

    def set_log_level(self, val: LogLevel, sink: Literal["all", "redis", "file", "stderr"] = "all"):
        """
        Set the log level for a specific sink.

        Args:
            val (LogLevel): Log level.
            sink (str, optional): Sink name. Defaults to "all".
                Options are: "all", "redis", "file", "stderr".
        """
        if sink == "all":
            self._redis_log_level = val
            self._file_log_level = val
            self._stderr_log_level = val
        elif sink == "redis":
            self._redis_log_level = val
        elif sink == "file":
            self._file_log_level = val
        elif sink == "stderr":
            self._stderr_log_level = val
        else:
            raise ValueError(f"Unknown sink: {sink}")
        self._update_sinks()

    def _file_opener(self, path: str, mode: int, **kwargs):
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
