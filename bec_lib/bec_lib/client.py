"""
BECClient class. This class is the main entry point for the BEC client and all 
derived classes. It is used to initialize the client and start the client.
"""

from __future__ import annotations, print_function

import builtins
import getpass
import importlib
import inspect
from types import SimpleNamespace
from typing import TYPE_CHECKING

import redis
from pydantic import BaseModel, Field, field_validator
from rich.console import Console
from rich.table import Table

from bec_lib.alarm_handler import AlarmHandler, Alarms
from bec_lib.bec_service import BECService, BECStatus
from bec_lib.bl_checks import BeamlineChecks
from bec_lib.callback_handler import CallbackHandler, EventType
from bec_lib.dap_plugins import DAPPlugins
from bec_lib.device_monitor_plugin import DeviceMonitorPlugin
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.plugin_helper import _get_available_plugins
from bec_lib.scan_history import ScanHistory
from bec_lib.service_config import ServiceConfig
from bec_lib.user_scripts_mixin import UserScriptsMixin
from bec_lib.utils.import_utils import lazy_import_from

if TYPE_CHECKING:
    from bec_lib.connector import ConnectorBase

logger = bec_logger.logger
# TODO: put back normal import when Pydantic gets faster
VariableMessage = lazy_import_from("bec_lib.messages", ("VariableMessage",))
BECStatus = lazy_import_from("bec_lib.messages", ("BECStatus",))
RedisConnector = lazy_import_from("bec_lib.redis_connector", ("RedisConnector",))
ScanManager = lazy_import_from("bec_lib.scan_manager", ("ScanManager",))
Scans = lazy_import_from("bec_lib.scans", ("Scans",))


class SystemConfig(BaseModel):
    """System configuration model"""

    file_suffix: str | None = Field(default=None)
    file_directory: str | None = Field(default=None)
    model_config: dict = {"validate_assignment": True}

    @field_validator("file_suffix", "file_directory")
    @staticmethod
    def check_validity(value: str, field: Field) -> str:
        """Check the validity of the value

        Args:
            value (str): The value to check
            field_name (str): The name of the field

        Returns:
            str: The value if it is valid
        """
        if value is None:
            return value
        field_name = field.field_name
        check_value = value.replace("_", "").replace("-", "")
        if field_name == "file_directory":
            value = value.strip("/")
            check_value = check_value.replace("/", "")
        if not check_value.isalnum() or not check_value.isascii():
            raise ValueError(
                f"{field_name} must only contain alphanumeric ASCII characters. Provided string is: {value}"
            )
        return value


class BECClient(BECService, UserScriptsMixin):
    """
    The BECClient class is the main entry point for the BEC client and all derived classes.
    """

    _client = None
    _initialized = False
    started = False

    def __init__(
        self,
        config: ServiceConfig = None,
        connector_cls: ConnectorBase = None,
        wait_for_server=False,
        forced=False,
        parent=None,
        name: str | None = None,
    ) -> None:
        """
        Initialize the BECClient

        Args:
            config (ServiceConfig, optional): The configuration for the client. Defaults to None.
            connector_cls (ConnectorBase, optional): The connector class to use. Defaults to None.
            wait_for_server (bool, optional): Whether to wait for the server to be available before starting. Defaults to False.
            forced (bool, optional): Whether to force the initialization of a new client. Defaults to False.
            name (str, optional): The name of the client. Defaults to None.
        """
        if self._initialized:
            return
        self.__init_params = {
            "config": config if config is not None else ServiceConfig(),
            "connector_cls": connector_cls if connector_cls is not None else RedisConnector,
            "wait_for_server": wait_for_server,
        }
        self._name = name
        self.device_manager = None
        self.queue = None
        self.alarm_handler = None
        self.config = None
        self.history = None
        self.live_updates = None
        self.dap = None
        self.device_monitor = None
        self.bl_checks = None
        self.scans_namespace = SimpleNamespace()
        self._hli_funcs = {}
        self.metadata = {}
        self.system_config = SystemConfig()
        self.callbacks = CallbackHandler()
        self._parent = parent if parent is not None else self
        self._initialized = True

    def __new__(cls, *args, forced=False, **kwargs):
        if forced or BECClient._client is None:
            BECClient._client = super(BECClient, cls).__new__(cls)
            BECClient._initialized = False
        return BECClient._client

    def __str__(self) -> str:
        return "BECClient\n\nTo get a list of available commands, type `bec.show_all_commands()`"

    @classmethod
    def _reset_singleton(cls):
        BECClient._client = None
        BECClient._initialized = False
        BECClient.started = False

    @property
    def username(self) -> str:
        """get the current username"""
        return self._username

    @property
    def active_account(self) -> str:
        """get the currently active target (e)account"""
        msg = self.connector.get(MessageEndpoints.account())
        if msg:
            return msg.value
        return ""

    def start(self):
        """start the client"""
        if self.started:
            return
        try:
            self.started = True
            config = self.__init_params["config"]
            connector_cls = self.__init_params["connector_cls"]
            wait_for_server = self.__init_params["wait_for_server"]
            super().__init__(
                config, connector_cls, wait_for_server=wait_for_server, name=self._name
            )
            builtins.bec = self._parent
            self._start_services()
            default_namespace = {"dev": self.device_manager.devices, "scans": self.scans_namespace}
            self.callbacks.run(
                EventType.NAMESPACE_UPDATE, action="add", ns_objects=default_namespace
            )
            logger.info("Starting new client")
            self.status = BECStatus.RUNNING
        except redis.exceptions.ConnectionError:
            logger.error("Failed to start the client: Could not connect to Redis server.")
            self.shutdown()

    def _start_services(self):
        self._load_scans()
        # self.logbook = LogbookConnector(self.connector)
        self._update_username()
        self._start_device_manager()
        self._start_scan_queue()
        self._start_alarm_handler()
        self.load_all_user_scripts()
        self.config = self.device_manager.config_helper
        self.history = ScanHistory(self.connector)
        self.dap = DAPPlugins(self)
        self.bl_checks = BeamlineChecks(self)
        self.bl_checks.start()
        self.device_monitor = DeviceMonitorPlugin(self.connector)

    def alarms(self, severity=Alarms.WARNING):
        """get the next alarm with at least the specified severity"""
        if self.alarm_handler is None:
            yield []
        yield from self.alarm_handler.get_alarm(severity=severity)

    def show_all_alarms(self, severity=Alarms.WARNING):
        """print all unhandled alarms"""
        alarms = self.alarm_handler.get_unhandled_alarms(severity=severity)
        for alarm in alarms:
            print(alarm)

    def clear_all_alarms(self):
        """remove all alarms from stack"""
        self.alarm_handler.clear()

    @property
    def pre_scan_macros(self):
        """currently stored pre-scan macros"""
        return self.connector.lrange(MessageEndpoints.pre_scan_macros(), 0, -1)

    @pre_scan_macros.setter
    def pre_scan_macros(self, hooks: list[str]):
        self.connector.delete(MessageEndpoints.pre_scan_macros())
        for hook in hooks:
            msg = VariableMessage(value=hook)
            self.connector.lpush(MessageEndpoints.pre_scan_macros(), msg)

    def _load_scans(self):
        self.scans = Scans(self._parent)
        builtins.__dict__["scans"] = self.scans
        self.scans_namespace = SimpleNamespace(
            scan_def=self.scans.scan_def,
            **{scan_name: scan.run for scan_name, scan in self.scans._available_scans.items()},
        )

    def load_high_level_interface(self, module_name: str) -> None:
        """Load a high level interface module.
        Runs a callback of type `EventType.NAMESPACE_UPDATE`
        to inform clients about added objects in the namesapce.

        Args:
            module_name (str): The name of the module to load
        """
        plugins = _get_available_plugins("bec")
        for plugin in plugins:
            try:
                module = importlib.import_module(
                    f"{plugin.__name__}.bec_ipython_client.high_level_interface.{module_name}"
                )
                members = inspect.getmembers(module)
                logger.info(
                    f"Loaded high level interface {module_name} from plugin {plugin.__name__}."
                )
                break
            except Exception:
                continue
        else:
            mod = importlib.import_module(f"bec_ipython_client.high_level_interfaces.{module_name}")
            members = inspect.getmembers(mod)
            logger.info(f"Loaded high level interface {module_name} from bec.")
        funcs = {name: func for name, func in members if not name.startswith("__")}
        self._hli_funcs.update(funcs)
        builtins.__dict__.update(funcs)
        self.callbacks.run(EventType.NAMESPACE_UPDATE, action="add", ns_objects=funcs)

    def _update_username(self):
        self._username = getpass.getuser()

    def _start_scan_queue(self):
        self.queue = ScanManager(self.connector)

    def _start_device_manager(self):
        logger.info("Starting device manager")
        self.device_manager = DeviceManagerBase(self)
        self.device_manager.initialize(self.bootstrap_server)
        builtins.dev = self.device_manager.devices

    def _start_alarm_handler(self):
        logger.info("Starting alarm listener")
        self.alarm_handler = AlarmHandler(self.connector)
        self.alarm_handler.start()

    def shutdown(self):
        """shutdown the client and all its components"""
        super().shutdown()
        if self.device_manager:
            self.device_manager.shutdown()
        if self.queue:
            self.queue.shutdown()
        if self.alarm_handler:
            self.alarm_handler.shutdown()
        if self.bl_checks:
            self.bl_checks.stop()
        bec_logger.logger.remove()
        self.started = False

    def _print_available_commands(self, title: str, data: tuple) -> None:
        console = Console()
        table = Table(title=title)
        table.add_column("Name", justify="center")
        table.add_column("Description", justify="center")
        for name, descr in data:
            table.add_row(name, descr)
        console.print(table)

    def _print_user_script_commands(self) -> None:
        data = self._get_user_script_commands()
        self._print_available_commands("User scripts", data)

    def _get_user_script_commands(self) -> list:
        avail_commands = []
        for name, val in self._scripts.items():
            descr = self._get_description_from_doc_string(val["cls"].__doc__)
            avail_commands.append((name, descr))
        return avail_commands

    def _get_scan_commands(self) -> list:
        avail_commands = []
        for name, scan in self.scans._available_scans.items():
            descr = self._get_description_from_doc_string(scan.scan_info["doc"])
            avail_commands.append((name, descr))
        return avail_commands

    def _print_scan_commands(self) -> None:
        data = self._get_scan_commands()
        self._print_available_commands("Scans", data)

    def show_all_commands(self):
        self._print_user_script_commands()
        self._print_scan_commands()

    @staticmethod
    def _get_description_from_doc_string(doc_string: str) -> str:
        if not doc_string:
            return ""
        return doc_string.strip().split("\n")[0]
