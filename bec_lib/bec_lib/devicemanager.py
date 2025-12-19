"""
This module contains the DeviceManager class which is used to manage devices and their configuration.
"""

from __future__ import annotations

import collections
import copy
import functools
import re
import threading
import time
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable, Literal

import numpy as np
from rich.console import Console
from rich.table import Table
from typeguard import typechecked

from bec_lib.atlas_models import _DeviceModelCore
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.callback_handler import EventType
from bec_lib.config_helper import ConfigHelper
from bec_lib.device import (
    ComputedSignal,
    Device,
    DeviceBase,
    Positioner,
    ReadoutPriority,
    Signal,
    set_device_config,
)
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import AvailableResourceMessage, DeviceConfigMessage
from bec_lib.utils.import_utils import lazy_import_from
from bec_lib.utils.rpc_utils import rgetattr

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.bec_service import BECService
    from bec_lib.messages import (
        BECStatus,
        DeviceInfoMessage,
        ScanStatusMessage,
        ServiceResponseMessage,
    )
else:
    # TODO: put back normal import when Pydantic gets faster
    BECStatus, ServiceResponseMessage = lazy_import_from(
        "bec_lib.messages", ("BECStatus", "ServiceResponseMessage")
    )

BECSignals = Literal[
    "AsyncSignal",
    "AsyncMultiSignal",
    "DynamicSignal",
    "FileEventSignal",
    "PreviewSignal",
    "ProgressSignal",
]

logger = bec_logger.logger


class CancelledError(Exception):
    """Exception raised when a config request is cancelled."""


def _rgetattr_safe(obj, attr, *args):
    try:
        return rgetattr(obj, attr, *args)
    except DeviceConfigError:
        return None


class DeviceContainer(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.items():
                self[k] = v

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str) and "." in key:
            return _rgetattr_safe(self, key) is not None
        return super().__contains__(key)

    def __getattr__(self, attr):
        if attr.startswith("_"):
            # if dunder attributes are would not be caught, they
            # would raise a DeviceConfigError and kill the
            # IPython completer
            # pylint: disable=no-member
            return super().__getattr__(attr)
        dev = self.get(attr)
        if not dev:
            raise DeviceConfigError(f"Device {attr} does not exist.")
        return dev

    def __setattr__(self, key, value):
        if isinstance(value, DeviceBase):
            self.__setitem__(key, value)
        else:
            raise AttributeError("Unsupported device type.")

    def __getitem__(self, item):
        if isinstance(item, str) and "." in item:
            return rgetattr(self, item)
        return super().__getitem__(item)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super().__delitem__(key)
        del self.__dict__[key]

    def flush(self) -> None:
        """
        Remove all devices from the device manager
        """
        self.clear()
        self.__dict__.clear()

    @property
    def enabled_devices(self) -> list:
        """get a list of enabled devices"""
        return [dev for _, dev in self.items() if dev.enabled]

    @property
    def disabled_devices(self) -> list:
        """get a list of disabled devices"""
        return [dev for _, dev in self.items() if not dev.enabled]

    def readout_priority(self, readout_priority: ReadoutPriority) -> list:
        """get all devices with the specified readout proprity

        Args:
            readout_priority (str): Readout priority (e.g.  on_request, baseline, monitored, async, continuous)

        Returns:
            list: List of devices that belong to the specified acquisition readoutPriority
        """
        val = ReadoutPriority(readout_priority)
        # pylint: disable=protected-access
        return [dev for _, dev in self.items() if dev.root._config["readoutPriority"] == val]

    def _filter_devices(
        self, readout_priority: ReadoutPriority, readout_priority_mod: dict, devices: list = None
    ) -> list:
        """filter devices by readout priority"""
        if devices is None:
            devices = []

        if readout_priority_mod is None:
            readout_priority_mod = {}
        else:
            readout_priority_mod = copy.deepcopy(readout_priority_mod)

        bec_triggered = (
            set(readout_priority_mod.get("monitored", []))
            .union(readout_priority_mod.get("on_request", []))
            .union(readout_priority_mod.get("baseline", []))
        )

        if readout_priority == ReadoutPriority.MONITORED:
            for dev in readout_priority_mod.get("monitored", []):
                if dev in readout_priority_mod.get("on_request", []):
                    readout_priority_mod["on_request"].remove(dev)
                if dev in readout_priority_mod.get("baseline", []):
                    readout_priority_mod["baseline"].remove(dev)
        elif readout_priority == ReadoutPriority.BASELINE:
            for dev in readout_priority_mod.get("baseline", []):
                if dev in readout_priority_mod.get("on_request", []):
                    readout_priority_mod["on_request"].remove(dev)

        async_intersection = bec_triggered.intersection(set(readout_priority_mod.get("async", [])))
        if async_intersection:
            raise ValueError(
                f"Devices {async_intersection} cannot be async and monitored/baseline/on_request at the same time"
            )

        continuous_intersection = bec_triggered.intersection(
            set(readout_priority_mod.get("continuous", []))
        )
        if continuous_intersection:
            raise ValueError(
                f"Devices {continuous_intersection} cannot be continuous and monitored/baseline/on_request at the same time"
            )

        devices = [self[dev].root if isinstance(dev, str) else dev.root for dev in devices]

        devices.extend(self.readout_priority(readout_priority))
        devices.extend(
            [self[dev].root for dev in readout_priority_mod.get(readout_priority.name.lower(), [])]
        )

        excluded_readout_priority = [
            str(x.name).lower() for x in ReadoutPriority if x != readout_priority
        ]
        excluded_devices = self.disabled_devices
        for priority in excluded_readout_priority:
            excluded_devices.extend(
                self[dev].root for dev in readout_priority_mod.get(priority, [])
            )

        return [dev for dev in set(devices) if dev not in excluded_devices]

    def async_devices(self, readout_priority: dict | None = None) -> list:
        """get a list of all synchronous devices"""
        # pylint: disable=protected-access
        return self._filter_devices(ReadoutPriority.ASYNC, readout_priority)

    def continuous_devices(self, readout_priority: dict | None = None) -> list:
        """get a list of all continuous devices"""
        # pylint: disable=protected-access
        return self._filter_devices(ReadoutPriority.CONTINUOUS, readout_priority)

    def on_request_devices(self, readout_priority: dict | None = None) -> list:
        """get a list of all on request devices"""
        # pylint: disable=protected-access
        return self._filter_devices(ReadoutPriority.ON_REQUEST, readout_priority)

    @typechecked
    def monitored_devices(
        self, scan_motors: list | None = None, readout_priority: dict | None = None
    ) -> list:
        """get a list of all enabled monitored devices"""
        devices = []
        if scan_motors:
            if not isinstance(scan_motors, list):
                scan_motors = [scan_motors]
            for scan_motor in scan_motors:
                if scan_motor not in devices:
                    if isinstance(scan_motor, DeviceBase):
                        devices.append(scan_motor.root)
                    else:
                        devices.append(self.get(scan_motor).root)

        return self._filter_devices(ReadoutPriority.MONITORED, readout_priority, devices)

    @typechecked
    def baseline_devices(
        self, scan_motors: list | None = None, readout_priority: dict | None = None
    ) -> list:
        """
        Get a list of all enabled baseline devices
        Args:
            scan_motors(list): list of scan motors
            readout_priority(dict): readout priority

        Returns:
            list: list of baseline devices
        """
        devices = self.readout_priority(ReadoutPriority.BASELINE)
        if readout_priority is None:
            readout_priority = collections.defaultdict(list)

        if scan_motors:
            if not isinstance(scan_motors, list):
                scan_motors = [scan_motors]
            for scan_motor in scan_motors:
                if scan_motor not in devices:
                    if isinstance(scan_motor, DeviceBase):
                        readout_priority["monitored"].append(scan_motor)
                    else:
                        readout_priority["monitored"].append(self.get(scan_motor))

        return self._filter_devices(ReadoutPriority.BASELINE, readout_priority, devices)

    def get_devices_with_tags(self, tags: list) -> list:
        """
        Get a list of all devices with the specified tags
        Args:
            tags (list): List of tags

        Returns:
            list: List of devices with the specified tags
        """
        # pylint: disable=protected-access
        if not isinstance(tags, list):
            tags = [tags]
        return [
            dev for _, dev in self.items() if set(tags) & set(dev._config.get("deviceTags", []))
        ]

    def show_tags(self) -> list:
        """returns a list of used tags in the current config"""
        tags = set()
        for _, dev in self.items():
            # pylint: disable=protected-access
            dev_tags = dev._config.get("deviceTags")
            if dev_tags:
                tags.update(dev_tags)
        return list(tags)

    def get_software_triggered_devices(self) -> list:
        """get a list of all devices that should receive a software trigger detectors"""
        # pylint: disable=protected-access
        devices = [
            dev for _, dev in self.items() if dev._config.get("softwareTrigger", False) is True
        ]
        excluded_devices = self.disabled_devices
        return [dev for dev in set(devices) if dev not in excluded_devices]

    def _expand_device_name(self, device_name: str) -> list[str]:
        try:
            regex = re.compile(device_name)
        except re.error:
            return [device_name]
        return [dev for dev in self.keys() if regex.match(dev)]

    def wm(self, device_names: list[str | DeviceBase | None] = None, *args):
        """Get the current position of one or more devices.

        Args:
            device_names (list): List of device names or Device objects

        Examples:
            >>> dev.wm()
            >>> dev.wm('sam*')
            >>> dev.wm('samx')
            >>> dev.wm(['samx', 'samy'])
            >>> dev.wm(dev.monitored_devices())
            >>> dev.wm(dev.get_devices_with_tags('user motors'))

        """
        if not device_names:
            device_names = self.values()
        else:
            expanded_devices = []
            if not isinstance(device_names, list):
                device_names = [device_names]
            if len(device_names) == 0:
                return

            for dev in device_names:
                if isinstance(dev, DeviceBase):
                    expanded_devices.append(dev)
                else:
                    devs = self._expand_device_name(dev)
                    expanded_devices.extend([self.__dict__[dev] for dev in devs])
            device_names = expanded_devices
        console = Console()
        table = Table()
        table.add_column("", justify="center")
        table.add_column("readback", justify="center")
        table.add_column("setpoint", justify="center")
        table.add_column("limits", justify="center")
        dev_read = {dev.name: dev.read(cached=True) for dev in device_names}
        readbacks = {}
        setpoints = {}
        limits = {}
        for dev in device_names:
            if hasattr(dev, "limits"):
                limits[dev.name] = str(dev.limits)
            else:
                limits[dev.name] = "[]"

        def _get_value_str(val):
            if not isinstance(val, str):
                try:
                    return f"{val:.4f}"
                except Exception:
                    return "N/A"
            return val

        for dev, read in dev_read.items():
            if dev in read:
                val = read[dev]["value"]
                readbacks[dev] = _get_value_str(val)
            else:
                readbacks[dev] = "N/A"

            if f"{dev}_setpoint" in read:
                val = read[f"{dev}_setpoint"]["value"]
                setpoints[dev] = _get_value_str(val)
            elif f"{dev}_user_setpoint" in read:
                val = read[f"{dev}_user_setpoint"]["value"]
                setpoints[dev] = _get_value_str(val)
            else:
                setpoints[dev] = "N/A"
        for dev in device_names:
            table.add_row(dev.name, readbacks[dev.name], setpoints[dev.name], limits[dev.name])
        console.print(table)

    def _add_device(self, name, obj) -> None:
        """
        Add device a new device to the device manager
        Args:
            name: name of the device
            obj: instance of the device

        Returns:

        """
        self[name] = obj

    def describe(self) -> list:
        """
        Describe all devices associated with the DeviceManager
        Returns:

        """
        return [dev.describe() for name, dev in self.devices.items()]

    def show_all(self, console: Console = None) -> None:
        """print all devices"""

        if console is None:
            console = Console()
        table = Table()
        table.add_column("Device", justify="center")
        table.add_column("Description", justify="center")
        table.add_column("Status", justify="center")
        table.add_column("ReadOnly", justify="center")
        table.add_column("SoftwareTrigger", justify="center")
        table.add_column("Device class", justify="center")
        table.add_column("Readout priority", justify="center")
        table.add_column("Device tags", justify="center")

        # pylint: disable=protected-access
        for dev in self.values():
            table.add_row(
                dev.name,
                dev._config.get("description", dev.name),
                "enabled" if dev.enabled else "disabled",
                str(dev.read_only),
                str(dev.software_trigger),
                dev._config.get("deviceClass"),
                dev._config.get("readoutPriority"),
                str(dev._config.get("deviceTags", [])),
            )
        console.print(table)

    def __str__(self) -> str:
        return "Device container."


@dataclass
class ScanInfo:
    """Dataclass to store the scan status message.

    A thin wrapper is needed to store the scan status message in a mutable object.
    Within the device_manager on the device server, the dataclass is passed to any
    ophyd device requesting scan_info in its signature.
    """

    msg: ScanStatusMessage | None = None


class DeviceManagerBase:

    def __init__(
        self, service: BECService, status_cb: list[Callable] | Callable | None = None
    ) -> None:
        self._use_proxy_objects = True
        self.devices = DeviceContainer()
        self._config = {}  # valid config
        self._session = {}
        self._request = None  # requested config
        self._request_config_parsed = None  # parsed config request
        self._response = None  # response message
        self._connector_base_register = {}
        self._device_cls = DeviceBase

        self._service = service
        self.parent = service  # for backwards compatibility; will be removed in the future
        self.connector = self._service.connector
        self.scan_info = ScanInfo(msg=None)
        self.config_helper = ConfigHelper(
            connector=self.connector, service_name=self._service._service_name, device_manager=self
        )
        self._status_cb = status_cb if isinstance(status_cb, list) else [status_cb]

    def initialize(self, bootstrap_server) -> None:
        """
        Initialize the DeviceManager by starting all connectors.
        Args:
            bootstrap_server: Redis server address, e.g. 'localhost:6379'

        Returns:

        """
        self._start_connectors(bootstrap_server)
        try:
            self._get_config()
        except DeviceConfigError as dev_conf_error:
            logger.error(f"Failed to initialize DeviceManager. {dev_conf_error}")

    def update_status(self, status: BECStatus):
        """Update the status of the device manager
        Args:
            status (BECStatus): New status
        """
        for cb in self._status_cb:
            if cb:
                cb(status)

    def parse_config_message(self, msg: DeviceConfigMessage) -> None:
        """
        Parse a config message and update the device config accordingly.

        Args:
            msg (DeviceConfigMessage): Config message

        """
        # pylint: disable=protected-access
        action = msg.content["action"]
        config = msg.content["config"]
        self.update_status(BECStatus.BUSY)
        if action == "update":
            for dev in config:
                if "deviceConfig" in config[dev]:
                    logger.info(f"Updating device config for device {dev}.")
                    self.devices[dev]._config["deviceConfig"].update(config[dev]["deviceConfig"])
                    logger.debug(
                        f"New config for device {dev}: {self.devices[dev]._config['deviceConfig']}"
                    )
                if "enabled" in config[dev]:
                    self.devices[dev]._config["enabled"] = config[dev]["enabled"]
                    if self.devices[dev].enabled:
                        dev_info_msg = self._get_device_info(dev)
                        self.devices[dev]._info = dev_info_msg.info.get("device_info", {})
                        if self._use_proxy_objects:
                            self.devices[dev]._parse_info()
                    status = "enabled" if self.devices[dev].enabled else "disabled"
                    logger.info(f"Device {dev} has been {status}.")
                if "readOnly" in config[dev]:
                    self.devices[dev]._config["readOnly"] = config[dev]["readOnly"]
                if "userParameter" in config[dev]:
                    self.devices[dev]._config["userParameter"] = config[dev]["userParameter"]
                if "onFailure" in config[dev]:
                    self.devices[dev]._config["onFailure"] = config[dev]["onFailure"]
                if "deviceTags" in config[dev]:
                    self.devices[dev]._config["deviceTags"] = config[dev]["deviceTags"]
                if "readoutPriority" in config[dev]:
                    self.devices[dev]._config["readoutPriority"] = config[dev]["readoutPriority"]
                if "softwareTrigger" in config[dev]:
                    self.devices[dev]._config["softwareTrigger"] = config[dev]["softwareTrigger"]

        elif action == "add":
            self._add_action(config)
        elif action == "reload":
            self._reload_action()
        elif action == "remove":
            self._remove_action(config)
        self.update_status(BECStatus.RUNNING)
        self._acknowledge_config_request(msg)
        if hasattr(self._service, "callbacks"):
            self._service.callbacks.run(EventType.DEVICE_UPDATE, action, config)

    def _acknowledge_config_request(self, msg: DeviceConfigMessage) -> None:
        """
        Acknowledge a config request by sending a response message.
        Args:
            msg (DeviceConfigMessage): Config message

        Returns:

        """
        if not msg.metadata.get("RID"):
            return
        self.connector.lpush(
            MessageEndpoints.service_response(msg.metadata["RID"]),
            ServiceResponseMessage(
                # pylint: disable=no-member
                response={"accepted": True, "service": self._service._service_name}
            ),
            expire=100,
        )

    def _add_action(self, config) -> None:
        if not self._use_proxy_objects:
            return
        self._add_multiple_devices_with_log(
            (dev_config, self._get_device_info(dev)) for dev, dev_config in config.items()
        )

    def _reload_action(self) -> None:
        if not self._use_proxy_objects:
            return
        logger.info("Reloading config.")
        self.devices.flush()
        self._get_config()

    def _remove_action(self, config) -> None:
        if not self._use_proxy_objects:
            return
        for dev in config:
            self._remove_device(dev)

    def _start_connectors(self, bootstrap_server) -> None:
        self._start_base_register()

    def _start_base_register(self) -> None:
        """
        Start consuming messages for all base topics. This method will be called upon startup.
        Returns:

        """
        self.connector.register(
            MessageEndpoints.device_config_update(),
            cb=self._device_config_update_callback,
            parent=self,
        )
        self.connector.register(
            MessageEndpoints.scan_status(), cb=self._update_scan_info, parent=self
        )

    @staticmethod
    def _log_callback(msg, *, parent, **kwargs) -> None:
        """
        Consumer callback for handling log messages.
        Args:
            cls: Reference to the DeviceManager instance
            msg: log message of type LogMessage
            **kwargs: Additional keyword arguments for the callback function

        Returns:

        """
        logger.info(f"Received log message: {str(msg)}")

    @staticmethod
    def _device_config_update_callback(msg, *, parent, **kwargs) -> None:
        """
        Consumer callback for handling new device configuration
        Args:
            cls: Reference to the DeviceManager instance
            msg: message of type DeviceConfigMessage

        Returns:

        """
        logger.info(f"Received new config: {str(msg)}")
        parent.parse_config_message(msg.value)

    @staticmethod
    def _update_scan_info(msg, *, parent, **kwargs) -> None:
        msg = msg.value
        logger.info(f"Received new ScanStatusMessage with ID {msg.scan_id}")
        parent.scan_info.msg = msg

    def _get_config(self, cancel_event: threading.Event | None = None) -> None:
        self._session["devices"] = self._get_redis_device_config()
        if not self._session["devices"]:
            logger.warning("No config available.")
        self._load_session(cancel_event=cancel_event)

    def _get_redis_device_config(self) -> list:
        devices = self.connector.get(MessageEndpoints.device_config())
        if not devices:
            return []
        return devices.content["resource"]

    def _add_multiple_devices_with_log(self, devices: Iterable[tuple[dict, DeviceInfoMessage]]):
        logs = (self._add_device(*conf_msg) for conf_msg in devices if conf_msg is not None)
        logger.info(f"Adding new devices:\n" + ", ".join(f"{name}: {t}" for name, t in logs))  # type: ignore # filtered

    def _add_device(self, dev: dict, msg: DeviceInfoMessage) -> tuple[str, str] | None:
        name = msg.content["device"]
        info = msg.content["info"]

        base_class = info["device_info"]["device_base_class"]
        class_name = info["device_info"]["device_class"]

        if base_class == (t := "device"):
            obj = Device(name=name, info=info, config=dev, parent=self, class_name=class_name)
        elif base_class == (t := "positioner"):
            obj = Positioner(name=name, info=info, config=dev, parent=self, class_name=class_name)
        elif base_class == (t := "signal"):
            obj = Signal(name=name, info=info, config=dev, parent=self, class_name=class_name)
        elif base_class == (t := "computed_signal"):
            obj = ComputedSignal(
                name=name, info=info, config=dev, parent=self, class_name=class_name
            )
        else:
            logger.error(f"Trying to add new device {name} of type {base_class}")
            return None

        set_device_config(obj, dev)
        try:
            self.devices._add_device(name, obj)
        except Exception:
            logger.error(f"Failed to load device {dev}: {traceback.format_exc()}")

        return (name, t)

    def _remove_device(self, dev_name):
        if dev_name in self.devices:
            self.devices.pop(dev_name)

    def _load_session(self, cancel_event: threading.Event | None = None, _device_cls=None):
        if self._is_config_valid():
            self._add_multiple_devices_with_log(
                (dev, self._get_device_info(dev.get("name"))) for dev in self._session["devices"]
            )

    def _get_device_info(self, device_name) -> DeviceInfoMessage:
        return self.connector.get(MessageEndpoints.device_info(device_name))

    def check_request_validity(self, msg: DeviceConfigMessage) -> None:
        """
        Check if the config request is valid.

        Args:
            msg (DeviceConfigMessage): Config message

        """
        if not isinstance(msg, DeviceConfigMessage):
            raise DeviceConfigError("Message must be of type DeviceConfigMessage.")
        if msg.content["action"] in ["update", "add", "remove", "set"]:
            if not msg.content["config"]:
                raise DeviceConfigError(
                    "Config cannot be empty for an action of type add, remove, set or update."
                )
            if not isinstance(msg.content["config"], dict):
                raise DeviceConfigError("Config must be of type dict.")
        if msg.content["action"] in ["update", "remove"]:
            for dev in msg.content["config"].keys():
                if dev not in self.devices:
                    raise DeviceConfigError(
                        f"Device {dev} does not exist and cannot be updated / removed."
                    )
        if msg.content["action"] == "add":
            for dev in msg.content["config"].keys():
                if dev in self.devices:
                    raise DeviceConfigError(f"Device {dev} already exists and cannot be added.")

    def _is_config_valid(self) -> bool:
        if self._config is None:
            return False
        if not isinstance(self._config, dict):
            return False
        return True

    @typechecked
    def get_bec_signals(
        self, signal_type: list[BECSignals] | BECSignals
    ) -> list[tuple[str, str, dict]]:
        """
        Get a list of BEC signals of the specified type.

        Args:
            signal_type (list[BECSignals] | BECSignals): Type of signal to retrieve.
        Supported types are "AsyncSignal", "AsyncMultiSignal", "DynamicSignal", "FileEventSignal", "PreviewSignal", and "ProgressSignal".

        Returns:
            list: List of tuples containing the device name, component name and the signal info.
        """
        signals = []
        if not isinstance(signal_type, list):
            signal_type = [signal_type]
        for device in self.devices.values():
            for comp, signal_info in device._info.get("signals", {}).items():
                if signal_info.get("signal_class") in signal_type:
                    signals.append((device.name, comp, signal_info))
        return signals

    def get_device_config_cached(
        self, update_signals: bool = True, exclude_defaults: bool = False
    ) -> dict:
        """
        Get the current device configuration from Redis. If update_signals is True,
        also update the config's signal values if they have changed.
        The result is cached based on the provided hash value.

        Args:
            update_signals (bool): Whether to update signal values in the config.
            exclude_defaults (bool): Whether to exclude default values in the config.
        Returns:
            dict: Device configuration.
        """

        ttl_hash = int(np.round(time.time() / 5))
        return self._get_device_config_cached_internal(
            ttl_hash=ttl_hash, update_signals=update_signals, exclude_defaults=exclude_defaults
        )

    @functools.lru_cache(maxsize=2)
    def _get_device_config_cached_internal(
        self, ttl_hash: int, update_signals: bool = True, exclude_defaults: bool = False
    ) -> dict:
        return self.get_device_config(
            update_signals=update_signals, exclude_defaults=exclude_defaults
        )

    def get_device_config(
        self, update_signals: bool = True, exclude_defaults: bool = False
    ) -> dict:
        """
        Get the current device configuration from Redis. If update_signals is True,
        also update the config's signal values if they have changed.

        Args:
            update_signals (bool): Whether to update signal values in the config.
            exclude_defaults (bool): Whether to exclude default values in the config.
        Returns:
            dict: Device configuration.
        """
        config_msg = self.connector.get(MessageEndpoints.device_config())
        config = {}
        if not config_msg or not isinstance(config_msg, AvailableResourceMessage):
            return config
        for dev_conf in config_msg.resource:
            dev_name = dev_conf.pop("name", None)
            if not dev_name:
                continue
            if update_signals:
                # If the device config specifies start values for signals, update them
                available_signal = self.devices[dev_name]._info.get("signals", {})
                device_config = dev_conf.get("deviceConfig", {})
                if not device_config:
                    continue
                for signal_name in device_config:
                    if signal_name in available_signal:
                        signal_obj_name = available_signal[signal_name].get("obj_name")
                        signal_obj = getattr(self.devices[dev_name], signal_name, None)
                        if not signal_obj or not signal_obj_name:
                            continue
                        sig_read = signal_obj.read(cached=True)
                        if sig_read is None:
                            continue
                        dev_conf["deviceConfig"][signal_name] = sig_read[signal_obj_name]["value"]

            config[dev_name] = _DeviceModelCore(**dev_conf).model_dump(
                exclude_defaults=exclude_defaults
            )
        return config

    def shutdown(self):
        """
        Shutdown all connectors.
        """
        self.connector.shutdown()

    def __del__(self):
        self.shutdown()
