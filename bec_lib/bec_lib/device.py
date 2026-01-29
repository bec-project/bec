"""
This module provides the DeviceBase class as well as its derived classes Signal, ComputedSignal, Positioner, and Device.
"""

from __future__ import annotations

import enum
import functools
import inspect
import threading
import time
import uuid
from collections import defaultdict, namedtuple
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterable

import numpy as np
from pydantic import ConfigDict
from rich.console import Console
from rich.table import Table
from typeguard import typechecked

from bec_lib.alarm_handler import AlarmBase, Alarms
from bec_lib.atlas_models import _DeviceModelCore
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.queue_items import QueueItem
from bec_lib.utils.import_utils import lazy_import

if TYPE_CHECKING:  # pragma: no cover
    from IPython.lib.pretty import PrettyPrinter

    from bec_lib import messages
    from bec_lib.client import BECClient
    from bec_lib.redis_connector import RedisConnector
else:
    # TODO: put back normal import when Pydantic gets faster
    messages = lazy_import("bec_lib.messages")

logger = bec_logger.logger
_MAX_RECURSION_DEPTH = 100


class RPCError(AlarmBase):
    """Exception raised when an RPC call fails."""

    def __init__(
        self, msg: messages.DeviceRPCMessage | messages.DeviceReqStatusMessage | str
    ) -> None:
        if isinstance(msg, messages.DeviceRPCMessage):
            if isinstance(msg.out, dict):
                msg.out = messages.ErrorInfo(**msg.out)
            alarm = messages.AlarmMessage(severity=Alarms.MAJOR, info=msg.out)
        elif isinstance(msg, messages.DeviceReqStatusMessage):
            alarm = messages.AlarmMessage(
                severity=Alarms.MAJOR,
                info=messages.ErrorInfo(
                    exception_type="RPCError",
                    error_message=f"Device request failure: {msg}",
                    compact_error_message=f"Device request for device {msg.device} resolved unsuccessfully.",
                    device=msg.device,
                ),
            )
        else:
            alarm = messages.AlarmMessage(
                severity=Alarms.MAJOR,
                info=messages.ErrorInfo(
                    exception_type="RPCError",
                    error_message=str(msg),
                    compact_error_message=str(msg),
                ),
            )
        super().__init__(alarm, Alarms.MAJOR, handled=False)


class ScanRequestError(Exception):
    """Exception raised when a scan request is rejected."""


def rpc(fcn):
    """Decorator to perform rpc calls."""

    @functools.wraps(fcn)
    def wrapper(self, *args, **kwargs):
        # pylint: disable=protected-access
        return self._run(*args, fcn=fcn, **kwargs)

    return wrapper


class DeviceStatus(enum.Enum):
    """Device status"""

    IDLE = 0
    RUNNING = 1
    BUSY = 2


class OnFailure(str, enum.Enum):
    """On failure behaviour for devices"""

    RAISE = "raise"
    BUFFER = "buffer"
    RETRY = "retry"


class ReadoutPriority(str, enum.Enum):
    """Readout priority for devices"""

    ON_REQUEST = "on_request"
    BASELINE = "baseline"
    MONITORED = "monitored"
    ASYNC = "async"
    CONTINUOUS = "continuous"


class Status:
    """
    Status object for RPC calls
    """

    def __init__(self, connector: RedisConnector, RID: str) -> None:
        """
        Status object for RPC calls

        Args:
            connector (RedisConnector): Redis connector
            RID (str): Request ID
        """
        self._connector = connector
        self._request_id = RID
        self._status_done = threading.Event()
        self._device_req_thread = None
        self._request_status: messages.DeviceReqStatusMessage | None = None
        self._connector.register(
            MessageEndpoints.device_req_status(self._request_id),
            cb=self._on_status_update,
            parent=self,
            from_start=True,
        )

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, Status):
            return self._request_id == __value._request_id
        return False

    @staticmethod
    def _on_status_update(msg: dict[str, messages.DeviceReqStatusMessage], parent: Status):
        # pylint: disable=protected-access
        parent._request_status = msg["data"]
        parent._set_done()

    def _set_done(self):
        self._status_done.set()
        self._connector.unregister(
            MessageEndpoints.device_req_status(self._request_id), cb=self._on_status_update
        )

    def wait(self, timeout=None, raise_on_failure=True):
        """
        Wait for the request to complete. If the request is not completed within the specified time,
        a TimeoutError is raised.

        Args:
            timeout (float, optional): Timeout in seconds. Defaults to None. If None, the method waits indefinitely.
            raise_on_failure (bool, optional): If True, an RPCError is raised if the request fails. Defaults to True.
        """
        try:

            if not self._status_done.wait(timeout):
                raise TimeoutError("The request has not been completed within the specified time.")
        finally:
            self._set_done()

        if not raise_on_failure or self._request_status is None:
            return

        if self._request_status is not None and not self._request_status.success:
            raise RPCError(self._request_status)


class _PermissiveDeviceModel(_DeviceModelCore):
    model_config = ConfigDict(extra="allow")


def set_device_config(device: "DeviceBase", config: dict | _PermissiveDeviceModel | None):
    # device._config = config
    device._config = (  # pylint: disable=protected-access
        _PermissiveDeviceModel.model_validate(config).model_dump() if config is not None else None
    )


class DeviceBase:
    """
    The DeviceBase class is the base class for all devices that are controlled via
    the DeviceManager. It provides a simple interface to perform RPC calls on the
    device. The DeviceBase class is not meant to be used directly, but rather to be subclassed.
    """

    def __init__(
        self,
        *,
        name: str,
        info: dict = None,
        config: dict | _PermissiveDeviceModel | None = None,
        parent=None,
        signal_info: dict = None,
        class_name: str | None = None,
    ) -> None:
        """
        Args:
            name (dict): The name of the device.
            info (dict, optional): The device info dictionary. Defaults to None.
            parent ([type], optional): The parent object. Defaults to None.
            signal_info (dict, optional): The signal info dictionary. Defaults to None.
        """
        self.name = name
        self._class_name = class_name or object.__getattribute__(self, "__class__").__name__
        self._signal_info = signal_info
        set_device_config(self, config)
        if info is None:
            info = {}
        self._info = info.get("device_info", {})
        self.parent = parent
        self._custom_rpc_methods = {}
        self._property_container = set()
        if self._info:
            self._parse_info()

        # the following lambda is needed to support customized RPC methods with
        # doc strings and function signatures.
        # pylint: disable=unnecessary-lambda
        self.run = lambda *args, **kwargs: self._run(*args, **kwargs)

    def _run(self, *args, fcn=None, cached=False, **kwargs):
        device, func_call = self._get_rpc_func_name(fcn=fcn)

        if cached:
            return fcn(self, *args, **kwargs)
        return self._run_rpc_call(device, func_call, *args, **kwargs)

    def __getattribute__(self, name: str) -> Any:
        if name in object.__getattribute__(self, "_property_container"):
            caller_frame = inspect.currentframe().f_back
            while caller_frame:
                if "jedi" in caller_frame.f_globals:
                    # Jedi module is present, likely tab completion
                    return object.__getattribute__(self, name)
                caller_frame = caller_frame.f_back
            return object.__getattribute__(self, "_run")(fcn=name, cached=False)
        return object.__getattribute__(self, name)

    def __getattr__(self, name: str) -> Any:
        if name in self._property_container:
            return object.__getattribute__(self, "_run")(fcn=name, cached=False)
        return object.__getattribute__(self, name)

    def __setattr__(self, name: str, value: Any) -> None:
        try:
            container = object.__getattribute__(self, "_property_container")
        except AttributeError:
            return super().__setattr__(name, value)

        if name in container:
            return object.__getattribute__(self, "_run")(
                value, fcn=name, cached=False, _set_property=True
            )

        super().__setattr__(name, value)

    @property
    def _hints(self):
        hints = self._info.get("hints")
        if not hints:
            return []
        return hints.get("fields", [])

    @property
    def root(self):
        """Returns the root object of the device tree."""
        return self._get_root_recursively(self)[0]

    def _prepare_rpc_msg(
        self, rpc_id: str, request_id: str, device: str, func_call: str, *args, **kwargs
    ) -> messages.ScanQueueMessage:
        """
        Prepares an RPC message.

        Args:
            rpc_id (str): The RPC ID.
            request_id (str): The request ID.
            device (str): The device name.
            func_call (str): The function call.
            *args: The function arguments.
            **kwargs: The function keyword arguments.

        Returns:
            messages.ScanQueueMessage: The RPC message.
        """

        params = {
            "device": device,
            "rpc_id": rpc_id,
            "func": func_call,
            "args": args,
            "kwargs": kwargs,
        }
        client: BECClient = self.root.parent.parent
        msg = messages.ScanQueueMessage(
            scan_type="device_rpc",
            parameter=params,
            queue=client.queue.get_default_scan_queue(),  # type: ignore
            metadata={"RID": request_id, "response": True},
        )
        return msg

    def _handle_rpc_response(self, msg: messages.DeviceRPCMessage) -> Any:
        if not msg.content["success"]:
            error = msg.content["out"]
            if not isinstance(error, dict):
                error = {"error": "Exception", "msg": error, "traceback": ""}
            raise RPCError(msg)
        if msg.content.get("out"):
            print(msg.content.get("out"))
        return_val = msg.content.get("return_val")
        if not isinstance(return_val, dict):
            return return_val
        if return_val.get("type") == "status" and return_val.get("RID"):
            return Status(self.root.parent.connector, return_val.get("RID"))
        return return_val

    def _get_rpc_response(self, request_id, rpc_id) -> Any:
        queue = self.root.parent.parent.queue
        while queue.request_storage.find_request_by_ID(request_id) is None:
            self.root.parent.parent.alarm_handler.raise_alarms()
            time.sleep(0.01)
        scan_queue_request = queue.request_storage.find_request_by_ID(request_id)
        while scan_queue_request.decision_pending:
            self.root.parent.parent.alarm_handler.raise_alarms()
            time.sleep(0.01)
        if not all(scan_queue_request.accepted):
            raise ScanRequestError(
                "Function call was rejected by the server:"
                f" {scan_queue_request.response.content['message']}"
            )
        while True:
            msg = self.root.parent.connector.get(MessageEndpoints.device_rpc(rpc_id))
            if msg:
                break
            self.root.parent.parent.alarm_handler.raise_alarms()
            self._handle_client_info_msg()
            time.sleep(0.01)
        return self._handle_rpc_response(msg)

    def _handle_client_info_msg(self):
        """Handle client messages during RPC calls"""
        msgs = self.root.parent.connector.xread(MessageEndpoints.client_info(), block=200)
        # The client is the parent.parent of the device
        client: BECClient = self.root.parent.parent
        if client.live_updates_config.print_client_messages is False:
            return
        if not msgs:
            return
        for msg in msgs:
            print(QueueItem.format_client_msg(msg["data"]))

    def _run_rpc_call(self, device, func_call, *args, wait_for_rpc_response=True, **kwargs) -> Any:
        """
        Runs an RPC call on the device. This method is used internally by the RPC decorator.
        If a call is interrupted by the user, the a stop signal is sent to this device.

        Args:
            device (str): The device name.
            func_call (str): The function call.
            *args: The function arguments.
            wait_for_rpc_response (bool, optional): If True, the method waits for the RPC response. Defaults to True.
            **kwargs: The function keyword arguments.

        Returns:
            Any: The return value of the RPC call.
        """

        try:
            client: BECClient = self.root.parent.parent
            self._validate_rpc_client()

            # clear the alarm stack
            client.alarm_handler.clear()  # type: ignore

            # prepare RPC message
            rpc_id = str(uuid.uuid4())
            request_id = str(uuid.uuid4())
            msg = self._prepare_rpc_msg(rpc_id, request_id, device, func_call, *args, **kwargs)

            # pylint: disable=protected-access
            if client.scans._scan_def_id:
                msg.metadata["scan_def_id"] = client.scans._scan_def_id
            # send RPC message
            client.connector.send(MessageEndpoints.scan_queue_request(), msg)

            # wait for RPC response
            if not wait_for_rpc_response:
                return None
            return_val = self._get_rpc_response(request_id, rpc_id)
        except KeyboardInterrupt as exc:
            self.root.stop(wait_for_rpc_response=False)
            raise RPCError("User interruption during RPC call.") from exc

        return return_val

    def _validate_rpc_client(self) -> None:
        """
        Check if the device can run RPC calls.

        Raises:
            RPCError: If the device cannot run RPC calls.
        """
        # avoid circular imports
        # only needed here for type checking
        from bec_lib.client import BECClient  # pylint: disable=import-outside-toplevel

        client: BECClient = self.root.parent.parent
        if not isinstance(client, BECClient):
            raise RPCError(
                "RPC calls can only be made from a BECClient instance. Either use a"
                " properly configured BECClient or set 'cached=True' to avoid an RPC call."
            )

        if client.alarm_handler is None:
            raise RPCError("RPC calls require an alarm handler to be set in the BECClient.")

    def _get_rpc_func_name(self, fcn=None, use_parent=False):
        func_call = [self._compile_function_path(use_parent=use_parent)]

        if fcn:
            if isinstance(fcn, str):
                func_call.append(fcn)
            else:
                func_call.append(fcn.__name__)

        full_func_call = ".".join(func_call)
        device = full_func_call.split(".", maxsplit=1)[0]
        func_call = ".".join(full_func_call.split(".")[1:])
        return (device, func_call)

    def _compile_function_path(self, use_parent=False) -> str:
        if use_parent:
            parent = self.parent
        else:
            parent = self
        parent, func_call = self._get_root_recursively(parent)
        func_call.append(parent.name)
        return ".".join(func_call[::-1])

    def _get_root_recursively(self, parent) -> tuple[Any, list]:
        # pylint: disable=import-outside-toplevel
        from bec_lib.devicemanager import DeviceManagerBase

        max_depth = _MAX_RECURSION_DEPTH
        func_call = []

        while not isinstance(parent.parent, DeviceManagerBase):
            func_call.append(parent.name)
            if parent.parent is None:
                return (parent, func_call)
            parent = parent.parent
            max_depth -= 1
            if max_depth == 0:
                logger.error("Max depth reached in device tree.")
                raise RecursionError("Max depth reached in device tree.")
        return (parent, func_call)

    def _parse_info(self):
        if self._info.get("signals"):
            for signal_name, signal_info in self._info.get("signals", {}).items():
                if (
                    not signal_info.get("describe", {})
                    .get("signal_info", {})
                    .get("rpc_access", True)
                ):
                    continue

                setattr(
                    self,
                    signal_name,
                    Signal(name=signal_name, signal_info=signal_info, parent=self),
                )
                precision = (
                    self._info.get("describe", {})
                    .get(f"{self.name}_{signal_name}", {})
                    .get("precision")
                )
                if precision is not None:
                    getattr(self, signal_name).precision = precision
        precision = self._info.get("describe", {}).get(self.name, {}).get("precision")
        if precision is not None:
            self.precision = precision
        if self._info.get("sub_devices"):
            for dev in self._info.get("sub_devices"):
                base_class = dev["device_info"].get("device_base_class")
                attr_name = dev["device_info"].get("device_attr_name")
                if base_class == "positioner":
                    setattr(self, attr_name, Positioner(name=attr_name, info=dev, parent=self))
                elif base_class == "device":
                    setattr(self, attr_name, Device(name=attr_name, info=dev, parent=self))

        for user_access_name, descr in self._info.get("custom_user_access", {}).items():
            # avoid circular imports as the signature serializer imports the DeviceBase class
            # pylint: disable=import-outside-toplevel
            from bec_lib.signature_serializer import dict_to_signature

            if "type" in descr:
                if descr["type"] == "func":
                    self._custom_rpc_methods[user_access_name] = DeviceBase(
                        name=user_access_name, info=descr, parent=self
                    )
                    setattr(self, user_access_name, self._custom_rpc_methods[user_access_name].run)
                    setattr(getattr(self, user_access_name), "__doc__", descr.get("doc"))
                    setattr(
                        getattr(self, user_access_name),
                        "__signature__",
                        dict_to_signature(descr.get("signature")),
                    )
                else:
                    # only update the property container if the user access name is not already in it
                    # otherwise, we would run an RPC call instead of accessing the property
                    if user_access_name not in self._property_container:
                        setattr(self, user_access_name, user_access_name)
                        self._property_container.add(user_access_name)
            else:
                self._custom_rpc_methods[user_access_name] = DeviceBase(
                    name=user_access_name,
                    info={"device_info": {"custom_user_access": descr["info"]}},
                    parent=self,
                    class_name=descr["device_class"],
                )
                setattr(self, user_access_name, self._custom_rpc_methods[user_access_name])

    def __eq__(self, other):
        if isinstance(other, DeviceBase):
            return other.name == self.name
        return False

    def __hash__(self):
        return self.name.__hash__()

    def __str__(self):
        """Simple string representation for non-IPython contexts"""
        # pylint: disable=import-outside-toplevel
        from bec_lib.devicemanager import DeviceManagerBase

        class_name = self._class_name
        if isinstance(self.parent, DeviceManagerBase):
            return f"{class_name}(name={self.name}, enabled={self.root.enabled})"
        return f"{class_name}(name={self.name}, root_device={self.root.name}, enabled={self.root.enabled})"

    def _repr_pretty_(self, p: PrettyPrinter, cycle: bool):
        """
        Pretty print for IPython contexts. The _repr_pretty_ method is called by IPython
        to display the object in a pretty way. Any other tool will simply use the __str__
        or __repr__ methods.

        Since IPython does not provide a direct support for rich, we compile a rich
        formatted string and pass it to the pretty printer.

        Args:
            p (PrettyPrinter): The pretty printer
            cycle (bool): Whether or not we're in a cycle
        """
        if cycle:
            p.text(f"{self._class_name}(...)")
            return

        # Use simple representation if we're inside a container (list, dict, etc.)
        # The pretty printer provides this through the 'p' object
        if hasattr(p, "indentation") and p.indentation > 0:
            p.text(str(self))
            return

        result = self._compile_rich_str(self)
        if result:
            p.text(result)
        else:
            # Fallback to simple representation
            p.text(str(self))

    @staticmethod
    def _compile_rich_str(obj: DeviceBase) -> str | None:
        """Compile rich formatted string for IPython display"""
        # pylint: disable=import-outside-toplevel
        # avoid circular imports
        from bec_lib.devicemanager import DeviceManagerBase

        if not isinstance(obj.parent, DeviceManagerBase):
            return None

        class_name = obj._class_name

        console = Console()

        # Create main table
        table = Table(title=f"{class_name}: {obj.name}", show_header=False, box=None)
        table.add_column("Property", style="cyan", no_wrap=True)
        table.add_column("Value", style="white")

        # Basic info
        table.add_row("Enabled", str(obj.root.enabled))
        table.add_row("Description", str(obj._config.get("description", obj.name)))
        table.add_row("Read only", str(obj.read_only))
        table.add_row("Software Trigger", str(obj.root.software_trigger))
        table.add_row("Device class", str(obj._config.get("deviceClass", "N/A")))
        table.add_row("Readout Priority", str(obj._config.get("readoutPriority", "N/A")))

        if obj._config.get("deviceTags"):
            tags = ", ".join(obj._config.get("deviceTags", []))
            table.add_row("Device tags", tags)

        if obj._config.get("userParameter"):
            table.add_row("User parameter", str(obj._config.get("userParameter")))

        if hasattr(obj, "limits"):
            table.add_row("Limits", str(obj.limits))

        # Get the updated device config. We use the cached version to avoid
        # excessive calls to Redis.
        device_config = (
            obj.parent.get_device_config_cached().get(obj.name, {}).get("deviceConfig", {})
        )
        # Filter down to only config signals
        config_signals = [
            sig_name
            for sig_name, sig in obj._info.get("signals", {}).items()
            if sig.get("kind_str") == "config"
        ]
        device_config = {k: v for k, v in device_config.items() if k in config_signals}

        # Get current values
        current_values = obj.read(cached=True)

        # Render main table to string
        with console.capture() as capture:
            console.print(table)
        output = capture.get()

        # Add current values section if available
        if current_values:
            value_table = Table(title="Current Values", show_header=True, box=None)
            value_table.add_column("Signal", style="cyan")
            value_table.add_column("Value", style="yellow")
            value_table.add_column("Timestamp", style="dim")

            for signal_name, signal_data in current_values.items():
                value = signal_data.get("value")
                timestamp = signal_data.get("timestamp")

                # Format value (handle numpy arrays)
                if isinstance(value, np.ndarray):
                    with np.printoptions(precision=4, suppress=True, threshold=10):
                        value_str = f"{str(value)}, shape={value.shape}, dtype={value.dtype}"
                else:
                    value_str = str(value)

                # Format timestamp
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp)
                    timestamp_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    timestamp_str = "N/A"

                value_table.add_row(signal_name, value_str, timestamp_str)

            with console.capture() as capture:
                console.print(value_table)
            output += "\n" + capture.get()

        # Add config signals section if available
        if device_config:
            config_table = Table(title="Config Signals", show_header=True, box=None)
            config_table.add_column("Signal", style="cyan")
            config_table.add_column("Value", style="green")

            for key, val in device_config.items():
                # Format value (handle numpy arrays)
                if isinstance(val, np.ndarray):
                    with np.printoptions(precision=4, suppress=True, threshold=10):
                        val_str = f"{str(val)}, shape={val.shape}, dtype={val.dtype}"
                else:
                    val_str = str(val)
                config_table.add_row(key, val_str)

            with console.capture() as capture:
                console.print(config_table)
            output += "\n" + capture.get()

        return output


class DeviceBaseWithConfig(DeviceBase):

    @property
    def full_name(self):
        """Returns the full name of the device or signal, separated by "_" e.g. samx_velocity"""
        return self._compile_function_path().replace(".", "_")

    @property
    def dotted_name(self):
        """Returns the full dotted name of the device or signal e.g. samx.velocity"""
        return self._compile_function_path()

    @property
    def enabled(self):
        """Returns True if the device is enabled, otherwise False."""
        # pylint: disable=protected-access
        return self.root._config["enabled"]

    @enabled.setter
    def enabled(self, val):
        # pylint: disable=protected-access
        self._update_config({"enabled": val})
        self.root._config["enabled"] = val

    def _update_config(self, update: dict) -> None:
        """
        Updates the device configuration.

        Args:
            update (dict): The update dictionary.

        """
        self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: update}
        )

    def get_device_tags(self) -> list:
        """get the device tags for this device"""
        # pylint: disable=protected-access
        return self.root._config.get("deviceTags", [])

    @typechecked
    def set_device_tags(self, val: Iterable):
        """set the device tags for this device - any duplicates will be discarded"""
        # pylint: disable=protected-access
        self.root._config["deviceTags"] = set(val)
        return self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"deviceTags": self.root._config["deviceTags"]}}
        )

    @typechecked
    def add_device_tag(self, val: str):
        """add a device tag for this device"""
        # pylint: disable=protected-access
        self.root._config["deviceTags"].add(val)
        return self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"deviceTags": self.root._config["deviceTags"]}}
        )

    def remove_device_tag(self, val: str):
        """remove a device tag for this device"""
        # pylint: disable=protected-access
        self.root._config["deviceTags"].remove(val)
        return self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"deviceTags": self.root._config["deviceTags"]}}
        )

    @property
    def wm(self) -> None:
        """get the current position of a device"""
        self.parent.devices.wm(self.name)

    @property
    def readout_priority(self) -> ReadoutPriority:
        """get the readout priority for this device"""
        # pylint: disable=protected-access
        return ReadoutPriority(self.root._config["readoutPriority"])

    @readout_priority.setter
    def readout_priority(self, val: ReadoutPriority):
        """set the readout priority for this device"""
        if not isinstance(val, ReadoutPriority):
            val = ReadoutPriority(val)
        # pylint: disable=protected-access
        self.root._config["readoutPriority"] = val
        return self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"readoutPriority": val}}
        )

    @property
    def on_failure(self) -> OnFailure:
        """get the failure behaviour for this device"""
        # pylint: disable=protected-access
        return OnFailure(self.root._config.get("onFailure", "retry"))

    @on_failure.setter
    def on_failure(self, val: OnFailure):
        """set the failure behaviour for this device"""
        if not isinstance(val, OnFailure):
            val = OnFailure(val)
        # pylint: disable=protected-access
        self.root._config["onFailure"] = val
        return self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"onFailure": self.root._config["onFailure"]}}
        )

    @property
    def read_only(self):
        """Whether or not the device can be set"""
        # pylint: disable=protected-access
        return self.root._config.get("readOnly", False)

    @read_only.setter
    def read_only(self, value: bool):
        """Whether or not the device is read only"""
        # pylint: disable=protected-access
        self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"readOnly": value}}
        )
        self.root._config["readOnly"] = value

    @property
    def software_trigger(self):
        """Whether or not the device can be software triggered"""
        # pylint: disable=protected-access
        return self.root._config.get("softwareTrigger", False)

    @software_trigger.setter
    def software_trigger(self, value: bool):
        """Whether or not the device can be software triggered"""
        # pylint: disable=protected-access
        self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"softwareTrigger": value}}
        )
        self.root._config["softwareTrigger"] = value

    @property
    def user_parameter(self) -> dict:
        """get the user parameter for this device"""
        # pylint: disable=protected-access
        return self.root._config.get("userParameter", {})

    @typechecked
    def set_user_parameter(self, val: dict):
        """set the user parameter for this device"""
        self.root.parent.config_helper.send_config_request(
            action="update", config={self.name: {"userParameter": val}}
        )

    @typechecked
    def update_user_parameter(self, val: dict):
        """update the user parameter for this device
        Args:
            val (dict): New user parameter
        """
        param = self.user_parameter
        if param is None:
            param = {}
        param.update(val)
        self.set_user_parameter(param)


class OphydInterfaceBase(DeviceBaseWithConfig):
    @rpc
    def trigger(self, rpc_id: str):
        """
        Triggers the device.
        """

    def read(
        self, cached=False, use_readback=True, filter_to_hints=False
    ) -> dict[str, dict[str, Any]] | None:
        """
        Reads the device.

        Args:
            cached (bool, optional): If True, the cached value is returned. Defaults to False.
            use_readback (bool, optional): If True, the readback value is returned, otherwise the read value. Defaults to True.
            filter_to_hints (bool, optional): If True, the readback value is filtered to the hinted values. Defaults to False.

        Returns:
            dict: The device signals.
        """
        _, is_config_signal, cached = self._get_rpc_signal_info(cached)

        if not cached:
            signals = self._run(cached=cached, fcn=self.read)
        else:
            if is_config_signal:
                return self.read_configuration(cached=cached)
            if use_readback:
                val = self.root.parent.connector.get(
                    MessageEndpoints.device_readback(self.root.name)
                )
            else:
                val = self.root.parent.connector.get(MessageEndpoints.device_read(self.root.name))

            if not val:
                return None
            signals = val.content["signals"]
        if filter_to_hints:
            signals = {key: val for key, val in signals.items() if key in self._hints}
        return self._filter_rpc_signals(signals)

    def read_configuration(self, cached=False) -> dict[str, dict[str, Any]] | None:
        """
        Reads the device configuration.

        Args:
            cached (bool, optional): If True, the cached value is returned. Defaults to False.
        """

        is_signal, is_config_signal, cached = self._get_rpc_signal_info(cached)

        if not cached:
            fcn = self.read_configuration if (not is_signal or is_config_signal) else self.read
            signals = self._run(cached=False, fcn=fcn)
        else:
            if is_signal and not is_config_signal:
                return self.read(cached=True)

            val = self.root.parent.connector.get(
                MessageEndpoints.device_read_configuration(self.root.name)
            )
            if not val:
                return None
            signals = val.content["signals"]

        return self._filter_rpc_signals(signals)

    def _filter_rpc_signals(self, signals: dict) -> dict:
        if self._signal_info:
            obj_name = self._signal_info.get("obj_name")
            return {obj_name: signals.get(obj_name, {})}
        return {key: val for key, val in signals.items() if key.startswith(self.full_name)}

    def _get_rpc_signal_info(self, cached: bool):
        is_config_signal = False
        is_signal = self._signal_info is not None
        if is_signal:
            kind = self._signal_info.get("kind_str")
            if kind == "config":
                is_config_signal = True
            elif kind == "omitted":
                cached = False
        return is_signal, is_config_signal, cached

    def describe(self):
        """
        Describes the device and yields information about the device's signals, including
        the signal's name, source, shape, data type, precision etc.
        """
        return self._info.get("describe", {})

    def describe_configuration(self):
        """Describes the device configuration."""
        return self._info.get("describe_configuration", {})

    def get(self, cached=False) -> Any:
        """
        Gets the device value.

        Args:
            cached (bool, optional): If True, the cached value is returned. Defaults to False
        """

        is_signal = self._signal_info is not None
        if not cached or not is_signal:
            res = self._run(cached=False, fcn=self.get)
            if isinstance(res, dict) and res.get("type") == "namedtuple":
                par = namedtuple(self.name, res.get("fields"))
                return par(**res.get("values"))
            return res

        ret = self.read(cached=cached)
        if ret is None:
            return None
        return ret.get(self._signal_info.get("obj_name"), {}).get("value")

    def put(self, value: Any, wait=False):
        """
        Puts the device value. In contrast to the set method, the put method does not
        return a status object and does not wait for the device to settle. However,
        the put method can be converted into a blocking call by setting the wait argument to True,
        which will be equivalent to the set method + wait.

        Use the put method if you want to update a value without waiting for the device to settle.
        Use the set method if you want to update a value and wait for the device to settle.

        Args:
            value (Any): The value to put.
            wait (bool, optional): If True, the method waits for the device to settle. Defaults to False.
        """
        if wait:
            return self.set(value).wait()
        self._run(value, fcn=self.put)
        return


class Device(OphydInterfaceBase):
    """
    Device (bluesky interface):
    * trigger
    * read
    * describe
    * stage
    * unstage
    * pause
    * resume
    """

    @rpc
    def configure(self, config: dict):
        """
        Configures the device.
        """

    @rpc
    def stop(self):
        """
        Stops the device.
        """

    @rpc
    def stage(self):
        """
        Stages the device. This method should normally not be called directly, but rather
        via the scan server.
        """

    @rpc
    def unstage(self):
        """
        Unstages the device. This method should normally not be called directly, but rather
        via the scan server.
        """

    def summary(self):
        """
        Provides a summary of the device, all associated signals and their type.
        """
        table = Table(title=f"{self.name} - Summary of Available Signals")
        table.add_column("Name")
        table.add_column("Data Name")
        table.add_column("Kind")
        table.add_column("Source")
        table.add_column("Type")
        table.add_column("Description")
        signals = self._info.get("signals", {})
        signals_grouped = defaultdict(dict)
        for kind in ["hinted", "normal", "config", "omitted"]:
            signals_grouped[kind] = {
                signal_name: signal_info
                for signal_name, signal_info in signals.items()
                if kind in signal_info.get("kind_str")
            }
        kind_added = False
        bec_signals = []
        for kind in ["hinted", "normal", "config", "omitted"]:
            if kind_added:
                table.add_row()

            for signal_name, signal_info in signals_grouped[kind].items():
                if (
                    signal_info.get("describe", {})
                    .get("source", "")
                    .startswith("BECMessageSignal:")
                ):
                    bec_signals.append((signal_name, signal_info))
                    continue

                table.add_row(
                    signal_name,
                    signal_info.get("obj_name"),
                    signal_info.get("kind_str"),
                    signal_info.get("describe", {}).get("source"),
                    signal_info.get("describe", {}).get("dtype"),
                    signal_info.get("doc"),
                )
                kind_added = True
        if bec_signals:
            table.add_row()
            table.add_section()
            table.add_row("BECMessageSignals", "", "", "", "", "", style="bold")
            table.add_section()
            for signal_name, signal_info in bec_signals:
                table.add_row(
                    signal_name,
                    signal_info.get("obj_name"),
                    signal_info.get("kind_str"),
                    signal_info.get("describe", {}).get("source"),
                    signal_info.get("describe", {}).get("dtype"),
                    signal_info.get("doc"),
                )

        console = Console()
        console.print(table)


class AdjustableMixin:
    @rpc
    def set(self, val: Any) -> Status:
        """
        Sets the device value. This method returns a status object that can be used to wait for the device to settle.
        In contrast to the put method, the set method can be made into a blocking call by calling the wait method on the
        returned status object.

        Args:
            val (Any): The value to set.
        """

    @property
    def limits(self):
        """
        Returns the device limits.
        """
        limit_msg = self.root.parent.connector.get(MessageEndpoints.device_limits(self.root.name))
        if not limit_msg:
            return [0, 0]
        limits = [
            limit_msg.content["signals"].get("low", {}).get("value", 0),
            limit_msg.content["signals"].get("high", {}).get("value", 0),
        ]
        return limits

    @limits.setter
    def limits(self, val: list):
        self._update_config({"deviceConfig": {"limits": val}})

    @property
    def low_limit(self):
        """
        Returns the low limit.
        """
        return self.limits[0]

    @low_limit.setter
    def low_limit(self, val: float):
        limits = [val, self.high_limit]
        self._update_config({"deviceConfig": {"limits": limits}})

    @property
    def high_limit(self):
        """
        Returns the high limit.
        """
        return self.limits[1]

    @high_limit.setter
    def high_limit(self, val: float):
        limits = [self.low_limit, val]
        self._update_config({"deviceConfig": {"limits": limits}})


class Signal(AdjustableMixin, OphydInterfaceBase):
    pass


class ComputedSignal(Signal):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._num_args_method = None
        self._num_signals = None
        self._header = "# Do not update the method manually. This method was set via the set_compute_method function.\n"

    def set_compute_method(self, method: Callable) -> None:
        """Set the compute method for the ComputedSignal.

        We allow users to use two additional packages which are imported to simplify computations:
        - numpy as np
        - scipy as sp

        Args:
            method (callable) : Method to execute as computation method, i.e.
            def calculate_readback(signal):
            ...:     return -1 * signal.get()

        >>> dev.<dev_name>.set_compute_method(calculate_readback)

        """
        if not callable(method):
            raise ValueError("The compute method must be callable.")

        # check if it is a bound method
        if hasattr(method, "__self__") and method.__self__ is not None:
            raise ValueError("The compute method must be an unbound function, not a bound method.")

        # check if it is a lambda function
        if method.__name__ == "<lambda>":
            raise ValueError("The compute method cannot be a lambda function.")

        method_code = inspect.getsource(method)
        self._num_args_method = len(inspect.signature(method).parameters)

        self._update_config({"deviceConfig": {"compute_method": self._header + method_code}})
        if self._num_signals is None:
            return
        if self._num_args_method != self._num_signals:
            print(
                f"Warning: The number of input signals ({self._num_signals}) does not match "
                f"the number of arguments in the compute method ({self._num_args_method}). Please update the input signals accordingly."
            )

    def set_input_signals(self, *signals) -> None:
        """Set input signals for compute method.
        The signals need to be valid signals and match the computation of the compute method.

        Args:
            signals : All signals to be added for the comput method

        >>> dev.<dev_name>.set_input_signals(dev.bpm4i.readback, dev.bpm5i.readback)
        """
        if not signals:
            self._update_config({"deviceConfig": {"input_signals": []}})
            return
        if not all(isinstance(signal, Signal) for signal in signals):
            raise ValueError("All input signals must be of type Signal.")
        signals = [signal.dotted_name for signal in signals]
        self._update_config({"deviceConfig": {"input_signals": signals}})
        self._num_signals = len(signals)
        if self._num_args_method is None:
            return
        if self._num_args_method != self._num_signals:
            print(
                f"Warning: The number of input signals ({self._num_signals}) does not match "
                f"the number of arguments in the compute method ({self._num_args_method}). Please update the compute method accordingly."
            )

    def show_all(self):
        """Show all information about the ComputedSignal."""
        console = Console()
        table = Table(title=f"{self.name} - ComputedSignal Information")
        table.add_column("Property")
        table.add_column("Value")

        config = self._config.get("deviceConfig", {})
        compute_method = config.get("compute_method", "Not set")
        input_signals = config.get("input_signals", [])

        compute_method = compute_method.replace(self._header, "").strip()

        table.add_row("Compute Method", compute_method if compute_method else "Not set")
        table.add_row(
            "Input Signals", ", ".join(input_signals) if input_signals else "No input signals set"
        )

        console.print(table)


class Positioner(AdjustableMixin, Device):
    """
    Positioner:
    * trigger
    * read
    * set
    * stop
    * settle_time
    * timeout
    * egu
    * limits
    * low_limit
    * high_limit
    * move
    * position
    * moving
    """

    def stop(self):
        msg = messages.VariableMessage(value=[self.root.name])
        self.root.parent.parent.connector.send(MessageEndpoints.stop_devices(), msg)

    @rpc
    def settle_time(self):
        pass

    @rpc
    def timeout(self):
        pass

    def egu(self):
        return self.describe_configuration().get("egu")

    def move(self, val: float, relative=False):
        return self.root.parent.parent.scans.mv(self, val, relative=relative)

    @property
    @rpc
    def position(self):
        pass

    @rpc
    def moving(self):
        pass
