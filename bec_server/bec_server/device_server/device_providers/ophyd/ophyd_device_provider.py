"""Device construction and lifecycle support for Ophyd devices."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING, Any

import ophyd
import ophyd_devices as opd

from bec_lib import messages, plugin_helper
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.logger import bec_logger
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.device_providers.device_errors import InvalidDeviceError
from bec_server.device_server.device_providers.device_interfaces import (
    Completion,
    DeviceKind,
    DeviceTarget,
    ReadbackRefresh,
    wait_for_completion,
)
from bec_server.device_server.device_providers.device_provider import DeviceProvider
from bec_server.device_server.device_providers.ophyd.callback_monitor import OphydCallbackMonitor
from bec_server.device_server.device_providers.ophyd.device_serializer import (
    disable_lazy_wait_for_connection,
    get_device_base_class,
    serialize_device_info,
)
from bec_server.device_server.device_providers.ophyd.friendly_device_exceptions import (
    reformat_known_device_exceptions,
)
from bec_server.device_server.device_providers.ophyd.ophyd_completion import (
    to_instruction_execution,
)
from bec_server.device_server.device_providers.ophyd.ophyd_rpc import OphydRPC
from bec_server.device_server.devices import OwnershipMode
from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig
from bec_server.device_server.instruction_execution import InstructionExecution

if TYPE_CHECKING:
    from bec_server.device_server.devices.devicemanager import DeviceManagerDS
    from bec_server.device_server.rpc_handler import RPCHandler


logger = bec_logger.logger


OPHYD_DEVICE_MODULES = (opd, ophyd)


def get_native_device_class(dev_type: str) -> type:
    """
    Resolve a classic device class for offline construction.

    Args:
        dev_type (str): Configured device class name.

    Returns:
        type: Resolved plugin class.
    """
    return plugin_helper.get_plugin_class(dev_type, list(OPHYD_DEVICE_MODULES))


def construct_native_device(
    dev_cls: type, name: str, device_config: dict | None, device_manager: DeviceManagerDS | None
) -> tuple[ophyd.OphydObject, dict]:
    """Construct a native device for both session loading and offline callers."""
    device_classes = [dev_cls]
    if issubclass(dev_cls, ophyd.Signal):
        device_classes.append(ophyd.Signal)
    if issubclass(dev_cls, ophyd.signal.EpicsSignalBase):
        device_classes.append(ophyd.signal.EpicsSignalBase)
    if issubclass(dev_cls, ophyd.OphydObject):
        device_classes.append(ophyd.OphydObject)

    config = device_config.copy() if device_config else {}
    config["name"] = name

    class_params = set()
    for device_class in device_classes:
        class_params.update(inspect.signature(device_class)._parameters)
    class_params_and_config_keys = class_params & config.keys()

    init_kwargs = {key: config.pop(key) for key in class_params_and_config_keys}
    device_access = config.pop("device_access", None)
    if device_access or (device_access is None and config.get("device_mapping")):
        init_kwargs["device_manager"] = device_manager

    signature = inspect.signature(dev_cls)
    if "device_manager" in signature.parameters:
        init_kwargs["device_manager"] = device_manager
    if "scan_info" in signature.parameters:
        # Offline construction can run without a device manager.
        init_kwargs["scan_info"] = device_manager.scan_info if device_manager else None

    return dev_cls(**init_kwargs), config


def connect_native_device(
    obj: ophyd.OphydObject, wait_for_all: bool = False, timeout: float = 5, **kwargs
) -> Exception | None:
    """Connect a native Ophyd object for either runtime or offline callers."""
    try:
        if hasattr(obj, "wait_for_connection"):
            try:
                with disable_lazy_wait_for_connection(obj):
                    obj.wait_for_connection(all_signals=wait_for_all, timeout=timeout)  # type: ignore
            except TypeError:
                with disable_lazy_wait_for_connection(obj):
                    obj.wait_for_connection(timeout=timeout)  # type: ignore
            return
        # Check connected last, as an ophyd device with only lazy signals will always
        # be obj.connected == True. Therefore, we have to call wait_for_connection first
        # for any ophyd devices. This anyways falls back to checking obj.connected.
        # For simulated devices or non-ophyd devices that do not implement wait_for_connection
        # we still want to check obj.connected to allow for them to load.
        if obj.connected:
            return

        logger.error(
            f"Device {obj.name} does not implement the socket controller interface nor"
            " wait_for_connection and cannot be turned on."
        )
        return ConnectionError(f"Failed to establish a connection to device {obj.name}")
    except Exception as exc:
        logger.error(f"Failed to connect for {obj.name}: {exc}")
        return exc


class _OphydSetSubscription:  # pylint: disable=too-few-public-methods
    """Own one temporary native readback subscription."""

    def __init__(self, obj: ophyd.OphydObject, token: int) -> None:
        self._obj = obj
        self._token = token
        self._closed: Future[None] | None = None
        self._lock = Lock()

    def close(self) -> Completion[None]:
        """Unsubscribe once, including token zero, and expose cleanup errors."""
        with self._lock:
            if self._closed is None:
                future = Future()
                self._closed = future
                try:
                    self._obj.unsubscribe(self._token)
                    future.set_result(None)
                except Exception as exc:
                    future.set_exception(exc)
            return self._closed


class OphydDeviceProvider(DeviceProvider):
    device_modules = OPHYD_DEVICE_MODULES

    def __init__(self, device_manager: DeviceManagerDS):
        self.device_manager = device_manager
        self.callback_monitor = OphydCallbackMonitor(queue_threshold=1000)
        self.rpc = OphydRPC(self)

    ############################################################################
    ############################ Provider Selection ############################
    ############################################################################

    @staticmethod
    def supports_device_class(device_class: type) -> bool:
        """Return whether the class belongs to the supported Ophyd device families."""
        return issubclass(
            device_class,
            (ophyd.OphydObject, opd.BECDeviceBase, opd.DeviceProxy, opd.ComputedSignal),
        )

    ############################################################################
    ############################# Device Lifecycle #############################
    ############################################################################

    def construct_device(
        self, device_config: DSDeviceConfig, dev_cls: type
    ) -> tuple[DSDevice, dict]:
        """Construct and wrap an Ophyd object, separating remaining configuration.

        Args:
            device_config (DSDeviceConfig): Full device configuration.
            dev_cls (type): Resolved Ophyd class to construct.

        Returns:
            tuple[DSDevice, dict]: Device wrapper and configuration remaining after construction.
        """
        obj, config = construct_native_device(
            dev_cls, device_config.name, device_config.deviceConfig, self.device_manager
        )
        device = DSDevice(
            name=device_config.name,
            obj=obj,
            config=device_config.model_dump(exclude_unset=True),
            parent=self.device_manager,
        )
        return device, config

    def connect_device(self, device: DSDevice) -> Exception | None:
        """
        Connect all declared signals using the configured connection timeout.

        Args:
            device (DSDevice): Constructed Ophyd device wrapper.

        Returns:
            Exception | None: Recoverable connection error, or None on success.
        """
        # pylint: disable=protected-access
        timeout = (device._config or {}).get("connectionTimeout", 5)
        return connect_native_device(device.obj, wait_for_all=True, timeout=timeout)

    def initialize_enabled_device(self, device: DSDevice, config: dict) -> None:
        """Finish enabled-device setup after metadata publication and registration.

        Args:
            device (DSDevice): Registered device wrapper.
            config (dict): Configuration remaining after construction.
        """
        device_manager = self.device_manager
        if hasattr(device.obj, "on_connected"):
            device.obj.on_connected()
        device_manager.initialize_enabled_device(device)
        obj = device.obj
        if hasattr(obj, "event_types"):
            # pylint: disable=protected-access
            device_manager._subscribe_to_device_events(obj, device)
            device_manager._subscribe_to_auto_monitors(obj)
            device_manager._subscribe_to_limit_updates(obj)
            device_manager._subscribe_to_bec_signals(obj)
        # Configuration may set signals, so subscriptions must already be installed.
        self.update_device_config(device, config)

    def requires_delayed_initialization(self, device_class: type) -> bool:
        """
        Identify classic devices that rely on other session devices.

        Args:
            device_class (type): Resolved device class.

        Returns:
            bool: Whether the class is a proxy or computed signal.
        """
        return issubclass(device_class, (opd.DeviceProxy, opd.ComputedSignal))

    def register_device_proxy(self, device: DSDevice) -> None:
        """
        Register a delayed device proxy against its native targets.

        Args:
            device (DSDevice): Delayed device wrapper to register if it is a proxy.
        """
        if not hasattr(device.obj, "lookup"):
            return
        name = device.name
        devices = self.device_manager.devices
        obj_lookup = device.obj.lookup
        for key in obj_lookup.keys():
            signal_name = obj_lookup[key].get("signal_name")
            if key not in devices:
                raise DeviceConfigError(
                    f"Failed to init DeviceProxy {name}, no device {key} found in device manager."
                )
            dev_obj = devices[key].obj
            registered_proxies = dev_obj.registered_proxies
            if not hasattr(dev_obj, signal_name):
                raise DeviceConfigError(
                    f"Failed to init DeviceProxy {name}, no signal {signal_name} found for device {key}."
                )
            if key not in registered_proxies:
                # pylint: disable=protected-access
                devices[key].obj._registered_proxies.update({name: signal_name})
                continue
            if key in registered_proxies and signal_name not in registered_proxies[key]:
                # pylint: disable=protected-access
                devices[key].obj._registered_proxies.update({name: signal_name})
                continue
            if key in registered_proxies.keys() and signal_name in registered_proxies[key]:
                raise RuntimeError(
                    f"Failed to init DeviceProxy {name}, device {key} already has a registered DeviceProxy for {signal_name}. Only one DeviceProxy can be active per signal."
                )

    def disconnect_device(self, device: DSDevice) -> None:
        """Disconnect and release the device's resources.

        Args:
            device (DSDevice): The device wrapper to disconnect.
        """
        device.obj.destroy()

    ############################################################################
    ############################# Device Metadata ##############################
    ############################################################################

    @staticmethod
    def get_ownership_mode(obj: ophyd.OphydObject | opd.BECDeviceBase) -> OwnershipMode:
        """
        Resolve ownership policy for a device or signal.
        The logic is as follows:
        1. If the class has an explicit ownership_mode attribute, use that.
        2. If the object is a PSIDeviceBase, it is pinned.
        3. If the object is a signal and has write_access set to False, it is free.
        4. Otherwise, it is claimable.

        Args:
            obj (ophyd.OphydObject | opd.BECDeviceBase): Device or signal to classify.

        Returns:
            OwnershipMode: Resolved ownership policy.
        """
        explicit_mode = getattr(obj.__class__, "ownership_mode", None)
        if explicit_mode is not None:
            try:
                return OwnershipMode(explicit_mode)
            except ValueError as exc:
                raise DeviceConfigError(
                    f"Invalid ownership_mode {explicit_mode!r} configured on "
                    f"{obj.__class__.__name__}. Expected one of: "
                    f"{', '.join(mode.value for mode in OwnershipMode)}."
                ) from exc

        if isinstance(obj, opd.PSIDeviceBase):
            return OwnershipMode.PINNED

        if get_device_base_class(obj) == "signal" and getattr(obj, "write_access", None) is False:
            return OwnershipMode.FREE

        return OwnershipMode.CLAIMABLE

    def get_device_info(self, device: DSDevice, connect: bool = True) -> DeviceInfo:
        """
        Extract device metadata for publication by BEC.

        Args:
            device (DSDevice): Device wrapper to inspect.
            connect (bool): Include metadata that requires a connected device. Defaults to True.

        Returns:
            DeviceInfo: Device interface metadata for publication by the device manager.
        """
        return serialize_device_info(
            device.obj, connect, get_ownership_mode=self.get_ownership_mode
        )

    def get_runtime_signal_info(self, device: DSDevice) -> dict[str, messages.SignalInfo]:
        """Extract BEC signal runtime metadata without selecting publication endpoints."""
        info = {}
        if isinstance(device.obj, ophyd.Device):
            for _, name, item in device.obj.walk_signals():
                if isinstance(item, opd.utils.bec_signals.BECMessageSignal):
                    info[name] = item.signal_info
        return info

    def is_connected(self, device: DSDevice) -> bool:
        """Retain classic Ophyd's root connection check before monitor acquisition."""
        return device.obj.connected

    ############################################################################
    ######################### Configuration And Limits #########################
    ############################################################################

    def update_device_config(self, device: DSDevice, config: dict) -> None:
        """Apply Ophyd configuration without reading or publishing updated limits.

        Args:
            device (DSDevice): Enabled Ophyd device wrapper.
            config (dict): Configuration values to apply.
        """
        obj = device.obj
        if hasattr(obj, "_update_device_config"):
            # If the device has implemented its own config update method, use it
            # pylint: disable=protected-access
            obj._update_device_config(config)  # type: ignore
            return

        signal_updated = False
        for config_key, config_value in config.items():
            # first handle the ophyd exceptions...
            if config_key == "limits":
                if hasattr(obj, "low_limit_travel") and hasattr(obj, "high_limit_travel"):
                    low_limit_status = obj.low_limit_travel.set(config_value[0])  # type: ignore
                    high_limit_status = obj.high_limit_travel.set(config_value[1])  # type: ignore
                    # Respect Timeout to avoid blocking the device server indefinitely
                    low_limit_status.wait(timeout=2)
                    high_limit_status.wait(timeout=2)
                    continue
            if config_key == "labels":
                if not config_value:
                    config_value = set()
                # pylint: disable=protected-access
                obj._ophyd_labels_ = set(config_value)
                continue
            if not hasattr(obj, config_key):
                raise DeviceConfigError(
                    f"Unknown config parameter {config_key} for device of type"
                    f" {obj.__class__.__name__}."
                )

            config_attr = getattr(obj, config_key)
            if isinstance(config_attr, ophyd.Signal):
                config_attr.set(config_value).wait(timeout=2)
                if not hasattr(config_attr, "_auto_monitor"):
                    # only signal values that are not auto monitored need
                    # to trigger a manual buffer update
                    signal_updated = True
            elif callable(config_attr):
                config_attr(config_value)
            else:
                setattr(obj, config_key, config_value)

        self.device_manager.on_configuration_applied(device, refresh_buffer=signal_updated)

    def get_device_limits(self, device: DSDevice) -> dict | None:
        """
        Read current travel limits when the device exposes both limit signals.

        Args:
            device (DSDevice): Device wrapper whose limits should be read.

        Returns:
            dict | None: Low and high limit readings, or None when unsupported.
        """
        obj = device.obj
        if not hasattr(obj, "low_limit_travel") or not hasattr(obj, "high_limit_travel"):
            return None
        return {
            "low": {"value": obj.low_limit_travel.get()},
            "high": {"value": obj.high_limit_travel.get()},
        }

    ############################################################################
    ######################### Data Roles #########################
    ############################################################################

    def get_kind(self, target: DeviceTarget) -> DeviceKind:
        """Map native flags to BEC roles without changing existing Ophyd routing.

        Args:
            target (DeviceTarget): Root wrapper and relative backend path.

        Returns:
            DeviceKind: Independent BEC roles; hinted selection includes READOUT explicitly.

        Raises:
            RuntimeError: A lazy target needs runtime preparation before its effective kind
                can be inspected. This method never constructs a component.
        """
        obj = target.device.obj
        for part in target.path.split(".") if target.path else ():
            # Component access can construct hardware and wait for a connection. Only
            # runtime preparation may do that; introspection uses existing instances.
            if isinstance(obj, ophyd.Device):
                # pylint: disable=protected-access
                if part in obj._sig_attrs:
                    if part not in obj._signals:
                        raise RuntimeError(
                            f"Target {target.name!r} requires runtime preparation before kind lookup"
                        )
                    obj = obj._signals[part]
                    continue
            obj = inspect.getattr_static(obj, part)
        return self._map_kind(obj.kind)

    @staticmethod
    def _map_kind(native_kind: ophyd.Kind) -> DeviceKind:
        known_flags = ophyd.Kind.hinted | ophyd.Kind.config
        if int(native_kind) & ~int(known_flags):
            raise ValueError(f"Unsupported Ophyd kind flags: {native_kind!r}")
        kind = DeviceKind.OMITTED
        if native_kind & ophyd.Kind.normal:
            kind |= DeviceKind.READOUT
        if native_kind & ophyd.Kind.config:
            kind |= DeviceKind.CONFIGURATION
        hint_flag = ophyd.Kind.hinted ^ ophyd.Kind.normal
        if native_kind & hint_flag:
            kind |= DeviceKind.HINTED
        return kind

    ############################################################################
    ############################ Device Operations #############################
    ############################################################################

    def set(
        self, target: DeviceTarget, value: Any, *, refresh: ReadbackRefresh
    ) -> InstructionExecution[None]:
        """
        Subscribe to nested readback before invoking native set.

        Args:
            target (DeviceTarget): Device or component to set.
            value (Any): Value to write, converted to an enum index where appropriate.
            refresh (ReadbackRefresh): Callback for native readback updates.

        Returns:
            InstructionExecution[None]: Execution tracking the status and any temporary
                readback subscription.
        """
        obj = self._resolve(target)
        resolved_target = self.resolve_target(obj, self.device_manager)
        subscription = None
        if target.path and ("readback" in obj.event_types or "value" in obj.event_types):

            def notify(*_args, **_kwargs):
                if obj.connected:
                    refresh(resolved_target, inline=True)

            subscription = _OphydSetSubscription(obj, obj.subscribe(notify, run=True))

        try:
            try:
                value = self.convert_value_if_needed(obj, value)
                result = obj.set(value)
            except Exception as exc:
                error = reformat_known_device_exceptions(
                    exc, "set", f"Device: {target.device.name}, value: {value}."
                )
                result = ophyd.DeviceStatus(device=obj)
                result.set_exception(error)
            execution = self._status_result(resolved_target, "set", result, obj)
        except Exception:
            if subscription is not None:
                try:
                    wait_for_completion(subscription.close())
                except Exception:
                    logger.exception("Failed to close set subscription")
            raise
        execution.subscription = subscription
        return execution

    def trigger(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """
        Invoke native trigger inline and track its status.

        Args:
            target (DeviceTarget): Device or native component to operate on.

        Returns:
            InstructionExecution[None]: Execution tracking the native trigger status.
        """
        obj = self._resolve(target)
        return self._status_result(target, "trigger", obj.trigger(), obj)

    def kickoff(
        self,
        target: DeviceTarget,
        *,
        metadata: dict,
        configure: dict | None = None,
        on_status_expected: Callable[[], None] | None = None,
    ) -> InstructionExecution[None] | None:
        """
        Start classic Ophyd acquisition using its native kickoff signature.

        Args:
            target (DeviceTarget): Device or native component to start.
            metadata (dict): Instruction metadata, including the scan context when available.
            configure (dict | None): Acquisition parameters. None uses an empty configuration.
            on_status_expected (Callable[[], None] | None): Callback to register the running request
                before starting work that returns a status.

        Returns:
            InstructionExecution[None] | None: Tracked execution, or None for an immediate kickoff.
        """
        configure = {} if configure is None else configure
        obj = self._resolve(target)
        kickoff = obj.kickoff
        if len(inspect.getfullargspec(kickoff).args) > 1:
            kickoff(metadata=metadata, **configure)
            return None
        if on_status_expected is not None:
            on_status_expected()
        obj.configure(configure)
        return self._status_result(target, "kickoff", kickoff(), obj)

    def complete(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """
        Finish acquisition when the native device supports completion.

        Args:
            target (DeviceTarget): Device or native component to operate on.

        Returns:
            InstructionExecution[None] | None: Tracked execution, or None when no status is
                returned.

        Raises:
            InvalidDeviceError: The native complete method returns None.
        """
        obj = self._resolve(target)
        if not hasattr(obj, "complete"):
            return None
        result = obj.complete()
        if result is None:
            raise InvalidDeviceError(
                f"The complete method of device {target.name} does not return a StatusBase object."
            )
        return self._status_result(target, "complete", result, obj)

    def pre_scan(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """
        Run the optional native pre-scan hook.

        Args:
            target (DeviceTarget): Device or native component to operate on.

        Returns:
            InstructionExecution[None] | None: Tracked execution, or None when no status is
                returned.
        """
        obj = self._resolve(target)
        if not hasattr(obj, "pre_scan"):
            return None
        result = obj.pre_scan()
        return None if result is None else self._status_result(target, "pre_scan", result, obj)

    def stage(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """
        Stage a native device, unstaging it first when required.

        Args:
            target (DeviceTarget): Device or native component to operate on.

        Returns:
            InstructionExecution[None] | None: Tracked execution, or None when no status is
                returned.
        """
        obj = self._resolve(target)
        if not hasattr(obj, "_staged"):
            return None
        # pylint: disable=protected-access
        if obj._staged == ophyd.Staged.yes:
            logger.info(f"Device {obj.name} was already staged and will be first unstaged.")
            status = obj.unstage()
            if isinstance(status, ophyd.StatusBase):
                for ii in range(3):
                    try:
                        status.wait(timeout=10)
                        status = None
                        break
                    except ophyd.utils.errors.WaitTimeoutError:
                        logger.warning(
                            f"Unstaging device {target.name} still running, "
                            f"{10 * (ii + 1)} seconds passed."
                        )
                if status is not None:
                    raise ValueError(
                        f"Unstaging device {target.name} failed to finish in 30 seconds"
                    )
        result = obj.stage()
        if result is None or isinstance(result, list):
            return None
        return self._status_result(target, "stage", result, obj)

    def unstage(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """
        Unstage a native device that is currently staged.

        Args:
            target (DeviceTarget): Device or native component to operate on.

        Returns:
            InstructionExecution[None] | None: Tracked execution, or None when no status is
                returned.
        """
        obj = self._resolve(target)
        if not hasattr(obj, "_staged"):
            return None
        # pylint: disable=protected-access
        if obj._staged == ophyd.Staged.yes:
            result = obj.unstage()
        else:
            logger.debug(f"Device {obj.name} was already unstaged.")
            return None
        if result is None or isinstance(result, list):
            return None
        return self._status_result(target, "unstage", result, obj)

    def is_staged(self, target: DeviceTarget) -> bool:
        """
        Inspect the native staging state after completion.

        Args:
            target (DeviceTarget): Device or component that was staged.

        Returns:
            bool: True when staged or when the native object has no staging state.
        """
        obj = self._resolve(target)
        return getattr(obj, "_staged", ophyd.Staged.yes) == ophyd.Staged.yes

    def read(self, target: DeviceTarget) -> Completion[dict]:
        """Read native values inline without copying their buffers."""
        return self._read(target, "read")

    def read_configuration(
        self, target: DeviceTarget, *, separate_only: bool = False
    ) -> Completion[dict | None]:
        """
        Read native configuration inline without copying its buffers.

        Args:
            target (DeviceTarget): Device or component to read.
            separate_only (bool): Skip standalone signals when refreshing a cache.

        Returns:
            Completion[dict | None]: Configuration readings, or None for skipped signals.
        """
        return self._read(target, "read_configuration", separate_only=separate_only)

    def _read(self, target: DeviceTarget, method: str, *, separate_only: bool = False) -> Future:
        future = Future()
        try:
            obj = self._resolve(target)
            result = (
                None if separate_only and isinstance(obj, ophyd.Signal) else getattr(obj, method)()
            )
            future.set_result(result)
        except Exception as exc:
            future.set_exception(exc)
        return future

    def stop_device(self, device: DSDevice) -> None:
        """Request hardware stop without confusing it with completion cancellation."""
        if hasattr(device.obj, "stop"):
            device.obj.stop()

    ############################################################################
    ############################# Backend Helpers ##############################
    ############################################################################

    @staticmethod
    def _resolve(target: DeviceTarget) -> Any:
        return rgetattr(target.device.obj, target.path) if target.path else target.device.obj

    @staticmethod
    def resolve_target(
        obj: ophyd.OphydObject, device_manager: DeviceManagerDS | None
    ) -> DeviceTarget:
        """Retain the canonical root and path of an already resolved native object.

        Args:
            obj (ophyd.OphydObject): Object resolved for invocation and refresh.
            device_manager (DeviceManagerDS | None): Registry owning native root wrappers.

        Returns:
            DeviceTarget: Actual root wrapper and canonical path. An unmanaged or replaced
                root receives an execution-local wrapper without registration or initialization.
        """
        root = obj.root
        device = device_manager.devices.get(root.name) if device_manager is not None else None
        if device is None or device.obj is not root:
            device = DSDevice(root.name, root, config=None, parent=device_manager)
        return DeviceTarget(device, obj.dotted_name)

    @staticmethod
    def convert_value_if_needed(obj: Any, val: Any) -> Any:
        """Preserve integer enum selection when a set receives an integral float."""
        if (
            hasattr(obj, "enum_strs")
            and obj.enum_strs is not None
            and len(obj.enum_strs) > 0
            and isinstance(val, float)
        ):
            if not val.is_integer():
                raise ValueError(
                    f"Cannot convert float {val} to enum index for {obj.name}. "
                    f"Value must be an integer to select one of the enum strings: {obj.enum_strs}."
                )
            val = int(val)
        return val

    @staticmethod
    def _status_result(
        target: DeviceTarget, action: str, result: Any, obj: ophyd.OphydObject | None = None
    ) -> InstructionExecution[None]:
        """
        Adapt a native status while preserving its classic response name.

        Args:
            target (DeviceTarget): Registered operation target.
            action (str): Instruction action used for diagnostics and completion routing.
            result (Any): Value returned by the native operation.
            obj (ophyd.OphydObject | None): Native object invoked by the operation, when available.

        Returns:
            InstructionExecution[None]: Execution that tracks the native status.

        Raises:
            ValueError: The result is not an Ophyd status.
        """
        if not isinstance(result, ophyd.StatusBase):
            raise ValueError(
                f"The {action} method of {target.name} does not return a StatusBase object."
            )
        execution = (
            OphydDeviceProvider.adapt_status(result, obj, target)
            if action == "set" and obj is not None
            else to_instruction_execution(result, target)
        )
        native_name = getattr(obj, "name", None)
        if not target.path and isinstance(native_name, str):
            execution.response_device_name = native_name
        return execution

    def start_runtime(self) -> None:
        """
        Start monitoring the classic Ophyd callback queue.
        """
        self.callback_monitor.start()

    @contextmanager
    def stop_runtime(self) -> Iterator[None]:
        """
        Stop the callback monitor around connector shutdown.

        Yields:
            None: Control so connector shutdown can release a blocked log write.
        """
        self.callback_monitor.request_stop()
        self.callback_monitor.join(timeout=1)
        try:
            yield
        finally:
            self.callback_monitor.join()

    @staticmethod
    def get_device_from_exception(exc: Exception) -> str | None:
        """
        Find the native device that raised an exception.

        Args:
            exc (Exception): Operation failure with its traceback.

        Returns:
            str | None: Native device name, if a device appears in the traceback.
        """
        tb = exc.__traceback__
        while tb:
            obj = tb.tb_frame.f_locals.get("self")
            if isinstance(obj, ophyd.OphydObject):
                return obj.dotted_name or obj.name
            tb = tb.tb_next
        return None

    def run_rpc(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        """
        Resolve and execute an RPC using classic Ophyd semantics.

        Args:
            device (DSDevice): Registered wrapper for the device that owns the RPC.
            instr (messages.DeviceInstructionMessage): RPC request and its metadata.
            handler (RPCHandler): BEC handler for read publication, completion tracking, and alarms.

        Returns:
            Any: Client-compatible value produced by the Ophyd RPC handler.
        """
        return self.rpc.run(device, instr, handler)

    def adapt_rpc_status(
        self, obj: ophyd.OphydObject, result: ophyd.StatusBase
    ) -> InstructionExecution[None]:
        """Adapt a classic RPC status while retaining the resolved target."""
        target = self.resolve_target(obj, self.device_manager)
        return self.adapt_status(result, obj, target)

    @staticmethod
    def adapt_status(
        status: ophyd.StatusBase, obj: ophyd.OphydObject, target: DeviceTarget
    ) -> InstructionExecution[None]:
        """Adapt a native status while retaining the actual requested object's identity.

        Args:
            status (ophyd.StatusBase): Status returned by a native operation or RPC.
            obj (ophyd.OphydObject): Requested object, which may differ from the status owner.
            target (DeviceTarget): Canonical target resolved from that same object.

        Returns:
            InstructionExecution[None]: Unbound execution with live kind and response ownership.
        """
        operation = to_instruction_execution(status, target)
        operation.target_kind = lambda: OphydDeviceProvider._map_kind(obj.kind)
        return operation
