"""Device construction and lifecycle support for Ophyd devices."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import ophyd
import ophyd_devices as opd

from bec_lib import plugin_helper
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.logger import bec_logger
from bec_server.device_server.device_providers.device_provider import DeviceProvider
from bec_server.device_server.device_providers.ophyd.device_serializer import (
    disable_lazy_wait_for_connection,
    get_device_base_class,
    serialize_device_info,
)
from bec_server.device_server.devices import OwnershipMode
from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig

if TYPE_CHECKING:
    from bec_server.device_server.devices.devicemanager import DeviceManagerDS


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


class OphydDeviceProvider(DeviceProvider):
    device_modules = OPHYD_DEVICE_MODULES

    @staticmethod
    def supports_device_class(device_class: type) -> bool:
        """Return whether the class belongs to the supported Ophyd device families."""
        return issubclass(
            device_class,
            (ophyd.OphydObject, opd.BECDeviceBase, opd.DeviceProxy, opd.ComputedSignal),
        )

    def __init__(self, device_manager: DeviceManagerDS):
        self.device_manager = device_manager

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
        device_manager.update_config(device, config)

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

    def read_initial_buffer(self, device: DSDevice) -> tuple[dict, dict | None]:
        """
        Acquire initial readback and any travel limits in classic order.

        Args:
            device (DSDevice): Device wrapper whose buffer is being initialized.

        Returns:
            tuple[dict, dict | None]: Native readback and optional travel limits.
        """
        obj = device.obj
        readings = obj.read()
        limits = None
        if hasattr(obj, "low_limit_travel") and hasattr(obj, "high_limit_travel"):
            limits = self.get_device_limits(device)
        return readings, limits

    def read_initial_configuration(self, device: DSDevice) -> dict | None:
        """
        Read the separate initial configuration of a device.

        Args:
            device (DSDevice): Device wrapper whose buffer is being initialized.

        Returns:
            dict | None: Configuration readings, or None for a standalone signal.
        """
        if isinstance(device.obj, ophyd.Signal):
            return None
        return device.obj.read_configuration()

    def disconnect_device(self, device: DSDevice) -> None:
        """Disconnect and release the device's resources.

        Args:
            device (DSDevice): The device wrapper to disconnect.
        """
        device.obj.destroy()

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

    def update_device_config(self, device: DSDevice, config: dict) -> None:
        """Apply Ophyd configuration without reading or publishing updated limits.

        Args:
            device (DSDevice): Enabled Ophyd device wrapper.
            config (dict): Configuration values to apply.
        """
        self.device_manager.update_config(device, config)

    def get_device_limits(self, device: DSDevice) -> dict:
        """Read the current travel limits after configuration has been applied.

        Args:
            device (DSDevice): Device wrapper whose limits should be read.

        Returns:
            dict: Current low and high limit readings.
        """
        return {
            "low": {"value": device.obj.low_limit_travel.get()},
            "high": {"value": device.obj.high_limit_travel.get()},
        }
