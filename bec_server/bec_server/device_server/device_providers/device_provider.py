"""Framework-independent device provider contract."""

from __future__ import annotations

from types import ModuleType
from typing import Protocol

from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig


class DeviceProvider(Protocol):
    """Define framework-specific construction, inspection, and lifecycle operations.

    Providers handle the underlying device framework. The provider manager selects a
    provider, publishes returned metadata through the device manager, registers devices,
    and coordinates initialization failures. Providers must leave Redis publication and
    device-manager registration to those managers.

    ``device_modules`` lists the modules searched when resolving configured device classes.
    Configuration updates use ``update_device_config``; teardown uses ``disconnect_device``.
    """

    device_modules: tuple[ModuleType, ...]

    @staticmethod
    def supports_device_class(device_class: type) -> bool:
        """Identify classes whose framework and lifecycle this provider can handle.

        This check selects a provider before construction or operations on an existing
        device. It must not instantiate devices, connect hardware, or change device state.

        Args:
            device_class (type): Resolved device class to check.

        Returns:
            bool: True if this provider can construct and manage this class, otherwise False.
        """
        ...

    def construct_device(
        self, device_config: DSDeviceConfig, dev_cls: type
    ) -> tuple[DSDevice, dict]:
        """Construct and wrap a framework object, separating constructor configuration.

        Pass supported constructor arguments to ``dev_cls`` and supply framework-specific
        dependencies from the provider's device manager. Work on a copy of the configuration
        so the caller's model remains unchanged. Return a DSDevice containing the object and
        full device configuration, together with configuration not consumed by construction.
        Do not register the wrapper or publish metadata. Framework constructors may connect
        eagerly. All subsequent lifecycle operations use this same wrapper.

        Args:
            device_config (DSDeviceConfig): Full configuration for the new device.
            dev_cls (type): Resolved class supported by this provider.

        Returns:
            tuple[DSDevice, dict]: Constructed wrapper and unconsumed configuration.

        Raises:
            Exception: Class inspection or framework construction failed.
        """

    def connect_device(self, device: DSDevice) -> Exception | None:
        """
        Connect a constructed device that the manager has checked is enabled.

        Return recoverable connection errors so the provider manager can still describe,
        publish, and register the wrapper before raising the error. Leave metadata extraction
        and its fallback to the manager, which must also handle publication and encoding
        failures. Do not replace or register the wrapper, install subscriptions, or apply
        remaining configuration here. The manager calls ``initialize_enabled_device`` only
        after publication and registration, for enabled devices without preparation errors.

        Args:
            device (DSDevice): Constructed wrapper containing the full device configuration.

        Returns:
            Exception | None: Connection error, or None on success.
        """

    def initialize_enabled_device(self, device: DSDevice, config: dict) -> None:
        """Complete setup of a successfully connected, enabled, registered device.

        Run framework connection hooks, initialize its data buffer, and install supported
        event subscriptions using the manager's update callbacks. Apply configuration left
        over from construction last, because setting signals may trigger those callbacks.
        The manager must have published metadata and registered the wrapper before calling
        this method. Disabled devices and devices with preparation errors skip this phase.

        Exceptions propagate to the manager, which tracks the already registered device as
        failed. This method does not remove the device or roll back partial setup.

        Args:
            device (DSDevice): Registered device wrapper.
            config (dict): Configuration remaining after construction.

        Raises:
            Exception: Connection hooks, buffer initialization, subscriptions, or configuration
                application failed.
        """

    def disconnect_device(self, device: DSDevice) -> None:
        """Release framework resources when a device is disabled, removed, or shut down.

        Perform framework teardown on the wrapped object, including the framework's cleanup
        of connections and subscriptions. This operation may destroy the underlying object;
        reconnecting the same object is not guaranteed. Leave registry removal, Redis cleanup,
        and wrapper state updates to the manager. Propagate teardown errors to the caller.

        Args:
            device (DSDevice): The device wrapper to disconnect.

        Raises:
            Exception: Framework teardown failed.
        """

    def update_device_config(self, device: DSDevice, config: dict) -> None:
        """Apply framework configuration without reading or publishing updated limits.

        The caller owns rollback and persistence. Do not modify the wrapper's stored config.
        Propagate application errors so the caller can restore the previous configuration.
        Limit reads and publication happen separately, outside the application rollback.

        Args:
            device (DSDevice): Enabled device wrapper to update.
            config (dict): Framework configuration values to apply.
        """

    def get_device_limits(self, device: DSDevice) -> dict | None:
        """Read limits for publication after a successful configuration update.

        Args:
            device (DSDevice): Device wrapper whose limits should be read.

        Returns:
            dict | None: Limit readings suitable for DeviceMessage, or None when unsupported.
        """
