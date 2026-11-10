"""Route device operations to the matching framework provider."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib import messages, plugin_helper
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig

if TYPE_CHECKING:
    from bec_server.device_server.devices.devicemanager import DeviceManagerDS


logger = bec_logger.logger


class DeviceProviderManager:

    def __init__(self, device_manager: DeviceManagerDS):
        self.device_manager = device_manager
        self.providers = {"ophyd": OphydDeviceProvider(device_manager)}

    def construct_device(self, device_config: DSDeviceConfig) -> tuple[DSDevice, dict]:
        """Construct a wrapped device and separate its remaining configuration.

        Args:
            device_config (DSDeviceConfig): Full configuration for the device to construct.

        Returns:
            tuple[DSDevice, dict]: Device wrapper and configuration remaining after construction.
        """
        dev_cls = self._get_device_class(device_config.deviceClass)
        provider = self.get_provider(dev_cls)
        return provider.construct_device(device_config, dev_cls)

    def initialize_device(self, device: DSDevice, config: dict) -> DSDevice:
        """Initialize and register a constructed device wrapper.

        Args:
            device (DSDevice): Constructed device wrapper.
            config (dict): Configuration remaining after construction.

        Returns:
            DSDevice: The same wrapper, registered and initialized when enabled.
        """
        provider = self.get_provider(type(device.obj))
        manager = self.device_manager
        pipe = manager.connector.pipeline()
        manager.reset_device_data(device, pipe)
        error = provider.connect_device(device) if device.enabled else None
        try:
            info = provider.get_device_info(device, connect=device.enabled and error is None)
            manager.publish_device_info(device, info.to_wire(), pipe=pipe)
        except Exception as exc:
            logger.error(f"Failed to publish device info for {device.name}: {exc}")
            # The fallback covers extraction, message validation, and eager encoding in set().
            # Do not retry errors from this fallback; preserve the original retry boundary.
            info = provider.get_device_info(device, connect=False)
            manager.publish_device_info(device, info.to_wire(), pipe=pipe)
            if error is None:
                error = exc
        pipe.execute()

        # Register failed devices as well so the manager can track and clean them up.
        # pylint: disable=protected-access
        manager.devices._add_device(device.name, device)
        if error is not None:
            raise error
        if device.enabled:
            provider.initialize_enabled_device(device, config)
        return device

    def requires_delayed_initialization(self, device_type: str) -> bool:
        """
        Determine when to initialize a configured device.

        Args:
            device_type (str): Configured class name.

        Returns:
            bool: Whether initialization must follow the other devices.
        """
        device_class = self._get_device_class(device_type)
        return self.get_provider(device_class).requires_delayed_initialization(device_class)

    def register_device_proxy(self, device: DSDevice) -> None:
        """
        Register links for a delayed device proxy through its provider.

        Args:
            device (DSDevice): Delayed wrapper after initialization.
        """
        self.get_provider(type(device.obj)).register_device_proxy(device)

    def get_device_info(self, device: DSDevice, connect: bool = True) -> DeviceInfo:
        """Extract device metadata without publishing it.

        Args:
            device (DSDevice): Device wrapper to inspect.
            connect (bool): Include metadata that requires a connected device. Defaults to True.

        Returns:
            DeviceInfo: Device interface metadata for publication by the device manager.
        """
        provider = self.get_provider(type(device.obj))
        return provider.get_device_info(device, connect=connect)

    def disconnect_device(self, device: DSDevice) -> None:
        """Disconnect and release the device's resources.

        Args:
            device (DSDevice): The device wrapper to disconnect.
        """
        provider = self.get_provider(type(device.obj))
        provider.disconnect_device(device)

    def _get_device_class(self, dev_type: str) -> type:
        """Get the device class from the device type"""
        modules = [
            module for provider in self.providers.values() for module in provider.device_modules
        ]
        return plugin_helper.get_plugin_class(dev_type, modules)

    def get_provider(self, device_class):
        """
        Get the provider for the given device class.

        Args:
            device_class (type): The class of the device for which to get the provider.

        Returns:
            Provider: An instance of the provider for the specified device class.
        """
        for provider in self.providers.values():
            if provider.supports_device_class(device_class):
                return provider
        # Legacy class discovery also accepts duck-typed beamline plugins.
        if classic := self.providers.get("ophyd"):
            return classic
        raise ValueError(f"No provider found for device class: {device_class}")

    def update_device_config(self, device: DSDevice, config: dict) -> None:
        """Apply configuration through the selected provider.

        Args:
            device (DSDevice): Enabled device wrapper to update.
            config (dict): Framework configuration values to apply.
        """
        provider = self.get_provider(type(device.obj))
        provider.update_device_config(device, config)

    def publish_device_limits(self, device: DSDevice) -> None:
        """Read and publish limits separately from configuration application and rollback.

        Args:
            device (DSDevice): Updated device wrapper whose limits should be published.
        """
        provider = self.get_provider(type(device.obj))
        limits = provider.get_device_limits(device)
        if limits is not None:
            self.device_manager.connector.set_and_publish(
                MessageEndpoints.device_limits(device.name), messages.DeviceMessage(signals=limits)
            )

    @staticmethod
    def reset_device(device: DSDevice) -> None:
        """Mark a wrapper as uninitialized after teardown.

        Args:
            device (DSDevice): Device wrapper whose initialization state should be reset.
        """
        device.initialized = False
