"""Route device operations to the matching framework provider."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any

from bec_lib import messages, plugin_helper
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.device_server.device_providers.device_interfaces import (
    Completion,
    DeviceKind,
    DeviceTarget,
    ReadbackRefresh,
    Readings,
)
from bec_server.device_server.device_providers.device_provider import DeviceProvider
from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
    OphydDeviceProvider,
)
from bec_server.device_server.devices import is_serializable
from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig
from bec_server.device_server.instruction_execution import InstructionExecution

if TYPE_CHECKING:
    from redis.client import Pipeline

    from bec_server.device_server.devices.devicemanager import DeviceManagerDS
    from bec_server.device_server.rpc_handler import RPCHandler


logger = bec_logger.logger


class DeviceProviderManager:

    def __init__(self, device_manager: DeviceManagerDS):
        self.device_manager = device_manager
        self.providers: dict[str, DeviceProvider] = {"ophyd": OphydDeviceProvider(device_manager)}

    ############################################################################
    ############################ Provider Selection ############################
    ############################################################################

    def get_provider(self, device_class: type) -> DeviceProvider:
        """
        Get the provider for the given device class.

        Args:
            device_class (type): The class of the device for which to get the provider.

        Returns:
            DeviceProvider: The provider for the specified device class.
        """
        for provider in self.providers.values():
            if provider.supports_device_class(device_class):
                return provider
        # Legacy class discovery also accepts duck-typed beamline plugins.
        if classic := self.providers.get("ophyd"):
            return classic
        raise ValueError(f"No provider found for device class: {device_class}")

    def _get_device_class(self, dev_type: str) -> type:
        """Get the device class from the device type"""
        modules = [
            module for provider in self.providers.values() for module in provider.device_modules
        ]
        return plugin_helper.get_plugin_class(dev_type, modules)

    ############################################################################
    ############################# Device Lifecycle #############################
    ############################################################################

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
        publish_error = self.publish_device_info(
            device, connect=device.enabled and error is None, pipe=pipe
        )
        if error is None:
            error = publish_error
        pipe.execute()

        # Register failed devices as well so the manager can track and clean them up.
        # pylint: disable=protected-access
        manager.devices._add_device(device.name, device)
        if error is not None:
            raise error
        if device.enabled:
            provider.initialize_enabled_device(device, config)
        return device

    def disconnect_device(self, device: DSDevice) -> None:
        """Disconnect and release the device's resources.

        Args:
            device (DSDevice): The device wrapper to disconnect.
        """
        provider = self.get_provider(type(device.obj))
        provider.disconnect_device(device)

    ############################################################################
    ############################# Device Metadata ##############################
    ############################################################################

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

    def publish_device_info(
        self, device: DSDevice, connect: bool = True, pipe: Pipeline | None = None
    ) -> Exception | None:
        """Extract and publish metadata, returning errors after the disconnected fallback.

        Args:
            device (DSDevice): Device wrapper whose metadata should be refreshed.
            connect (bool): Include connected metadata. Defaults to True.
            pipe (Pipeline | None): Optional Redis pipeline. Defaults to None.

        Returns:
            Exception | None: Original extraction or publication error, or None on success.
                Errors from the disconnected fallback propagate to the caller.
        """
        try:
            info = self.get_device_info(device, connect=connect)
            self.device_manager.publish_device_info(device, info.to_wire(), pipe=pipe)
        except Exception as exc:
            logger.error(f"Failed to publish device info for {device.name}: {exc}")
            info = self.get_device_info(device, connect=False)
            self.device_manager.publish_device_info(device, info.to_wire(), pipe=pipe)
            return exc
        return None

    def get_runtime_signal_info(self, device: DSDevice) -> dict[str, messages.SignalInfo]:
        """Extract signal runtime metadata without performing publication."""
        return self.get_provider(type(device.obj)).get_runtime_signal_info(device)

    def is_connected(self, device: DSDevice) -> bool:
        """Inspect refresh readiness through the owning provider."""
        return self.get_provider(type(device.obj)).is_connected(device)

    ############################################################################
    ######################### Configuration And Limits #########################
    ############################################################################

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

    ############################################################################
    ######################### Data Roles #########################
    ############################################################################

    def get_kind(self, target: DeviceTarget) -> DeviceKind:
        """Resolve BEC roles through the provider owning the root wrapper.

        Args:
            target (DeviceTarget): Requested root or nested target.

        Returns:
            DeviceKind: Provider-mapped BEC data roles.
        """
        provider = self.get_provider(type(target.device.obj))
        return provider.get_kind(target)

    ############################################################################
    ############################ Device Operations #############################
    ############################################################################

    def start_runtime(self) -> None:
        """
        Start service-wide provider resources.
        """
        for provider in self.providers.values():
            provider.start_runtime()

    @contextmanager
    def stop_runtime(self) -> Iterator[None]:
        """
        Release provider resources around service shutdown.

        Yields:
            None: Control while the service shuts down.
        """
        with ExitStack() as stack:
            for provider in self.providers.values():
                stack.enter_context(provider.stop_runtime())
            yield

    def get_error_info(self, exc: Exception) -> messages.ErrorInfo | None:
        """
        Find backend diagnostics for an operation failure.

        Args:
            exc (Exception): Operation failure to inspect.

        Returns:
            messages.ErrorInfo | None: Diagnostics supplied by the first matching provider.
        """
        for provider in self.providers.values():
            if (info := provider.get_error_info(exc)) is not None:
                return info
        return None

    def get_device_from_exception(self, exc: Exception) -> str | None:
        """
        Find the device associated with a backend operation failure.

        Args:
            exc (Exception): Operation failure with its traceback.

        Returns:
            str | None: First device name recognized by a provider.
        """
        for provider in self.providers.values():
            if name := provider.get_device_from_exception(exc):
                return name
        return None

    def run_rpc(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        """
        Run an RPC through the owning provider and validate its response.

        Args:
            device (DSDevice): Registered wrapper for the device that owns the RPC.
            instr (messages.DeviceInstructionMessage): RPC request and its metadata.
            handler (RPCHandler): BEC handler for read publication, completion tracking, and alarms.

        Returns:
            Any: Client-ready value, or None when a non-read RPC returns an unsupported value.
        """
        provider = self.get_provider(type(device.obj))
        result = provider.run_rpc(device, instr, handler)
        # Read RPCs historically went straight to the BEC response encoder,
        # which supports more value types than the raw MsgPack precheck.
        operation = instr.parameter.get("func", "").rsplit(".", 1)[-1]
        if operation not in ("read", "read_configuration") and not is_serializable(result):
            handler.warn_unserializable_rpc(instr)
            return None
        return result

    def set(
        self, target: DeviceTarget, value: Any, *, refresh: ReadbackRefresh
    ) -> InstructionExecution[None] | None:
        """
        Set a value through its owning backend.

        Args:
            target (DeviceTarget): Device or component to set.
            value (Any): Value to write.
            refresh (ReadbackRefresh): Callback for readback updates.

        Returns:
            InstructionExecution[None] | None: Execution owning any temporary subscription,
                or None for immediate completion.
        """
        return self.get_provider(type(target.device.obj)).set(target, value, refresh=refresh)

    def trigger(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Trigger acquisition through the owning backend."""
        return self.get_provider(type(target.device.obj)).trigger(target)

    def kickoff(
        self,
        target: DeviceTarget,
        *,
        metadata: dict,
        configure: dict | None = None,
        on_status_expected: Callable[[], None] | None = None,
    ) -> InstructionExecution[None] | None:
        """
        Start acquisition through the owning provider.

        Args:
            target (DeviceTarget): Device or native component to start.
            metadata (dict): Instruction metadata, including the scan context when available.
            configure (dict | None): Acquisition parameters. None uses an empty configuration.
            on_status_expected (Callable[[], None] | None): Callback to register the running request
                before starting work that returns a status.

        Returns:
            InstructionExecution[None] | None: Tracked execution, or None for an immediate kickoff.
        """
        return self.get_provider(type(target.device.obj)).kickoff(
            target, configure=configure, metadata=metadata, on_status_expected=on_status_expected
        )

    def complete(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Finish acquisition through the owning backend."""
        return self.get_provider(type(target.device.obj)).complete(target)

    def pre_scan(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Run the owning backend's optional pre-scan hook."""
        return self.get_provider(type(target.device.obj)).pre_scan(target)

    def stage(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Stage through the owning backend with its restaging wait policy."""
        return self.get_provider(type(target.device.obj)).stage(target)

    def unstage(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Unstage through the owning backend."""
        return self.get_provider(type(target.device.obj)).unstage(target)

    def is_staged(self, target: DeviceTarget) -> bool:
        """
        Check staging state through the owning provider.

        Args:
            target (DeviceTarget): Device or component that was staged.

        Returns:
            bool: Whether the target passes the provider's staging check.
        """
        return self.get_provider(type(target.device.obj)).is_staged(target)

    def read(self, target: DeviceTarget) -> Completion[Readings]:
        """Acquire instruction readings through the owning backend."""
        return self.get_provider(type(target.device.obj)).read(target)

    def read_configuration(self, target: DeviceTarget) -> Completion[Readings | None]:
        """Acquire explicitly requested configuration readings."""
        return self.get_provider(type(target.device.obj)).read_configuration(target)

    def stop_device(self, device: DSDevice) -> None:
        """Request hardware stop through the owning backend."""
        self.get_provider(type(device.obj)).stop_device(device)
