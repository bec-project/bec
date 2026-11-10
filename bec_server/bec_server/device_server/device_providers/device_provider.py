"""Framework-independent device provider contract."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol

from bec_lib import messages
from bec_server.device_server.device_providers.device_interfaces import (
    Completion,
    DeviceKind,
    DeviceTarget,
    ReadbackRefresh,
    Readings,
)
from bec_server.device_server.devices.device_info import DeviceInfo
from bec_server.device_server.devices.ds_device import DSDevice, DSDeviceConfig
from bec_server.device_server.instruction_execution import InstructionExecution

if TYPE_CHECKING:
    from bec_server.device_server.rpc_handler import RPCHandler


class DeviceProvider(Protocol):
    """Define framework-specific construction, inspection, and lifecycle operations.

    Providers handle the underlying device framework. The provider manager selects a
    provider, publishes returned metadata through the device manager, registers devices,
    and coordinates initialization failures. Providers must leave Redis publication and
    device-manager registration to those managers.

    ``device_modules`` lists the modules searched when resolving configured device classes.
    Configuration updates use ``update_device_config``; teardown uses ``disconnect_device``.
    Operation methods are called on BEC coordinator threads. Classic backends may invoke
    inline; asynchronous backends marshal work onto their owned runtime and return promptly.
    Executions retain the resolved target used by the backend; coordinators bind the
    original instruction without replacing that identity. Pending completions carry
    backend outcomes, while BEC owns callback processing,
    temporary-subscription cleanup, and Redis publication. None denotes immediate or
    unsupported work as documented by each operation; hardware stop is separate.
    """

    device_modules: tuple[ModuleType, ...]

    ############################################################################
    ############################ Provider Selection ############################
    ############################################################################

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

    ############################################################################
    ############################# Device Lifecycle #############################
    ############################################################################

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

    ############################################################################
    ############################# Device Metadata ##############################
    ############################################################################

    def requires_delayed_initialization(self, device_class: type) -> bool:
        """
        Determine whether a class must initialize after its dependencies.

        Args:
            device_class (type): Resolved device class.

        Returns:
            bool: Whether the class requires delayed initialization.
        """
        return False

    def register_device_proxy(self, device: DSDevice) -> None:
        """
        Register backend-specific links for a delayed proxy, when applicable.

        Args:
            device (DSDevice): Delayed wrapper after initialization.
        """

    def get_device_info(self, device: DSDevice, connect: bool = True) -> DeviceInfo:
        """Describe the device's framework interface without publishing or registering it.

        Return metadata suitable for DeviceInfoMessage, including the device class, signals,
        user-accessible functions, and available descriptions. With ``connect=False``, avoid
        inspection that requires a live connection so disabled or unreachable devices can
        still be described. The flag controls inspection; it is not a request to connect.
        Propagate inspection errors; initialization owns the retry with ``connect=False``.

        Args:
            device (DSDevice): Device wrapper to inspect.
            connect (bool): Include metadata that requires a connected device. Defaults to True.

        Returns:
            DeviceInfo: Device interface metadata for publication by the device manager.

        Raises:
            Exception: Framework inspection or device-interface validation failed.
        """

    def get_runtime_signal_info(self, device: DSDevice) -> dict[str, messages.SignalInfo]:
        """Extract signal runtime metadata without performing publication."""

    def is_connected(self, device: DSDevice) -> bool:
        """Return cached backend connection readiness without waiting for hardware."""

    ############################################################################
    ######################### Configuration And Limits #########################
    ############################################################################

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
        """
        Read travel limits for initialization, refresh, or configuration publication.

        Args:
            device (DSDevice): Device wrapper whose limits should be read.

        Returns:
            dict | None: Limit readings suitable for DeviceMessage, or None when unsupported.
        """

    ############################################################################
    ######################### Data Roles #########################
    ############################################################################

    def get_kind(self, target: DeviceTarget) -> DeviceKind:
        """Describe the target's BEC data roles without selecting a publication policy.

        Args:
            target (DeviceTarget): Root wrapper and relative backend path.

        Returns:
            DeviceKind: BEC-owned roles, independent of backend enum representation.
        """

    ############################################################################
    ############################ Device Operations #############################
    ############################################################################

    def start_runtime(self) -> None:
        """
        Start any service-wide resources required by the provider.
        """

    def stop_runtime(self) -> AbstractContextManager[None]:
        """
        Release service-wide resources around connector shutdown.

        Returns:
            AbstractContextManager[None]: Context surrounding service shutdown.
        """
        return nullcontext()

    def get_error_info(self, exc: Exception) -> messages.ErrorInfo | None:
        """
        Supply diagnostics for a backend-specific error.

        Args:
            exc (Exception): Operation failure to inspect.

        Returns:
            messages.ErrorInfo | None: Backend diagnostics, or None for default formatting.
        """
        return None

    def get_device_from_exception(self, exc: Exception) -> str | None:
        """
        Identify a native device in an exception traceback.

        Args:
            exc (Exception): Operation failure with its traceback.

        Returns:
            str | None: Device name, when recognized by this provider.
        """
        return None

    def run_rpc(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        """
        Execute a native RPC and normalize its result for the client.

        Args:
            device (DSDevice): Registered wrapper for the device that owns the RPC.
            instr (messages.DeviceInstructionMessage): RPC request and its metadata.
            handler (RPCHandler): BEC handler for read publication, completion tracking, and alarms.

        Returns:
            Any: Client-compatible value produced by the backend.
        """

    def set(
        self, target: DeviceTarget, value: Any, *, refresh: ReadbackRefresh
    ) -> InstructionExecution[None] | None:
        """
        Set a value, forwarding readback updates during the operation.

        Args:
            target (DeviceTarget): Device or component to set.
            value (Any): Value to write.
            refresh (ReadbackRefresh): Callback for readback updates.

        Returns:
            InstructionExecution[None] | None: Execution owning any temporary subscription,
                or None for immediate completion.
        """

    def trigger(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Trigger acquisition and return its completion, or None for immediate completion."""

    def kickoff(
        self,
        target: DeviceTarget,
        *,
        metadata: dict,
        configure: dict | None = None,
        on_status_expected: Callable[[], None] | None = None,
    ) -> InstructionExecution[None] | None:
        """
        Start acquisition using the backend's calling convention.

        Args:
            target (DeviceTarget): Device or native component to start.
            metadata (dict): Instruction metadata, including the scan context when available.
            configure (dict | None): Acquisition parameters. None uses an empty configuration.
            on_status_expected (Callable[[], None] | None): Callback to register the running request
                before starting work that returns a status.

        Returns:
            InstructionExecution[None] | None: Tracked execution, or None for an immediate kickoff.
        """

    def complete(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Finish acquisition; return None for absent or immediately completed work.

        Preserve backend validation: classic Ophyd requires a native status from a
        supported complete hook and rejects None instead of treating it as immediate.
        """

    def pre_scan(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Run an optional pre-scan hook; None means absent or synchronously complete."""

    def stage(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Stage a device, handling existing staging according to the backend policy.

        Restaging waits and timeouts belong to the backend. Return None for unsupported
        or immediate staging, otherwise a completion.
        BEC publishes stage state and checks is_staged after tracked completion.
        """

    def unstage(self, target: DeviceTarget) -> InstructionExecution[None] | None:
        """Unstage a device; None means unsupported, already unstaged, or immediate."""

    def is_staged(self, target: DeviceTarget) -> bool:
        """
        Check whether the target passes validation after staging.

        Args:
            target (DeviceTarget): Device or component that was staged.

        Returns:
            bool: True when staged or when the backend exposes no staging state.
        """

    def read(self, target: DeviceTarget) -> Completion[Readings]:
        """
        Read native values for an instruction or cache update.

        Args:
            target (DeviceTarget): Device or component to read.

        Returns:
            Completion[Readings]: Readings produced using the backend's execution policy.
        """

    def read_configuration(
        self, target: DeviceTarget, *, separate_only: bool = False
    ) -> Completion[Readings | None]:
        """
        Read native configuration with the backend's execution policy.

        Args:
            target (DeviceTarget): Device or component to read.
            separate_only (bool): Skip targets without a distinct configuration section.

        Returns:
            Completion[Readings | None]: Readings, or None for a skipped target.
        """

    def stop_device(self, device: DSDevice) -> None:
        """Request hardware stop through the owning backend."""
