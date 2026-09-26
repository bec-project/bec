"""Internal API contracts for device-server hardware layers."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

from bec_lib import messages

if TYPE_CHECKING:
    from bec_lib.devicemanager import ScanInfo
    from bec_server.device_server.device_server import DeviceServer


class ManagedDevice(Protocol):  # pylint: disable=too-few-public-methods
    """Device state used by the service without access to native hardware."""

    metadata: dict

    @property
    def enabled(self) -> bool:
        """Whether instructions may access this device."""


class DeviceManagerView(Protocol):
    """The manager information needed for validation and alarm context."""

    @property
    def devices(self) -> Mapping[str, ManagedDevice]:
        """Configured devices indexed by their root names."""

    @property
    def scan_info(self) -> ScanInfo | None:
        """Current scan metadata, when available."""


class InstructionAPI(Protocol):
    """Instruction execution and stop routes provided by a hardware layer."""

    def dispatch(self, instruction: messages.DeviceInstructionMessage) -> None:
        """Execute or submit work, registering completion with the request handler.

        Called from a service worker. Returning need not mean hardware completion;
        synchronous results and asynchronous statuses resolve through RequestHandler.
        """

    def stop_devices(self, devices: list[str] | None = None) -> None:
        """Stop selected devices, or all enabled devices when names are omitted."""


class RPCAPI(Protocol):
    """RPC execution and error reply routes."""

    def run_rpc(self, instruction: messages.DeviceInstructionMessage) -> None:
        """Execute an RPC on a service worker and publish its return value.

        Status-returning operations report their later completion through RequestHandler.
        """

    def send_rpc_exception(
        self, exc: Exception, instruction: messages.DeviceInstructionMessage
    ) -> None:
        """Publish an RPC failure using the existing wire contract."""


class SerializationAPI(Protocol):  # pylint: disable=too-few-public-methods
    """Device introspection route with the established BEC device-info format."""

    def get_device_info(self, obj: Any, connect: bool = True) -> dict:
        """Return complete device information for serialization to BEC clients.

        The backend resolves any asynchronous introspection before returning.
        """


class ConfigurationAPI(Protocol):
    """Configuration request and native configuration application routes."""

    def parse_config_request(
        self, msg: messages.DeviceConfigMessage, cancel_event: threading.Event
    ) -> None:
        """Apply a configuration request, raising on failure or cancellation.

        Called from the configuration worker; changes are complete on return.
        """

    def apply_device_config(self, obj: Any, config: dict) -> None:
        """Apply device settings using the native backend's semantics."""

    def destroy_device(self, obj: Any) -> None:
        """Release a native device and its connections."""

    def get_device_limits(self, obj: Any) -> dict:
        """Read travel limits in the established BEC signal format."""


class DeviceLayer(Protocol):
    """Bound hardware implementation used by the neutral device service.

    The manager exposes device collections, enabled flags, metadata and scan_info.
    Native objects remain private to the routes; the service never accesses a device's obj.
    The layer owns its execution resources. An asynchronous implementation schedules work
    onto its own event loop and joins/cancels that work during shutdown. Status callbacks
    may run on any thread, but must leave an event loop before synchronous BEC publication.
    """

    device_manager: DeviceManagerView
    instructions: InstructionAPI
    rpc: RPCAPI
    serialization: SerializationAPI
    configuration: ConfigurationAPI

    def initialize(self, bootstrap_server: str) -> None:
        """Initialize devices after all routes have been bound."""

    def shutdown(self) -> None:
        """Stop backend workers and callbacks while service transport is still available."""

    def get_device_from_exception(self, exc: Exception) -> str | None:
        """Return device identity from backend-specific exception context, if available."""


def create_default_device_layer(server: DeviceServer) -> DeviceLayer:
    """Construct the default hardware layer without importing it in neutral modules.

    Args:
        server (DeviceServer): Owning BEC device service.
    Returns:
        DeviceLayer: The bound default implementation.
    """
    # Deferred deliberately: importing the neutral service must not import Ophyd.
    # pylint: disable=import-outside-toplevel
    from bec_server.device_server.ophyd.backend import OphydDeviceLayer

    return OphydDeviceLayer(server)
