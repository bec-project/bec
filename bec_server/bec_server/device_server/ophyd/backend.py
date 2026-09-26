"""Composition and lifecycle of the Ophyd hardware layer."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bec_server.device_server.ophyd.device_manager import DeviceManagerDS
from bec_server.device_server.ophyd.instructions import OphydInstructions
from bec_server.device_server.ophyd.rpc import RPCHandler

if TYPE_CHECKING:
    from bec_server.device_server.device_server import DeviceServer


class OphydDeviceLayer:
    """Expose Ophyd instructions, RPC, serialization and configuration routes."""

    def __init__(self, server: DeviceServer, device_manager: DeviceManagerDS | None = None) -> None:
        self.device_manager = (
            device_manager
            if device_manager is not None
            else DeviceManagerDS(server, status_cb=server.update_status)
        )
        self.serialization = self.device_manager.serialization
        self.configuration = self.device_manager.configuration
        self.instructions = OphydInstructions(
            self.device_manager,
            server.connector,
            server.requests_handler,
            alarm_metadata=server.get_metadata_for_alarm,
        )
        self.rpc = RPCHandler(
            self.instructions, assert_device_is_enabled=server.assert_device_is_enabled
        )
        self._closed = False

    def initialize(self, bootstrap_server: str) -> None:
        """Load and connect devices after route construction.

        Args:
            bootstrap_server (str): Redis bootstrap address used by the device manager.
        """
        self.device_manager.initialize(bootstrap_server)

    def shutdown(self) -> None:
        """Quiesce completion effects before shutting down the manager once."""
        if self._closed:
            return
        self.instructions.close()
        self.device_manager.shutdown()
        self._closed = True

    @staticmethod
    def get_device_from_exception(exc: Exception) -> str | None:
        """Find the native device associated with an exception.

        Args:
            exc (Exception): Exception raised by device execution.
        Returns:
            str | None: Device identity, if present in the traceback.
        """
        return OphydInstructions.get_device_from_exception(exc)
