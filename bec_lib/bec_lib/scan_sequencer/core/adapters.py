"""Sequence execution using the blocking BEC IPython client interfaces."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib.bec_errors import DeviceConfigError
from bec_lib.device import Signal
from bec_lib.scan_report import ScanReport

from .catalog import ScanCatalog

if TYPE_CHECKING:
    from bec_ipython_client import BECIPythonClient


class BECAdapter:
    """Use IPython scan calls, umv/mv and signal set/wait directly.

    Args:
        client (BECIPythonClient): Existing BEC client providing scan info and device proxies.
    """

    def __init__(self, client: BECIPythonClient) -> None:
        """Initialize BECAdapter parameters.

        Args:
            client (BECIPythonClient): Existing BEC client providing scan info and device proxies.
        """
        self.client = client
        self.device_manager = client.device_manager

    def catalog(self) -> ScanCatalog:
        """Read live subscribed scan info.

        Returns:
            ScanCatalog: Catalog from current client scan metadata.
        """
        return ScanCatalog.from_client(self.client)

    def _set_target(self, name: str) -> object:
        """Resolve a device or signal using DeviceContainer's dotted-name lookup.

        Args:
            name (str): Device, signal or scan name to look up.

        Returns:
            object: Settable device or signal proxy.
        """
        try:
            target = self.device_manager.devices[name]
        except (KeyError, AttributeError, DeviceConfigError) as exc:
            raise ValueError(f"Unknown device or signal: {name}") from exc
        if not callable(getattr(target, "set", None)):
            raise ValueError(f"Non-settable device or signal: {name}")
        if getattr(target, "read_only", False):
            raise ValueError(f"Read-only device or signal: {name}")
        return target

    def validate_device(self, name: str) -> None:
        """Check a device or signal's dotted name and advertised set interface.

        Args:
            name (str): Device, signal or scan name to look up.
        """
        self._set_target(name)

    def set(self, name: str, value: object, *, wait: bool = True) -> object | None:
        """Use signal set/wait, or umv/mv for device moves.

        Blocking calls complete before returning and need no runtime handle.

        Args:
            name (str): Device, signal or scan name to look up.
            value (object): Value to write to the target.
            wait (bool): Whether to wait for completion. Defaults to True.

        Returns:
            object | None: Outstanding status for asynchronous operations, or None after a blocking
                call.
        """
        target = self._set_target(name)
        if isinstance(target, Signal):
            status = target.set(value)
            if wait:
                status.wait()
                return None
            return status
        if wait:
            self.client.scans.umv(target, value, relative=False)
            return None
        return self.client.scans.mv(target, value, relative=False)

    def scan(self, name: str, positional: list, arguments: dict) -> None:
        """Use the public blocking IPython scan interface with its normal report.

        Args:
            name (str): Device, signal or scan name to look up.
            positional (list): Resolved positional scan arguments.
            arguments (dict): Resolved keyword arguments.
        """
        getattr(self.client.scans, name)(*positional, **arguments)

    def done(self, status: object) -> bool:
        """Observe outstanding mv/signal handles, propagating completion failures.

        Args:
            status (object): Runtime asynchronous handle, or None for a completed blocking call.

        Returns:
            bool: True after checked successful completion; False while still running.
        """
        if status is None:
            return True
        if isinstance(status, ScanReport):
            state = status.status
            if state in {"STOPPED", "CANCELLED", "FAILED", "ERROR", "ABORTED"}:
                raise RuntimeError(f"Move ended with {state}")
            if state != "COMPLETED":
                return False
            status.wait(timeout=0, file_written=False)
            return True
        if not status.done:
            return False
        status.wait(timeout=0, raise_on_failure=True)
        return True

    def cancel(self, status: object) -> None:
        """Use mv's existing cancellation; signal Status has no general abort API.

        Args:
            status (object): Runtime asynchronous handle, or None for a completed blocking call.
        """
        if isinstance(status, ScanReport):
            status.cancel()
