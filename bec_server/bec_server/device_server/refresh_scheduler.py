"""Coalesce device cache refreshes between worker drains."""

from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING

from bec_server.device_server.device_providers.device_interfaces import DeviceTarget, RefreshSection

if TYPE_CHECKING:
    from bec_server.device_server.devices.ds_device import DSDevice


class RefreshScheduler:
    """Coalesce notifications by wrapper identity and section between drain boundaries.

    Timing belongs to the caller. Draining swaps pending work before acquisition, so
    notifications arriving during a read belong to the next batch. Section order
    preserves classic Ophyd's readback, configuration, then limits ordering.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending = {section: set() for section in RefreshSection}

    def request_refresh(self, device: DSDevice, sections: frozenset[RefreshSection]) -> None:
        """Queue sections for one wrapper without acquiring or publishing data."""
        with self._lock:
            for section in sections:
                self._pending[section].add(DeviceTarget(device))

    def pending_names(self, section: RefreshSection) -> set[str]:
        """Return a diagnostic snapshot of pending root names."""
        with self._lock:
            return {target.name for target in self._pending[section]}

    def clear(self) -> None:
        """Discard pending notifications during teardown."""
        with self._lock:
            for pending in self._pending.values():
                pending.clear()

    def drain(self) -> list[tuple[DSDevice, RefreshSection]]:
        """Take one batch without retaining the lock during backend work."""
        with self._lock:
            batch = self._pending
            self._pending = {section: set() for section in RefreshSection}
            return [
                (target.device, section) for section in RefreshSection for target in batch[section]
            ]
