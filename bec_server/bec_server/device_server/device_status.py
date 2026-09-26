"""Hardware-independent status contract used by device instruction tracking."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol


class DeviceStatus(Protocol):
    """An operation whose completion includes its backend's finalization work.

    Backends publish ``done`` only after cleanup and cache updates have finished. Status
    accessors must not block. Callbacks receive this generalized status, run once per
    registration, and also run when registered after completion.
    """

    device_name: str | None
    status_type: str

    @property
    def done(self) -> bool:
        """Whether the operation and its backend finalization have completed."""

    @property
    def success(self) -> bool | None:
        """Whether the operation succeeded, or None while its outcome is unknown."""

    def exception(self) -> Exception | None:
        """Return the operation's failure, without waiting for completion."""

    def add_callback(self, callback: Callable[[DeviceStatus], None]) -> None:
        """Run callback once this operation completes, including late registration."""
