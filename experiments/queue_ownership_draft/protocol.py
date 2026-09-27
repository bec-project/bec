"""Small in-process protocol for the queue ownership sketch.

Direct scans only. Production assembly, Redis messages, and control sequencing are
deliberately omitted. A prepared scan is transferred, not copied or inspected.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from threading import Event
from typing import Generic, Protocol, TypeVar, cast

T = TypeVar("T")


class Reply(Generic[T]):
    """One owner writes; one caller waits. No callbacks run on the owner thread."""

    def __init__(self) -> None:
        self._ready = Event()
        self._value: T | None = None
        self._error: Exception | None = None

    def resolve(self, value: T) -> None:
        """Deliver a result from the owner thread."""
        self._value = value
        self._ready.set()

    def reject(self, error: Exception) -> None:
        """Deliver an error from the owner thread."""
        self._error = error
        self._ready.set()

    def result(self, timeout: float | None = None) -> T:
        """Wait outside the owner; a timeout does not cancel the submitted operation."""
        if not self._ready.wait(timeout):
            raise TimeoutError("Sketch reply timed out; the command may still be applied")
        if self._error is not None:
            raise self._error
        return cast(T, self._value)


class QueueStatus(Enum):
    """Admission state, separate from execution state."""

    PAUSED = 0
    RUNNING = 1
    LOCKED = 2


@dataclass(frozen=True)
class QueueRef:
    """A name plus lifetime identity; names alone are insufficient."""

    name: str
    generation: str


@dataclass(frozen=True)
class ExecutionToken:
    """Identify one exact assignment, including its queue generation."""

    queue: QueueRef
    item_id: str
    dispatch_id: int


@dataclass
class Cancellation:
    """Narrow shared capability: owner sets events, worker observes them.

    No event is cleared. A second abort also interrupts exception cleanup.
    Full token/phase/sequence controls and pause handling belong in the real protocol.
    """

    execution: Event = field(default_factory=Event)
    cleanup: Event = field(default_factory=Event)
    shutdown: Event = field(default_factory=Event)

    def stop(self) -> None:
        """Request abortion, tightening cancellation on a repeated stop."""
        if self.execution.is_set():
            self.cleanup.set()
        self.execution.set()

    def stop_service(self) -> None:
        """Interrupt execution and cleanup without resetting any signal."""
        self.shutdown.set()
        self.cleanup.set()
        self.execution.set()

    def checkpoint(self) -> None:
        """Let execution cooperate with an interruption request."""
        if self.execution.is_set():
            raise ScanInterrupted


class ScanInterrupted(Exception):
    """The demo execution reached an interruption checkpoint."""


class DirectScan(Protocol):
    """Sketch adapter exposing the direct ScanBase lifecycle.

    bind_cancellation/release_device_locks stand in for scan.actions and registry
    integration. They are draft adapter methods, not additions to real ScanBase.
    """

    def bind_cancellation(self, cancellation: Cancellation) -> None:
        """Wire cooperative checks into actions, including waits inside scan_core."""
        ...

    def prepare_scan(self) -> None:
        """Prepare the scan."""
        ...

    def open_scan(self) -> None:
        """Open the scan."""
        ...

    def stage(self) -> None:
        """Stage devices."""
        ...

    def pre_scan(self) -> None:
        """Run pre-scan work."""
        ...

    def scan_core(self) -> None:
        """Run the direct scan body."""
        ...

    def post_scan(self) -> None:
        """Run post-scan work."""
        ...

    def unstage(self) -> None:
        """Unstage devices."""
        ...

    def close_scan(self) -> None:
        """Close the scan."""
        ...

    def on_exception(self, cause: Exception) -> None:
        """Run exception cleanup using the separate cleanup cancellation signal."""
        ...

    def release_device_locks(self) -> None:
        """Release ownership in finally; production requires stop-scope fencing."""
        ...


@dataclass(frozen=True)
class PreparedScan:
    """Preparation-lane output; caller relinquishes the direct scan on submission."""

    label: str
    is_scan: bool
    scan: DirectScan
    run_on_exception_hook: bool = True


@dataclass(frozen=True)
class Assignment:
    """Metadata and exclusive execution ownership transferred to a worker."""

    token: ExecutionToken
    scan: DirectScan
    cancellation: Cancellation
    run_on_exception_hook: bool


@dataclass(frozen=True)
class Finished:
    """Copied terminal outcome; never send a live scan back to the owner."""

    token: ExecutionToken
    outcome: str
    error: str | None = None


@dataclass(frozen=True)
class ItemSnapshot:
    """Copied item metadata without a reference to the live scan."""

    queue_id: str
    label: str
    is_scan: bool
    status: str


@dataclass(frozen=True)
class QueueSnapshot:
    """Immutable metadata; includes no execution objects or cancellation handles."""

    queue: QueueRef
    status: QueueStatus
    items: tuple[ItemSnapshot, ...]
    active: ExecutionToken | None
