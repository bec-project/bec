"""In-process channels and execution messages for the scan queue.

Queue metadata belongs to the coordinator. Channels transfer commands, prepared
scans and copied reports; cancellation events also wake device waits immediately.
"""

from __future__ import annotations

import collections
import threading
from dataclasses import dataclass
from enum import Enum
from queue import Empty
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal, TypeAlias, TypeVar

from bec_lib import messages

from .errors import UserScanInterruption

if TYPE_CHECKING:
    from .scans.scan_base import ScanBase

T = TypeVar("T")
ExitInfoType: TypeAlias = tuple[
    Literal["halted", "aborted", "user_completed"], Literal["user", "alarm"]
]


class ChannelClosed(RuntimeError):
    """A channel no longer accepts messages and has been drained."""


class Channel(Generic[T]):
    """FIFO channel with atomic close/send and drain-before-close receive semantics."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._messages: collections.deque[T] = collections.deque()
        self._closed = False

    def send(self, message: T) -> None:
        """Send without waiting for a receiver; reject sends after closure."""
        with self._condition:
            if self._closed:
                raise ChannelClosed("Channel is closed")
            self._messages.append(message)
            self._condition.notify()

    def receive(self, timeout: float | None = None) -> T:
        """Receive one message, raising Empty on timeout or ChannelClosed after drain."""
        with self._condition:
            ready = self._condition.wait_for(
                lambda: self._messages or self._closed, timeout=timeout
            )
            if not ready:
                raise Empty
            if self._messages:
                return self._messages.popleft()
            raise ChannelClosed("Channel is closed")

    def close(self) -> None:
        """Reject new sends and wake all receivers, retaining already accepted messages."""
        with self._condition:
            self._closed = True
            self._condition.notify_all()


@dataclass(frozen=True)
class Result(Generic[T]):
    """One reply value or exception; no callbacks execute on the sending thread."""

    value: T | None = None
    error: Exception | None = None

    def unwrap(self) -> T:
        """Return the result or propagate the operation's exception."""
        if self.error is not None:
            raise self.error
        return self.value


class InstructionQueueStatus(Enum):
    """Wire-compatible queue item status values."""

    STOPPED = -1
    PENDING = 0
    IDLE = 1
    PAUSED = 2
    DEFERRED_PAUSE = 3
    RUNNING = 4
    COMPLETED = 5
    CANCELLED = 6


class ScanQueueStatus(Enum):
    """Admission status, independent of the current worker's execution state."""

    PAUSED = 0
    RUNNING = 1
    LOCKED = 2


@dataclass(frozen=True)
class ExecutionToken:
    """An exact assignment, independent of queue ordering and name reuse."""

    generation: str
    queue_id: str
    dispatch_id: int


class ExecutionControl:
    """Small shared cancellation capability, separate from owner-only queue metadata.

    Execution and exception cleanup have distinct events. No event is cleared to
    begin cleanup, so a later stop or service shutdown cannot be lost.
    """

    def __init__(self) -> None:
        self.execution_event = threading.Event()
        self.cleanup_event = threading.Event()
        self.shutdown_event = threading.Event()
        self._condition = threading.Condition()
        self._status = InstructionQueueStatus.RUNNING
        self._exit_info: ExitInfoType | None = None
        self._cleanup = False
        self._allow_cleanup = True
        self._stop_count = 0
        self._finished = False
        self._stop_effects: list[threading.Event] = []

    @property
    def status(self) -> InstructionQueueStatus:
        """Return the requested cooperative execution state."""
        with self._condition:
            return self._status

    @property
    def exit_info(self) -> ExitInfoType | None:
        """Return the first interruption reason."""
        with self._condition:
            return self._exit_info

    def set_status(self, status: InstructionQueueStatus) -> None:
        """Apply pause/continue without undoing a stop or shutdown."""
        with self._condition:
            if self._stop_count or self.shutdown_event.is_set():
                return
            self._status = status
            self._condition.notify_all()

    def stop(self, exit_info: ExitInfoType, *, cleanup: bool = True) -> threading.Event | None:
        """Signal interruption and return the receipt that fences stop-device delivery."""
        with self._condition:
            if self._finished:
                return None
            receipt = threading.Event()
            self._stop_effects.append(receipt)
            self._stop_count += 1
            self._exit_info = self._exit_info or exit_info
            self._allow_cleanup = self._allow_cleanup and cleanup
            self._status = InstructionQueueStatus.STOPPED
            self.execution_event.set()
            if self._cleanup or self._stop_count > 1 or not cleanup:
                self.cleanup_event.set()
            self._condition.notify_all()
            return receipt

    def shutdown(self) -> None:
        """Wake scan/device waits and suppress exception cleanup during service exit."""
        with self._condition:
            self.shutdown_event.set()
            self.execution_event.set()
            self.cleanup_event.set()
            self._status = InstructionQueueStatus.STOPPED
            self._condition.notify_all()

    def checkpoint(self) -> None:
        """Wait during pause, then raise an interruption when cancellation is requested."""
        with self._condition:
            self._condition.wait_for(
                lambda: self._status != InstructionQueueStatus.PAUSED
                or self.shutdown_event.is_set()
            )
            event = self.cleanup_event if self._cleanup else self.execution_event
            if event.is_set() or self.shutdown_event.is_set():
                raise UserScanInterruption(exit_info=self._exit_info or ("aborted", "alarm"))

    def wait_for_stop_effects(self) -> None:
        """Keep ownership until all already-issued stop-device sends finish."""
        index = 0
        while True:
            with self._condition:
                if index == len(self._stop_effects):
                    return
                receipt = self._stop_effects[index]
            receipt.wait()
            index += 1

    def finish(self) -> None:
        """Seal cancellation before waiting for stop delivery and releasing locks."""
        with self._condition:
            self._finished = True
        self.wait_for_stop_effects()

    def begin_cleanup(self) -> bool:
        """Enter cleanup without erasing an intervening halt, repeated stop or shutdown."""
        while True:
            self.wait_for_stop_effects()
            with self._condition:
                if any(not receipt.is_set() for receipt in self._stop_effects):
                    continue
                if (
                    self.shutdown_event.is_set()
                    or self.cleanup_event.is_set()
                    or not self._allow_cleanup
                ):
                    return False
                self._cleanup = True
                self._status = InstructionQueueStatus.RUNNING
                return True


@dataclass(frozen=True)
class ScanAssignment:
    """Transfer a prepared direct scan to one worker; metadata is copied."""

    token: ExecutionToken
    queue_name: str
    scan: ScanBase
    msg: messages.ScanQueueMessage
    control: ExecutionControl
    scan_number: int | None
    dataset_number: int | None


@dataclass(frozen=True)
class ScanReport:
    """Worker-created metadata copy; never a reference to its live scan."""

    token: ExecutionToken
    request: messages.RequestBlock
    terminal: bool = False
    status: InstructionQueueStatus = InstructionQueueStatus.RUNNING
    exit_info: ExitInfoType | None = None
    error: str | None = None


@dataclass(frozen=True)
class QueueCommand:
    """A facade command or executor completion delivered to the state owner."""

    operation: str
    arguments: tuple = ()
    reply: Channel[Result] | None = None


@dataclass(frozen=True)
class LaneJob:
    """A blocking operation and a completion callback that only sends a message."""

    execute: Callable[[], Any]
    complete: Callable[[Result], None]


class SerialLane(threading.Thread):
    """Execute preparation or I/O outside the coordinator, preserving submission order."""

    def __init__(self, name: str) -> None:
        super().__init__(name=name, daemon=True)
        self.jobs: Channel[LaneJob] = Channel()

    def run(self) -> None:
        """Drain jobs until the lane closes, reporting each success or failure."""
        while True:
            try:
                job = self.jobs.receive()
            except ChannelClosed:
                return
            try:
                result = Result(value=job.execute())
            except Exception as exc:  # pylint: disable=broad-except
                result = Result(error=exc)
            job.complete(result)
            # Do not retain a transferred scan while waiting for the next job.
            del job, result
