"""Single-owner event loop for scan queue state and idle expiry."""

from __future__ import annotations

import heapq
import itertools
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from typing import Any, ParamSpec, TypeVar

from bec_lib.logger import bec_logger

logger = bec_logger.logger
P = ParamSpec("P")
R = TypeVar("R")


class QueueClosedError(RuntimeError):
    """A request reached a coordinator after external admission closed."""


@dataclass
class ScheduledEvent:
    """Coordinator-owned cancellation handle for an idle expiry event."""

    callback: Callable[[], None]
    cancelled: bool = False

    def cancel(self) -> None:
        """Cancel this event from its owning coordinator."""
        self.cancelled = True


@dataclass(frozen=True)
class QueueEvent:
    """One operation and its optional synchronous acknowledgement."""

    callback: Callable[[], Any]
    reply: Future[Any] | None = None


@dataclass
class LatestEvent:
    """Mutable callback slot for one pending, replaceable status update."""

    callback: Callable[[], Any]


class QueueCoordinator:
    """Serialize queue operations without holding locks while executing them."""

    def __init__(self, thread_name: str = "ScanQueueCoordinator") -> None:
        """Start the thread that owns mailbox operations and scheduled events.

        Args:
            thread_name (str): Name assigned to the owning thread.
        """
        self._events: queue.Queue[QueueEvent | None] = queue.Queue()
        self._scheduled: list[tuple[float, int, ScheduledEvent]] = []
        self._sequence = itertools.count()
        self._latest: dict[str, LatestEvent] = {}
        # This lock protects admission to the mailbox, never queue state.
        self._admission_lock = threading.Lock()
        self._accepting = True
        self._stopped = False
        self.thread = threading.Thread(target=self._run, name=thread_name)
        self.thread.start()

    @property
    def is_owner(self) -> bool:
        """Whether the caller is the thread that owns queue state.

        Returns:
            bool: Whether the caller is the owning thread.
        """
        return threading.current_thread() is self.thread

    def call(self, callback: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute an operation on the owner and return its result or exception.

        Args:
            callback (Callable[P, R]): Operation to execute.
            *args (P.args): Positional arguments for the operation.
            **kwargs (P.kwargs): Keyword arguments for the operation.

        Returns:
            R: Result returned by the operation.

        Raises:
            QueueClosedError: External admission has closed.
        """
        return self._call(False, callback, *args, **kwargs)

    def call_internal(self, callback: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """Acknowledge a task event while accepted workers drain during shutdown.

        Args:
            callback (Callable[P, R]): Operation to execute on the owning thread.
            *args (P.args): Positional arguments passed to the operation.
            **kwargs (P.kwargs): Keyword arguments passed to the operation.

        Returns:
            R: Result returned by the operation.

        Raises:
            QueueClosedError: The owning thread has stopped.
        """
        return self._call(True, callback, *args, **kwargs)

    def post(
        self, callback: Callable[..., Any], *args: Any, internal: bool = False, **kwargs: Any
    ) -> bool:
        """Enqueue an event without blocking its producer.

        Worker completions and status events use ``internal=True`` during shutdown.

        Args:
            callback (Callable[..., Any]): Operation to execute on the owning thread.
            internal (bool): Whether to admit this event while external admission is closed.
            *args (Any): Positional arguments passed to the operation.
            **kwargs (Any): Keyword arguments passed to the operation.

        Returns:
            bool: Whether the event was admitted to the mailbox.
        """
        with self._admission_lock:
            if self._stopped or (not internal and not self._accepting):
                return False
            self._enqueue(QueueEvent(partial(callback, *args, **kwargs)))
        return True

    def post_latest(
        self, key: str, callback: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> bool:
        """Replace a pending status update without crossing an ordered operation.

        Only the latest callback for a key is retained between mailbox barriers. Ordinary
        posts and acknowledged calls seal pending updates, preserving their FIFO position.

        Args:
            key (str): Identifier for the replaceable update slot.
            callback (Callable[..., Any]): Operation to execute on the owning thread.
            *args (Any): Positional arguments passed to the operation.
            **kwargs (Any): Keyword arguments passed to the operation.

        Returns:
            bool: Whether the update was admitted.
        """
        with self._admission_lock:
            if self._stopped or not self._accepting:
                return False
            pending = self._latest.get(key)
            if pending is None:
                pending = LatestEvent(partial(callback, *args, **kwargs))
                self._latest[key] = pending
                self._events.put(QueueEvent(partial(self._execute_latest, key, pending)))
            else:
                pending.callback = partial(callback, *args, **kwargs)
        return True

    def schedule(self, delay: float, callback: Callable[[], None]) -> ScheduledEvent:
        """Schedule an expiry from the owner thread.

        Args:
            delay (float): Delay in seconds.
            callback (Callable[[], None]): Operation to execute on expiry.

        Returns:
            ScheduledEvent: Handle used to cancel the scheduled event on its owner.

        Raises:
            RuntimeError: The caller is not the owning thread.
        """
        if not self.is_owner:
            raise RuntimeError("Idle expiry must be scheduled by its coordinator")
        event = ScheduledEvent(callback)
        # Discard cancelled entries even when an earlier live expiry remains.
        self._scheduled = [entry for entry in self._scheduled if not entry[2].cancelled]
        heapq.heapify(self._scheduled)
        heapq.heappush(self._scheduled, (time.monotonic() + delay, next(self._sequence), event))
        return event

    def begin_shutdown(self, callback: Callable[[], R]) -> R:
        """Close external admission and enqueue shutdown after accepted requests.

        Args:
            callback (Callable[[], R]): Operation to execute on the owning thread.

        Returns:
            R: Result returned by the shutdown operation.

        Raises:
            RuntimeError: The caller is the owning thread.
            QueueClosedError: The coordinator has stopped.
        """
        if self.is_owner:
            raise RuntimeError("Shutdown must be joined outside the queue coordinator")
        reply: Future[R] = Future()
        with self._admission_lock:
            if self._stopped:
                raise QueueClosedError("Scan queue coordinator has stopped")
            self._accepting = False
            self._enqueue(QueueEvent(callback, reply))
        return reply.result()

    def join(self) -> None:
        """Stop the owner after worker threads have posted all terminal events."""
        with self._admission_lock:
            self._stopped = True
            self._events.put(None)
        self.thread.join()

    #############################################
    ############### Helper Methods ##############
    #############################################

    def _enqueue(self, event: QueueEvent) -> None:
        """Seal coalesced updates and enqueue an ordered event under the admission lock.

        Args:
            event (QueueEvent): Ordered operation to enqueue.
        """
        self._latest.clear()
        self._events.put(event)

    def _execute_latest(self, key: str, pending: LatestEvent) -> None:
        """Execute a pending update outside the admission lock.

        Args:
            key (str): Identifier of the coalesced update slot.
            pending (LatestEvent): Callback slot associated with this mailbox event.
        """
        with self._admission_lock:
            if self._latest.get(key) is pending:
                del self._latest[key]
            callback = pending.callback
        callback()

    def _call(
        self, internal: bool, callback: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute an acknowledged operation with the requested admission policy.

        Args:
            internal (bool): Whether to admit operations during shutdown drainage.
            callback (Callable[P, R]): Operation to execute on the owner.
            *args (P.args): Positional arguments passed to the operation.
            **kwargs (P.kwargs): Keyword arguments passed to the operation.

        Returns:
            R: Result returned by the operation.

        Raises:
            QueueClosedError: The admission policy rejects the operation.
        """
        if self.is_owner:
            return callback(*args, **kwargs)
        reply: Future[R] = Future()
        with self._admission_lock:
            rejected = self._stopped if internal else not self._accepting
            if rejected:
                message = (
                    "Scan queue coordinator has stopped"
                    if internal
                    else "Scan queue coordinator is shutting down"
                )
                raise QueueClosedError(message)
            self._enqueue(QueueEvent(partial(callback, *args, **kwargs), reply))
        return reply.result()

    def _run(self) -> None:
        """Process mailbox operations and scheduled expiry events until shutdown."""
        while True:
            while self._scheduled and self._scheduled[0][2].cancelled:
                heapq.heappop(self._scheduled)
            if self._scheduled and self._scheduled[0][0] <= time.monotonic():
                _, _, scheduled = heapq.heappop(self._scheduled)
                self._execute(QueueEvent(scheduled.callback))
                continue
            timeout = (
                max(0.0, self._scheduled[0][0] - time.monotonic()) if self._scheduled else None
            )
            try:
                event = self._events.get(timeout=timeout)
            except queue.Empty:
                _, _, scheduled = heapq.heappop(self._scheduled)
                if not scheduled.cancelled:
                    self._execute(QueueEvent(scheduled.callback))
                continue
            if event is None:
                self._scheduled.clear()
                return
            self._execute(event)

    @staticmethod
    def _execute(event: QueueEvent) -> None:
        """Execute one mailbox event and resolve its acknowledgement or log a failure.

        Args:
            event (QueueEvent): Mailbox operation and optional acknowledgement.
        """
        try:
            result = event.callback()
        except BaseException as exc:  # Keep the owner alive after a failed event.
            if event.reply is not None:
                event.reply.set_exception(exc)
            else:
                logger.exception("Failed to process scan queue event")
        else:
            if event.reply is not None:
                event.reply.set_result(result)
