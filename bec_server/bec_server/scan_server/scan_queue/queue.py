"""Scheduling and lifecycle of one named v4 scan queue."""

from __future__ import annotations

import collections
import functools
import threading
from collections.abc import Callable
from concurrent.futures import Future
from itertools import chain
from typing import TYPE_CHECKING, Any

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .coordinator import ScheduledEvent
from .item import DirectInstructionQueueItem
from .operations import coordinated
from .types import InstructionQueueStatus, ScanQueueStatus

if TYPE_CHECKING:
    from ..direct_scan_worker import ScanOutcome, ScanTask
    from ..scans.scan_base import ScanBase
    from .manager import QueueManager

logger = bec_logger.logger


class ScanQueue:
    """Schedule v4 scan requests in one named queue."""

    MAX_HISTORY = 100
    AUTO_SHUTDOWN_TIME: int = 60  # seconds
    DEFAULT_QUEUE_STATUS = ScanQueueStatus.RUNNING

    def __init__(self, queue_manager: QueueManager, queue_name: str = "primary") -> None:
        """Initialize a named queue and its scheduling state.

        Args:
            queue_manager (QueueManager): Manager that owns this named queue.
            queue_name (str): Name of the queue to manage.
        """
        self.queue: collections.deque[DirectInstructionQueueItem] = collections.deque()
        self._deferred_inserts: collections.deque[tuple[DirectInstructionQueueItem, int]] = (
            collections.deque()
        )
        self.queue_name = queue_name
        self.history_queue: collections.deque[DirectInstructionQueueItem] = collections.deque(
            maxlen=self.MAX_HISTORY
        )
        self.active_instruction_queue: DirectInstructionQueueItem | None = None
        self.queue_manager = queue_manager
        self._status = self.DEFAULT_QUEUE_STATUS
        self.signal_event = threading.Event()
        self.active_task: ScanTask | None = None
        self._dispatching = False  # Prevent reentrant callbacks from dispatching an item twice.
        self._idle_expiry: ScheduledEvent | None = None
        self.locks: dict[str, messages.ScanQueueLock] = {}
        self.release_lock_status: ScanQueueStatus = ScanQueueStatus.RUNNING

    @coordinated
    def stop_active(self) -> None:
        """Signal the executing task without waiting for its thread."""
        item = self.active_instruction_queue
        if self.active_task is not None:
            self.active_task.control.stop(shutdown=True)
        if item is not None:
            item.stop()

    @property
    @coordinated
    def worker_status(self) -> InstructionQueueStatus | None:
        """Read the instruction queue state.

        Returns:
            InstructionQueueStatus | None: Current queue state, or None if no instruction item is
                available.
        """
        item = self.active_instruction_queue or (self.queue[0] if self.queue else None)
        if item is not None:
            return item.status
        return None

    @worker_status.setter
    @coordinated
    def worker_status(self, val: InstructionQueueStatus) -> None:
        """Set the instruction queue state.

        Args:
            val (InstructionQueueStatus): New queue state.
        """
        item = self.active_instruction_queue or (self.queue[0] if self.queue else None)
        if item is not None:
            item.status = val

    @property
    @coordinated
    def status(self) -> ScanQueueStatus:
        """Read the queue dispatch state.

        Returns:
            ScanQueueStatus: Current queue state.
        """
        return self._status

    @status.setter
    @coordinated
    def status(self, val: ScanQueueStatus) -> None:
        """Set the queue dispatch state.

        Args:
            val (ScanQueueStatus): New queue state.
        """
        if self.locks and val != ScanQueueStatus.LOCKED:
            logger.warning(
                f"Queue {self.queue_name} is locked. Cannot change status to {val}. Current locks: {self.locks}"
            )
            return
        self._status = val
        self.queue_manager.send_queue_status()
        if val == ScanQueueStatus.RUNNING:
            self.dispatch()

    @coordinated
    def add_lock(self, lock: messages.ScanQueueLock) -> None:
        """Add a lock to the queue.

        Args:
            lock (messages.ScanQueueLock): Queue lock to add or remove.
        """
        logger.info(f"Adding lock to queue {self.queue_name}: {lock}")
        if self.status != ScanQueueStatus.LOCKED:
            self.release_lock_status = self.status
            self.status = ScanQueueStatus.LOCKED
        self.cancel_idle_expiry()
        self.locks[lock.identifier] = lock
        logger.info(f"Lock '{lock.identifier}' added to queue {self.queue_name}")
        self.dispatch()

    @coordinated
    def remove_lock(self, lock: messages.ScanQueueLock) -> None:
        """Remove a lock from the queue.

        Args:
            lock (messages.ScanQueueLock): Queue lock to add or remove.
        """
        logger.info(f"Removing lock from queue {self.queue_name}: {lock}")
        if lock.identifier in self.locks:
            del self.locks[lock.identifier]
            logger.info(f"Lock '{lock.identifier}' removed from queue '{self.queue_name}'")
            if not self.locks:
                self.status = self.release_lock_status
                self.schedule_idle_expiry()
            else:
                self.dispatch()
        else:
            logger.warning(
                f"Lock with identifier '{lock.identifier}' not found in queue '{self.queue_name}'. Nothing to remove."
            )

    @coordinated
    def remove_queue_item(self, scan_id: str | list[str | None]) -> None:
        """Remove a queue item from the queue.

        Args:
            scan_id (str | list[str | None]): Scan identifier or identifiers selecting the request.
        """
        if not scan_id:
            return
        if not isinstance(scan_id, list):
            scan_id = [scan_id]
        scan_ids = set(scan_id)
        for item in tuple(self.queue):
            if not scan_ids.isdisjoint(item.scan_id):
                self.queue.remove(item)

    @coordinated
    def remove_queue_item_by_request_id(self, request_id: str) -> None:
        """Remove a queue item from the queue by request ID.

        Args:
            request_id (str): Request identifier selecting a queue item.
        """
        if not request_id:
            return
        matches = [
            item
            for item in self.queue
            if any(msg.metadata.get("RID") == request_id for msg in item.scan_msgs)
        ]
        for item in matches:
            self.queue.remove(item)

    @coordinated
    def clear(self) -> None:
        """Clear the queue."""
        self.queue.clear()
        self._deferred_inserts.clear()
        if self.active_task is None:
            self.active_instruction_queue = None

    @coordinated
    def dispatch(self) -> None:
        """Submit the next v4 scan when this queue has no task in flight."""
        if (
            self._dispatching
            or self.queue_manager._closing
            or self.queue_name in self.queue_manager._closing_queues
        ):
            return
        self._dispatching = True
        try:
            while not self.signal_event.is_set() and self.active_task is None:
                if not self.queue:
                    if self._status == ScanQueueStatus.PAUSED:
                        self._status = ScanQueueStatus.RUNNING
                        self.queue_manager.send_queue_status()
                    self.schedule_idle_expiry()
                    return
                if not self._queue_should_continue():
                    return
                item = self.active_instruction_queue or self.queue[0]
                if item.status == InstructionQueueStatus.STOPPED:
                    if item in self.queue:
                        self.queue.remove(item)
                    self.active_instruction_queue = None
                    continue
                if item.status == InstructionQueueStatus.PAUSED:
                    return
                if self.active_instruction_queue is not item:
                    self.active_instruction_queue = item
                    self.history_queue.append(item)
                try:
                    scan = item.move_to_next_scan()
                except StopIteration:
                    item.status = InstructionQueueStatus.COMPLETED
                    item.append_to_queue_history()
                    if item in self.queue:
                        self.queue.remove(item)
                    self.active_instruction_queue = None
                    continue
                self.queue_manager.send_queue_status()
                self._start_task(item, scan)
        finally:
            self._dispatching = False

    @coordinated
    def schedule_idle_expiry(self) -> None:
        """Schedule expiry of an idle unlocked named queue on its owner."""
        if (
            self.queue_name == "primary"
            or self.signal_event.is_set()
            or self.queue_manager._queues.get(self.queue_name) is not self
            or self._idle_expiry is not None
            or self.queue
            or self._deferred_inserts
            or self.active_task is not None
            or self.locks
        ):
            return
        expiry = self.queue_manager._coordinator.schedule(
            self.AUTO_SHUTDOWN_TIME, lambda: self.queue_manager.remove_idle_queue(self, expiry)
        )
        self._idle_expiry = expiry

    @coordinated
    def cancel_idle_expiry(self) -> ScheduledEvent | None:
        """Cancel this queue's current expiry on its coordinator.

        Returns:
            ScheduledEvent | None: Cancelled expiry handle, or None if no expiry was scheduled.
        """
        expiry = self._idle_expiry
        if expiry is not None:
            expiry.cancel()
            self._idle_expiry = None
        return expiry

    @coordinated
    def insert(self, msg: messages.ScanQueueMessage, position: int = -1, **_kwargs: Any) -> None:
        """Insert a new message into the queue or buffer it until a stopped head item clears.

        Args:
            msg (messages.ScanQueueMessage): Request or event message to handle.
            position (int): Insertion index; -1 appends to the queue.
            **_kwargs (Any): Unused compatibility parameter.
        """
        if self.worker_status == InstructionQueueStatus.STOPPED:
            logger.info("Deferring queue insert until worker becomes active again.")
            self._deferred_inserts.append((self._assemble_item(msg), position))
            return
        self._flush_deferred_inserts()
        self._insert_now(msg, position=position)
        self.dispatch()

    @coordinated
    def abort(self) -> None:
        """Abort the current queue item."""
        logger.debug("Aborting scan.")
        if self.active_instruction_queue is not None:
            self.active_instruction_queue.abort()

    @coordinated
    def get_scan(self, scan_id: str) -> DirectInstructionQueueItem | None:
        """Get the instruction queue item based on its scan_id.

        Args:
            scan_id (str): Scan identifier or identifiers selecting the request.

        Returns:
            DirectInstructionQueueItem | None: Matching instruction queue item, or None if no item
                matches.
        """
        for item in chain(self.history_queue, self.queue):
            if scan_id in item.scan_id:
                return item
        return None

    #############################################
    ############### Helper Methods ##############
    #############################################

    def _start_task(self, item: DirectInstructionQueueItem, scan: ScanBase) -> None:
        """Submit the selected scan and register its completion with the coordinator.

        Args:
            item (DirectInstructionQueueItem): Request selected for execution.
            scan (ScanBase): Assembled scan to execute.
        """
        from ..direct_scan_worker import DirectScanWorker, ScanControl, ScanTask

        control = ScanControl(run_on_exception_hook=item.run_on_exception_hook)
        item.control = control
        worker = DirectScanWorker(
            scan=scan,
            control=control,
            on_status=self.queue_manager.request_status_update,
            device_lock_registry=getattr(self.queue_manager.parent, "device_lock_registry", None),
        )
        self.cancel_idle_expiry()
        future: Future[ScanOutcome] = Future()
        thread = threading.Thread(
            target=self._run_task,
            args=(
                functools.partial(
                    worker.run,
                    functools.partial(
                        self.queue_manager.prepare_task,
                        item,
                        scan,
                        control,
                        bool(item.scan_msgs[0].metadata.get("dataset_id_on_hold")),
                    ),
                ),
                future,
            ),
            name=f"ScanTask-{self.queue_name}",
        )
        self.active_task = ScanTask(future=future, control=control, thread=thread)
        self.queue_manager._task_threads.add(thread)
        future.add_done_callback(
            lambda done, expected=item: self.queue_manager._coordinator.post(
                self._task_finished, expected, done, internal=True
            )
        )
        try:
            thread.start()
        except BaseException as exc:
            self.queue_manager._task_threads.discard(thread)
            future.set_exception(exc)

    def _run_task(self, run: Callable[[], ScanOutcome], future: Future[ScanOutcome]) -> None:
        """Execute a task on its independent thread and complete its future.

        Args:
            run (Callable[[], ScanOutcome]): Callable that executes the scan task.
            future (Future[ScanOutcome]): Future containing the task result or exception.
        """
        self.queue_manager._task_context.active = True
        try:
            if not future.set_running_or_notify_cancel():
                return
            try:
                result = run()
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)
        finally:
            self.queue_manager._task_context.active = False

    @coordinated
    def _task_finished(self, item: DirectInstructionQueueItem, future: Future[ScanOutcome]) -> None:
        """Retire only the item whose execution produced this future.

        Args:
            item (DirectInstructionQueueItem): Queue item associated with this task.
            future (Future[ScanOutcome]): Future containing the task result or exception.
        """
        if self.active_task is None or self.active_task.future is not future:
            return
        self.queue_manager.reap_task(self.active_task.thread)
        self.active_task = None
        item.control = None
        if (
            self.signal_event.is_set()
            or self.queue_manager._queues.get(self.queue_name) is not self
        ):
            return
        active_scan = item.active_scan
        try:
            outcome = future.result()
        except BaseException as exc:
            logger.exception("Unrecoverable v4 scan worker failure")
            self.queue_manager.connector.raise_alarm(
                severity=Alarms.MAJOR,
                info=messages.ErrorInfo(
                    error_message=f"Unrecoverable v4 scan worker failure: {exc}",
                    compact_error_message=f"{type(exc).__name__}: {exc}",
                    exception_type=type(exc).__name__,
                    device=None,
                ),
                metadata={
                    "queue": self.queue_name,
                    **({"scan_id": active_scan.scan_info.scan_id} if active_scan else {}),
                },
            )
            outcome = "aborted"
        if outcome == "completed":
            if active_scan not in item.scans:
                logger.error("Completed v4 scan is no longer in its queue item")
                outcome = "aborted"
        if outcome == "completed":
            item.status = InstructionQueueStatus.COMPLETED
        else:
            item.status = InstructionQueueStatus.STOPPED
        item.append_to_queue_history()
        if item in self.queue:
            self.queue.remove(item)
        if outcome != "completed":
            item.abort()
        self.active_instruction_queue = None
        self._flush_deferred_inserts()
        self.queue_manager.send_queue_status()
        self.dispatch()

    @coordinated
    def _queue_should_continue(self) -> bool:
        """Check if the queue should continue to the next instruction queue.

        Returns:
            bool: Whether the next queue item is eligible for dispatch.
        """
        if self.status not in [ScanQueueStatus.PAUSED, ScanQueueStatus.LOCKED]:
            return True
        if self.status == ScanQueueStatus.LOCKED:
            if any(not lock.allow_device_instructions for lock in self.locks.values()):
                # if any of the locks forbid device instructions, we should not continue
                return False
            # We allow the queue to continue if the next queue item is not a scan
            if len(self.queue) > 0 and not any(self.queue[0].is_scan):
                return True
        return False

    @coordinated
    def _flush_deferred_inserts(self) -> None:
        """Move buffered inserts into the live queue once the stopped head no longer blocks them."""
        if not self._deferred_inserts or self.worker_status == InstructionQueueStatus.STOPPED:
            return
        while self._deferred_inserts:
            item, position = self._deferred_inserts.popleft()
            self._enqueue_item(item, position)

    @coordinated
    def _insert_now(self, msg: messages.ScanQueueMessage, position: int = -1) -> None:
        """Insert a new message into the live queue without waiting.

        Args:
            msg (messages.ScanQueueMessage): Request or event message to handle.
            position (int): Insertion index; -1 appends to the queue.
        """
        self._enqueue_item(self._assemble_item(msg), position)

    def _assemble_item(self, msg: messages.ScanQueueMessage) -> DirectInstructionQueueItem:
        """Assemble a request before it can receive a positive admission response.

        Args:
            msg (messages.ScanQueueMessage): Request to validate and assemble.

        Returns:
            DirectInstructionQueueItem: Assembled request item.
        """
        logger.debug(f"Inserting new queue message {msg}")
        item = DirectInstructionQueueItem(
            parent=self, assembler=self.queue_manager.parent.scan_assembler
        )
        item.append_scan_request(msg)
        return item

    def _enqueue_item(self, item: DirectInstructionQueueItem, position: int) -> None:
        """Insert an already assembled item and publish the updated queue.

        Args:
            item (DirectInstructionQueueItem): Validated request item to insert.
            position (int): Insertion index; -1 appends to the queue.
        """
        if position == -1:
            self.queue.append(item)
        else:
            self.queue.insert(position, item)
        self.queue_manager.send_queue_status()
