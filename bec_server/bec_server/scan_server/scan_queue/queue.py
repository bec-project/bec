"""Scheduling and lifecycle of one named v4 scan queue."""

from __future__ import annotations

import collections
import functools
import threading
import uuid
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
from .types import ExitInfoType, InstructionQueueStatus, QueueParameter, ScanQueueStatus, ScanTarget

if TYPE_CHECKING:
    from ..direct_scan_worker import ScanControl, ScanOutcome, ScanTask
    from ..scans.scan_base import ScanBase
    from .manager import QueueManager

logger = bec_logger.logger


class ScanQueue:
    # pylint: disable=too-many-public-methods
    """Own queue behavior; access and mutation run on the manager coordinator.

    Public callers use QueueManager commands or detached snapshots.
    """

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
        self.queue_name = queue_name
        self.instance_id = str(uuid.uuid4())
        self.history_queue: collections.deque[DirectInstructionQueueItem] = collections.deque(
            maxlen=self.MAX_HISTORY
        )
        self.active_instruction_queue: DirectInstructionQueueItem | None = None
        self.queue_manager = queue_manager
        self._status = self.DEFAULT_QUEUE_STATUS
        self.signal_event = threading.Event()
        self.active_task: ScanTask | None = None
        self._idle_expiry: ScheduledEvent | None = None
        self.locks: dict[str, messages.ScanQueueLock] = {}

    #############################################
    ############## Queue Management #############
    #############################################

    @property
    def status(self) -> ScanQueueStatus:
        """Read the queue dispatch state.

        Returns:
            ScanQueueStatus: Current queue state.
        """
        return ScanQueueStatus.LOCKED if self.locks else self._status

    @status.setter
    def status(self, val: ScanQueueStatus) -> None:
        """Set admission state without dispatching work or publishing status.

        Args:
            val (ScanQueueStatus): New queue state.
        """
        if val != ScanQueueStatus.LOCKED:
            self._status = val

    def add_lock(self, lock: messages.ScanQueueLock) -> None:
        """Add a lock to the queue.

        Args:
            lock (messages.ScanQueueLock): Queue lock to add or remove.
        """
        logger.info(f"Adding lock to queue {self.queue_name}: {lock}")
        self.cancel_idle_expiry()
        self.locks[lock.identifier] = lock
        logger.info(f"Lock '{lock.identifier}' added to queue {self.queue_name}")

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
                self.schedule_idle_expiry()
        else:
            logger.warning(
                f"Lock with identifier '{lock.identifier}' not found in queue '{self.queue_name}'. Nothing to remove."
            )

    def insert(self, msg: messages.ScanQueueMessage, position: int = -1, **_kwargs: Any) -> None:
        """Validate and insert a request immediately; the command owner dispatches afterward.

        Args:
            msg (messages.ScanQueueMessage): Request or event message to handle.
            position (int): Insertion index; -1 appends to the queue.
            **_kwargs (Any): Unused compatibility parameter.
        """
        item = self._assemble_item(msg)
        if position == -1:
            self.queue.append(item)
        else:
            self.queue.insert(position, item)

    def clear(self) -> None:
        """Pause and stop the queue, then remove its queued requests."""
        self.status = ScanQueueStatus.PAUSED
        if self.active_instruction_queue is not None:
            self.queue_manager._interrupt_item(self.active_instruction_queue)
        for item in tuple(self.queue):
            if item is not self.active_instruction_queue:
                self._cancel_item(item)
        self.queue.clear()
        if self.active_task is None:
            self.active_instruction_queue = None

    def dispatch(self) -> None:
        """Submit the next v4 scan when this queue has no task in flight."""
        if self.queue_manager._closing or self.queue_name in self.queue_manager._closing_queues:
            return
        while not self.signal_event.is_set() and self.active_task is None:
            if not self.queue:
                self.schedule_idle_expiry()
                return
            if self.active_instruction_queue is None and not self._queue_should_continue():
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
            item.active_scan = item.scan
            self._start_task(item, item.scan)

    def schedule_idle_expiry(self) -> None:
        """Schedule expiry of an idle unlocked named queue on its owner."""
        if (
            self.queue_name == "primary"
            or self.signal_event.is_set()
            or self.queue_manager._queues.get(self.queue_name) is not self
            or self._idle_expiry is not None
            or self.queue
            or self.active_task is not None
            or self.locks
        ):
            return
        expiry = self.queue_manager._coordinator.schedule(
            self.AUTO_SHUTDOWN_TIME, lambda: self.queue_manager.remove_idle_queue(self, expiry)
        )
        self._idle_expiry = expiry

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

    #############################################
    ############ Instruction Methods ############
    #############################################

    def pause(self, item: DirectInstructionQueueItem) -> None:
        """Pause the currently executing item without mutating pending requests."""
        if item is self.active_instruction_queue and item.status in (
            InstructionQueueStatus.RUNNING,
            InstructionQueueStatus.PENDING,
        ):
            item.pause()

    def deferred_pause(self) -> None:
        """Pause dispatch after the entire submitted scan finishes."""
        self.status = ScanQueueStatus.PAUSED

    def resume(self, item: DirectInstructionQueueItem | None = None) -> None:
        """Resume the active item and queue dispatch, subject to queue locks."""
        if self.locks:
            self.status = ScanQueueStatus.RUNNING
            return
        if item is not None:
            if (
                item.status
                in (InstructionQueueStatus.PAUSED, InstructionQueueStatus.DEFERRED_PAUSE)
                or item.requested_action == "pause"
            ):
                item.resume()
        self.status = ScanQueueStatus.RUNNING

    def abort(
        self, targets: tuple[DirectInstructionQueueItem, ...], exit_info: ExitInfoType
    ) -> bool:
        """Cancel or interrupt only the captured queue items.

        Args:
            targets (tuple[DirectInstructionQueueItem, ...]): Items bound at admission.
            exit_info (ExitInfoType): Terminal status and interruption source.

        Returns:
            bool: Whether a captured item was still present.
        """
        targets = tuple(item for item in targets if self.contains(item))
        if not targets:
            return False
        active = self.active_instruction_queue
        for item in targets:
            if item is not active:
                self._cancel_item(item)
                self.queue.remove(item)
                continue
            self.status = ScanQueueStatus.PAUSED
            if self.active_task is None or self.active_task.future.done():
                continue
            if item.exit_info is None or exit_info[0] == "halted":
                item.exit_info = exit_info
            self.queue_manager._interrupt_item(item)
        return True

    def halt(self, targets: tuple[DirectInstructionQueueItem, ...], user_call: bool = True) -> bool:
        """Interrupt captured items without exception cleanup."""
        return self.abort(targets, ("halted", "user" if user_call else "alarm"))

    def user_completed(
        self, targets: tuple[DirectInstructionQueueItem, ...], user_call: bool = True
    ) -> bool:
        """Complete captured items with cleanup, preserving dispatch policy."""
        queue_state_prior_abort = self._status
        if not self.abort(targets, ("user_completed", "user" if user_call else "alarm")):
            return False
        self.status = queue_state_prior_abort
        return True

    def restart(
        self, instruction_queue: DirectInstructionQueueItem, parameter: QueueParameter = None
    ) -> bool:
        """Prepare a replacement and interrupt the active scan for restart.

        Args:
            instruction_queue (DirectInstructionQueueItem): Captured active item.
            parameter (QueueParameter): Must provide a nonempty replacement RID distinct from
                the original request's RID.

        Returns:
            bool: Whether the restart was valid and the active scan was interrupted.
        """
        request_id = parameter.get("RID") if parameter else None
        if (
            not isinstance(request_id, str)
            or not request_id.strip()
            or request_id == instruction_queue.request.metadata.get("RID")
        ):
            return False
        if self.active_instruction_queue is not instruction_queue:
            return False
        if self.active_task is None or self.active_task.future.done():
            return False
        scan_id = instruction_queue.scan_id[0]
        if scan_id is None:
            return False
        if instruction_queue.status in [
            InstructionQueueStatus.IDLE,
            InstructionQueueStatus.PENDING,
        ]:
            # If the scan is not running, we don't need to restart it.
            return False
        restart_scan_msg = instruction_queue.request.model_copy(deep=True)
        restart_scan_msg.metadata["RID"] = request_id
        if restart_scan_msg.allow_restart:
            logger.info(f"Restarting scan {scan_id} in queue {self.queue_name}")
            # Admit the replacement before stopping the original, respecting the requested order.
            position = -1 if parameter and parameter.get("position") == "append" else 1
            self.insert(restart_scan_msg, position=position)
        else:
            logger.info(f"Scan {scan_id} restart not allowed, only sending ScanRestartMessage")

        instruction_queue.reason = "restart"
        scan_restart_msg = messages.ScanRestartMessage(
            original_scan_id=scan_id, scan_msg=restart_scan_msg
        )
        self.queue_manager.connector.send(MessageEndpoints.scan_restart(), scan_restart_msg)

        if self.active_instruction_queue is not instruction_queue:
            return False
        original_queue_status = self._status
        self.status = ScanQueueStatus.PAUSED
        if instruction_queue.status in [
            InstructionQueueStatus.RUNNING,
            InstructionQueueStatus.PAUSED,
            InstructionQueueStatus.DEFERRED_PAUSE,
        ]:
            self.queue_manager._interrupt_item(instruction_queue)
        self.status = original_queue_status
        return True

    def reorder(
        self, msg: messages.ScanQueueOrderMessage, queue_item: DirectInstructionQueueItem
    ) -> bool:
        """Reorder a paused queue. Return whether the order was updated."""
        logger.info(f"Handling scan queue order change: {msg}")
        target_queue = msg.queue
        if self.status != ScanQueueStatus.PAUSED:
            logger.warning(f"Queue {target_queue} is no longer available for reordering")
            return False
        queue = self.queue
        if queue_item not in queue:
            logger.error(f"Scan {msg.scan_id} not found in queue {target_queue}")
            return False
        if msg.action == "move_to":
            # move the scan to the target position
            if msg.target_position is None:
                logger.error("Missing target_position")
                return False
            position = max(0, min(msg.target_position, len(queue) - 1))
            queue.remove(queue_item)
            queue.insert(position, queue_item)
        if msg.action == "move_up":
            # move the scan up by one position
            idx = queue.index(queue_item)
            if idx == 0:
                return False
            queue.remove(queue_item)
            queue.insert(idx - 1, queue_item)
        if msg.action == "move_down":
            # move the scan down by one position
            idx = queue.index(queue_item)
            if idx == len(queue) - 1:
                return False
            queue.remove(queue_item)
            queue.insert(idx + 1, queue_item)
        if msg.action == "move_top":
            # move the scan to the top of the queue
            queue.remove(queue_item)
            queue.insert(0, queue_item)
        if msg.action == "move_bottom":
            # move the scan to the bottom of the queue
            queue.remove(queue_item)
            queue.append(queue_item)
        return True

    def stop_active(self) -> None:
        """Signal the executing task without waiting for its thread."""
        item = self.active_instruction_queue
        if item is not None:
            self.queue_manager._interrupt_item(item, shutdown=True)

    #############################################
    ############### Helper Methods ##############
    #############################################

    def contains(self, item: DirectInstructionQueueItem) -> bool:
        """Whether this exact item remains pending or active, including after clear."""
        return item in self.queue or item is self.active_instruction_queue

    def resolve_targets(
        self, scan_id: ScanTarget, request_id: str | None
    ) -> list[DirectInstructionQueueItem]:
        """Resolve public identifiers once; internal subscan IDs are not targets.

        Args:
            scan_id (ScanTarget): Public identifiers selecting one or more items.
            request_id (str | None): Submitted request identifier, taking precedence.

        Returns:
            list[DirectInstructionQueueItem]: Matches in queue order, or the active item
                when no explicit identifier is supplied.
        """
        items = tuple(self.queue)
        if self.active_instruction_queue is not None and self.active_instruction_queue not in items:
            items += (self.active_instruction_queue,)
        if request_id is not None:
            return [item for item in items if item.request.metadata.get("RID") == request_id]
        if scan_id:
            ids = {
                identifier
                for identifier in (scan_id if isinstance(scan_id, list) else [scan_id])
                if identifier is not None
            }
            return [item for item in items if not ids.isdisjoint(item.scan_id)]
        return [self.active_instruction_queue] if self.active_instruction_queue else []

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

    def get_item_by_scan_id(self, scan_id: str) -> DirectInstructionQueueItem | None:
        """Find a live queue item by its scan identifier."""
        return next((item for item in self.queue if scan_id in item.scan_id), None)

    @property
    def active_scan_id(self) -> str | None:
        """Read the identifier of the currently executing scan."""
        item = self.active_instruction_queue
        return item.scan_id[0] if item and item.active_scan else None

    def scan_offset(self, item: DirectInstructionQueueItem) -> int:
        """Calculate a scan's one-based number offset within this queue."""
        offset = 1
        for queue in self.queue:
            if queue.status in [InstructionQueueStatus.COMPLETED, InstructionQueueStatus.RUNNING]:
                continue
            if queue.queue_id != item.queue_id:
                offset += sum(bool(scan_id) for scan_id in queue.scan_id)
                continue
            return offset
        return offset

    def describe_items(self) -> list[messages.QueueInfoEntry]:
        """Describe queue contents using offsets calculated in one pass."""
        offset = 1
        offsets: list[int | None] = []
        for item in self.queue:
            if item.status in (InstructionQueueStatus.RUNNING, InstructionQueueStatus.COMPLETED):
                offsets.append(None)
                continue
            offsets.append(offset)
            offset += sum(bool(scan_id) for scan_id in item.scan_id)
        return [
            item.describe_at_offset(offset if item_offset is None else item_offset)
            for item, item_offset in zip(self.queue, offsets)
        ]

    def _start_task(self, item: DirectInstructionQueueItem, scan: ScanBase) -> None:
        """Submit the selected scan and register its completion with the coordinator.

        Args:
            item (DirectInstructionQueueItem): Request selected for execution.
            scan (ScanBase): Assembled scan to execute.
        """
        from ..direct_scan_worker import DirectScanWorker, ScanControl, ScanTask

        control = ScanControl(
            run_on_exception_hook=item.run_on_exception_hook,
            on_execution_status=lambda status: self.queue_manager._coordinator.post(
                self._task_status, item, status, internal=True
            ),
        )
        item.control = control
        worker = DirectScanWorker(
            scan=scan,
            control=control,
            on_status=self.queue_manager.request_status_update,
            device_lock_registry=getattr(self.queue_manager.parent, "device_lock_registry", None),
            on_failure=lambda: self.queue_manager._coordinator.call_internal(
                self._task_failed, item, control
            ),
            on_complete=lambda success, stop_confirmed: self.queue_manager._coordinator.call_internal(
                control.complete, success, stop_confirmed
            ),
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
                        control,
                        bool(item.request.metadata.get("dataset_id_on_hold")),
                    ),
                ),
                future,
                item,
                control,
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

    def _run_task(
        self,
        run: Callable[[], ScanOutcome],
        future: Future[ScanOutcome],
        item: DirectInstructionQueueItem,
        control: ScanControl,
    ) -> None:
        """Execute a task on its independent thread and complete its future.

        Args:
            run (Callable[[], ScanOutcome]): Callable that executes the scan task.
            future (Future[ScanOutcome]): Future containing the task result or exception.
            item (DirectInstructionQueueItem): Captured executing item.
            control (ScanControl): Captured execution and interruption channel.
        """
        self.queue_manager._task_context.active = True
        try:
            if not future.set_running_or_notify_cancel():
                return
            try:
                result = run()
            except BaseException as exc:
                try:
                    self.queue_manager._coordinator.call_internal(self._task_failed, item, control)
                    control.wait_for_device_stops()
                except Exception:  # pylint: disable=broad-except
                    logger.exception("Unexpected worker failure has an unconfirmed device stop")
                future.set_exception(exc)
            else:
                future.set_result(result)
        finally:
            self.queue_manager._task_context.active = False

    def _task_failed(self, item: DirectInstructionQueueItem, control: ScanControl) -> None:
        """Apply failure interruption to its original execution before cleanup."""
        if (
            self.active_instruction_queue is item
            and self.active_task is not None
            and self.active_task.control is control
            and self.queue_manager._queues.get(self.queue_name) is self
            and control.requested_action not in ("abort", "halt")
        ):
            self.abort((item,), ("aborted", "alarm"))
            self.queue_manager.send_queue_status()

    def _task_status(
        self, item: DirectInstructionQueueItem, status: InstructionQueueStatus
    ) -> None:
        """Record a worker acknowledgement while its task still owns the queue.

        Args:
            item (DirectInstructionQueueItem): Item whose worker acknowledged a transition.
            status (InstructionQueueStatus): Observed execution status.
        """
        if (
            self.active_instruction_queue is item
            and self.active_task is not None
            and not self.active_task.future.done()
        ):
            item.status = status
            self.queue_manager.send_queue_status()

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
            self.status = ScanQueueStatus.PAUSED
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
                    "RID": item.request.metadata.get("RID"),
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
        self.queue_manager.send_queue_status()
        self.queue_manager.append_to_queue_history(item)
        if item in self.queue:
            self.queue.remove(item)
        self.active_instruction_queue = None
        self.dispatch()
        self.queue_manager.send_queue_status()

    def _queue_should_continue(self) -> bool:
        """Check if the queue should continue to the next instruction queue.

        Returns:
            bool: Whether the next queue item is eligible for dispatch.
        """
        if self._status == ScanQueueStatus.PAUSED:
            return False
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

    def _cancel_item(self, item: DirectInstructionQueueItem) -> None:
        """Record cancellation before removing the item from the visible queue."""
        item.status = InstructionQueueStatus.CANCELLED
        self.queue_manager.send_queue_status()
        self.queue_manager.append_to_queue_history(item)
