"""Single-owner management and public commands for named scan queues."""

from __future__ import annotations

import functools
import threading
import traceback
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, cast

from rich.console import Console
from rich.table import Table

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.serialization import msgpack

from ..instruction_handler import InstructionHandler
from .coordinator import QueueClosedError, QueueCoordinator, ScheduledEvent
from .item import DirectInstructionQueueItem
from .operations import coordinated
from .queue import ScanQueue
from .types import (
    ExitInfoType,
    InstructionQueueStatus,
    QueueParameter,
    QueueSnapshot,
    ScanQueueStatus,
    ScanTarget,
)

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_server.scan_server.direct_scan_worker import ScanControl, ScanOutcome
    from bec_server.scan_server.scan_server import ScanServer
    from bec_server.scan_server.scans.scan_base import ScanBase


def requires_queue(fcn: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to ensure that the requested queue exists.

    Args:
        fcn (Callable[..., Any]): Queue operation to wrap.

    Returns:
        Callable[..., Any]: Wrapper that creates a missing named queue before execution.
    """

    @functools.wraps(fcn)
    def wrapper(self: QueueManager, *args: Any, queue: str = "primary", **kwargs: Any) -> Any:
        if queue not in self._queues:
            self.add_queue(queue)
        return fcn(self, *args, queue=queue, **kwargs)

    return wrapper


class QueueManager:
    """Manage named scan queues on a single coordinator thread."""

    def __init__(self, parent: ScanServer) -> None:
        """Initialize queue coordination, publication, and request subscriptions.

        Args:
            parent (ScanServer): Owning scan server.
        """
        self.parent = parent
        self.connector = parent.connector
        self._queues: dict[str, ScanQueue] = {}
        self._closing_queues: dict[str, Future[ScanOutcome]] = {}
        self._task_threads: set[threading.Thread] = set()
        self._snapshot = QueueSnapshot()
        self._registry_snapshot: Mapping[str, ScanQueue] = MappingProxyType({})
        self._closing = False
        # Shutdown callers synchronize here; this never protects queue state.
        self._shutdown_lock = threading.Lock()
        self._shutdown_done = False
        self._shutdown_request_lock = threading.Lock()
        self._shutdown_joiner: threading.Thread | None = None
        self._task_context = threading.local()
        # Counter I/O is serialized between task threads, never on the queue owner.
        self._number_lock = threading.Lock()
        self._last_scan_number = 0
        self._number_revision = 0
        self._counter_monitor_started = False
        self._terminal_states: dict[str, InstructionQueueStatus] = {}
        self.instruction_handler = InstructionHandler(self.connector)
        self._coordinator = QueueCoordinator()
        publisher: QueueCoordinator | None = None
        try:
            publisher = QueueCoordinator(thread_name="ScanQueuePublisher")
            self._publisher = publisher
            self._start_scan_queue_register()
        except BaseException:
            for endpoint, callback in self._subscriptions():
                try:
                    self.connector.unregister(topics=endpoint, cb=callback)
                except Exception:
                    logger.exception(
                        "Failed to unregister queue ingress after initialization failure"
                    )
            self._coordinator.begin_shutdown(lambda: None)
            self._coordinator.join()
            if publisher is not None:
                publisher.begin_shutdown(lambda: None)
                publisher.join()
            raise

    @property
    def queues(self) -> Mapping[str, ScanQueue]:
        """Read-only registry view; use get_snapshot for detached queue contents.

        Returns:
            Mapping[str, ScanQueue]: Read-only queue registry view; queue objects remain live.
        """
        try:
            return self._coordinator.call(self._queue_view)
        except QueueClosedError:
            return self._registry_snapshot

    @coordinated
    def add_to_queue(
        self, scan_queue: str, msg: messages.ScanQueueMessage, position: int = -1
    ) -> bool:
        """Add a new ScanQueueMessage to the queue.

        Args:
            scan_queue (str): the queue that should receive the new message
            msg (messages.ScanQueueMessage): ScanQueueMessage
            position (int): Insertion index; -1 appends to the queue.

        Returns:
            bool: Whether the request was inserted or buffered successfully.
        """
        try:
            self.add_queue(scan_queue)
            queue = self._queues[scan_queue]
            queue.insert(msg, position=position)
            return True
        # pylint: disable=broad-except
        except Exception as exc:
            content = traceback.format_exc()
            error_info = messages.ErrorInfo(
                error_message=content,
                compact_error_message=traceback.format_exc(limit=0),
                exception_type=exc.__class__.__name__,
                device=None,
            )
            self.connector.raise_alarm(
                severity=Alarms.MAJOR,
                info=error_info,
                metadata={**msg.metadata, "queue": scan_queue, "request_rejected": True},
            )
            return False

    @coordinated
    def add_queue(self, queue_name: str) -> None:
        """Create a queue; execution waits for an older queue with this name to retire.

        Args:
            queue_name (str): Name of the queue to manage.
        """
        if self._closing:
            raise RuntimeError("Scan queues are shutting down")
        if queue_name in self._queues:
            return
        self._queues[queue_name] = ScanQueue(self, queue_name=queue_name)
        self._queues[queue_name].schedule_idle_expiry()
        self.send_queue_status()

    @coordinated
    def remove_queue(
        self, queue_name: str, skip_primary: bool = True, emit_status: bool = True
    ) -> None:
        """Remove a queue and stop its task without blocking the coordinator.

        Args:
            queue_name (str): Queue to remove.
            skip_primary (bool): Preserve primary when True.
            emit_status (bool): Publish the resulting queue snapshot when True.
        """
        if queue_name == "primary" and skip_primary:
            return
        queue = self._queues.pop(queue_name, None)
        if queue is None:
            return
        queue.cancel_idle_expiry()
        queue.signal_event.set()
        task = queue.active_task
        if task is not None:
            self._closing_queues[queue_name] = task.future
            task.future.add_done_callback(
                lambda done: self._coordinator.post(
                    self._finish_queue_removal, queue_name, done, internal=True
                )
            )
        queue.stop_active()
        if emit_status:
            self.send_queue_status()

    @coordinated
    def remove_idle_queue(self, queue: ScanQueue, expiry: ScheduledEvent) -> None:
        """Remove the same still-idle, unlocked queue whose expiry fired.

        Args:
            queue (ScanQueue): Queue to receive the operation.
            expiry (ScheduledEvent): Expiry handle identifying the scheduled removal.
        """
        if self._queues.get(queue.queue_name) is not queue or queue._idle_expiry is not expiry:
            return
        queue._idle_expiry = None
        if queue.queue or queue._deferred_inserts or queue.active_task is not None or queue.locks:
            return
        self._queues.pop(queue.queue_name)
        queue.signal_event.set()
        self.send_queue_status()

    @coordinated
    def add_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> None:
        """Add a lock to the specified queue.

        Args:
            queue_name (str): The name of the queue to lock
            lock (messages.ScanQueueLock): The lock to add
        """
        self.add_queue(queue_name)
        logger.info(f"Adding lock to queue {queue_name}: {lock}")
        self._queues[queue_name].add_lock(lock)
        self.send_queue_status()

    @coordinated
    def remove_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> None:
        """Remove a lock from the specified queue.

        Args:
            queue_name (str): The name of the queue to unlock
            lock (messages.ScanQueueLock): The lock to remove
        """
        if queue_name not in self._queues:
            return
        logger.info(f"Removing lock from queue {queue_name}: {lock}")
        self._queues[queue_name].remove_lock(lock)
        self.send_queue_status()

    def stop_all_devices(
        self, stop_id: str | list[str] | None = None, devices: list[str] | None = None
    ) -> None:
        """Send a message to the device server to stop devices.

        Args:
            stop_id (str | list[str] | None): An optional identifier for the stop request. If
                provided, this ID will be added to the list of stopped requests in the device
                server to prevent any instructions associated with this ID raising alarms after the
                stop command is issued. The stop_id can be a scan ID, request ID, or queue ID.
            devices (list[str] | None): Optional list of devices to stop. `None` means stop all
                devices, while an empty list means stop no devices.
        """
        msg = messages.VariableMessage(value=devices, metadata={})
        if stop_id is not None:
            msg.metadata["stop_id"] = stop_id
        self.connector.send(MessageEndpoints.stop_devices(), msg)

    @coordinated
    def scan_interception(self, scan_mod_msg: messages.ScanQueueModificationMessage) -> None:
        """Forward a queue modification to the corresponding command method.

        Args:
            scan_mod_msg (messages.ScanQueueModificationMessage): ScanQueueModificationMessage
        """
        logger.info(f"Scan interception: {scan_mod_msg}")
        action = scan_mod_msg.action
        parameters = {
            "scan_id": scan_mod_msg.scan_id,
            "request_id": scan_mod_msg.request_id,
            "queue": scan_mod_msg.queue,
            "parameter": scan_mod_msg.parameter,
        }
        getattr(self, f"set_{action}")(**parameters)

    @coordinated
    @requires_queue
    def set_pause(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """Pause the queue and the currently running instruction queue.

        Args:
            scan_id (ScanTarget): Unused compatibility parameter.
            request_id (str | None): Unused compatibility parameter.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Unused compatibility parameter.
        """
        que = self._queues[queue]
        if que.worker_status == InstructionQueueStatus.RUNNING or (
            que.active_task is not None and que.worker_status == InstructionQueueStatus.PENDING
        ):
            que.worker_status = InstructionQueueStatus.PAUSED
        que.dispatch()

    @coordinated
    @requires_queue
    def set_deferred_pause(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """Pause dispatch after the currently executing scan finishes.

        Args:
            scan_id (ScanTarget): Unused compatibility parameter.
            request_id (str | None): Unused compatibility parameter.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Unused compatibility parameter.
        """
        que = self._queues[queue]
        que.status = ScanQueueStatus.PAUSED
        if que.worker_status == InstructionQueueStatus.RUNNING:
            que.worker_status = InstructionQueueStatus.DEFERRED_PAUSE
        que.dispatch()

    @coordinated
    @requires_queue
    def set_continue(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """Continue with the currently scheduled queue and instruction queue.

        Args:
            scan_id (ScanTarget): Unused compatibility parameter.
            request_id (str | None): Unused compatibility parameter.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Unused compatibility parameter.
        """
        que = self._queues[queue]
        if que.locks:
            que.status = ScanQueueStatus.RUNNING
            return
        if que.worker_status in (
            InstructionQueueStatus.PAUSED,
            InstructionQueueStatus.DEFERRED_PAUSE,
        ):
            que.worker_status = InstructionQueueStatus.RUNNING
        que.status = ScanQueueStatus.RUNNING

    @coordinated
    @requires_queue
    def set_abort(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        exit_info: ExitInfoType | None = None,
        user_call: bool = True,
    ) -> None:
        """Abort the scan and remove it from the queue.

        Leave the queue paused after cleanup.

        Args:
            scan_id (ScanTarget): The scan ID to abort. If None, the currently active scan will be
                aborted.
            request_id (str | None): Request identifier selecting a queue item.
            queue (str): The queue name. Defaults to "primary".
            parameter (QueueParameter): Unused compatibility parameter.
            exit_info (ExitInfoType | None): The exit information to set for the aborted scan.
            user_call (bool): Whether the abort was initiated by a user action.
        """
        if exit_info is None:
            exit_info = ("aborted", "user" if user_call else "alarm")
        que = self._queues[queue]
        if request_id is not None:
            target_queue_item = self._get_queue_item_by_request_id(queue, request_id)
            if target_queue_item is None:
                logger.warning(f"Request {request_id} not found in queue {queue}")
                return
            if target_queue_item is not que.active_instruction_queue:
                self._cancel_queue_item(target_queue_item, queue=queue)
                que.remove_queue_item_by_request_id(request_id)
                que.dispatch()
                return
            scan_id = target_queue_item.scan_id
        if scan_id:
            if not isinstance(scan_id, list):
                scan_id = [scan_id]
            current_scan_id = self._get_active_scan_id(queue)
            if not isinstance(current_scan_id, list):
                current_scan_id = [current_scan_id]
            if len(set(scan_id) & set(current_scan_id)) == 0:
                # The scan to abort is not the currently running scan, so we just remove it from the queue
                target_queue_item = next(
                    (
                        instruction_queue
                        for instruction_queue in self._queues[queue].queue
                        if len(set(scan_id) & set(instruction_queue.scan_id)) > 0
                    ),
                    None,
                )
                if target_queue_item is not None:
                    self._cancel_queue_item(target_queue_item, queue=queue)
                self._queues[queue].remove_queue_item(scan_id)
                que.dispatch()
                return

        if que.queue:
            que.status = ScanQueueStatus.PAUSED
        instruction_queue = que.active_instruction_queue
        if (
            instruction_queue is not None
            and que.active_task is not None
            and not que.active_task.future.done()
        ):
            if not instruction_queue.exit_info:
                instruction_queue.exit_info = exit_info
            if instruction_queue.scan_id and instruction_queue.scan_id[-1] is None:
                stop_id: str | list[str] = instruction_queue.queue_id
            else:
                stop_id = [scan_id for scan_id in instruction_queue.scan_id if scan_id is not None]
            self.stop_all_devices(
                stop_id=stop_id,
                devices=self._get_owned_devices_for_instruction_queue(instruction_queue),
            )
            que.worker_status = InstructionQueueStatus.STOPPED
        que.dispatch()

    @coordinated
    @requires_queue
    def set_halt(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        user_call: bool = True,
    ) -> None:
        """Abort the scan and do not perform any cleanup routines.

        Args:
            scan_id (ScanTarget): Scan identifier or identifiers selecting the request.
            request_id (str | None): Request identifier selecting a queue item.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Unused compatibility parameter.
            user_call (bool): Whether the interruption was requested by a user.
        """
        exit_info = ("halted", "user" if user_call else "alarm")
        instruction_queue = self._queues[queue].active_instruction_queue
        if request_id is not None:
            halt_active = self._get_queue_item_by_request_id(queue, request_id) is instruction_queue
        elif scan_id is not None:
            scan_ids = scan_id if isinstance(scan_id, list) else [scan_id]
            halt_active = bool(instruction_queue and set(scan_ids) & set(instruction_queue.scan_id))
        else:
            halt_active = instruction_queue is not None
        if halt_active and instruction_queue is not None:
            instruction_queue.run_on_exception_hook = False
        self.set_abort(scan_id=scan_id, request_id=request_id, queue=queue, exit_info=exit_info)

    @coordinated
    @requires_queue
    def set_user_completed(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        user_call: bool = True,
    ) -> None:
        """Mark the scan as user completed and perform cleanup routines.

        Args:
            scan_id (ScanTarget): Scan identifier or identifiers selecting the request.
            request_id (str | None): Request identifier selecting a queue item.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Unused compatibility parameter.
            user_call (bool): Whether the interruption was requested by a user.
        """
        exit_info = ("user_completed", "user" if user_call else "alarm")
        queue_state_prior_abort = self._queues[queue].status
        self.set_abort(scan_id=scan_id, request_id=request_id, queue=queue, exit_info=exit_info)
        self._queues[queue].status = queue_state_prior_abort

    @coordinated
    @requires_queue
    def set_clear(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """Pause the queue and clear all its elements.

        Args:
            scan_id (ScanTarget): Unused compatibility parameter.
            request_id (str | None): Unused compatibility parameter.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Unused compatibility parameter.
        """
        logger.info("clearing queue")
        que = self._queues[queue]
        que.status = ScanQueueStatus.PAUSED
        que.worker_status = InstructionQueueStatus.STOPPED
        que.clear()
        que.dispatch()

    @coordinated
    @requires_queue
    def set_restart(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        """Abort and restart the currently running scan. The active scan will be aborted.

        Args:
            scan_id (ScanTarget): Scan identifier or identifiers selecting the request.
            request_id (str | None): Request identifier selecting a queue item.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Additional command parameters.
        """
        que = self._queues.get(queue)
        if que is None:
            return
        if not scan_id:
            scan_id = self._get_active_scan_id(queue)
        if not scan_id:
            return
        if isinstance(scan_id, list):
            scan_id = next((current_id for current_id in scan_id if current_id), None)
        if not scan_id:
            return
        # Find the scan in the active queue.
        instruction_queue = next((iq for iq in que.queue if scan_id in iq.scan_id), None)
        if instruction_queue is None:
            logger.error(f"Scan {scan_id} not found in queue {queue}")
            return
        if instruction_queue.status in [
            InstructionQueueStatus.IDLE,
            InstructionQueueStatus.PENDING,
        ]:
            # If the scan is not running, we don't need to restart it.
            return
        restart_scan_msg = instruction_queue.scan_msgs[0].model_copy(deep=True)
        request_id = parameter.get("RID") if parameter else None
        if request_id:
            restart_scan_msg.metadata["RID"] = request_id
        instruction_queue.reason = "restart"

        scan_restart_msg = messages.ScanRestartMessage(
            original_scan_id=scan_id, scan_msg=restart_scan_msg
        )
        self.connector.send(MessageEndpoints.scan_restart(), scan_restart_msg)
        if restart_scan_msg.allow_restart:
            logger.info(f"Restarting scan {scan_id} in queue {queue}")
            # Queue the replacement before stopping the original so the restarted scan is next.
            self.add_to_queue(queue, restart_scan_msg, 1)
        else:
            logger.info(f"Scan {scan_id} restart not allowed, only sending ScanRestartMessage")

        if que.active_instruction_queue is not instruction_queue:
            return
        original_queue_status = que.status
        que.status = ScanQueueStatus.PAUSED
        if que.worker_status in [
            InstructionQueueStatus.RUNNING,
            InstructionQueueStatus.PAUSED,
            InstructionQueueStatus.DEFERRED_PAUSE,
        ]:
            devices = self._get_owned_devices_for_instruction_queue(instruction_queue)
            self.stop_all_devices(stop_id=scan_id, devices=devices)
            que.worker_status = InstructionQueueStatus.STOPPED
        que.status = original_queue_status

    @coordinated
    @requires_queue
    def set_lock(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        """Add a lock to the queue.

        When allow_device_instructions is False, block dispatch until the lock is released.
        When it is True, allow dispatch of requests that are not scans.

        Args:
            scan_id (ScanTarget): Unused compatibility parameter.
            request_id (str | None): Unused compatibility parameter.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Additional command parameters.

        Raises:
            ValueError: The lock parameters, reason, or identifier are missing.
        """
        if not parameter:
            raise ValueError("Missing parameter for lock action")
        lock_reason = parameter.get("reason")
        if not lock_reason:
            raise ValueError("Missing lock reason in lock parameter")
        identifier = parameter.get("identifier")
        if not identifier:
            raise ValueError("Missing lock identifier in lock parameter")
        allow_device_instructions = parameter.get("allow_device_instructions", True)
        self.add_queue_lock(
            queue_name=queue,
            lock=messages.ScanQueueLock(
                reason=lock_reason,
                identifier=identifier,
                allow_device_instructions=allow_device_instructions,
            ),
        )

    @coordinated
    @requires_queue
    def set_release_lock(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        """Remove a lock from the queue. The queue will proceed if no more locks are present.

        Args:
            scan_id (ScanTarget): Unused compatibility parameter.
            request_id (str | None): Unused compatibility parameter.
            queue (str): Queue to receive the operation.
            parameter (QueueParameter): Additional command parameters.

        Raises:
            ValueError: The lock parameters or identifier are missing.
        """
        if not parameter:
            raise ValueError("Missing parameter for release_lock action")
        identifier = parameter.get("identifier")
        if not identifier:
            raise ValueError("Missing lock identifier in release_lock parameter")
        self.remove_queue_lock(
            queue_name=queue, lock=messages.ScanQueueLock(reason="", identifier=identifier)
        )

    @coordinated
    def send_queue_status(self) -> None:
        """Publish current state off the owner, coalescing ordinary pending snapshots.

        Terminal snapshots are ordered barriers so cancellation and completion cannot disappear
        behind a newer status update. Intermediate nonterminal snapshots may be replaced when
        publication is slower than queue operations.
        """
        snapshot = self._take_snapshot()
        terminal_states = {
            item.queue_id: item.status
            for queue in self._queues.values()
            for item in queue.queue
            if item.status
            in (
                InstructionQueueStatus.CANCELLED,
                InstructionQueueStatus.COMPLETED,
                InstructionQueueStatus.STOPPED,
            )
        }
        terminal = any(
            self._terminal_states.get(queue_id) != status
            for queue_id, status in terminal_states.items()
        )
        self._terminal_states = terminal_states
        if terminal:
            self._publisher.post(self._publish_snapshot, snapshot)
        else:
            self._publisher.post_latest("queue_status", self._publish_snapshot, snapshot)

    def describe_queue(self) -> list[str]:
        """Describe a detached queue snapshot without accessing live items.

        Returns:
            list[str]: Formatted table strings for the current queues.
        """
        return self._describe_snapshot(self.export_queue())

    def export_queue(self) -> dict[str, messages.ScanQueueStatus]:
        """Return detached status messages without exposing live queue state.

        Returns:
            dict[str, messages.ScanQueueStatus]: Detached status messages indexed by queue name.
        """
        return self.get_snapshot().to_messages()

    def get_snapshot(self) -> QueueSnapshot:
        """Return an immutable status snapshot, also available after shutdown.

        Pending scan numbers are estimates based on the last counter observation. External
        counter changes are refreshed on the publication thread at 0.25-second intervals.

        Returns:
            QueueSnapshot: Immutable queue snapshot, including the cached snapshot after shutdown.
        """
        try:
            return self._coordinator.call(self._take_snapshot)
        except QueueClosedError:
            return self._snapshot

    def post_abort(self, *, scan_id: ScanTarget, queue: str, exit_info: ExitInfoType) -> None:
        """Forward an alarm interruption to the coordinator without blocking Redis.

        Args:
            scan_id (ScanTarget): Scan identifier or identifiers selecting the request.
            queue (str): Queue to receive the operation.
            exit_info (ExitInfoType): Terminal status and interruption source used during cleanup.
        """
        self._coordinator.post(self.set_abort, scan_id=scan_id, queue=queue, exit_info=exit_info)

    def request_status_update(self) -> None:
        """Let a worker request a snapshot without waiting for the coordinator."""
        self._coordinator.post(self.send_queue_status, internal=True)

    def shutdown(self) -> None:
        """Close admission, stop tasks, drain completions, and join the coordinator."""
        if (
            self._coordinator.is_owner
            or self._publisher.is_owner
            or getattr(self._task_context, "active", False)
        ):
            self._request_shutdown()
            return
        with self._shutdown_lock:
            if not self._shutdown_done:
                threads = self._coordinator.begin_shutdown(self._begin_shutdown)
                try:
                    for endpoint, callback in self._subscriptions():
                        self.connector.unregister(topics=endpoint, cb=callback)
                finally:
                    for thread in threads:
                        thread.join()
                    self._coordinator.join()
                    self._publisher.begin_shutdown(lambda: None)
                    self._publisher.join()
                    self._shutdown_done = True
        joiner = self._shutdown_joiner
        if joiner is not None and joiner is not threading.current_thread():
            joiner.join()

    @coordinated
    def set_number_baseline(self, scan_number: int) -> None:
        """Cache the initialized counter for pending-item status snapshots.

        Args:
            scan_number (int): Reserved or initialized scan counter value.
        """
        self._last_scan_number = scan_number
        self._number_revision += 1
        if not self._counter_monitor_started:
            self._counter_monitor_started = True
            self._publisher.post(self._schedule_counter_refresh)

    def prepare_task(
        self,
        item: DirectInstructionQueueItem,
        scan: ScanBase,
        control: ScanControl,
        dataset_on_hold: bool,
    ) -> None:
        """Reserve counters off the owner and acknowledge startup before scan hooks.

        Args:
            item (DirectInstructionQueueItem): Queue item associated with this task.
            scan (ScanBase): Scan instance to execute or describe.
            control (ScanControl): Channel carrying pause, stop, and cleanup instructions.
            dataset_on_hold (bool): Whether to reuse the current dataset number.
        """
        control.checkpoint(lambda: None)
        scan_number = scan.scan_info.scan_number
        dataset_number = scan.scan_info.dataset_number
        if scan.is_scan and scan_number is None:
            with self._number_lock:
                scan_number = self.parent.scan_number + 1
                self.parent.scan_number = scan_number
                dataset_number = self.parent.dataset_number
                if not dataset_on_hold:
                    dataset_number += 1
                    self.parent.dataset_number = dataset_number
                self._coordinator.call_internal(
                    self._task_started, item, scan_number, dataset_number
                )
        else:
            self._coordinator.call_internal(self._task_started, item, scan_number, dataset_number)

    @coordinated
    def reap_task(self, thread: threading.Thread) -> None:
        """Join finished threads without blocking owner events on future callbacks.

        Args:
            thread (threading.Thread): Task thread to join once its callbacks finish.
        """
        if thread.is_alive():
            self._coordinator.schedule(0.01, lambda: self.reap_task(thread))
            return
        if thread.ident is not None:
            thread.join()
        self._task_threads.discard(thread)

    #############################################
    ############### Helper Methods ##############
    #############################################

    def _queue_view(self) -> Mapping[str, ScanQueue]:
        """Capture a read-only view of the queue registry.

        Returns:
            Mapping[str, ScanQueue]: Read-only snapshot of the queue registry.
        """
        self._registry_snapshot = MappingProxyType(dict(self._queues))
        return self._registry_snapshot

    def _finish_queue_removal(self, queue_name: str, future: Future[ScanOutcome]) -> None:
        """Dispatch a recreated queue only after its predecessor has completed.

        Args:
            queue_name (str): Name of the queue to manage.
            future (Future[ScanOutcome]): Future containing the task result or exception.
        """
        if self._closing_queues.get(queue_name) is future:
            del self._closing_queues[queue_name]
            queue = self._queues.get(queue_name)
            if queue is not None:
                queue.dispatch()

    def _start_scan_queue_register(self) -> None:
        """Register callbacks for queue insert, modification, and order messages."""
        self.connector.register(MessageEndpoints.scan_queue_insert(), cb=self._scan_queue_callback)
        self.connector.register(
            MessageEndpoints.scan_queue_modification(), cb=self._scan_queue_modification_callback
        )
        self.connector.register(
            MessageEndpoints.scan_queue_order_change(), cb=self._scan_queue_order_callback
        )

    def _scan_queue_callback(self, msg: MessageObject[messages.ScanQueueMessage]) -> None:
        """Submit a Redis insert request to the coordinator or reject closed admission.

        Args:
            msg (MessageObject[messages.ScanQueueMessage]): Request or event message to handle.
        """
        scan_msg = cast(messages.ScanQueueMessage, msg.value)
        logger.info(f"Receiving scan: {scan_msg.content}")
        queue = scan_msg.content.get("queue", "primary")
        if not self._coordinator.post(self.add_to_queue, queue, scan_msg.model_copy(deep=True)):
            self.connector.send(
                MessageEndpoints.scan_queue_request_response(),
                messages.RequestResponseMessage(
                    accepted=False,
                    message="Scan queue is shutting down",
                    metadata=scan_msg.metadata,
                ),
            )

    def _scan_queue_modification_callback(
        self, msg: MessageObject[messages.ScanQueueModificationMessage]
    ) -> None:
        """Forward a detached modification request to the coordinator.

        Args:
            msg (MessageObject[messages.ScanQueueModificationMessage]): Request or event message to
                handle.
        """
        scan_mod_msg = cast(messages.ScanQueueModificationMessage, msg.value)
        logger.info(f"Receiving scan modification: {scan_mod_msg.content}")
        if scan_mod_msg:
            self._coordinator.post(self._apply_modification, scan_mod_msg.model_copy(deep=True))

    def _apply_modification(self, msg: messages.ScanQueueModificationMessage) -> None:
        """Apply a queue modification and publish the resulting queue status.

        Args:
            msg (messages.ScanQueueModificationMessage): Request or event message to handle.
        """
        self.scan_interception(msg)
        self.send_queue_status()

    def _scan_queue_order_callback(
        self, msg: MessageObject[messages.ScanQueueOrderMessage]
    ) -> None:
        """Forward a queue order request to its handler.

        Args:
            msg (MessageObject[messages.ScanQueueOrderMessage]): Request or event message to
                handle.
        """
        message = cast(messages.ScanQueueOrderMessage, msg.value)
        self._coordinator.post(self._handle_scan_order_change, message.model_copy(deep=True))

    @coordinated
    def _handle_scan_order_change(self, msg: messages.ScanQueueOrderMessage) -> None:
        """Handle the scan queue order change request.

        Args:
            msg (messages.ScanQueueOrderMessage): ScanQueueOrderMessage
        """
        logger.info(f"Handling scan queue order change: {msg}")
        target_queue = msg.queue
        scan_queue = self._queues.get(target_queue)
        if scan_queue is None or scan_queue.status != ScanQueueStatus.PAUSED:
            logger.warning(f"Queue {target_queue} is no longer available for reordering")
            return
        queue = scan_queue.queue
        queue_item = self._get_queue_item_by_scan_id(msg)
        if not queue_item:
            logger.error(f"Scan {msg.scan_id} not found in queue {target_queue}")
            return
        if msg.action == "move_to":
            # move the scan to the target position
            if msg.target_position is None:
                logger.error("Missing target_position")
                return
            position = max(0, min(msg.target_position, len(queue) - 1))
            queue.remove(queue_item)
            queue.insert(position, queue_item)
        if msg.action == "move_up":
            # move the scan up by one position
            idx = queue.index(queue_item)
            if idx == 0:
                return
            queue.remove(queue_item)
            queue.insert(idx - 1, queue_item)
        if msg.action == "move_down":
            # move the scan down by one position
            idx = queue.index(queue_item)
            if idx == len(queue) - 1:
                return
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
        self.send_queue_status()

    def _get_queue_item_by_scan_id(
        self, msg: messages.ScanQueueOrderMessage
    ) -> DirectInstructionQueueItem | None:
        """Get the queue item by scan_id.

        Args:
            msg (messages.ScanQueueOrderMessage): ScanQueueOrderMessage

        Returns:
            DirectInstructionQueueItem | None: Matching queue item, or None if no item matches.
        """
        queue = self._queues[msg.queue]
        for instruction_queue in queue.queue:
            if msg.scan_id in instruction_queue.scan_id:
                return instruction_queue
        return None

    def _cancel_queue_item(self, target_queue_item: DirectInstructionQueueItem, queue: str) -> None:
        """Publish cancellation of a pending queue item.

        Publish its terminal state before removal so clients can distinguish cancellation
        from a missing item.

        Args:
            target_queue_item (DirectInstructionQueueItem): The queue item to cancel.
            queue (str): Unused compatibility parameter.
        """
        del queue  # queue kept for signature symmetry with callers
        target_queue_item._status = InstructionQueueStatus.CANCELLED
        self.send_queue_status()

    def _get_queue_item_by_request_id(
        self, queue: str, request_id: str
    ) -> DirectInstructionQueueItem | None:
        """Find a queued item by its request identifier.

        Args:
            queue (str): Queue to receive the operation.
            request_id (str): Request identifier selecting a queue item.

        Returns:
            DirectInstructionQueueItem | None: Matching queue item, or None if no item matches.
        """
        for instruction_queue in self._queues[queue].queue:
            if any(msg.metadata.get("RID") == request_id for msg in instruction_queue.scan_msgs):
                return instruction_queue
        return None

    def _get_active_scan_id(self, queue: str) -> str | None:
        """Read the active scan identifier for a named queue.

        Args:
            queue (str): Queue to receive the operation.

        Returns:
            str | None: Active scan identifier, or None if the queue has no active scan.
        """
        instr_queue = self._queues[queue].active_instruction_queue
        if instr_queue is None or instr_queue.active_scan is None:
            return None
        return instr_queue.active_scan.scan_info.scan_id

    def _get_owned_devices_for_instruction_queue(
        self, instruction_queue: DirectInstructionQueueItem
    ) -> list[str]:
        """Find devices whose locks belong to the active request.

        Args:
            instruction_queue (DirectInstructionQueueItem): Queue item whose active request owns
                the locks.

        Returns:
            list[str]: Device names whose locks belong to the request.
        """
        registry = getattr(self.parent, "device_lock_registry", None)
        if registry is None:
            return []
        if instruction_queue.active_scan is None:
            return []
        request_id = instruction_queue.active_scan.scan_info.metadata.get("RID")
        if request_id is None:
            return []
        return registry.get_owned_devices(request_id)

    def _publish_snapshot(self, snapshot: QueueSnapshot) -> None:
        """Publish a detached snapshot, queue display, and queue-length metrics.

        Args:
            snapshot (QueueSnapshot): Detached queue statuses to publish.
        """
        queues = snapshot.to_messages()
        logger.info("New scan queue:")
        for table in self._describe_snapshot(queues):
            logger.info(f"\n {table}")
        self.connector.set_and_publish(
            MessageEndpoints.scan_queue_status(), messages.ScanQueueStatusMessage(queue=queues)
        )
        self.connector.publish_metrics(
            "scan_queue_length", {name: len(status.info) for name, status in queues.items()}
        )

    @staticmethod
    def _describe_snapshot(queues: dict[str, messages.ScanQueueStatus]) -> list[str]:
        """Format detached queue statuses as display tables.

        Args:
            queues (dict[str, messages.ScanQueueStatus]): Detached queue statuses to format.

        Returns:
            list[str]: Formatted table strings for the supplied queues.
        """
        tables: list[str] = []
        console = Console()
        for name, queue in queues.items():
            table = Table(title=f"{name} queue / {queue.status}")
            for column in ("queue_id", "scan_id", "is_scan", "type", "scan_number", "IQ status"):
                table.add_column(column, justify="center")
            for item in queue.info:
                table.add_row(
                    item.queue_id,
                    ", ".join(str(value) for value in item.scan_id),
                    ", ".join(str(value) for value in item.is_scan),
                    ", ".join(block.msg.scan_type for block in item.request_blocks),
                    ", ".join(str(value) for value in item.scan_number),
                    item.status,
                )
            with console.capture() as capture:
                console.print(table)
            tables.append(capture.get())
        return tables

    def _describe_queue_items(self, queue: ScanQueue) -> list[messages.QueueInfoEntry]:
        """Describe queue items using scan-number offsets calculated in one queue walk.

        Args:
            queue (ScanQueue): Queue whose item order determines pending scan numbers.

        Returns:
            list[messages.QueueInfoEntry]: Item descriptions in queue order.
        """
        offset = 1
        offsets: list[int | None] = []
        for item in queue.queue:
            if item.status in (InstructionQueueStatus.RUNNING, InstructionQueueStatus.COMPLETED):
                offsets.append(None)
                continue
            offsets.append(offset)
            offset += sum(bool(scan_id) for scan_id in item.scan_id)
        return [
            item.describe_at_offset(offset if item_offset is None else item_offset)
            for item, item_offset in zip(queue.queue, offsets)
        ]

    def _take_snapshot(self) -> QueueSnapshot:
        """Serialize current queue statuses and cache the registry view.

        Returns:
            QueueSnapshot: Immutable snapshot of current queue statuses.
        """
        self._queue_view()
        self._snapshot = QueueSnapshot(
            tuple(
                (
                    name,
                    cast(
                        bytes,
                        msgpack.dumps(
                            messages.ScanQueueStatus(
                                info=self._describe_queue_items(queue),
                                status=cast(
                                    Literal["PAUSED", "RUNNING", "LOCKED"], queue.status.name
                                ),
                                locks=list(queue.locks.values()),
                            ).model_dump()
                        ),
                    ),
                )
                for name, queue in self._queues.items()
            )
        )
        return self._snapshot

    def _schedule_counter_refresh(self) -> None:
        """Schedule the next scan counter observation on the publication thread."""
        self._publisher.schedule(0.25, self._refresh_scan_counter)

    def _refresh_scan_counter(self) -> None:
        """Observe externally changed counters without blocking queue operations."""
        try:
            revision = self._coordinator.call_internal(lambda: self._number_revision)
            scan_number = self.parent.scan_number
        except QueueClosedError:
            return
        except Exception:
            logger.exception("Failed to refresh the scan counter")
        else:
            self._coordinator.post(
                self._apply_counter_refresh, revision, scan_number, internal=True
            )
        self._schedule_counter_refresh()

    def _apply_counter_refresh(self, revision: int, scan_number: int) -> None:
        """Apply an observation unless a task has reserved a newer counter in the meantime.

        Args:
            revision (int): Counter revision observed before the external read.
            scan_number (int): Latest external scan counter value.
        """
        if self._closing or revision != self._number_revision:
            return
        if scan_number != self._last_scan_number:
            self._last_scan_number = scan_number
            self._number_revision += 1
            self.send_queue_status()

    def _request_shutdown(self) -> None:
        """Delegate joins when shutdown originates in a managed callback."""
        with self._shutdown_request_lock:
            if self._shutdown_joiner is None:
                self._shutdown_joiner = threading.Thread(
                    target=self.shutdown, name="ScanQueueShutdown"
                )
                self._shutdown_joiner.start()

    def _task_started(
        self, item: DirectInstructionQueueItem, scan_number: int | None, dataset_number: int | None
    ) -> None:
        """Assign reserved counters and acknowledge task startup on the coordinator.

        Args:
            item (DirectInstructionQueueItem): Queue item associated with this task.
            scan_number (int | None): Reserved or initialized scan counter value.
            dataset_number (int | None): Reserved dataset counter value.
        """
        if self._closing or item.parent.signal_event.is_set():
            return
        scan = item.active_scan
        if scan is None:
            return
        scan.scan_info.scan_number = scan_number
        scan.scan_info.dataset_number = dataset_number
        if scan_number is not None:
            self._last_scan_number = scan_number
            self._number_revision += 1
        item.set_active()
        self.send_queue_status()

    def _begin_shutdown(self) -> tuple[threading.Thread, ...]:
        """Stop queue admission and collect task threads to join.

        Returns:
            tuple[threading.Thread, ...]: Task threads to join outside the coordinator.
        """
        self._closing = True
        for queue_name in tuple(self._queues):
            self.remove_queue(queue_name, skip_primary=False, emit_status=False)
        self._take_snapshot()
        return tuple(self._task_threads)

    def _subscriptions(self) -> tuple[tuple[Any, Callable[..., None]], ...]:
        """List queue endpoints and their registered callbacks.

        Returns:
            tuple[tuple[Any, Callable[..., None]], ...]: Endpoint and callback pairs registered for
                queue ingress.
        """
        return (
            (MessageEndpoints.scan_queue_insert(), self._scan_queue_callback),
            (MessageEndpoints.scan_queue_modification(), self._scan_queue_modification_callback),
            (MessageEndpoints.scan_queue_order_change(), self._scan_queue_order_callback),
        )
