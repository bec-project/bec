"""Single-owner management and public commands for named scan queues."""

from __future__ import annotations

import threading
import traceback
import uuid
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
        self._targets: dict[
            str,
            tuple[
                ScanQueue, tuple[DirectInstructionQueueItem, ...], DirectInstructionQueueItem | None
            ],
        ] = {}
        self._closing = False
        self._device_stops: dict[str, Future[messages.DeviceStopResponse]] = {}
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

    #############################################
    ############## Queue Management #############
    #############################################

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
    def add_queue(self, queue_name: str) -> None:
        """Create a queue; execution waits for an older queue with this name to retire.

        Args:
            queue_name (str): Name of the queue to manage.
        """
        queue = self._get_or_create_queue(queue_name)
        queue.schedule_idle_expiry()
        self.send_queue_status()

    def remove_queue(
        self, queue_name: str, skip_primary: bool = True, emit_status: bool = True
    ) -> None:
        """Remove a queue and stop its task without blocking the coordinator.

        Args:
            queue_name (str): Queue to remove.
            skip_primary (bool): Preserve primary when True.
            emit_status (bool): Publish the resulting queue snapshot when True.
        """
        queue = self._bind_queue(queue_name)
        if queue is None:
            return
        self._coordinator.call(self._remove_queue, queue, skip_primary, emit_status)

    def _remove_queue(self, queue: ScanQueue, skip_primary: bool, emit_status: bool) -> None:
        """Remove only the queue instance captured by its caller."""
        queue_name = queue.queue_name
        if queue_name == "primary" and skip_primary:
            return
        if self._queues.get(queue_name) is not queue:
            return
        self._queues.pop(queue_name)
        self._capture_targets()
        queue.cancel_idle_expiry()
        queue.signal_event.set()
        task = queue.active_task
        if task is not None:
            self._closing_queues[queue_name] = task.future
            task.future.add_done_callback(
                lambda done: self._coordinator.post(
                    self._finish_queue_removal, queue, done, internal=True
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
        if queue.queue or queue.active_task is not None or queue.locks:
            return
        self._queues.pop(queue.queue_name)
        queue.signal_event.set()
        self.send_queue_status()

    def add_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> bool:
        """Bind lock addition to the queue instance created or captured at admission."""
        return self._submit_instruction("lock", queue=queue_name, parameter=lock.model_dump())

    def remove_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> bool:
        """Release a lock only on the queue instance captured at admission."""
        return self._submit_instruction(
            "release_lock", queue=queue_name, parameter=lock.model_dump()
        )

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
                        if callback != self._device_stop_callback:
                            self.connector.unregister(topics=endpoint, cb=callback)
                finally:
                    for thread in threads:
                        thread.join()
                    self.connector.unregister(
                        topics=MessageEndpoints.device_stop_response(),
                        cb=self._device_stop_callback,
                    )
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
        self, item: DirectInstructionQueueItem, control: ScanControl, dataset_on_hold: bool
    ) -> None:
        """Reserve counters off the owner and acknowledge startup before scan hooks.

        Args:
            item (DirectInstructionQueueItem): Queue item associated with this task.
            control (ScanControl): Channel carrying pause, stop, and cleanup instructions.
            dataset_on_hold (bool): Whether to reuse the current dataset number.
        """
        control.checkpoint(lambda: None)
        scan_number = item.assigned_scan_number
        dataset_number = item.assigned_dataset_number
        if scan_number is None:
            scan_number = item.scan.scan_info.scan_number
            dataset_number = item.scan.scan_info.dataset_number
        if item.is_scan[0] and scan_number is None:
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
    ############ Instruction Methods ############
    #############################################

    def add_to_queue(
        self, scan_queue: str, msg: messages.ScanQueueMessage, position: int = -1
    ) -> bool:
        """Add a new ScanQueueMessage to the queue.

        Args:
            scan_queue (str): the queue that should receive the new message
            msg (messages.ScanQueueMessage): ScanQueueMessage
            position (int): Insertion index; -1 appends to the queue.

        Returns:
            bool: Whether the request was validated and inserted successfully.
        """
        queue = self._bind_queue(scan_queue, create=True)
        return self._coordinator.call(
            self._insert_into_queue, queue, msg.model_copy(deep=True), position
        )

    def _insert_into_queue(
        self, queue: ScanQueue, msg: messages.ScanQueueMessage, position: int = -1
    ) -> bool:
        """Admit new work only to its captured queue instance."""
        if self._queues.get(queue.queue_name) is not queue or self._closing:
            return False
        try:
            queue.insert(msg, position=position)
            queue.dispatch()
            self.send_queue_status()
            return True
        # pylint: disable=broad-except
        except Exception as exc:
            queue.schedule_idle_expiry()
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
                metadata={**msg.metadata, "queue": queue.queue_name, "request_rejected": True},
            )
            return False

    def scan_interception(self, scan_mod_msg: messages.ScanQueueModificationMessage) -> bool:
        """Bind a Redis modification to its queue instance and acquisition before posting."""
        return self._submit_instruction(
            "continue" if scan_mod_msg.action == "resume" else scan_mod_msg.action,
            scan_id=scan_mod_msg.scan_id,
            request_id=scan_mod_msg.request_id,
            queue=scan_mod_msg.queue,
            parameter=scan_mod_msg.parameter,
            queue_instance_id=scan_mod_msg.metadata.get("queue_instance_id"),
            post=True,
            require_identity=True,
        )

    def set_pause(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> bool:
        """Bind pause to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "pause", scan_id=scan_id, request_id=request_id, queue=queue, parameter=parameter
        )

    def set_deferred_pause(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> bool:
        """Bind deferred_pause to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "deferred_pause",
            scan_id=scan_id,
            request_id=request_id,
            queue=queue,
            parameter=parameter,
        )

    def set_continue(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> bool:
        """Bind continue to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "continue", scan_id=scan_id, request_id=request_id, queue=queue, parameter=parameter
        )

    def set_abort(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        exit_info: ExitInfoType | None = None,
        user_call: bool = True,
    ) -> bool:
        """Bind abort to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.
            exit_info (ExitInfoType | None): Terminal status and interruption source.
            user_call (bool): Whether the request came from a user.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "abort",
            scan_id=scan_id,
            request_id=request_id,
            queue=queue,
            parameter=parameter,
            exit_info=exit_info,
            user_call=user_call,
        )

    def set_halt(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        user_call: bool = True,
    ) -> bool:
        """Bind halt to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.
            user_call (bool): Whether the request came from a user.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "halt",
            scan_id=scan_id,
            request_id=request_id,
            queue=queue,
            parameter=parameter,
            user_call=user_call,
        )

    def set_user_completed(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        user_call: bool = True,
    ) -> bool:
        """Bind user_completed to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.
            user_call (bool): Whether the request came from a user.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "user_completed",
            scan_id=scan_id,
            request_id=request_id,
            queue=queue,
            parameter=parameter,
            user_call=user_call,
        )

    def set_clear(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> bool:
        """Bind clear to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "clear", scan_id=scan_id, request_id=request_id, queue=queue, parameter=parameter
        )

    def set_restart(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> bool:
        """Bind restart to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "restart", scan_id=scan_id, request_id=request_id, queue=queue, parameter=parameter
        )

    def set_lock(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> bool:
        """Bind lock to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "lock", scan_id=scan_id, request_id=request_id, queue=queue, parameter=parameter
        )

    def set_release_lock(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> bool:
        """Bind release_lock to its current target before entering the coordinator.

        Args:
            scan_id (ScanTarget): Public scan identifiers; request_id takes precedence.
            request_id (str | None): Acquisition request identifier.
            queue (str): Queue whose current instance receives the operation.
            parameter (QueueParameter): Additional command parameters.

        Returns:
            bool: Whether the bound operation was applied.
        """
        return self._submit_instruction(
            "release_lock", scan_id=scan_id, request_id=request_id, queue=queue, parameter=parameter
        )

    def post_abort(
        self,
        *,
        scan_id: ScanTarget,
        queue: str,
        exit_info: ExitInfoType,
        request_id: str | None = None,
    ) -> bool:
        """Bind an acquisition alarm before asynchronously submitting its abort."""
        return self._submit_instruction(
            "abort",
            scan_id=scan_id,
            request_id=request_id,
            queue=queue,
            exit_info=exit_info,
            post=True,
            require_identity=True,
        )

    #############################################
    ############### Helper Methods ##############
    #############################################

    def _capture_targets(self) -> None:
        """Publish immutable target references for non-owner admission."""
        self._targets = {
            name: (queue, tuple(queue.queue), queue.active_instruction_queue)
            for name, queue in self._queues.items()
        }

    def _bind_queue(self, name: str, *, create: bool = False) -> ScanQueue | None:
        """Capture a queue instance; creation is a manager registry operation."""
        if self._coordinator.is_owner:
            queue = self._queues.get(name)
        else:
            binding = self._targets.get(name)
            queue = binding[0] if binding is not None else None
        if queue is None and create:
            queue = self._coordinator.call(self._get_or_create_queue, name)
        return queue

    def _bind_items(
        self, queue: ScanQueue, scan_id: ScanTarget, request_id: str | None, *, current: bool = True
    ) -> tuple[DirectInstructionQueueItem, ...]:
        """Capture item references once; never inspect a live deque off-owner."""
        if self._coordinator.is_owner:
            items = tuple(queue.resolve_targets(scan_id, request_id))
            return items if scan_id or request_id is not None or current else ()
        binding = self._targets.get(queue.queue_name)
        if binding is None or binding[0] is not queue:
            return ()
        _, items, active = binding
        if active is not None and active not in items:
            items += (active,)
        if request_id is not None:
            return tuple(item for item in items if item.request.metadata.get("RID") == request_id)
        if scan_id:
            ids = {
                identifier
                for identifier in (scan_id if isinstance(scan_id, list) else [scan_id])
                if identifier is not None
            }
            return tuple(item for item in items if not ids.isdisjoint(item.scan_id))
        if current:
            if active is not None:
                return (active,)
            # Retain local compatibility for an explicitly paused pending head.
            if items and items[0].status == InstructionQueueStatus.PAUSED:
                return (items[0],)
        return ()

    def _submit_instruction(
        self,
        action: str,
        *,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        exit_info: ExitInfoType | None = None,
        user_call: bool = True,
        queue_instance_id: str | None = None,
        post: bool = False,
        require_identity: bool = False,
    ) -> bool:
        """Bind command scope before admission, then apply only those references."""
        if action in ("lock", "release_lock"):
            if not parameter:
                raise ValueError(f"Missing parameter for {action} action")
            if action == "lock" and not parameter.get("reason"):
                raise ValueError("Missing lock reason in lock parameter")
            if not parameter.get("identifier"):
                raise ValueError("Missing lock identifier in lock parameter")
        parameter = dict(parameter) if parameter is not None else None
        target = self._bind_queue(queue, create=action == "lock" and queue_instance_id is None)
        if target is None or (
            queue_instance_id is not None and target.instance_id != queue_instance_id
        ):
            return False
        acquisition = action in ("pause", "abort", "halt", "user_completed", "restart")
        items = self._bind_items(
            target,
            scan_id,
            request_id,
            current=action not in ("clear", "lock", "release_lock") and not require_identity,
        )
        if (acquisition or scan_id or request_id is not None) and not items:
            return False
        if action == "restart":
            replacement_id = parameter.get("RID") if parameter else None
            if (
                not isinstance(replacement_id, str)
                or not replacement_id.strip()
                or any(replacement_id == item.request.metadata.get("RID") for item in items)
            ):
                return False
        arguments = (target, items, action, parameter, exit_info, user_call)
        if post:
            return self._coordinator.post(self._apply_instruction, *arguments)
        return self._coordinator.call(self._apply_instruction, *arguments)

    def _apply_instruction(
        self,
        queue: ScanQueue,
        items: tuple[DirectInstructionQueueItem, ...],
        action: str,
        parameter: QueueParameter,
        exit_info: ExitInfoType | None,
        user_call: bool,
    ) -> bool:
        """Apply a bound command, rejecting retired targets instead of retargeting."""
        if self._queues.get(queue.queue_name) is not queue or self._closing:
            return False
        if items and not all(queue.contains(item) for item in items):
            return False
        if any(item.control is not None and item.control.terminal for item in items):
            return False
        if any(
            item is queue.active_instruction_queue
            and queue.active_task is not None
            and queue.active_task.future.done()
            for item in items
        ):
            return False
        if action in ("pause", "deferred_pause", "continue") and items:
            for item in items:
                if item is queue.active_instruction_queue:
                    continue
                if (
                    action == "continue"
                    and queue.active_instruction_queue is None
                    and queue.queue
                    and queue.queue[0] is item
                    and item.status
                    in (InstructionQueueStatus.PENDING, InstructionQueueStatus.PAUSED)
                ):
                    continue
                return False
        if action == "pause":
            for item in items:
                queue.pause(item)
        elif action == "deferred_pause":
            queue.deferred_pause()
        elif action == "continue":
            queue.resume(next(iter(items), None))
        elif action == "abort":
            if not queue.abort(items, exit_info or ("aborted", "user" if user_call else "alarm")):
                return False
        elif action in ("halt", "user_completed"):
            if not getattr(queue, action)(items, user_call):
                return False
        elif action == "restart":
            if not queue.restart(items[0], parameter):
                return False
        elif action == "clear":
            queue.clear()
        elif action == "lock":
            if not parameter or not parameter.get("reason") or not parameter.get("identifier"):
                raise ValueError("Missing lock parameter, reason or identifier")
            queue.add_lock(
                messages.ScanQueueLock(
                    reason=parameter["reason"],
                    identifier=parameter["identifier"],
                    allow_device_instructions=parameter.get("allow_device_instructions", True),
                )
            )
        elif action == "release_lock":
            if not parameter or not parameter.get("identifier"):
                raise ValueError("Missing release_lock parameter or identifier")
            queue.remove_lock(messages.ScanQueueLock(reason="", identifier=parameter["identifier"]))
        else:
            raise ValueError(f"Unsupported queue action: {action}")
        if action != "release_lock" or queue.status == ScanQueueStatus.RUNNING or queue.locks:
            queue.dispatch()
        self.send_queue_status()
        return True

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

    def request_status_update(self) -> None:
        """Let a worker request a snapshot without waiting for the coordinator."""
        self._coordinator.post(self.send_queue_status, internal=True)

    def stop_all_devices(
        self,
        stop_id: str | list[str] | None = None,
        devices: list[str] | None = None,
        response: Future[messages.DeviceStopResponse] | None = None,
    ) -> None:
        """Send a message to the device server to stop devices.

        Args:
            stop_id (str | list[str] | None): An optional identifier for the stop request. If
                provided, this ID will be added to the list of stopped requests in the device
                server to prevent any instructions associated with this ID raising alarms after the
                stop command is issued. The stop_id can be a scan ID, request ID, or queue ID.
            devices (list[str] | None): Optional list of devices to stop. `None` means stop all
                devices, while an empty list means stop no devices.
            response (Future[messages.DeviceStopResponse] | None): Future for a correlated
                acknowledgement. None preserves manual fire-and-forget device stops.
        """
        msg = messages.VariableMessage(value=devices, metadata={})
        if stop_id is not None:
            msg.metadata["stop_id"] = stop_id
        if response is not None:
            request_id = str(uuid.uuid4())
            self._device_stops[request_id] = response
            msg.metadata["stop_request_id"] = request_id
            self._coordinator.schedule(30, lambda: self._expire_device_stop(request_id, response))
        self.connector.send(MessageEndpoints.stop_devices(), msg)

    def _interrupt_item(self, item: DirectInstructionQueueItem, *, shutdown: bool = False) -> bool:
        """Attach the acknowledgement before signalling the captured worker."""
        if item.control is None:
            return False
        response: Future[messages.DeviceStopResponse] = Future()
        if not item.control.stop(item.exit_info, shutdown=shutdown, device_stop=response):
            return False
        identities = [item.queue_id]
        if item.control.requested_action == "halt":
            identities += [f"{identity}__on-exception" for identity in identities]
        try:
            self.stop_all_devices(
                stop_id=identities, devices=self.get_owned_devices_for_item(item), response=response
            )
        except Exception as exc:
            response.set_exception(exc)
            raise
        return True

    def _device_stop_callback(self, msg: MessageObject[messages.DeviceStopResponse]) -> None:
        """Resolve only the future identified by this device-stop response."""
        self._coordinator.post(self._acknowledge_device_stop, msg.value, internal=True)

    def _acknowledge_device_stop(self, msg: messages.DeviceStopResponse) -> None:
        response = self._device_stops.pop(msg.request_id, None)
        if response is not None and not response.done():
            response.set_result(msg)

    def _expire_device_stop(
        self, request_id: str, response: Future[messages.DeviceStopResponse]
    ) -> None:
        """Expire this exact response future without releasing acquisition ownership."""
        if self._device_stops.get(request_id) is response:
            self._device_stops.pop(request_id)
            if not response.done():
                response.set_exception(TimeoutError("Device-stop acknowledgement timed out"))

    def get_owned_devices_for_item(
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

    def append_to_queue_history(self, item: DirectInstructionQueueItem) -> None:
        """Publish a detached history entry off the queue owner thread."""
        self._publisher.post(
            self.connector.lpush,
            MessageEndpoints.scan_queue_history(),
            item.describe_history(),
            max_size=100,
        )

    def _get_or_create_queue(self, queue_name: str) -> ScanQueue:
        """Return a named queue without publishing an unfinished enclosing command.

        Args:
            queue_name (str): Name of the queue to retrieve or create.

        Returns:
            ScanQueue: Existing or newly created queue owned by this manager.
        """
        if self._closing:
            raise RuntimeError("Scan queues are shutting down")
        if queue_name not in self._queues:
            self._queues[queue_name] = ScanQueue(self, queue_name=queue_name)
            queue = self._queues[queue_name]
            predecessor = self._closing_queues.get(queue_name)
            if predecessor is not None:
                predecessor.add_done_callback(
                    lambda done: self._coordinator.post(
                        self._dispatch_recreated_queue, queue, done, internal=True
                    )
                )
            self._capture_targets()
        return self._queues[queue_name]

    def _queue_view(self) -> Mapping[str, ScanQueue]:
        """Capture a read-only view of the queue registry.

        Returns:
            Mapping[str, ScanQueue]: Read-only snapshot of the queue registry.
        """
        self._capture_targets()
        self._registry_snapshot = MappingProxyType(dict(self._queues))
        return self._registry_snapshot

    def _finish_queue_removal(self, queue: ScanQueue, future: Future[ScanOutcome]) -> None:
        """Retire the captured predecessor's dependency without targeting its replacement."""
        if self._closing_queues.get(queue.queue_name) is future:
            del self._closing_queues[queue.queue_name]

    def _dispatch_recreated_queue(self, queue: ScanQueue, future: Future[ScanOutcome]) -> None:
        """Wake only the replacement instance that registered this dependency."""
        if (
            self._queues.get(queue.queue_name) is queue
            and future.done()
            and queue.queue_name not in self._closing_queues
        ):
            queue.dispatch()
            self.send_queue_status()

    def _start_scan_queue_register(self) -> None:
        """Register queue ingress and device interruption acknowledgements."""
        for endpoint, callback in self._subscriptions():
            self.connector.register(endpoint, cb=callback)

    def _scan_queue_callback(self, msg: MessageObject[messages.ScanQueueMessage]) -> None:
        """Submit a Redis insert request to the coordinator or reject closed admission.

        Args:
            msg (MessageObject[messages.ScanQueueMessage]): Request or event message to handle.
        """
        scan_msg = cast(messages.ScanQueueMessage, msg.value)
        logger.info(f"Receiving scan: {scan_msg.content}")
        try:
            queue = self._bind_queue(scan_msg.queue, create=True)
            admitted = self._coordinator.call(
                self._insert_into_queue, queue, scan_msg.model_copy(deep=True)
            )
            rejection = "Scan queue rejected the request"
        except QueueClosedError:
            admitted = False
            rejection = "Scan queue is shutting down"
        if not admitted:
            self.connector.send(
                MessageEndpoints.scan_queue_request_response(),
                messages.RequestResponseMessage(
                    accepted=False, message=rejection, metadata=scan_msg.metadata
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
            self.scan_interception(scan_mod_msg.model_copy(deep=True))

    def _scan_queue_order_callback(
        self, msg: MessageObject[messages.ScanQueueOrderMessage]
    ) -> None:
        """Capture queue/item targets before submitting an order change."""
        message = cast(messages.ScanQueueOrderMessage, msg.value).model_copy(deep=True)
        self._handle_scan_order_change(message, post=True)

    def _handle_scan_order_change(
        self, msg: messages.ScanQueueOrderMessage, *, post: bool = False
    ) -> bool:
        """Bind order changes to exact queue and item instances."""
        queue = self._bind_queue(msg.queue)
        if (
            queue is None
            or msg.metadata.get("queue_instance_id", queue.instance_id) != queue.instance_id
        ):
            return False
        items = self._bind_items(queue, msg.scan_id, None, current=False)
        if not items:
            return False
        if post:
            return self._coordinator.post(self._apply_order_change, queue, items[0], msg)
        return self._coordinator.call(self._apply_order_change, queue, items[0], msg)

    def _apply_order_change(
        self,
        queue: ScanQueue,
        item: DirectInstructionQueueItem,
        msg: messages.ScanQueueOrderMessage,
    ) -> bool:
        """Reorder only the original queue/item while both remain present."""
        if self._queues.get(queue.queue_name) is not queue or not queue.contains(item):
            return False
        if not queue.reorder(msg, item):
            return False
        self.send_queue_status()
        return True

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
                                info=queue.describe_items(),
                                status=cast(
                                    Literal["PAUSED", "RUNNING", "LOCKED"], queue.status.name
                                ),
                                locks=list(queue.locks.values()),
                                queue_instance_id=queue.instance_id,
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
        first_start = item.assigned_scan_number is None
        item.assign_numbers(scan_number, dataset_number)
        if first_start and scan_number is not None:
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
            (MessageEndpoints.device_stop_response(), self._device_stop_callback),
            (MessageEndpoints.scan_queue_insert(), self._scan_queue_callback),
            (MessageEndpoints.scan_queue_modification(), self._scan_queue_modification_callback),
            (MessageEndpoints.scan_queue_order_change(), self._scan_queue_order_callback),
        )
