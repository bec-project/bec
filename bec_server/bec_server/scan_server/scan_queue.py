from __future__ import annotations

import collections
import functools
import threading
import traceback
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from rich.console import Console
from rich.table import Table

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .instruction_handler import InstructionHandler
from .scan_assembler import ScanAssembler

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_server.scan_server.direct_scan_worker import ScanControl, ScanOutcome, ScanTask
    from bec_server.scan_server.scan_server import ScanServer
    from bec_server.scan_server.scans.scan_base import ScanBase as ScanBase_v4


def requires_queue(fcn: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to ensure that the requested queue exists."""

    @functools.wraps(fcn)
    def wrapper(self: QueueManager, *args: Any, queue: str = "primary", **kwargs: Any) -> Any:
        if queue not in self.queues:
            self.add_queue(queue)
        return fcn(self, *args, queue=queue, **kwargs)

    return wrapper


ExitInfoType: TypeAlias = tuple[
    Literal["halted", "aborted", "user_completed"], Literal["user", "alarm"]
]
ScanTarget: TypeAlias = str | list[str | None] | None
QueueParameter: TypeAlias = dict[str, Any] | None


class InstructionQueueStatus(Enum):
    STOPPED = -1
    PENDING = 0
    IDLE = 1
    PAUSED = 2
    DEFERRED_PAUSE = 3
    RUNNING = 4
    COMPLETED = 5
    CANCELLED = 6


class ScanQueueStatus(Enum):
    PAUSED = 0
    RUNNING = 1
    LOCKED = 2


class QueueManager:
    """The QueueManager manages multiple ScanQueues"""

    def __init__(self, parent: ScanServer) -> None:
        self.parent = parent
        self.connector = parent.connector
        self.queues: dict[str, ScanQueue] = {}
        # Queue registry, queue bookkeeping, and status snapshots share this lock.
        self._lock = threading.RLock()
        self._closing_queue_condition = threading.Condition(self._lock)
        self._closing_queues: dict[str, Future[ScanOutcome]] = {}
        self._timer_threads: set[threading.Timer] = set()
        self.executor = ThreadPoolExecutor(thread_name_prefix="ScanTask")
        self._start_scan_queue_register()
        self.instruction_handler = InstructionHandler(self.connector)

    def add_to_queue(
        self, scan_queue: str, msg: messages.ScanQueueMessage, position: int = -1
    ) -> None:
        """Add a new ScanQueueMessage to the queue.

        Args:
            scan_queue (str): the queue that should receive the new message
            msg (messages.ScanQueueMessage): ScanQueueMessage

        """
        try:
            with self._lock:
                self.add_queue(scan_queue)
                queue = self.queues[scan_queue]
                queue.insert(msg, position=position)
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
                severity=Alarms.MAJOR, info=error_info, metadata=msg.metadata
            )

    def add_queue(self, queue_name: str) -> None:
        """add a new queue to the queue manager"""
        with self._closing_queue_condition:
            while queue_name in self._closing_queues:
                self._closing_queue_condition.wait()
            if queue_name in self.queues:
                return
            self.queues[queue_name] = ScanQueue(self, queue_name=queue_name)
            self.queues[queue_name]._start_auto_shutdown_timer()  # pylint: disable=protected-access
        self.send_queue_status()

    def remove_queue(
        self, queue_name: str, skip_primary: bool = True, emit_status: bool = True
    ) -> None:
        """
        Remove a queue from the queue manager. If the queue is "primary" and skip_primary is True,
        the queue will not be removed to avoid removing the default queue.
        The emit_status flag controls whether the queue status will be sent after removal. This should only
        be set to False during shutdown to avoid unnecessary status updates.

        Args:
            queue_name (str): The name of the queue to remove
            skip_primary (bool): If True, the primary queue will not be removed. Default is True.
            emit_status (bool): If True, the queue status will be sent after removal. Default is True.

        """
        if queue_name == "primary" and skip_primary:
            return
        with self._lock:
            if queue_name not in self.queues:
                return
            queue = self.queues.pop(queue_name)
            timer = queue._cancel_auto_shutdown_timer_locked()  # pylint: disable=protected-access
            queue.signal_event.set()
            task = queue.active_task
            if task is not None and not task.future.done():
                self._closing_queues[queue_name] = task.future
                task.future.add_done_callback(
                    lambda done, name=queue_name: self._finish_queue_removal(name, done)
                )

        if timer is not None and timer is not threading.current_thread():
            timer.join()
            with self._lock:
                self._timer_threads.discard(timer)
        queue.stop_active()
        if task is not None:
            task.future.cancel()
        if emit_status:
            self.send_queue_status()

    def _finish_queue_removal(self, queue_name: str, future: Future[ScanOutcome]) -> None:
        """Allow recreation once a removed queue's worker has actually exited."""
        with self._closing_queue_condition:
            if self._closing_queues.get(queue_name) is future:
                del self._closing_queues[queue_name]
                self._closing_queue_condition.notify_all()

    def _remove_idle_queue(self, queue: ScanQueue) -> None:
        """Remove a still-idle queue when its current auto-shutdown timer expires."""
        # pylint: disable=protected-access
        with self._lock:
            if self.queues.get(queue.queue_name) is not queue:
                return
            if queue._auto_shutdown_timer is not threading.current_thread():
                return
            queue._auto_shutdown_timer = None
            if queue.queue or queue._deferred_inserts or queue.active_task is not None:
                return
            self.queues.pop(queue.queue_name)
            queue.signal_event.set()

        self.send_queue_status()

    def add_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> None:
        """Add a lock to the specified queue.

        Args:
            queue_name (str): The name of the queue to lock
            lock (messages.ScanQueueLock): The lock to add

        """
        with self._lock:
            self.add_queue(queue_name)
            logger.info(f"Adding lock to queue {queue_name}: {lock}")
            self.queues[queue_name].add_lock(lock)
            self.send_queue_status()

    def remove_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> None:
        """Remove a lock from the specified queue.

        Args:
            queue_name (str): The name of the queue to unlock
            lock (messages.ScanQueueLock): The lock to remove
        """
        with self._lock:
            if queue_name not in self.queues:
                return
            logger.info(f"Removing lock from queue {queue_name}: {lock}")
            self.queues[queue_name].remove_lock(lock)
            self.send_queue_status()

    def _start_scan_queue_register(self) -> None:
        self.connector.register(MessageEndpoints.scan_queue_insert(), cb=self._scan_queue_callback)
        self.connector.register(
            MessageEndpoints.scan_queue_modification(), cb=self._scan_queue_modification_callback
        )
        self.connector.register(
            MessageEndpoints.scan_queue_order_change(), cb=self._scan_queue_order_callback
        )

    def _scan_queue_callback(self, msg: MessageObject[messages.ScanQueueMessage]) -> None:
        scan_msg = cast(messages.ScanQueueMessage, msg.value)
        logger.info(f"Receiving scan: {scan_msg.content}")
        queue = scan_msg.content.get("queue", "primary")
        self.add_to_queue(queue, scan_msg)

    def _scan_queue_modification_callback(
        self, msg: MessageObject[messages.ScanQueueModificationMessage]
    ) -> None:
        scan_mod_msg = cast(messages.ScanQueueModificationMessage, msg.value)
        logger.info(f"Receiving scan modification: {scan_mod_msg.content}")
        if scan_mod_msg:
            self.scan_interception(scan_mod_msg)
            self.send_queue_status()

    def _scan_queue_order_callback(
        self, msg: MessageObject[messages.ScanQueueOrderMessage]
    ) -> None:
        self._handle_scan_order_change(cast(messages.ScanQueueOrderMessage, msg.value))

    def _handle_scan_order_change(self, msg: messages.ScanQueueOrderMessage) -> None:
        """Handle the scan queue order change request.

        Args:
            msg (messages.ScanQueueOrderMessage): ScanQueueOrderMessage

        """
        with self._lock:
            logger.info(f"Handling scan queue order change: {msg}")
            target_queue = msg.queue
            scan_queue = self.queues.get(target_queue)
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
        """
        Get the queue item by scan_id.

        Args:
            msg (messages.ScanQueueOrderMessage): ScanQueueOrderMessage
        """
        queue = self.queues[msg.queue]
        for instruction_queue in queue.queue:
            if msg.scan_id in instruction_queue.scan_id:
                return instruction_queue
        return None

    def stop_all_devices(
        self, stop_id: str | list[str] | None = None, devices: list[str] | None = None
    ) -> None:
        """
        Send a message to the device server to stop devices.
        Args:
            stop_id (str | None): An optional identifier for the stop request.
                If provided, this ID will be added to the list of stopped requests in the device server to
                prevent any instructions associated with this ID raising alarms after the stop command is issued.
                The stop_id can be a scan ID, request ID, or queue ID.
            devices (list[str] | None): Optional list of devices to stop.
                `None` means stop all devices, while an empty list means stop no devices.
        """
        msg = messages.VariableMessage(value=devices, metadata={})
        if stop_id is not None:
            msg.metadata["stop_id"] = stop_id
        self.connector.send(MessageEndpoints.stop_devices(), msg)

    def scan_interception(self, scan_mod_msg: messages.ScanQueueModificationMessage) -> None:
        """handle a scan interception by compiling the requested method name and forwarding the request.

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
        if action == "restart":
            # Restart manages its own locks so replacement insertion can release the manager.
            self.set_restart(**parameters)
            return
        with self._lock:
            getattr(self, f"set_{action}")(**parameters)

    @requires_queue
    def set_pause(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """pause the queue and the currently running instruction queue"""
        que = self.queues[queue]
        if que.worker_status == InstructionQueueStatus.RUNNING:
            que.worker_status = InstructionQueueStatus.PAUSED
        que._maybe_dispatch()  # pylint: disable=protected-access

    @requires_queue
    def set_deferred_pause(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """pause the queue but continue with the currently running instruction queue until the next checkpoint"""
        que = self.queues[queue]
        que.status = ScanQueueStatus.PAUSED
        if que.worker_status == InstructionQueueStatus.RUNNING:
            que.worker_status = InstructionQueueStatus.DEFERRED_PAUSE
        que._maybe_dispatch()  # pylint: disable=protected-access

    @requires_queue
    def set_continue(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """continue with the currently scheduled queue and instruction queue"""
        with self._lock:
            que = self.queues[queue]
            if que.locks:
                que.status = ScanQueueStatus.RUNNING
                return
            que.worker_status = InstructionQueueStatus.RUNNING
            que.status = ScanQueueStatus.RUNNING

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
        """
        Abort the scan and remove it from the queue. This will leave the queue in a paused state after the cleanup.

        Args:
            scan_id: The scan ID to abort. If None, the currently active scan will be aborted.
            queue: The queue name. Defaults to "primary".
            parameter: Additional parameters for the abort action.
            exit_info: The exit information to set for the aborted scan.
            user_call: Whether the abort was initiated by a user action.
        """
        if exit_info is None:
            exit_info = ("aborted", "user" if user_call else "alarm")
        que = self.queues[queue]
        if request_id is not None:
            target_queue_item = self._get_queue_item_by_request_id(queue, request_id)
            if target_queue_item is None:
                logger.warning(f"Request {request_id} not found in queue {queue}")
                return
            if target_queue_item is not que.active_instruction_queue:
                self._cancel_queue_item(target_queue_item, queue=queue)
                que.remove_queue_item_by_request_id(request_id)
                que._maybe_dispatch()  # pylint: disable=protected-access
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
                        for instruction_queue in self.queues[queue].queue
                        if len(set(scan_id) & set(instruction_queue.scan_id)) > 0
                    ),
                    None,
                )
                if target_queue_item is not None:
                    self._cancel_queue_item(target_queue_item, queue=queue)
                self.queues[queue].remove_queue_item(scan_id)
                que._maybe_dispatch()  # pylint: disable=protected-access
                return

        if que.queue:
            que.status = ScanQueueStatus.PAUSED
        instruction_queue = que.active_instruction_queue
        if (
            instruction_queue is not None
            and que.active_task is not None
            and not que.active_task.future.done()
            and que.active_instruction_queue is instruction_queue
        ):
            if not instruction_queue.exit_info:
                instruction_queue.exit_info = exit_info
            que.worker_status = InstructionQueueStatus.STOPPED
            if instruction_queue.scan_id and instruction_queue.scan_id[-1] is None:
                stop_id: str | list[str] = instruction_queue.queue_id
            else:
                stop_id = [scan_id for scan_id in instruction_queue.scan_id if scan_id is not None]
            self.stop_all_devices(
                stop_id=stop_id,
                devices=self._get_owned_devices_for_instruction_queue(instruction_queue),
            )
        que._maybe_dispatch()  # pylint: disable=protected-access

    def _cancel_queue_item(self, target_queue_item: DirectInstructionQueueItem, queue: str) -> None:
        """
        Mark a pending queue item as cancelled before removing it from the queue.
        This is to allow clients to recognize that the scan was cancelled and did not just
        disappear from the queue.

        Args:
            target_queue_item (DirectInstructionQueueItem): The queue item to cancel.
            queue (str): The name of the queue the item is in, e.g. "primary".
        """
        del queue  # queue kept for signature symmetry with callers
        target_queue_item._status = InstructionQueueStatus.CANCELLED
        self.send_queue_status()

    @requires_queue
    def set_halt(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        user_call: bool = True,
    ) -> None:
        """abort the scan and do not perform any cleanup routines"""
        exit_info = ("halted", "user" if user_call else "alarm")
        instruction_queue = self.queues[queue].active_instruction_queue
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

    @requires_queue
    def set_user_completed(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
        user_call: bool = True,
    ) -> None:
        """mark the scan as user completed and perform cleanup routines"""
        exit_info = ("user_completed", "user" if user_call else "alarm")
        queue_state_prior_abort = self.queues[queue].status
        self.set_abort(scan_id=scan_id, request_id=request_id, queue=queue, exit_info=exit_info)
        self.queues[queue].status = queue_state_prior_abort

    @requires_queue
    def set_clear(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        # pylint: disable=unused-argument
        """pause the queue and clear all its elements"""
        logger.info("clearing queue")
        que = self.queues[queue]
        que.status = ScanQueueStatus.PAUSED
        que.worker_status = InstructionQueueStatus.STOPPED
        que.clear()
        que._maybe_dispatch()  # pylint: disable=protected-access

    @requires_queue
    def set_restart(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        """abort and restart the currently running scan. The active scan will be aborted."""
        # pylint: disable=protected-access
        with self._lock:
            que = self.queues.get(queue)
        if que is None:
            return
        with self._lock:
            if self.queues.get(queue) is not que:
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

        # The original may have finished while its replacement was being inserted.
        # Only stop that original, never a new head or a recreated queue's worker.
        with self._lock:
            if (
                self.queues.get(queue) is not que
                or que.active_instruction_queue is not instruction_queue
            ):
                return
            original_queue_status = que.status
            que.status = ScanQueueStatus.PAUSED
            if que.worker_status in [
                InstructionQueueStatus.RUNNING,
                InstructionQueueStatus.PAUSED,
                InstructionQueueStatus.DEFERRED_PAUSE,
            ]:
                que.worker_status = InstructionQueueStatus.STOPPED
            que.status = original_queue_status

    @requires_queue
    def set_lock(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        """
        Add a lock to the queue. Whether the queue will proceed depends on the
        allow_device_instructions flag in the lock parameter. If
        allow_device_instructions is False, the queue will not proceed until
        the lock is released. If allow_device_instructions is True, the
        queue will proceed if the next queue item is not a scan.
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

    @requires_queue
    def set_release_lock(
        self,
        scan_id: ScanTarget = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: QueueParameter = None,
    ) -> None:
        """
        Remove a lock from the queue. The queue will proceed if no more locks are present.
        """
        if not parameter:
            raise ValueError("Missing parameter for release_lock action")
        identifier = parameter.get("identifier")
        if not identifier:
            raise ValueError("Missing lock identifier in release_lock parameter")
        self.remove_queue_lock(
            queue_name=queue, lock=messages.ScanQueueLock(reason="", identifier=identifier)
        )

    def _get_queue_item_by_request_id(
        self, queue: str, request_id: str
    ) -> DirectInstructionQueueItem | None:
        for instruction_queue in self.queues[queue].queue:
            if any(msg.metadata.get("RID") == request_id for msg in instruction_queue.scan_msgs):
                return instruction_queue
        return None

    def _get_active_scan_id(self, queue: str) -> str | None:
        instr_queue = self.queues[queue].active_instruction_queue
        if instr_queue is None or instr_queue.active_scan is None:
            return None
        return instr_queue.active_scan.scan_info.scan_id

    def _get_owned_devices_for_instruction_queue(
        self, instruction_queue: DirectInstructionQueueItem
    ) -> list[str]:
        registry = getattr(self.parent, "device_lock_registry", None)
        if registry is None:
            return []
        if instruction_queue.active_scan is None:
            return []
        request_id = instruction_queue.active_scan.scan_info.metadata.get("RID")
        if request_id is None:
            return []
        return registry.get_owned_devices(request_id)

    def send_queue_status(self) -> None:
        """send the current queue to redis"""
        with self._lock:
            queue_export = self.export_queue()
            if not queue_export:
                return
            logger.info("New scan queue:")
            for queue in self.describe_queue():
                logger.info(f"\n {queue}")
            self.connector.set_and_publish(
                MessageEndpoints.scan_queue_status(),
                messages.ScanQueueStatusMessage(queue=queue_export),
            )
            self.connector.publish_metrics(
                "scan_queue_length",
                {queue_name: len(queue.queue) for queue_name, queue in self.queues.items()},
            )

    def describe_queue(self) -> list[str]:
        """create a rich.table description of the current scan queue"""
        queue_tables = []
        console = Console()
        for queue_name, scan_queue in self.queues.items():
            table = Table(title=f"{queue_name} queue / {scan_queue.status}")
            table.add_column("queue_id", justify="center")
            table.add_column("scan_id", justify="center")
            table.add_column("is_scan", justify="center")
            table.add_column("type", justify="center")
            table.add_column("scan_number", justify="center")
            table.add_column("IQ status", justify="center")

            queue = list(scan_queue.queue)  # local ref for thread safety
            for instruction_queue in queue:
                table.add_row(
                    instruction_queue.queue_id,
                    ", ".join([str(s) for s in instruction_queue.scan_id]),
                    ", ".join([str(s) for s in instruction_queue.is_scan]),
                    ", ".join([msg.content["scan_type"] for msg in instruction_queue.scan_msgs]),
                    ", ".join([str(s) for s in instruction_queue.scan_number]),
                    str(instruction_queue.status.name),
                )
            with console.capture() as capture:
                console.print(table)
            queue_tables.append(capture.get())

        return queue_tables

    def export_queue(self) -> dict[str, messages.ScanQueueStatus]:
        """extract the queue info from the queue"""
        queue_export: dict[str, messages.ScanQueueStatus] = {}
        for queue_name, scan_queue in self.queues.items():
            queue_info = []
            instruction_queues = list(scan_queue.queue)  # local ref for thread safety
            for instruction_queue in instruction_queues:
                queue_info.append(instruction_queue.describe())
            # Convert locks dict to list for export
            locks_list = list(scan_queue.locks.values())
            queue_export[queue_name] = messages.ScanQueueStatus(
                info=queue_info,
                status=cast(Literal["PAUSED", "RUNNING", "LOCKED"], scan_queue.status.name),
                locks=locks_list,
            )
        return queue_export

    def shutdown(self) -> None:
        """shutdown the queue"""
        for queue_name in list(self.queues.keys()):
            self.remove_queue(queue_name, skip_primary=False, emit_status=False)
        with self._lock:
            timers = tuple(self._timer_threads)
            for timer in timers:
                timer.cancel()
        for timer in timers:
            if timer is not threading.current_thread():
                timer.join()
        with self._lock:
            self._timer_threads.difference_update(timers)
        self.executor.shutdown(wait=True, cancel_futures=True)


class ScanQueue:
    """The ScanQueue manages a queue of v4 instruction items.
    While for most scenarios a single ScanQueue is sufficient,
    multiple ScanQueues can be used to run experiments in parallel.
    The default ScanQueue is always "primary".
    If a ScanQueue is inactive for the specified AUTO_SHUTDOWN_TIME,
    it will be automatically removed.

    """

    MAX_HISTORY = 100
    AUTO_SHUTDOWN_TIME: int = 60  # seconds
    DEFAULT_QUEUE_STATUS = ScanQueueStatus.RUNNING

    def __init__(self, queue_manager: QueueManager, queue_name: str = "primary") -> None:
        self.queue: collections.deque[DirectInstructionQueueItem] = collections.deque()
        self._deferred_inserts: collections.deque[tuple[messages.ScanQueueMessage, int]] = (
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
        self._dispatching = False  # Immediate futures can invoke completion callbacks inline.
        self._auto_shutdown_timer: threading.Timer | None = None
        self.locks: dict[str, messages.ScanQueueLock] = {}
        self.release_lock_status: ScanQueueStatus = ScanQueueStatus.RUNNING

    def stop_active(self) -> None:
        """Stop the executing task without waiting under a queue or manager lock."""
        with self.queue_manager._lock:  # pylint: disable=protected-access
            item = self.active_instruction_queue
            if self.active_task is not None:
                self.active_task.control.stop(shutdown=True)
            if item is not None:
                item.stop()

    @property
    def worker_status(self) -> InstructionQueueStatus | None:
        """current status of the instruction queue"""
        item = self.active_instruction_queue or (self.queue[0] if self.queue else None)
        if item is not None:
            return item.status
        return None

    @worker_status.setter
    def worker_status(self, val: InstructionQueueStatus) -> None:
        item = self.active_instruction_queue or (self.queue[0] if self.queue else None)
        if item is not None:
            item.status = val

    @property
    def status(self) -> ScanQueueStatus:
        """current status of the queue"""
        return self._status

    @status.setter
    def status(self, val: ScanQueueStatus) -> None:
        with self.queue_manager._lock:  # pylint: disable=protected-access
            if self.locks and val != ScanQueueStatus.LOCKED:
                logger.warning(
                    f"Queue {self.queue_name} is locked. Cannot change status to {val}. Current locks: {self.locks}"
                )
                return
            self._status = val
            self.queue_manager.send_queue_status()
            if val == ScanQueueStatus.RUNNING:
                self._maybe_dispatch()

    def add_lock(self, lock: messages.ScanQueueLock) -> None:
        """add a lock to the queue"""
        logger.info(f"Adding lock to queue {self.queue_name}: {lock}")
        if self.status != ScanQueueStatus.LOCKED:
            self.release_lock_status = self.status
            self.status = ScanQueueStatus.LOCKED
        self.locks[lock.identifier] = lock
        logger.info(f"Lock '{lock.identifier}' added to queue {self.queue_name}")
        self._maybe_dispatch()

    def remove_lock(self, lock: messages.ScanQueueLock) -> None:
        """remove a lock from the queue"""
        logger.info(f"Removing lock from queue {self.queue_name}: {lock}")
        if lock.identifier in self.locks:
            del self.locks[lock.identifier]
            logger.info(f"Lock '{lock.identifier}' removed from queue '{self.queue_name}'")
            if not self.locks:
                self.status = self.release_lock_status
        else:
            logger.warning(
                f"Lock with identifier '{lock.identifier}' not found in queue '{self.queue_name}'. Nothing to remove."
            )

    def remove_queue_item(self, scan_id: str | list[str | None]) -> None:
        """remove a queue item from the queue"""
        if not scan_id:
            return
        if not isinstance(scan_id, list):
            scan_id = [scan_id]
        scan_ids = set(scan_id)
        for item in tuple(self.queue):
            if not scan_ids.isdisjoint(item.scan_id):
                self.queue.remove(item)

    def remove_queue_item_by_request_id(self, request_id: str) -> None:
        """remove a queue item from the queue by request ID"""
        if not request_id:
            return
        remove = []
        for queue in self.queue:
            if any(msg.metadata.get("RID") == request_id for msg in queue.scan_msgs):
                remove.append(queue)
        if remove:
            for rmv in remove:
                self.queue.remove(rmv)

    def clear(self) -> None:
        """clear the queue"""
        self.queue.clear()
        self._deferred_inserts.clear()
        if self.active_task is None:
            self.active_instruction_queue = None

    def _maybe_dispatch(self) -> None:
        """Submit the next v4 scan when this queue has no task in flight."""
        # The direct worker imports the queue's status enum.
        # pylint: disable=import-outside-toplevel,protected-access
        from .direct_scan_worker import DirectScanWorker, ScanControl, ScanTask

        with self.queue_manager._lock:
            if self._dispatching:
                return
            self._dispatching = True
            try:
                while not self.signal_event.is_set() and self.active_task is None:
                    if not self.queue:
                        if self._status == ScanQueueStatus.PAUSED:
                            self._status = ScanQueueStatus.RUNNING
                            self.queue_manager.send_queue_status()
                        self._start_auto_shutdown_timer()
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
                    control = ScanControl(run_on_exception_hook=item.run_on_exception_hook)
                    item.control = control
                    worker = DirectScanWorker(
                        scan=scan,
                        control=control,
                        on_status=self.queue_manager.send_queue_status,
                        device_lock_registry=getattr(
                            self.queue_manager.parent, "device_lock_registry", None
                        ),
                    )
                    self._cancel_auto_shutdown_timer_locked()
                    future = self.queue_manager.executor.submit(worker.run)
                    self.active_task = ScanTask(future=future, control=control)
                    future.add_done_callback(
                        lambda done, expected=item: self._task_finished(expected, done)
                    )
            finally:
                self._dispatching = False

    def _task_finished(self, item: DirectInstructionQueueItem, future: Future[ScanOutcome]) -> None:
        """Retire only the item whose execution produced this future."""
        with self.queue_manager._lock:  # pylint: disable=protected-access
            if self.active_task is None or self.active_task.future is not future:
                return
            self.active_task = None
            item.control = None
            if (
                self.signal_event.is_set()
                or self.queue_manager.queues.get(self.queue_name) is not self
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
                    metadata={"scan_id": active_scan.scan_info.scan_id} if active_scan else {},
                )
                outcome = "aborted"
            if outcome == "completed":
                if active_scan not in item.scans:
                    logger.error("Completed v4 scan is no longer in its queue item")
                    outcome = "aborted"
                elif item in self.queue and item.scans.index(active_scan) + 1 < len(item.scans):
                    self._maybe_dispatch()
                    return
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
            self._maybe_dispatch()

    def _start_auto_shutdown_timer(self) -> None:
        """
        Start the auto shutdown timer if it is not already running.
        """
        # pylint: disable=protected-access
        with self.queue_manager._lock:
            if (
                self.queue_name == "primary"
                or self.signal_event.is_set()
                or self.queue_manager.queues.get(self.queue_name) is not self
            ):
                return
            if (
                self._auto_shutdown_timer is not None
                or self.queue
                or self._deferred_inserts
                or self.active_task is not None
            ):
                return
            self._auto_shutdown_timer = threading.Timer(
                self.AUTO_SHUTDOWN_TIME, self.queue_manager._remove_idle_queue, args=[self]
            )
            self._auto_shutdown_timer.name = f"AutoShutdownTimer-{self.queue_name}"
            timers = self.queue_manager._timer_threads  # pylint: disable=protected-access
            timers.difference_update(timer for timer in tuple(timers) if not timer.is_alive())
            timers.add(self._auto_shutdown_timer)
            self._auto_shutdown_timer.start()

    def _cancel_auto_shutdown_timer_locked(self) -> threading.Timer | None:
        """Cancel the timer under the manager lock without waiting for its thread."""
        timer = self._auto_shutdown_timer
        if timer is not None:
            timer.cancel()
            self._auto_shutdown_timer = None
        return timer

    def _queue_should_continue(self) -> bool:
        """check if the queue should continue to the next instruction queue"""
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

    def _flush_deferred_inserts(self) -> None:
        """Move buffered inserts into the live queue once the stopped head no longer blocks them."""
        if not self._deferred_inserts or self.worker_status == InstructionQueueStatus.STOPPED:
            return
        while self._deferred_inserts:
            msg, position = self._deferred_inserts.popleft()
            self._insert_now(msg, position=position)

    def _insert_now(self, msg: messages.ScanQueueMessage, position: int = -1) -> None:
        """Insert a new message into the live queue without waiting."""
        target_group = msg.metadata.get("queue_group")
        logger.debug(f"Inserting new queue message {msg}")
        instruction_queue = (
            self.get_queue_item(group=target_group) if target_group is not None else None
        )
        if instruction_queue is None:
            assembler = self.queue_manager.parent.scan_assembler
            if not assembler.is_direct_scan_message(msg):
                raise TypeError(f"Scan {msg.scan_type} is not a v4 scan")
            instruction_queue = DirectInstructionQueueItem(parent=self, assembler=assembler)
            instruction_queue.append_scan_request(msg)
            instruction_queue.queue_group = target_group
            if position == -1:
                self.queue.append(instruction_queue)
            else:
                self.queue.insert(position, instruction_queue)
        else:
            instruction_queue.append_scan_request(msg)

        self.queue_manager.send_queue_status()

    def insert(self, msg: messages.ScanQueueMessage, position: int = -1, **_kwargs: Any) -> None:
        """Insert a new message into the queue or buffer it until a stopped head item clears."""
        with self.queue_manager._lock:  # pylint: disable=protected-access
            if self.worker_status == InstructionQueueStatus.STOPPED:
                logger.info("Deferring queue insert until worker becomes active again.")
                self._deferred_inserts.append((msg, position))
                return

            self._flush_deferred_inserts()
            self._insert_now(msg, position=position)
            self._maybe_dispatch()

    def get_queue_item(self, group: str | None = None) -> DirectInstructionQueueItem | None:
        """Get a queue item based on its group."""
        if group is not None:
            for instruction_queue in self.queue:
                if instruction_queue.queue_group == group:
                    return instruction_queue

        return None

    def abort(self) -> None:
        """abort the current queue item"""
        logger.debug("Aborting scan.")
        if self.active_instruction_queue is not None:
            self.active_instruction_queue.abort()

    def get_scan(self, scan_id: str) -> DirectInstructionQueueItem | None:
        """get the instruction queue item based on its scan_id"""
        for item in chain(self.history_queue, self.queue):
            if scan_id in item.scan_id:
                return item
        return None


class DirectInstructionQueueItem:
    """
    An instruction queue item for v4 scans.
    """

    def __init__(self, parent: ScanQueue, assembler: ScanAssembler) -> None:
        self.parent = parent
        self.assembler = assembler
        self.control: ScanControl | None = None
        self.exit_info: ExitInfoType | None = None
        self.queue_id = str(uuid.uuid4())
        self._scan_id = str(uuid.uuid4())
        self.queue_group: str | None = None

        self._status = InstructionQueueStatus.PENDING
        self._run_on_exception_hook: bool | None = None

        self.active_scan: ScanBase_v4 | None = None
        self.scans: list[ScanBase_v4] = []
        self.scan_msgs: list[messages.ScanQueueMessage] = []
        self.reason: Literal["user", "alarm", "restart"] | None = None

    @property
    def status(self) -> InstructionQueueStatus:
        """get the status of the instruction queue item"""
        return self._status

    @status.setter
    def status(self, val: InstructionQueueStatus) -> None:
        """set the status of the instruction queue item and update the worker and queue status accordingly"""
        logger.debug(
            f"Setting status of direct instruction queue {self.parent.queue_name} to {val.name} from thread {threading.current_thread().name}"
        )
        self._status = val
        if self.control is not None:
            if val == InstructionQueueStatus.STOPPED:
                self.control.stop(self.exit_info)
            elif val in (InstructionQueueStatus.RUNNING, InstructionQueueStatus.PAUSED):
                self.control.set_status(val)
        if val == InstructionQueueStatus.STOPPED:
            self.stop()
        self.parent.queue_manager.send_queue_status()

    @property
    def active_request_block(self) -> None | ScanBase_v4:
        """there are no request blocks for direct instruction queue items"""
        return self.active_scan

    @property
    def scan_id(self) -> list[str | None]:
        return [scan.scan_info.scan_id for scan in self.scans]

    @property
    def is_scan(self) -> list[bool]:
        return [scan.scan_info.scan_type is not None for scan in self.scans]

    @property
    def scan_number(self) -> list[int | None]:
        return [self._get_scan_number(scan) for scan in self.scans]

    def append_scan_request(self, msg: messages.ScanQueueMessage) -> None:
        """
        Append a new scan from a scan queue message. The scan will be assembled but not executed until it becomes active.

        Args:
            msg (ScanQueueMessage): the scan queue message containing the scan information
        """
        scan_cls = self.assembler.scan_manager.scan_dict[msg.scan_type]
        scan_id = self._scan_id if getattr(scan_cls, "is_scan", True) else None
        scan = self.assembler.assemble_direct_scan(msg, scan_id=scan_id)
        self.scans.append(scan)
        self.scan_msgs.append(msg)

    def set_active(self) -> None:
        """change the instruction queue status to RUNNING"""
        if self.status == InstructionQueueStatus.PENDING:
            self.status = InstructionQueueStatus.RUNNING

    @property
    def run_on_exception_hook(self) -> bool:
        """whether or not to run the direct scan on_exception hook after scan abortion"""
        if self._run_on_exception_hook is not None:
            return self._run_on_exception_hook
        if self.active_scan is not None:
            return bool(self.active_scan.scan_info.run_on_exception_hook)
        return False

    @run_on_exception_hook.setter
    def run_on_exception_hook(self, val: bool) -> None:
        self._run_on_exception_hook = val
        if self.control is not None:
            self.control.set_cleanup_enabled(val)

    def describe(self) -> messages.QueueInfoEntry:
        """description of the instruction queue"""
        request_blocks = self.describe_scans()
        content = messages.QueueInfoEntry(
            queue_id=self.queue_id,
            scan_id=self.scan_id,
            is_scan=self.is_scan,
            request_blocks=request_blocks,
            scan_number=self.scan_number,
            status=self.status.name,
            active_request_block=self.describe_active_scan(),
            reason=self.reason or (self.exit_info[1] if self.exit_info else None),
        )
        return content

    def describe_active_scan(self) -> messages.RequestBlock | None:
        """description of the active scan"""
        if self.active_scan is None:
            return None
        if self.active_scan not in self.scans:
            return None
        msg = self.scan_msgs[self.scans.index(self.active_scan)]
        scan_info = self._get_request_block_message(self.active_scan, msg)
        return scan_info

    def describe_scans(self) -> list[messages.RequestBlock]:
        """description of the scans in the instruction queue item"""
        info = []
        for scan, msg in zip(self.scans, self.scan_msgs):
            scan_info = self._get_request_block_message(scan, msg)
            info.append(scan_info)
        return info

    def _get_request_block_message(
        self, scan: ScanBase_v4, msg: messages.ScanQueueMessage
    ) -> messages.RequestBlock:
        """
        Get the request block message for a given scan and scan queue message

        Args:
            scan (ScanBase_v4): the scan for which to get the request block message
            msg (ScanQueueMessage): the scan queue message containing the scan information

        Returns:
            RequestBlock: the request block message containing the scan information
        """
        return messages.RequestBlock(
            msg=msg,
            RID=msg.metadata["RID"],
            readout_priority=scan.scan_info.readout_priority_modification,
            is_scan=scan.scan_info.scan_type is not None,
            scan_number=self._get_scan_number(scan),
            scan_id=scan.scan_info.scan_id,
            report_instructions=scan.scan_info.scan_report_instructions,
            owned_device_locks=scan.actions.get_owned_device_locks(),
            pending_device_locks=scan.actions.get_pending_device_locks(),
        )

    @property
    def _scan_server_scan_number(self) -> int:
        return self.parent.queue_manager.parent.scan_number

    def _get_scan_number(self, scan: ScanBase_v4) -> int | None:
        if not scan.is_scan:
            return None
        if scan.scan_info.scan_number is not None:
            # We've already assigned a scan number to this scan, return it
            return scan.scan_info.scan_number
        return self._scan_server_scan_number + self.scan_ids_head(scan)

    def scan_ids_head(self, target_scan: ScanBase_v4) -> int:
        """Calculate the scan-number offset for a scan within the current queue."""
        offset = 1
        # Status export can run while another thread inserts into the queue.
        for queue in list(self.parent.queue):
            if queue.status in [InstructionQueueStatus.COMPLETED, InstructionQueueStatus.RUNNING]:
                continue
            if queue.queue_id != self.queue_id:
                offset += len([scan_id for scan_id in queue.scan_id if scan_id])
                continue
            for scan in queue.scans:
                if scan is target_scan:
                    return offset
                if scan.scan_info.scan_id:
                    offset += 1
            return offset
        return offset

    def move_to_next_scan(self) -> ScanBase_v4:
        """move to the next scan in the instruction queue item"""
        if self.active_scan is None:
            if len(self.scans) > 0:
                scan = self.scans[0]
                self._set_scan_as_active(scan)
                return scan
            raise StopIteration("No active scan and no scans in the queue.")
        current_index = self.scans.index(self.active_scan)
        if current_index + 1 < len(self.scans):
            scan = self.scans[current_index + 1]
            self._set_scan_as_active(scan)
            return scan
        raise StopIteration("No more scans in the queue.")

    def _set_scan_as_active(self, scan: ScanBase_v4) -> None:
        """set a given scan as the active scan"""
        self.active_scan = scan
        if scan.scan_info.scan_number is None and scan.is_scan:
            with self.parent.queue_manager._lock:
                self.parent.queue_manager.parent.scan_number += 1
                if not self.scan_msgs[self.scans.index(scan)].metadata.get("dataset_id_on_hold"):
                    self.parent.queue_manager.parent.dataset_number += 1
                scan.scan_info.scan_number = self.parent.queue_manager.parent.scan_number
                scan.scan_info.dataset_number = self.parent.queue_manager.parent.dataset_number
        self.set_active()

    def append_to_queue_history(self) -> None:
        """append a new queue item to the redis history buffer"""
        msg = messages.ScanQueueHistoryMessage(
            status=self.status.name, queue_id=self.queue_id, info=self.describe()
        )
        self.parent.queue_manager.connector.lpush(
            MessageEndpoints.scan_queue_history(), msg, max_size=100
        )

    def stop(self) -> None:
        """stop the instruction queue item and all active scans"""
        for scan in self.scans:
            scan._shutdown_event.set()

    def abort(self) -> None:
        self.active_scan = None
        self.scans = []
        self.scan_msgs = []
