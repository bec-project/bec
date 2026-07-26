from __future__ import annotations

import collections
import functools
import threading
import traceback
import uuid
from enum import Enum
from typing import TYPE_CHECKING, Deque, Literal, TypeAlias

from rich.console import Console
from rich.table import Table

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .instruction_handler import InstructionHandler
from .scan_assembler import ScanAssembler

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_server.scan_server.scan_server import ScanServer
    from bec_server.scan_server.scan_worker import ScanWorker
    from bec_server.scan_server.scans.scan_base import ScanBase as ScanBase_v4


def requires_queue(fcn):
    """Decorator to ensure that the requested queue exists."""

    @functools.wraps(fcn)
    def wrapper(self, *args, queue="primary", **kwargs):
        if queue not in self.queues:
            self.add_queue(queue)
        return fcn(self, *args, queue=queue, **kwargs)

    return wrapper


ExitInfoType: TypeAlias = tuple[
    Literal["halted", "aborted", "user_completed"], Literal["user", "alarm"]
]


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
        self._start_scan_queue_register()
        self._lock = threading.RLock()
        self.instruction_handler = InstructionHandler(self.connector)

    def add_to_queue(self, scan_queue: str, msg: messages.ScanQueueMessage, position=-1) -> None:
        """Add a new ScanQueueMessage to the queue.

        Args:
            scan_queue (str): the queue that should receive the new message
            msg (messages.ScanQueueMessage): ScanQueueMessage

        """
        try:
            with self._lock:
                self.add_queue(scan_queue)
                self.queues[scan_queue].insert(msg, position=position)
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
        with self._lock:
            if queue_name in self.queues:
                queue = self.queues[queue_name]
                if not queue.scan_worker.is_alive():
                    logger.info(f"Restarting worker for queue {queue_name}")
                    queue.clear()
                    self.queues[queue_name] = ScanQueue(self, queue_name=queue_name)
                    self.queues[queue_name].start_worker()
                return
            self.queues[queue_name] = ScanQueue(self, queue_name=queue_name)
            self.queues[queue_name].start_worker()
        self.send_queue_status()

    def remove_queue(self, queue_name: str, skip_primary=True, emit_status=True) -> None:
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

        queue.signal_event.set()
        queue.stop_worker()
        if emit_status:
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

    def _scan_queue_callback(self, msg) -> None:
        scan_msg = msg.value
        logger.info(f"Receiving scan: {scan_msg.content}")
        queue = scan_msg.content.get("queue", "primary")
        self.add_to_queue(queue, scan_msg)

    def _scan_queue_modification_callback(self, msg):
        scan_mod_msg = msg.value
        logger.info(f"Receiving scan modification: {scan_mod_msg.content}")
        if scan_mod_msg:
            self.scan_interception(scan_mod_msg)
            self.send_queue_status()

    def _scan_queue_order_callback(self, msg):
        self._handle_scan_order_change(msg.value)

    def _handle_scan_order_change(self, msg: messages.ScanQueueOrderMessage) -> None:
        """Handle the scan queue order change request.

        Args:
            msg (messages.ScanQueueOrderMessage): ScanQueueOrderMessage

        """
        with self._lock:
            logger.info(f"Handling scan queue order change: {msg}")
            target_queue = msg.queue
            queue = self.queues[target_queue].queue
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
    ):
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
        with self._lock:
            logger.info(f"Scan interception: {scan_mod_msg}")
            action = scan_mod_msg.action
            parameter = scan_mod_msg.parameter
            queue = scan_mod_msg.queue
            getattr(self, f"set_{action}")(
                scan_id=scan_mod_msg.scan_id,
                request_id=scan_mod_msg.request_id,
                queue=queue,
                parameter=parameter,
            )

    @requires_queue
    def set_pause(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
    ) -> None:
        # pylint: disable=unused-argument
        """pause the queue and the currently running instruction queue"""
        que = self.queues[queue]
        with AutoResetCM(que):
            if que.worker_status == InstructionQueueStatus.RUNNING:
                que.worker_status = InstructionQueueStatus.PAUSED

    @requires_queue
    def set_deferred_pause(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
    ) -> None:
        # pylint: disable=unused-argument
        """pause the queue but continue with the currently running instruction queue until the next checkpoint"""
        que = self.queues[queue]
        with AutoResetCM(que):
            que.status = ScanQueueStatus.PAUSED
            if que.worker_status == InstructionQueueStatus.RUNNING:
                que.worker_status = InstructionQueueStatus.DEFERRED_PAUSE

    @requires_queue
    def set_continue(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
    ) -> None:
        # pylint: disable=unused-argument
        """continue with the currently scheduled queue and instruction queue"""
        self.queues[queue].status = ScanQueueStatus.RUNNING
        if self.queues[queue].status == ScanQueueStatus.RUNNING:
            self.queues[queue].worker_status = InstructionQueueStatus.RUNNING

    @requires_queue
    def set_abort(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
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
                return

        with AutoResetCM(que):
            if que.queue:
                que.status = ScanQueueStatus.PAUSED
            instruction_queue = que.active_instruction_queue
            if not instruction_queue:
                return
            if not instruction_queue.exit_info:
                instruction_queue.exit_info = exit_info

            if instruction_queue.worker.current_instruction_queue_item is not instruction_queue:
                logger.info(
                    f"Worker is not running the expected instruction queue item.\
                          Expected: {instruction_queue}, actual: {instruction_queue.worker.current_instruction_queue_item}. Skipping abort."
                )
                return
            que.worker_status = InstructionQueueStatus.STOPPED
            if instruction_queue.scan_id and instruction_queue.scan_id[-1] is None:
                stop_id = instruction_queue.queue_id
            else:
                stop_id = instruction_queue.scan_id
            self.stop_all_devices(
                stop_id=stop_id,
                devices=self._get_owned_devices_for_instruction_queue(instruction_queue),
            )

    def _cancel_queue_item(self, target_queue_item: DirectInstructionQueueItem, queue: str) -> None:
        """
        Mark a pending queue item as cancelled before removing it from the queue.
        This is to allow clients to recognize that the scan was cancelled and did not just
        disappear from the queue.

        Args:
            target_queue_item (InstructionQueueItem | DirectInstructionQueueItem): The queue item to cancel.
            queue (str): The name of the queue the item is in, e.g. "primary".
        """
        del queue  # queue kept for signature symmetry with callers
        target_queue_item._status = InstructionQueueStatus.CANCELLED
        self.send_queue_status()

    @requires_queue
    def set_halt(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
        user_call: bool = True,
    ) -> None:
        """abort the scan and do not perform any cleanup routines"""
        exit_info = ("halted", "user" if user_call else "alarm")
        instruction_queue = self.queues[queue].active_instruction_queue
        if instruction_queue:
            instruction_queue.run_on_exception_hook = False
        self.set_abort(scan_id=scan_id, request_id=request_id, queue=queue, exit_info=exit_info)

    @requires_queue
    def set_user_completed(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
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
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
    ) -> None:
        # pylint: disable=unused-argument
        """pause the queue and clear all its elements"""
        logger.info("clearing queue")
        que = self.queues[queue]
        with AutoResetCM(que):
            que.status = ScanQueueStatus.PAUSED
            que.worker_status = InstructionQueueStatus.STOPPED
            que.clear()

    @requires_queue
    def set_restart(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
    ) -> None:
        """abort and restart the currently running scan. The active scan will be aborted."""
        if not scan_id:
            scan_id = self._get_active_scan_id(queue)
        if not scan_id:
            return
        if isinstance(scan_id, list):
            scan_id = scan_id[0]
        que = self.queues[queue]

        # Find the scan in the active queue
        for iq in que.queue:
            if scan_id in iq.scan_id:
                instruction_queue = iq
                break
        else:
            logger.error(f"Scan {scan_id} not found in queue {queue}")
            return
        if instruction_queue.status in [
            InstructionQueueStatus.IDLE,
            InstructionQueueStatus.PENDING,
        ]:
            # If the scan is not running, we don't need to restart it
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

        # Abort the current scan after queueing its restart replacement.
        with AutoResetCM(que):
            original_queue_status = que.status
            que.status = ScanQueueStatus.PAUSED
            if que.worker_status in [
                InstructionQueueStatus.RUNNING,
                InstructionQueueStatus.PAUSED,
                InstructionQueueStatus.DEFERRED_PAUSE,
            ]:
                que.worker_status = InstructionQueueStatus.STOPPED

        self.queues[queue].status = original_queue_status

    @requires_queue
    def set_lock(
        self,
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
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
        scan_id=None,
        request_id: str | None = None,
        queue="primary",
        parameter: dict | None = None,
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
            request_blocks = instruction_queue.describe().request_blocks
            if any(request_block.RID == request_id for request_block in request_blocks):
                return instruction_queue
        return None

    def _get_active_scan_id(self, queue):
        if len(self.queues[queue].queue) == 0:
            return None
        instr_queue = self.queues[queue].queue[0]
        if instr_queue.active_request_block is None:
            return None
        if isinstance(instr_queue, DirectInstructionQueueItem):
            if instr_queue.active_scan is None:
                return None
            return instr_queue.active_scan.scan_info.scan_id
        return instr_queue.active_request_block.scan_id

    def _get_owned_devices_for_instruction_queue(
        self, instruction_queue: DirectInstructionQueueItem
    ) -> list[str] | None:
        registry = getattr(self.parent, "device_lock_registry", None)
        if registry is None:
            return []
        if instruction_queue.active_scan is None:
            return []
        request_id = instruction_queue.active_scan.scan_info.metadata.get("RID")
        if request_id is None:
            return []
        devices = registry.get_owned_devices(request_id)
        return devices

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

    def describe_queue(self) -> list:
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

    def export_queue(self) -> dict:
        """extract the queue info from the queue"""
        queue_export = {}
        for queue_name, scan_queue in self.queues.items():
            queue_info = []
            instruction_queues = list(scan_queue.queue)  # local ref for thread safety
            for instruction_queue in instruction_queues:
                queue_info.append(instruction_queue.describe())
            # Convert locks dict to list for export
            locks_list = list(scan_queue.locks.values())
            queue_export[queue_name] = {
                "info": queue_info,
                "status": scan_queue.status.name,
                "locks": locks_list,
            }
        return queue_export

    def shutdown(self):
        """shutdown the queue"""
        for queue_name in list(self.queues.keys()):
            self.remove_queue(queue_name, skip_primary=False, emit_status=False)


class ScanQueue:
    """The ScanQueue manages a queue of InstructionQueues.
    While for most scenarios a single ScanQueue is sufficient,
    multiple ScanQueues can be used to run experiments in parallel.
    The default ScanQueue is always "primary".
    If a ScanQueue is inactive for the specified AUTO_SHUTDOWN_TIME,
    it will be automatically removed.

    """

    MAX_HISTORY = 100
    AUTO_SHUTDOWN_TIME: int = 60  # seconds
    DEFAULT_QUEUE_STATUS = ScanQueueStatus.RUNNING

    def __init__(
        self,
        queue_manager: QueueManager,
        queue_name="primary",
        instruction_queue_item_cls: type[DirectInstructionQueueItem] | None = None,
    ) -> None:
        self.queue: Deque[DirectInstructionQueueItem] = collections.deque()
        self._deferred_inserts: Deque[tuple[messages.ScanQueueMessage, int]] = collections.deque()
        self.queue_name = queue_name
        self.history_queue: collections.deque[DirectInstructionQueueItem] = collections.deque(
            maxlen=self.MAX_HISTORY
        )
        self.active_instruction_queue = None
        self.queue_manager = queue_manager
        self._instruction_queue_item_cls_override = instruction_queue_item_cls
        # self.open_instruction_queue = None
        self._status = self.DEFAULT_QUEUE_STATUS
        self.signal_event = threading.Event()
        self.scan_worker = None
        self.auto_reset_enabled = True
        self.init_scan_worker()
        self._lock = threading.RLock()
        self._auto_shutdown_timer: threading.Timer | None = None
        self.locks: dict[str, messages.ScanQueueLock] = {}
        self.release_lock_status: ScanQueueStatus = ScanQueueStatus.RUNNING

    def init_scan_worker(self):
        """init the scan worker"""
        from .scan_worker import ScanWorker

        self.scan_worker = ScanWorker(parent=self.queue_manager.parent, queue_name=self.queue_name)

    def start_worker(self):
        """start the scan worker"""
        self.scan_worker.start()

    def stop_worker(self):
        """stop the scan worker"""
        if len(self.queue) > 0:
            self.queue[0].stop()
        self.scan_worker.shutdown()
        self._reset_auto_shutdown_timer()

    @property
    def worker_status(self) -> InstructionQueueStatus | None:
        """current status of the instruction queue"""
        if len(self.queue) > 0:
            return self.queue[0].status
        return None

    @worker_status.setter
    def worker_status(self, val: InstructionQueueStatus):
        if len(self.queue) > 0:
            self.queue[0].status = val

    @property
    def status(self):
        """current status of the queue"""
        return self._status

    @status.setter
    def status(self, val: ScanQueueStatus):
        if self.locks and val != ScanQueueStatus.LOCKED:
            logger.warning(
                f"Queue {self.queue_name} is locked. Cannot change status to {val}. Current locks: {self.locks}"
            )
            return
        self._status = val
        self.queue_manager.send_queue_status()

    def add_lock(self, lock: messages.ScanQueueLock) -> None:
        """add a lock to the queue"""
        logger.info(f"Adding lock to queue {self.queue_name}: {lock}")
        if self.status != ScanQueueStatus.LOCKED:
            self.release_lock_status = self.status
            self.status = ScanQueueStatus.LOCKED
        self.locks[lock.identifier] = lock
        logger.info(f"Lock '{lock.identifier}' added to queue {self.queue_name}")

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

    def remove_queue_item(self, scan_id: str) -> None:
        """remove a queue item from the queue"""
        if not scan_id:
            return
        if not isinstance(scan_id, list):
            scan_id = [scan_id]
        remove = []
        for queue in self.queue:
            if len(set(scan_id) & set(queue.scan_id)) > 0:
                remove.append(queue)
        if remove:
            for rmv in remove:
                self.queue.remove(rmv)

    def remove_queue_item_by_request_id(self, request_id: str) -> None:
        """remove a queue item from the queue by request ID"""
        if not request_id:
            return
        remove = []
        for queue in self.queue:
            request_blocks = queue.describe().request_blocks
            if any(request_block.RID == request_id for request_block in request_blocks):
                remove.append(queue)
        if remove:
            for rmv in remove:
                self.queue.remove(rmv)

    def clear(self):
        """clear the queue"""
        self.queue.clear()
        self.active_instruction_queue = None

    def __iter__(self):
        return self

    def __next__(self):
        while not self.signal_event.is_set():
            updated = self._next_instruction_queue()
            if updated:
                self._reset_auto_shutdown_timer()
                return self.active_instruction_queue
            self._start_auto_shutdown_timer()

    def _start_auto_shutdown_timer(self):
        """
        Start the auto shutdown timer if it is not already running.
        """
        with self._lock:
            if self._auto_shutdown_timer is None and len(self.queue) == 0:
                if self.queue_name == "primary":
                    # We don't auto-shutdown the primary queue, so there is no
                    # need to set a timer
                    return
                self._auto_shutdown_timer = threading.Timer(
                    self.AUTO_SHUTDOWN_TIME, self.queue_manager.remove_queue, args=[self.queue_name]
                )
                self._auto_shutdown_timer.name = f"AutoShutdownTimer-{self.queue_name}"
                self._auto_shutdown_timer.start()

    def _reset_auto_shutdown_timer(self):
        """
        Cancel and reset the auto shutdown timer.
        """
        with self._lock:
            if self._auto_shutdown_timer is not None:
                self._auto_shutdown_timer.cancel()
                if threading.current_thread() != self._auto_shutdown_timer:
                    self._auto_shutdown_timer.join()
                self._auto_shutdown_timer = None

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

    def _next_instruction_queue(self) -> bool:
        """get the next instruction queue from the queue. If no update is available, it will return False."""
        try:
            with self._lock:
                aiq = self.active_instruction_queue
                if (
                    aiq is not None
                    and len(self.queue) > 0
                    and self.queue[0].status != InstructionQueueStatus.PENDING
                ):
                    logger.debug(f"Removing queue item {self.queue[0].describe()} from queue")
                    self.queue.popleft()
                    self.queue_manager.send_queue_status()

                if self._queue_should_continue():
                    self._flush_deferred_inserts()
                    if len(self.queue) == 0:
                        if aiq is None:
                            wait_time = 0.1
                        else:
                            self.active_instruction_queue = None
                            wait_time = 0.01
                    else:
                        self.active_instruction_queue = self.queue[0]
                        self.history_queue.append(self.active_instruction_queue)
                        return True
                else:
                    wait_time = None

            if wait_time is not None:
                self.signal_event.wait(wait_time)
                return False

            while not self.signal_event.is_set():
                with self._lock:
                    if self.status != ScanQueueStatus.LOCKED or self._queue_should_continue():
                        break
                    self._flush_deferred_inserts()
                self.signal_event.wait(0.1)

            while not self.signal_event.is_set():
                with self._lock:
                    if self.status != ScanQueueStatus.PAUSED:
                        break
                    if len(self.queue) == 0 and self.auto_reset_enabled:
                        # we don't need to pause if there is no scan enqueued
                        self.status = ScanQueueStatus.RUNNING
                        logger.info("resetting queue status to running")
                        break
                    if (
                        len(self.queue) > 0
                        and self.queue[0].status == InstructionQueueStatus.STOPPED
                    ):
                        # The next instruction queue is stopped, we can remove it
                        break
                self.signal_event.wait(0.1)

            with self._lock:
                self._flush_deferred_inserts()
                self.active_instruction_queue = self.queue[0]
                self.history_queue.append(self.active_instruction_queue)
                return True
        except IndexError:
            self.signal_event.wait(0.01)
        return False

    def _flush_deferred_inserts(self) -> None:
        """Move buffered inserts into the live queue once the stopped head no longer blocks them."""
        if not self._deferred_inserts or self.worker_status == InstructionQueueStatus.STOPPED:
            return
        while self._deferred_inserts:
            msg, position = self._deferred_inserts.popleft()
            self._insert_now(msg, position=position)

    def _insert_now(self, msg: messages.ScanQueueMessage, position=-1) -> None:
        """Insert a new message into the live queue without waiting."""
        target_group = msg.metadata.get("queue_group")
        scan_def_id = msg.metadata.get("scan_def_id")
        logger.debug(f"Inserting new queue message {msg}")
        instruction_queue = None
        queue_exists = False
        if scan_def_id is not None:
            instruction_queue = self.get_queue_item(scan_def_id=scan_def_id)
            if instruction_queue is not None:
                queue_exists = True
        elif target_group is not None:
            instruction_queue = self.get_queue_item(group=target_group)
            if instruction_queue is not None:
                queue_exists = True
        if not queue_exists:
            # create new queue element (InstructionQueueItem)
            iq_class = self._instruction_queue_item_cls_override or DirectInstructionQueueItem

            instruction_queue = iq_class(
                parent=self,
                assembler=self.queue_manager.parent.scan_assembler,
                worker=self.scan_worker,
            )
        if instruction_queue is None:
            logger.error("Failed to create instruction queue item.")
            return
        instruction_queue.append_scan_request(msg)
        if not queue_exists:
            instruction_queue.queue_group = target_group
            if position == -1:
                self.queue.append(instruction_queue)
            else:
                self.queue.insert(position, instruction_queue)

        self.queue_manager.send_queue_status()

    def insert(self, msg: messages.ScanQueueMessage, position=-1, **_kwargs):
        """Insert a new message into the queue or buffer it until a stopped head item clears."""
        with self._lock:
            if self.worker_status == InstructionQueueStatus.STOPPED:
                logger.info("Deferring queue insert until worker becomes active again.")
                self._deferred_inserts.append((msg, position))
                return

            self._flush_deferred_inserts()

        while self.status == ScanQueueStatus.PAUSED and len(self.queue) == 0:
            logger.info("Waiting for queue to become active.")
            if self.signal_event.wait(0.1):
                break

        with self._lock:
            self._insert_now(msg, position=position)

    def get_queue_item(self, group=None, scan_def_id=None):
        """get a queue item based on its group or scan_def_id"""
        if scan_def_id is not None:
            for instruction_queue in self.queue:
                if scan_def_id in instruction_queue.queue.scan_def_ids:
                    return instruction_queue
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


class AutoResetCM:
    """Context manager to automatically reset the queue status"""

    def __init__(self, queue: ScanQueue) -> None:
        self.queue = queue

    def __enter__(self):
        self.queue.auto_reset_enabled = False
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.queue.auto_reset_enabled = True
        return False


class DirectInstructionQueueItem:
    """
    An instruction queue item for v4 scans.
    """

    def __init__(self, parent: ScanQueue, assembler: ScanAssembler, worker: ScanWorker) -> None:
        self.parent = parent
        self.assembler = assembler
        self.worker = worker
        self.exit_info: ExitInfoType | None = None
        self.queue_id = str(uuid.uuid4())
        self.stopped = False
        self._scan_id = str(uuid.uuid4())
        self.queue_group = None
        self.queue_group_is_closed = False

        self._status = InstructionQueueStatus.PENDING
        self._run_on_exception_hook = None

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
        self.worker.status = val
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

    def set_active(self):
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
    def run_on_exception_hook(self, val: bool):
        self._run_on_exception_hook = val

    def describe(self):
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

    def describe_active_scan(self):
        """description of the active scan"""
        if self.active_scan is None:
            return None
        if self.active_scan not in self.scans:
            return None
        msg = self.scan_msgs[self.scans.index(self.active_scan)]
        scan_info = self._get_request_block_message(self.active_scan, msg)
        return scan_info

    def describe_scans(self):
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
            scan_motors=scan.scan_info.readout_priority_modification.get("monitored", []),
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
        for queue in self.parent.queue:
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

    def move_to_next_scan(self):
        """move to the next scan in the instruction queue item"""
        if self.active_scan is None:
            if len(self.scans) > 0:
                self._set_scan_as_active(self.scans[0])
                return self.active_scan
            raise StopIteration("No active scan and no scans in the queue.")
        current_index = self.scans.index(self.active_scan)
        if current_index + 1 < len(self.scans):
            self._set_scan_as_active(self.scans[current_index + 1])
            return self.active_scan
        raise StopIteration("No more scans in the queue.")

    def _set_scan_as_active(self, scan: ScanBase_v4):
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

    def append_to_queue_history(self):
        """append a new queue item to the redis history buffer"""
        msg = messages.ScanQueueHistoryMessage(
            status=self.status.name, queue_id=self.queue_id, info=self.describe()
        )
        self.parent.queue_manager.connector.lpush(
            MessageEndpoints.scan_queue_history(), msg, max_size=100
        )

    def stop(self):
        """stop the instruction queue item and all active scans"""
        for scan in self.scans:
            scan._shutdown_event.set()

    def abort(self):
        self.active_scan = None
        self.scans = []
        self.scan_msgs = []
