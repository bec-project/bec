"""Channel-owned direct scan queues and their public command facade.

Only QueueCoordinator mutates ScanQueue or DirectInstructionQueueItem. Preparation
and Redis I/O have separate serial channels; each queue worker owns its live scan.
"""

from __future__ import annotations

import collections
import threading
import time
import traceback
import uuid
from functools import partial
from queue import Empty
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .direct_scan_worker import describe_scan
from .instruction_handler import InstructionHandler
from .queue_channels import (
    Channel,
    ChannelClosed,
    ExecutionControl,
    ExecutionToken,
    ExitInfoType,
    InstructionQueueStatus,
    LaneJob,
    QueueCommand,
    Result,
    ScanAssignment,
    ScanQueueStatus,
    ScanReport,
    SerialLane,
)
from .queue_state import DirectInstructionQueueItem, ScanQueue, _InFlight, _Preparation

# Public compatibility controls and owner transitions are deliberately colocated.
# pylint: disable=too-many-lines


if TYPE_CHECKING:
    from .scan_server import ScanServer
    from .scan_worker import ScanWorker


logger = bec_logger.logger
_PENDING = object()


class QueueCoordinator(threading.Thread):
    # The loop and facade jointly implement the same private ownership boundary.
    # pylint: disable=protected-access
    """Serialize queue state transitions and monotonic idle maintenance."""

    def __init__(self, manager: QueueManager) -> None:
        super().__init__(name="ScanQueueCoordinator", daemon=True)
        self.manager = manager

    def run(self) -> None:
        """Receive commands; blocking preparation, I/O and scans run on other channels."""
        manager = self.manager
        while True:
            try:
                command = manager._commands.receive(timeout=manager._idle_timeout())
            except Empty:
                manager._maintain()
                continue
            except ChannelClosed:
                return
            try:
                result = getattr(manager, f"_on_{command.operation}")(
                    *command.arguments, reply=command.reply
                )
                manager._maintain()
                if result is not _PENDING and command.reply is not None:
                    command.reply.send(Result(value=result))
            except Exception as exc:  # pylint: disable=broad-except
                if command.reply is not None:
                    command.reply.send(Result(error=exc))
                else:
                    logger.exception(f"Queue command {command.operation} failed: {exc}")


class QueueManager:
    # Keep the existing public control signatures during the ownership cutover.
    # pylint: disable=too-many-public-methods,too-many-arguments,too-many-positional-arguments
    """Public queue API backed by command, preparation, I/O and worker channels."""

    MAX_PENDING_REQUESTS = 1000

    def __init__(self, parent: ScanServer, *, activate: bool = True) -> None:
        self.parent = parent
        self.connector = parent.connector
        self.instruction_handler = InstructionHandler(self.connector)
        self._commands: Channel[QueueCommand] = Channel()
        self._submission_lock = threading.Lock()
        self._accepting = True
        self._closed = False
        self._started = False
        self._closing = False
        self._queues: dict[str, ScanQueue] = {}
        self._retired: dict[str, ScanQueue] = {}
        self._workers: list[ScanWorker] = []
        self._preparations: dict[str, _Preparation] = {}
        self._preparation_jobs = 0
        self._io_errors: list[Exception] = []
        self._scan_number = 0
        self._io_pending = 0
        self._io_waiters = []
        self._snapshot_inflight = False
        self._snapshot_pending = None
        self._snapshot_waiters = []
        self._preparation_lane = SerialLane("ScanQueuePreparation")
        self._io_lane = SerialLane("ScanQueueIO")
        self._owner = QueueCoordinator(self)
        self._preparation_lane.start()
        self._io_lane.start()
        self._owner.start()
        if activate:
            self.start()

    def assert_owner(self) -> None:
        """Reject metadata access from outside the coordinator thread."""
        if threading.current_thread() is not self._owner:
            raise RuntimeError("Scan queue state belongs to the coordinator thread")

    def _send(self, operation, *arguments, internal=False, wait=True, timeout=None):
        reply = Channel() if wait else None
        if threading.current_thread() is self._owner:
            raise RuntimeError("Owner must use transition methods, not synchronous facade calls")
        with self._submission_lock:
            if self._closed or (not internal and not self._accepting):
                raise ChannelClosed("Scan queue intake is closed")
            self._commands.send(QueueCommand(operation, arguments, reply))
        if reply is not None:
            return reply.receive(timeout=timeout).unwrap()
        return None

    def _post(self, operation, *arguments):
        self._send(operation, *arguments, internal=True, wait=False)

    def start(self) -> None:
        """Activate intake only after assembler and scan-number storage are initialized."""
        if self._started:
            return
        self._started = True
        self._send("start")
        self.connector.register(MessageEndpoints.scan_queue_insert(), cb=self._scan_queue_callback)
        self.connector.register(
            MessageEndpoints.scan_queue_modification(), cb=self._scan_queue_modification_callback
        )
        self.connector.register(
            MessageEndpoints.scan_queue_order_change(), cb=self._scan_queue_order_callback
        )

    @property
    def queues(self) -> dict:
        """Return copied queue metadata, never the writable registry."""
        return self.export_queue()

    def add_queue(self, queue_name: str) -> None:
        """Create a named queue and its worker if needed."""
        self._send("add_queue", queue_name)

    def add_to_queue(
        self, scan_queue: str, msg: messages.ScanQueueMessage, position: int = -1
    ) -> None:
        """Accept a direct request, awaiting preparation except when deferred by cleanup."""
        self._send("insert", scan_queue, msg.model_copy(deep=True), position)

    def remove_queue(
        self,
        queue_name: str,
        skip_primary: bool = True,
        emit_status: bool = True,
        skip_pending_inserts: bool = False,
    ) -> None:
        """Detach on the owner, then join the old worker outside it."""
        worker = self._send("remove", queue_name, skip_primary, emit_status, skip_pending_inserts)
        if worker is not None:
            worker.join(timeout=10)
            if worker.is_alive():
                raise TimeoutError(f"Queue {queue_name} is still cleaning up")

    def add_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> None:
        """Apply a copied named admission hold."""
        self._send("lock", queue_name, lock.model_copy(deep=True), False)

    def remove_queue_lock(self, queue_name: str, lock: messages.ScanQueueLock) -> None:
        """Release a named admission hold."""
        self._send("lock", queue_name, lock.model_copy(deep=True), True)

    def scan_interception(self, msg: messages.ScanQueueModificationMessage) -> None:
        """Apply a copied queue control through the coordinator."""
        self._send("control", msg.model_copy(deep=True), None)

    def _control(
        self, action, scan_id, request_id, queue, parameter, exit_info: ExitInfoType | None = None
    ):
        self._send(
            "control",
            messages.ScanQueueModificationMessage(
                action=action,
                scan_id=scan_id,
                request_id=request_id,
                queue=queue,
                parameter=parameter or {},
            ),
            exit_info,
        )

    def set_pause(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
    ) -> None:
        """Pause the executing scan without changing admission."""
        self._control("pause", scan_id, request_id, queue, parameter)

    def set_deferred_pause(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
    ) -> None:
        """Hold subsequent work while current direct execution continues."""
        self._control("deferred_pause", scan_id, request_id, queue, parameter)

    def set_continue(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
    ) -> None:
        """Resume only when admission holds permit it."""
        self._control("continue", scan_id, request_id, queue, parameter)

    def set_abort(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
        exit_info: ExitInfoType | None = None,
        user_call: bool = True,
    ) -> None:
        """Abort the targeted item, preserving the first exit reason."""
        self._control(
            "abort",
            scan_id,
            request_id,
            queue,
            parameter,
            exit_info or ("aborted", "user" if user_call else "alarm"),
        )

    def set_halt(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
        user_call: bool = True,
    ) -> None:
        """Stop without running an exception hook."""
        self._control(
            "halt",
            scan_id,
            request_id,
            queue,
            parameter,
            ("halted", "user" if user_call else "alarm"),
        )

    def set_user_completed(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
        user_call: bool = True,
    ) -> None:
        """Request user-completed cleanup while preserving admission."""
        self._control(
            "user_completed",
            scan_id,
            request_id,
            queue,
            parameter,
            ("user_completed", "user" if user_call else "alarm"),
        )

    def set_clear(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
    ) -> None:
        """Clear visible work while retaining in-flight cleanup ownership."""
        self._control("clear", scan_id, request_id, queue, parameter)

    def set_restart(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
    ) -> None:
        """Prepare a replacement without waiting for the original to enter history."""
        self._control("restart", scan_id, request_id, queue, parameter)

    def set_lock(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
    ) -> None:
        """Add a queue admission lock from a control request."""
        self._control("lock", scan_id, request_id, queue, parameter)

    def set_release_lock(
        self,
        scan_id: str | list[str] | None = None,
        request_id: str | None = None,
        queue: str = "primary",
        parameter: dict | None = None,
    ) -> None:
        """Release a queue admission lock from a control request."""
        self._control("release_lock", scan_id, request_id, queue, parameter)

    def _handle_scan_order_change(self, msg: messages.ScanQueueOrderMessage) -> None:
        self._send("order", msg.model_copy(deep=True))

    def export_queue(self) -> dict:
        """Query a coherent independent snapshot of all live queues."""
        return self._send("export", internal=True)

    def describe_queue(self) -> list[str]:
        """Return a readable description without sharing live queue records."""
        return [f"{name}: {state}" for name, state in self.export_queue().items()]

    def send_queue_status(self) -> None:
        """Request ordered publication of the current owner snapshot."""
        self._send("publish", internal=True)

    def worker_report(self, report: ScanReport) -> None:
        """Accept an independent worker report, including after external intake closes."""
        self._send("report", report, internal=True)

    def worker_failed(self, token: ExecutionToken, error: str) -> None:
        """Resolve an assignment even if plugin failure prevented a terminal description."""
        self._send("failed", token, error, internal=True)

    def flush(self) -> None:
        """Wait outside the owner for all previously submitted queue I/O."""
        drained = self._send("drain_io", internal=True)
        drained.receive()
        errors = self._send("take_io_errors", internal=True)
        if errors:
            raise ExceptionGroup("Scan queue I/O failed", errors)

    def shutdown(self, timeout: float | None = 10) -> None:
        """Close intake, join workers/executors externally, and stop the owner last."""
        if self._closed:
            return
        with self._submission_lock:
            self._accepting = False
        workers = self._send("shutdown", internal=True)
        deadline = None if timeout is None else time.monotonic() + timeout
        for worker in workers:
            worker.join(None if deadline is None else max(0, deadline - time.monotonic()))
            if worker.is_alive():
                raise TimeoutError(f"Shutdown incomplete: {worker.name}; coordinator remains alive")
        self._preparation_lane.jobs.close()
        self._preparation_lane.join(
            None if deadline is None else max(0, deadline - time.monotonic())
        )
        if self._preparation_lane.is_alive():
            raise TimeoutError("Scan constructor is still running; coordinator remains alive")
        self._send("barrier", internal=True)
        drained = self._send("drain_io", internal=True)
        try:
            drained.receive(None if deadline is None else max(0, deadline - time.monotonic()))
        except Empty as exc:
            raise TimeoutError("Queue I/O is still running; coordinator remains alive") from exc
        self._io_lane.jobs.close()
        self._io_lane.join(None if deadline is None else max(0, deadline - time.monotonic()))
        if self._io_lane.is_alive():
            raise TimeoutError("Queue I/O is still running; coordinator remains alive")
        self._send("barrier", internal=True)
        errors = self._send("take_io_errors", internal=True)
        with self._submission_lock:
            self._closed = True
            self._commands.close()
        self._owner.join()
        if errors:
            raise ExceptionGroup("Scan queue I/O failed during shutdown", errors)

    def _scan_queue_callback(self, msg) -> None:
        value = msg.value.model_copy(deep=True)
        try:
            self._send("insert", value.queue, value, -1, wait=False)
        except ChannelClosed:
            logger.info("Ignoring insertion after queue shutdown")

    def _scan_queue_modification_callback(self, msg) -> None:
        if msg.value:
            try:
                self._send("control", msg.value.model_copy(deep=True), None, wait=False)
            except ChannelClosed:
                logger.info("Ignoring control after queue shutdown")

    def _scan_queue_order_callback(self, msg) -> None:
        try:
            self._send("order", msg.value.model_copy(deep=True), wait=False)
        except ChannelClosed:
            logger.info("Ignoring reorder after queue shutdown")

    # Everything below runs on the coordinator, except the explicitly named lane jobs.
    # Handlers share a reply keyword; only asynchronous handlers retain it.
    # pylint: disable=unused-argument
    def _lane(self, lane, execute, operation, *identity):
        if lane is self._io_lane:
            self._io_pending += 1
            lane.jobs.send(
                LaneJob(execute, lambda result: self._post("io_done", operation, identity, result))
            )
        else:
            lane.jobs.send(
                LaneJob(execute, lambda result: self._post(operation, *identity, result))
            )

    def _on_io_done(self, operation, identity, result, *, reply=None):
        self._io_pending -= 1
        if result.error:
            self._io_errors.append(result.error)
        try:
            getattr(self, f"_on_{operation}")(*identity, result)
        finally:
            if not self._io_pending:
                for waiter in self._io_waiters:
                    waiter.send(None)
                self._io_waiters.clear()

    def _on_drain_io(self, *, reply=None):
        waiter = Channel()
        if self._io_pending:
            self._io_waiters.append(waiter)
        else:
            waiter.send(None)
        return waiter

    def _effect(self, execute):
        self._lane(self._io_lane, execute, "effect_done")

    def _on_effect_done(self, result, *, reply=None):
        if result.error:
            logger.error(f"Queue I/O failed: {result.error}")

    def _on_barrier(self, *, reply=None):
        return None

    def _on_take_io_errors(self, *, reply=None):
        errors, self._io_errors = self._io_errors, []
        return errors

    def _on_start(self, *, reply=None):
        self._ensure_queue("primary")
        self._lane(self._io_lane, lambda: self.parent.scan_number, "counter")

    def _on_counter(self, result, *, reply=None):
        if not result.error:
            self._scan_number = result.value
        self._publish()

    def _ensure_queue(self, name):
        self.assert_owner()
        if self._closing:
            raise ChannelClosed("Scan queue intake is closed")
        queue = self._queues.get(name)
        if queue is not None and not queue.worker.is_alive():
            self._detach(queue)
            queue = None
        if queue is None:
            if any(
                old.queue_name == name and old.active is not None for old in self._retired.values()
            ):
                raise RuntimeError(f"Queue {name} is still shutting down")
            queue = self._queues[name] = ScanQueue(self, name)
            self._workers.append(queue.worker)
            queue.worker.start()
        return queue

    def _on_add_queue(self, name, *, reply=None):
        self._ensure_queue(name)
        self._publish()

    def _on_insert(self, name, msg, position, *, reply=None, restart=None, accepted=False):
        queue = self._ensure_queue(name)
        count = sum(len(q.queue) + len(q.deferred) for q in self._queues.values())
        if not accepted and (
            count >= self.MAX_PENDING_REQUESTS
            or self._preparation_jobs >= self.MAX_PENDING_REQUESTS
        ):
            error = RuntimeError("Scan queue capacity exceeded")
            self._alarm(error, msg)
            raise error
        if (
            not restart
            and queue.active
            and queue.active.item.status == InstructionQueueStatus.STOPPED
        ):
            queue.deferred.append((msg, position))
            return None
        # Empty paused admission auto-resets before inserting, as in the old worker.
        if not queue.queue and not queue.active and queue.status == ScanQueueStatus.PAUSED:
            queue.set_status(ScanQueueStatus.RUNNING)
        # Each direct request has one lifecycle and one terminal acknowledgement.
        # queue_group remains descriptive; it must not merge independent executions.
        item = DirectInstructionQueueItem()
        if position == -1:
            queue.queue.append(item)
        else:
            queue.queue.insert(max(0, min(position, len(queue.queue))), item)
        item.scan_msgs.append(msg)
        item.preparing += 1
        preparation_id = str(uuid.uuid4())
        preparation = self._preparations[preparation_id] = _Preparation(
            queue.generation, item, msg, reply, restart
        )
        self._preparation_jobs += 1
        self._lane(
            self._preparation_lane,
            partial(
                self._prepare_scan,
                msg.model_copy(deep=True),
                item.scan_id_hint,
                preparation.cancelled,
            ),
            "prepared",
            preparation_id,
        )
        return _PENDING

    def _prepare_scan(self, msg, scan_id, cancelled):
        if cancelled.is_set():
            raise ChannelClosed("Preparation was cancelled before construction")
        assembler = self.parent.scan_assembler
        if not assembler.is_direct_scan_message(msg):
            raise ValueError(
                "The channel queue accepts direct scans only; legacy scans are retired"
            )
        scan_cls = assembler.scan_manager.scan_dict[msg.scan_type]
        scan = assembler.assemble_direct_scan(
            msg, scan_id=scan_id if getattr(scan_cls, "is_scan", True) else None
        )
        return scan, describe_scan(scan, msg)

    def _on_prepared(self, preparation_id, result, *, reply=None):
        self._preparation_jobs -= 1
        preparation = self._preparations.pop(preparation_id, None)
        if preparation is None:
            return
        queue = self._by_generation(preparation.generation)
        item = preparation.item
        item.preparing -= 1
        live = queue is not None and not queue.closed and any(q is item for q in queue.queue)
        if not live:
            if preparation.reply:
                preparation.reply.send(Result(error=ChannelClosed("Insertion was cancelled")))
            return
        if result.error:
            item.scan_msgs.remove(preparation.msg)
            self._alarm(result.error, preparation.msg, item.queue_id)
            if not item.preparing and not item.requests:
                queue.queue.remove(item)
        else:
            scan, description = result.value
            item.prepared_scans.append(scan)
            item.requests.append(description)
        if (
            not result.error
            and preparation.restart
            and queue.active
            and queue.active.token == preparation.restart
        ):
            self._stop(queue, queue.active.item, ("aborted", "user"), preserve_admission=True)
        self._publish()
        if preparation.reply:
            preparation.reply.send(Result(error=result.error))

    def _on_lock(self, name, lock, remove, *, reply=None):
        queue = self._queues.get(name) if remove else self._ensure_queue(name)
        if queue is None:
            return
        if remove:
            queue.remove_lock(lock.identifier)
        else:
            queue.add_lock(lock)
        self._publish()

    def _target(self, queue, scan_id, request_id):
        candidates = list(queue.queue)
        if queue.active and all(item is not queue.active.item for item in candidates):
            candidates.append(queue.active.item)
        if request_id and not scan_id:
            return next(
                (
                    item
                    for item in candidates
                    if any(msg.metadata.get("RID") == request_id for msg in item.scan_msgs)
                ),
                None,
            )
        if scan_id:
            ids = set(scan_id if isinstance(scan_id, list) else [scan_id])
            return next((item for item in candidates if ids.intersection(item.scan_id)), None)
        return queue.active.item if queue.active else next(iter(queue.queue), None)

    def _on_control(self, msg, exit_info, *, reply=None):
        # One explicit branch for each supported wire command.
        # pylint: disable=too-many-branches
        queue = self._ensure_queue(msg.queue)
        action = msg.action
        parameter = msg.parameter or {}
        if action in ("lock", "release_lock"):
            identifier = parameter.get("identifier")
            if not identifier or (action == "lock" and not parameter.get("reason")):
                raise ValueError("A lock requires an identifier and a reason")
            self._on_lock(
                msg.queue,
                messages.ScanQueueLock(
                    identifier=identifier,
                    reason=parameter.get("reason", ""),
                    allow_device_instructions=parameter.get("allow_device_instructions", True),
                ),
                action == "release_lock",
            )
            return
        item = self._target(queue, msg.scan_id, msg.request_id)
        if parameter.get("queue_id") and (item is None or item.queue_id != parameter["queue_id"]):
            return
        active = queue.active
        if action == "pause":
            if active and active.item.status == InstructionQueueStatus.RUNNING:
                active.item.status = InstructionQueueStatus.PAUSED
                active.control.set_status(InstructionQueueStatus.PAUSED)
        elif action == "deferred_pause":
            queue.set_status(ScanQueueStatus.PAUSED)
            if active and active.item.status == InstructionQueueStatus.RUNNING:
                active.item.status = InstructionQueueStatus.DEFERRED_PAUSE
                active.control.set_status(InstructionQueueStatus.DEFERRED_PAUSE)
        elif action == "continue":
            queue.set_status(ScanQueueStatus.RUNNING)
            if active and queue.status == ScanQueueStatus.RUNNING:
                if active.item.status != InstructionQueueStatus.STOPPED:
                    active.item.status = InstructionQueueStatus.RUNNING
                    active.control.set_status(InstructionQueueStatus.RUNNING)
        elif action == "clear":
            queue.set_status(ScanQueueStatus.PAUSED)
            if active:
                self._stop(queue, active.item, ("aborted", "user"), preserve_admission=True)
            queue.queue.clear()
            queue.deferred.clear()
            self._cancel_preparations(queue)
        elif action == "restart":
            self._restart(queue, item, parameter)
        elif action in ("abort", "halt", "user_completed"):
            if item is not None:
                terminal = {
                    "abort": "aborted",
                    "halt": "halted",
                    "user_completed": "user_completed",
                }[action]
                self._stop(
                    queue,
                    item,
                    exit_info or (terminal, "user"),
                    preserve_admission=action == "user_completed",
                    cleanup=action != "halt",
                )
        else:
            raise ValueError(f"Unknown queue control {action}")
        self._publish()

    def _stop(self, queue, item, exit_info, *, preserve_admission=False, cleanup=True):
        if queue.active is None or queue.active.item is not item:
            item.status = InstructionQueueStatus.CANCELLED
            self._publish(critical=True)  # Cancellation is visible before removal.
            queue.queue = collections.deque(other for other in queue.queue if other is not item)
            self._cancel_preparations(queue, item)
            return
        receipt = queue.active.control.stop(exit_info, cleanup=cleanup)
        if receipt is None:
            return
        if not preserve_admission:
            queue.set_status(ScanQueueStatus.PAUSED)
        item.exit_info = item.exit_info or exit_info
        item.status = InstructionQueueStatus.STOPPED
        rid = item.scan_msgs[0].metadata.get("RID")
        stop_id = [sid for sid in item.scan_id if sid] or item.queue_id
        self._effect(partial(self._send_stop, rid, stop_id, receipt))

    def _send_stop(self, rid, stop_id, receipt):
        try:
            registry = getattr(self.parent, "device_lock_registry", None)

            def send(devices):
                self.connector.send(
                    MessageEndpoints.stop_devices(),
                    messages.VariableMessage(value=devices, metadata={"stop_id": stop_id}),
                )

            if registry is None or rid is None:
                send([])
            else:
                registry.stop_request(rid, send)
        finally:
            receipt.set()

    def _restart(self, queue, item, parameter):
        if item is None or queue.active is None or queue.active.item is not item:
            return
        original = queue.active.token
        msg = item.scan_msgs[0].model_copy(deep=True)
        if parameter.get("RID"):
            msg.metadata["RID"] = parameter["RID"]
        item.reason = "restart"
        scan_id = next((sid for sid in item.scan_id if sid), None)
        if scan_id is None:
            return
        restart_message = messages.ScanRestartMessage(original_scan_id=scan_id, scan_msg=msg)
        self._effect(partial(self.connector.send, MessageEndpoints.scan_restart(), restart_message))
        if msg.allow_restart:
            position = next((idx + 1 for idx, entry in enumerate(queue.queue) if entry is item), 0)
            self._on_insert(queue.queue_name, msg, position, restart=original)
        elif queue.active and queue.active.token == original:
            self._stop(queue, item, ("aborted", "user"), preserve_admission=True)

    def _on_order(self, msg, *, reply=None):
        queue = self._queues.get(msg.queue)
        if queue is None or queue.status != ScanQueueStatus.PAUSED:
            return
        item = self._target(queue, msg.scan_id, None)
        if item is None or item not in queue.queue:
            return
        old = list(queue.queue).index(item)
        positions = {
            "move_up": old - 1,
            "move_down": old + 1,
            "move_top": 0,
            "move_bottom": len(queue.queue) - 1,
            "move_to": msg.target_position,
        }
        target = positions[msg.action]
        if target is None:
            return
        queue.queue.remove(item)
        queue.queue.insert(max(0, min(target, len(queue.queue))), item)
        self._publish()

    def _schedule(self, queue):
        if not queue.eligible():
            return
        item = queue.queue[0]
        queue.dispatch_id += 1
        token = ExecutionToken(queue.generation, item.queue_id, queue.dispatch_id)
        queue.active = _InFlight(token, item, ExecutionControl())
        item.status = InstructionQueueStatus.RUNNING
        self._lane(
            self._io_lane,
            partial(
                self._allocate_numbers,
                item.requests[0].is_scan,
                bool(item.scan_msgs[0].metadata.get("dataset_id_on_hold")),
            ),
            "numbered",
            token,
        )

    def _allocate_numbers(self, is_scan, dataset_hold):
        if not is_scan:
            return None, None
        number = self.parent.scan_number + 1
        dataset = self.parent.dataset_number + (not dataset_hold)
        self.parent.scan_number = number
        if not dataset_hold:
            self.parent.dataset_number = dataset
        return number, dataset

    def _on_numbered(self, token, result, *, reply=None):
        queue = self._by_generation(token.generation)
        if queue is None or queue.active is None or queue.active.token != token:
            return
        item = queue.active.item
        if result.error:
            self._alarm(result.error, item.scan_msgs[0], item.queue_id)
            self._finish(queue, item, InstructionQueueStatus.STOPPED)
            return
        number, dataset = result.value
        if number is not None:
            self._scan_number = number
        item.requests[0].scan_number = number
        item.active_request = item.requests[0].model_copy(deep=True)
        if queue.closed:
            self._finish(queue, item, InstructionQueueStatus.STOPPED)
            return
        scan = item.prepared_scans.pop(0)
        assignment = ScanAssignment(
            token,
            queue.queue_name,
            scan,
            item.scan_msgs[0].model_copy(deep=True),
            queue.active.control,
            number,
            dataset,
        )
        queue.active.dispatched = True
        queue.history_queue.append(item.describe(self._scan_number + 1)[0])
        queue.work.send(assignment)
        self._publish()

    def _on_report(self, report, *, reply=None):
        queue = self._by_generation(report.token.generation)
        if queue is None or queue.active is None or queue.active.token != report.token:
            return False
        item = queue.active.item
        item.requests[0] = report.request.model_copy(deep=True)
        item.active_request = item.requests[0].model_copy(deep=True)
        if report.terminal:
            self._finish(queue, item, report.status, report.exit_info)
        else:
            self._publish(reply=reply)
            return _PENDING
        return True

    def _on_failed(self, token, error, *, reply=None):
        queue = self._by_generation(token.generation)
        if queue and queue.active and queue.active.token == token:
            self._alarm(RuntimeError(error), queue.active.item.scan_msgs[0], token.queue_id)
            self._finish(queue, queue.active.item, InstructionQueueStatus.STOPPED)
        logger.error(f"Scan worker failed for {token}: {error}")

    def _finish(self, queue, item, status, exit_info=None):
        if status == InstructionQueueStatus.STOPPED and item.exit_info is None:
            queue.set_status(ScanQueueStatus.PAUSED)
            item.exit_info = exit_info or ("aborted", "alarm")
        item.status = status
        description = item.describe(self._scan_number + 1)[0]
        history = messages.ScanQueueHistoryMessage(
            status=status.name, queue_id=item.queue_id, info=description, queue=queue.queue_name
        )
        self._effect(
            partial(
                self.connector.lpush, MessageEndpoints.scan_queue_history(), history, max_size=100
            )
        )
        queue.queue = collections.deque(other for other in queue.queue if other is not item)
        self._cancel_preparations(queue, item)
        queue.active = None
        self._publish()

    def _by_generation(self, generation):
        return next(
            (queue for queue in self._queues.values() if queue.generation == generation),
            self._retired.get(generation),
        )

    def _cancel_preparations(self, queue, item=None):
        for key, preparation in list(self._preparations.items()):
            if preparation.generation != queue.generation:
                continue
            if item is not None and preparation.item is not item:
                continue
            self._preparations.pop(key)
            preparation.cancelled.set()
            preparation.item.preparing -= 1
            if preparation.reply:
                preparation.reply.send(Result(error=ChannelClosed("Insertion was cancelled")))

    def _detach(self, queue):
        self._queues.pop(queue.queue_name, None)
        self._retired[queue.generation] = queue
        queue.closed = True
        self._cancel_preparations(queue)
        queue.deferred.clear()
        if queue.active:
            self._stop(queue, queue.active.item, ("aborted", "alarm"), cleanup=False)
            queue.active.control.shutdown()
        queue.queue.clear()
        queue.worker.request_shutdown()
        return queue.worker

    def _on_remove(self, name, skip_primary, emit_status, skip_pending, *, reply=None):
        if name == "primary" and skip_primary:
            return None
        queue = self._queues.get(name)
        if queue is None:
            return None
        if skip_pending and any(
            p.generation == queue.generation for p in self._preparations.values()
        ):
            return None
        worker = self._detach(queue)
        if emit_status:
            self._publish()
        return worker

    def _on_shutdown(self, *, reply=None):
        self._closing = True
        for queue in list(self._queues.values()):
            self._detach(queue)
        return list(self._workers)

    def _on_export(self, *, reply=None):
        return {name: queue.describe(self._scan_number + 1) for name, queue in self._queues.items()}

    def _on_publish(self, *, reply=None):
        self._publish()

    def _publish(self, reply=None, critical=False):
        if self._closing or "primary" not in self._queues:
            if reply is not None:
                reply.send(Result())
            return
        export = self._on_export()
        msg = messages.ScanQueueStatusMessage(queue=export)
        lengths = {name: len(state["info"]) for name, state in export.items()}
        self._snapshot_pending = (msg, lengths)
        if reply is not None:
            self._snapshot_waiters.append(reply)
        if critical:
            waiters = self._snapshot_waiters
            self._snapshot_pending = None
            self._snapshot_waiters = []
            self._lane(
                self._io_lane,
                partial(self._publish_snapshot, msg, lengths),
                "critical_published",
                waiters,
            )
        elif not self._snapshot_inflight:
            self._submit_snapshot()

    def _submit_snapshot(self):
        snapshot = self._snapshot_pending
        waiters = self._snapshot_waiters
        self._snapshot_pending = None
        self._snapshot_waiters = []
        self._snapshot_inflight = True
        self._lane(self._io_lane, partial(self._publish_snapshot, *snapshot), "published", waiters)

    def _on_critical_published(self, waiters, result, *, reply=None):
        for waiter in waiters:
            waiter.send(result)
        if result.error:
            logger.error(f"Queue snapshot publication failed: {result.error}")

    def _on_published(self, waiters, result, *, reply=None):
        self._snapshot_inflight = False
        for waiter in waiters:
            waiter.send(result)
        if result.error:
            logger.error(f"Queue snapshot publication failed: {result.error}")
        if self._snapshot_pending is not None:
            self._submit_snapshot()

    def _publish_snapshot(self, msg, lengths):
        self.connector.set_and_publish(MessageEndpoints.scan_queue_status(), msg)
        self.connector.publish_metrics("scan_queue_length", lengths)

    def _alarm(self, exc, msg, queue_id=None):
        info = messages.ErrorInfo(
            error_message="".join(traceback.format_exception(exc)),
            compact_error_message=str(exc),
            exception_type=type(exc).__name__,
            device=None,
        )
        self._effect(
            partial(
                self.connector.raise_alarm,
                severity=Alarms.MAJOR,
                info=info,
                metadata={**msg.metadata, "queue": msg.queue, "queue_id": queue_id},
            )
        )

    def _idle_timeout(self):
        deadlines = [
            q.idle_since + q.AUTO_SHUTDOWN_TIME
            for q in self._queues.values()
            if q.queue_name != "primary" and q.idle_since is not None
        ]
        return max(0, min(deadlines) - time.monotonic()) if deadlines else None

    def _maintain(self):
        self.assert_owner()
        for queue in list(self._queues.values()):
            while (
                not queue.active
                and queue.deferred
                and self._preparation_jobs < self.MAX_PENDING_REQUESTS
            ):
                msg, position = queue.deferred.popleft()
                self._on_insert(queue.queue_name, msg, position, accepted=True)
            if not queue.queue and not queue.active and not queue.deferred:
                queue.idle_since = queue.idle_since or time.monotonic()
                if queue.status == ScanQueueStatus.PAUSED:
                    queue.set_status(ScanQueueStatus.RUNNING)
                    self._publish()
                if (
                    queue.queue_name != "primary"
                    and time.monotonic() - queue.idle_since >= queue.AUTO_SHUTDOWN_TIME
                ):
                    self._detach(queue)
                    self._publish()
                    continue
            else:
                queue.idle_since = None
            self._schedule(queue)
        self._workers = [worker for worker in self._workers if worker.is_alive()]
        for generation, queue in list(self._retired.items()):
            if queue.active is None and not queue.worker.is_alive():
                self._retired.pop(generation)
