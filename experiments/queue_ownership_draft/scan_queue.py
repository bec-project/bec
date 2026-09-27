"""Draft queue facade and owner thread, with no BEC/Redis integration.

ScanQueue holds per-queue policy and state; QueueCoordinator owns every ScanQueue.
The only explicit lock protects mailbox closure, not application queue state.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from queue import SimpleQueue
from threading import Lock, Thread, current_thread
from typing import Literal
from uuid import uuid4

from .direct_instruction_queue import DirectInstructionQueueItem
from .protocol import (
    Assignment,
    Cancellation,
    ExecutionToken,
    Finished,
    PreparedScan,
    QueueRef,
    QueueSnapshot,
    QueueStatus,
    Reply,
)


@dataclass
class _InFlight:
    token: ExecutionToken
    cancellation: Cancellation
    item: DirectInstructionQueueItem  # Metadata only; scan was transferred to worker.


@dataclass
class ScanQueue:
    """Queue policy, run exclusively on the coordinator thread.

    This is not a thread and exposes no worker execution methods. The facade sends
    commands; the coordinator invokes these methods. Public callers get snapshots.
    """

    ref: QueueRef
    _owner: Thread
    status: QueueStatus = QueueStatus.RUNNING
    allow_device_instructions: bool = False
    items: deque[DirectInstructionQueueItem] = field(default_factory=deque)
    active: _InFlight | None = None
    waiting_worker: Reply[Assignment | None] | None = None
    _dispatch_id: int = 0
    _closed: bool = False

    def _assert_owner(self) -> None:
        assert current_thread() is self._owner, "ScanQueue is coordinator-owned"

    def insert_prepared(self, prepared: PreparedScan) -> str:
        """Keep copied metadata and an opaque scan handle until dispatch."""
        self._assert_owner()
        item = DirectInstructionQueueItem(
            str(uuid4()),
            prepared.label,
            prepared.is_scan,
            prepared.scan,
            prepared.run_on_exception_hook,
        )
        self.items.append(item)
        return item.queue_id

    def set_admission(self, status: QueueStatus, allow_device_instructions: bool) -> None:
        """Change only admission; this does not interrupt an already running scan."""
        self._assert_owner()
        self.status, self.allow_device_instructions = status, allow_device_instructions

    def abort(self) -> None:
        """Record stop intent and signal the exact in-flight assignment."""
        self._assert_owner()
        if self.status != QueueStatus.LOCKED:
            self.status = QueueStatus.PAUSED
        if self.active:
            self.active.item.status = "STOPPED"
            self.active.cancellation.stop()

    def clear(self) -> None:
        """Remove visible entries without discarding in-flight cleanup ownership."""
        self._assert_owner()
        self.abort()
        self.items.clear()

    def request_work(self, reply: Reply[Assignment | None]) -> None:
        """Park a reply instead of waiting for work on the owner thread."""
        self._assert_owner()
        if self._closed:
            reply.resolve(None)
            return
        if self.waiting_worker or self.active:
            raise RuntimeError("One worker request / execution per queue")
        self.waiting_worker = reply
        self.schedule()

    def schedule(self) -> None:
        """Make one complete admission decision and transfer one direct scan."""
        self._assert_owner()
        if self._closed or self.active or not self.waiting_worker or not self.items:
            return
        head = self.items[0]
        permitted = self.status == QueueStatus.RUNNING or (
            self.status == QueueStatus.LOCKED
            and self.allow_device_instructions
            and not head.is_scan
        )
        if not permitted:
            return
        self._dispatch_id += 1
        token = ExecutionToken(self.ref, head.queue_id, self._dispatch_id)
        cancellation = Cancellation()
        assignment = head.claim(token, cancellation)
        self.active = _InFlight(token, cancellation, head)
        waiting, self.waiting_worker = self.waiting_worker, None
        waiting.resolve(assignment)

    def finished(self, report: Finished) -> bool:
        """Retire by exact token, even if clear removed the visible entry earlier."""
        self._assert_owner()
        if not self.active or self.active.token != report.token:
            return False
        self.active.item.status = "COMPLETED" if report.outcome == "completed" else "STOPPED"
        self.items = deque(item for item in self.items if item.queue_id != report.token.item_id)
        self.active = None
        # Production: emit copied terminal description/history exactly once here.
        return True

    def describe(self) -> QueueSnapshot:
        """Describe owner metadata without inspecting any live scan."""
        self._assert_owner()
        return QueueSnapshot(
            self.ref,
            self.status,
            tuple(item.describe() for item in self.items),
            self.active.token if self.active else None,
        )

    def begin_shutdown(self) -> None:
        """Close this queue, wake its worker, and retain cleanup tracking."""
        self._assert_owner()
        self._closed = True
        self.items.clear()
        if self.active:
            self.active.item.status = "STOPPED"
            self.active.cancellation.stop_service()
        if self.waiting_worker:
            self.waiting_worker.resolve(None)
            self.waiting_worker = None


@dataclass(frozen=True)
class _Command:
    # Compact sketch envelope. Use typed per-operation commands in the implementation.
    operation: Literal[
        "create",
        "insert",
        "admission",
        "abort",
        "clear",
        "work",
        "finished",
        "snapshot",
        "begin_shutdown",
        "check_shutdown",
        "finish_shutdown",
    ]
    queue: QueueRef | None = None
    value: object = None


class QueueManager:
    """Small facade; ordinary callers may wait, but the coordinator never does."""

    def __init__(self) -> None:
        self._mailbox: SimpleQueue[tuple[_Command, Reply]] = SimpleQueue()
        self._submission_lock = Lock()
        self._accepting = True
        self._closed = False
        self._owner = QueueCoordinator(self._mailbox)
        self._owner.start()

    def submit(self, command: _Command, *, internal: bool = False) -> Reply:
        """Enqueue atomically with intake closure; never execute caller callbacks."""
        reply = Reply()
        with self._submission_lock:
            if self._closed or (not internal and not self._accepting):
                raise RuntimeError("Queue intake is closed")
            self._mailbox.put((command, reply))
        return reply

    def create_queue(self, name: str = "primary") -> QueueRef:
        """Create a named queue or return its existing lifetime identity."""
        return self.submit(_Command("create", value=name)).result()

    def insert_prepared(self, queue: QueueRef, scan: PreparedScan) -> str:
        """Transfer prepared work; production preparation/reservation is omitted."""
        return self.submit(_Command("insert", queue, scan)).result()

    def set_admission(
        self, queue: QueueRef, status: QueueStatus, *, allow_device_instructions: bool = False
    ) -> None:
        """Set an illustrative admission gate; multiple named locks are omitted."""
        self.submit(_Command("admission", queue, (status, allow_device_instructions))).result()

    def abort(self, queue: QueueRef) -> None:
        """Stop the exact active assignment and request paused admission."""
        self.submit(_Command("abort", queue)).result()

    def clear(self, queue: QueueRef) -> None:
        """Clear visible work while retaining any execution awaiting cleanup."""
        self.submit(_Command("clear", queue)).result()

    def snapshot(self, queue: QueueRef) -> QueueSnapshot:
        """Read a copied metadata view, including during shutdown."""
        return self.submit(_Command("snapshot", queue), internal=True).result()

    def request_work(self, queue: QueueRef) -> Reply[Assignment | None]:
        """Return a parked reply; only the worker waits for admission."""
        return self.submit(_Command("work", queue), internal=True)

    def finished(self, report: Finished) -> bool:
        """Acknowledge an exact execution token, even after intake closes."""
        return self.submit(_Command("finished", report.token.queue, report), internal=True).result()

    def begin_shutdown(self) -> None:
        """Close public intake and signal workers; leave internal reporting open."""
        reply = Reply()
        with self._submission_lock:
            if self._closed:
                return
            self._accepting = False
            self._mailbox.put((_Command("begin_shutdown"), reply))
        reply.result()

    def finish_shutdown(self) -> None:
        """Call only after joining workers externally; owner refuses active work."""
        # Once stopping and quiescent, internal reports cannot create new work.
        # Validate first, then atomically seal intake and append the final marker.
        # All previously accepted internal commands remain ahead of that marker.
        with self._submission_lock:
            if self._closed:
                return
        self.submit(_Command("check_shutdown"), internal=True).result()
        reply = Reply()
        with self._submission_lock:
            self._closed = True
            self._mailbox.put((_Command("finish_shutdown"), reply))
        reply.result()
        self._owner.join()


class QueueCoordinator(Thread):
    """The only thread that can touch _queues and their mutable records."""

    def __init__(self, mailbox: SimpleQueue) -> None:
        super().__init__(name="draft-queue-owner", daemon=True)
        self._mailbox = mailbox
        self._queues: dict[str, ScanQueue] = {}
        self._stopping = False

    def run(self) -> None:
        """Apply state transitions and schedule work; never wait inside a turn."""
        while True:
            command, reply = self._mailbox.get()
            try:
                result = self._handle(command, reply)
                if command.operation == "work":
                    continue  # ScanQueue.schedule() or shutdown resolves the parked reply.
                for state in self._queues.values():
                    state.schedule()
                reply.resolve(result)
                if command.operation == "finish_shutdown":
                    return
            except Exception as exc:
                reply.reject(exc)
            # Production: emit copied effect batches and run idle-deadline maintenance.

    def _state(self, ref: QueueRef) -> ScanQueue:
        assert current_thread() is self
        state = self._queues[ref.name]
        if state.ref != ref:
            raise ValueError("Stale queue generation")
        return state

    def _handle(self, command: _Command, reply: Reply) -> object:
        assert current_thread() is self
        operation, ref, value = command.operation, command.queue, command.value
        if operation == "create":
            name = str(value)
            if name not in self._queues:
                self._queues[name] = ScanQueue(QueueRef(name, str(uuid4())), self)
            return self._queues[name].ref
        if operation == "begin_shutdown":
            self._stopping = True
            for state in self._queues.values():
                state.begin_shutdown()
            return None
        if operation in ("check_shutdown", "finish_shutdown"):
            if not self._stopping or any(state.active for state in self._queues.values()):
                raise RuntimeError("Begin shutdown and await worker cleanup first")
            return None
        if operation == "finished":
            state = self._queues.get(value.token.queue.name)
            return state.finished(value) if state else False
        state = self._state(ref)
        if operation == "insert":
            return state.insert_prepared(value)
        if operation == "admission":
            state.set_admission(*value)
        elif operation == "abort":
            state.abort()
        elif operation == "clear":
            state.clear()
        elif operation == "work":
            state.request_work(reply)
        elif operation == "snapshot":
            return state.describe()
        return None
