"""Queue records owned exclusively by the scan queue coordinator."""

from __future__ import annotations

import collections
import threading
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from bec_lib import messages

from .queue_channels import (
    Channel,
    ExecutionControl,
    ExecutionToken,
    ExitInfoType,
    InstructionQueueStatus,
    Result,
    ScanAssignment,
    ScanQueueStatus,
)
from .scan_worker import ScanWorker

if TYPE_CHECKING:
    from .scan_queue import QueueManager
    from .scans.scan_base import ScanBase


@dataclass
class DirectInstructionQueueItem:
    """Owner-only metadata and opaque, unclaimed direct scans.

    Status assignments have no publication or worker side effects. After dispatch
    the scan is removed from prepared_scans and only copied descriptions remain.
    """

    queue_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    scan_id_hint: str = field(default_factory=lambda: str(uuid.uuid4()))
    scan_msgs: list[messages.ScanQueueMessage] = field(default_factory=list)
    requests: list[messages.RequestBlock] = field(default_factory=list)
    prepared_scans: list[ScanBase] = field(default_factory=list, repr=False)
    preparing: int = 0
    status: InstructionQueueStatus = InstructionQueueStatus.PENDING
    exit_info: ExitInfoType | None = None
    reason: Literal["user", "alarm", "restart"] | None = None
    active_request: messages.RequestBlock | None = None

    @property
    def scan_id(self) -> list[str | None]:
        """Return scan identities without inspecting execution objects."""
        return [request.scan_id for request in self.requests]

    def describe(self, next_number: int) -> tuple[messages.QueueInfoEntry, int]:
        """Build an independent wire description and advance pending-number projection."""
        requests = [request.model_copy(deep=True) for request in self.requests]
        for request in requests:
            if request.is_scan and request.scan_number is None:
                request.scan_number = next_number
                next_number += 1
        return (
            messages.QueueInfoEntry(
                queue_id=self.queue_id,
                scan_id=[req.scan_id for req in requests],
                is_scan=[req.is_scan for req in requests],
                request_blocks=requests,
                scan_number=[req.scan_number for req in requests],
                status=self.status.name,
                active_request_block=(
                    self.active_request.model_copy(deep=True) if self.active_request else None
                ),
                reason=self.reason or (self.exit_info[1] if self.exit_info else None),
            ),
            next_number,
        )


@dataclass
class _InFlight:
    token: ExecutionToken
    item: DirectInstructionQueueItem
    control: ExecutionControl
    dispatched: bool = False


@dataclass
class _Preparation:
    generation: str
    item: DirectInstructionQueueItem
    msg: messages.ScanQueueMessage
    reply: Channel[Result] | None
    restart: ExecutionToken | None = None
    cancelled: threading.Event = field(default_factory=threading.Event)


class ScanQueue:
    """Named queue policy; all methods execute on the coordinator thread."""

    MAX_HISTORY = 100
    AUTO_SHUTDOWN_TIME = 60

    def __init__(self, manager: QueueManager, queue_name: str) -> None:
        manager.assert_owner()
        self.manager = manager
        self.queue_name = queue_name
        self.generation = str(uuid.uuid4())
        self.queue: collections.deque[DirectInstructionQueueItem] = collections.deque()
        self.history_queue: collections.deque[messages.QueueInfoEntry] = collections.deque(
            maxlen=self.MAX_HISTORY
        )
        self.status = ScanQueueStatus.RUNNING
        self.restore_status = ScanQueueStatus.RUNNING
        self.locks: dict[str, messages.ScanQueueLock] = {}
        self.active: _InFlight | None = None
        self.deferred: collections.deque[tuple[messages.ScanQueueMessage, int]] = (
            collections.deque()
        )
        self.work: Channel[ScanAssignment] = Channel()
        self.worker = ScanWorker(
            parent=manager.parent, queue_name=queue_name, work=self.work, manager=manager
        )
        self.dispatch_id = 0
        self.idle_since: float | None = None
        self.closed = False

    def set_status(self, status: ScanQueueStatus) -> None:
        """Change admission without overriding existing locks."""
        self.manager.assert_owner()
        if not self.locks or status == ScanQueueStatus.LOCKED:
            self.status = status

    def add_lock(self, lock: messages.ScanQueueLock) -> None:
        """Install or replace a named admission hold."""
        self.manager.assert_owner()
        if not self.locks:
            self.restore_status = self.status
        self.locks[lock.identifier] = lock.model_copy(deep=True)
        self.status = ScanQueueStatus.LOCKED

    def remove_lock(self, identifier: str) -> None:
        """Restore admission only after the last hold is removed."""
        self.manager.assert_owner()
        if self.locks.pop(identifier, None) is not None and not self.locks:
            self.status = self.restore_status

    def eligible(self) -> bool:
        """Evaluate the entire admission predicate immediately before dispatch."""
        self.manager.assert_owner()
        if self.closed or self.active or not self.queue:
            return False
        head = self.queue[0]
        if head.preparing or not head.prepared_scans:
            return False
        return self.status == ScanQueueStatus.RUNNING or (
            self.status == ScanQueueStatus.LOCKED
            and all(lock.allow_device_instructions for lock in self.locks.values())
            and not any(request.is_scan for request in head.requests)
        )

    def describe(self, number: int) -> dict:
        """Return independent descriptions; hidden preparation slots preserve ordering."""
        self.manager.assert_owner()
        info = []
        for item in self.queue:
            if not item.requests:
                continue
            description, number = item.describe(number)
            info.append(description)
        return {
            "info": info,
            "status": self.status.name,
            "locks": [lock.model_copy(deep=True) for lock in self.locks.values()],
        }
