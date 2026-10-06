"""Shared queue states, targets, and detached status snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, TypeAlias

from bec_lib import messages
from bec_lib.serialization import msgpack


@dataclass(frozen=True)
class QueueSnapshot:
    """Immutable serialized queue statuses, detached from live queue and scan objects."""

    queues: tuple[tuple[str, bytes], ...] = ()

    def to_messages(self) -> dict[str, messages.ScanQueueStatus]:
        """Return independent message models suitable for existing consumers.

        Returns:
            dict[str, messages.ScanQueueStatus]: Independent status messages indexed by queue name.
        """
        return {
            name: messages.ScanQueueStatus.model_validate(msgpack.loads(payload))
            for name, payload in self.queues
        }


ExitInfoType: TypeAlias = tuple[
    Literal["halted", "aborted", "user_completed"], Literal["user", "alarm"]
]
ScanTarget: TypeAlias = str | list[str | None] | None
QueueParameter: TypeAlias = dict[str, Any] | None


class InstructionQueueStatus(Enum):
    """Lifecycle states of a queued scan request."""

    STOPPED = -1
    PENDING = 0
    IDLE = 1
    PAUSED = 2
    DEFERRED_PAUSE = 3
    RUNNING = 4
    COMPLETED = 5
    CANCELLED = 6


class ScanQueueStatus(Enum):
    """Dispatch states of a named scan queue."""

    PAUSED = 0
    RUNNING = 1
    LOCKED = 2
