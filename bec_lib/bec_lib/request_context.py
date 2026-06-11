from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass


@dataclass
class ActiveRequestContext:
    request_id: str
    queue_status: str | None = None


active_request_context: ContextVar[ActiveRequestContext | None] = ContextVar(
    "active_request_context", default=None
)
