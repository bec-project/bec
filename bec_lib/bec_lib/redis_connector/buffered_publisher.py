from __future__ import annotations

import enum
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, TypeAlias

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.logger import bec_logger

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from redis.client import Pipeline

    from .managed_redis_connection import ManagedRedisConnection


TopicKey: TypeAlias = tuple[str, str, str]
PayloadBuilder: TypeAlias = Callable[[], Any]

if TYPE_CHECKING:  # pragma: no cover
    DispatchFn: TypeAlias = Callable[..., None]


class _BufferMode(str, enum.Enum):
    ALL = "all"
    LATEST_ONLY = "latest_only"


@dataclass
class _PendingPublish:
    operation: str
    topic: str
    builder: PayloadBuilder
    dispatch: DispatchFn
    dispatch_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class _TopicState:
    pending: deque[_PendingPublish] = field(default_factory=deque)
    dropped_messages: int = 0
    alarm_sent: float | None = None


class RateLimitedPipelinePublisher:
    """Buffered Redis writes with a shared flush cadence and per-topic queues."""

    def __init__(
        self,
        connector_getter: Callable[[], ManagedRedisConnection],
        rate_limit_s: float = 0.1,
        max_buffer_size: int = 100,
        overflow_warning_period_s: float = 30,
    ) -> None:
        if max_buffer_size <= 0:
            raise ValueError("max_buffer_size must be greater than 0")
        self._connector_getter = connector_getter
        self.rate_limit_s = rate_limit_s
        self.max_buffer_size = max_buffer_size
        self._overflow_warning_period_s = overflow_warning_period_s
        self._lock = threading.Lock()
        self._states: dict[TopicKey, _TopicState] = {}
        self._next_flush_at: float | None = None
        self._pending_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._dispatch_pending, name="rate-limited-publisher", daemon=True
        )
        self._thread.start()

    def execute(
        self,
        method: str,
        topic: str,
        builder: PayloadBuilder,
        dispatch: DispatchFn,
        dispatch_kwargs: dict[str, Any] | None = None,
        latest_only: bool = False,
    ) -> None:
        self._publish_buffered(
            (
                method,
                topic,
                _BufferMode.LATEST_ONLY.value if latest_only else _BufferMode.ALL.value,
            ),
            _PendingPublish(
                operation=method,
                topic=topic,
                builder=builder,
                dispatch=dispatch,
                dispatch_kwargs=dispatch_kwargs or {},
            ),
            latest_only=latest_only,
        )

    def shutdown(self) -> None:
        self._stop_event.set()
        self._pending_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1)
        pending_requests = self._collect_due_pending(force=True)
        if pending_requests:
            self._flush_requests(pending_requests)

    def _dispatch_pending(self) -> None:
        while True:
            pending_requests = self._collect_due_pending(force=self._stop_event.is_set())
            if pending_requests:
                self._flush_requests(pending_requests)
                continue

            if self._stop_event.is_set():
                return

            timeout = self._next_timeout()
            self._pending_event.wait(timeout=timeout)
            self._pending_event.clear()

    def _next_timeout(self) -> float | None:
        now = time.monotonic()
        with self._lock:
            if self._next_flush_at is None:
                return None
        return max(0.0, self._next_flush_at - now)

    def _collect_due_pending(self, force: bool = False) -> list[_PendingPublish]:
        now = time.monotonic()
        ready: list[_PendingPublish] = []
        with self._lock:
            if not force and (self._next_flush_at is None or self._next_flush_at > now):
                return ready

            empty_keys = []
            for key, state in self._states.items():
                while state.pending:
                    ready.append(state.pending.popleft())
                if not state.pending:
                    empty_keys.append(key)
            for key in empty_keys:
                del self._states[key]
            self._next_flush_at = None
        return ready

    def _dispatch_request(
        self, connector: ManagedRedisConnection, pipe: Pipeline, request: _PendingPublish
    ) -> None:
        payload = request.builder()
        request.dispatch(connector, pipe, request.topic, payload, **request.dispatch_kwargs)

    def _maybe_raise_overflow_warning(self, key: TopicKey, state: _TopicState) -> None:
        now = time.monotonic()
        if (
            state.alarm_sent is not None
            and now - state.alarm_sent < self._overflow_warning_period_s
        ):
            return
        state.alarm_sent = now
        self._connector_getter().raise_alarm(
            severity=Alarms.WARNING,
            info=messages.ErrorInfo(
                error_message=(
                    f"Buffered topic {key[1]} exceeded the maximum queue size of "
                    f"{self.max_buffer_size}. Dropped {state.dropped_messages} queued update(s)."
                ),
                compact_error_message=(
                    f"Buffered topic {key[1]} exceeded queue size {self.max_buffer_size}."
                ),
                exception_type="Warning",
                device=key[1],
            ),
        )

    def _flush_requests(self, requests: list[_PendingPublish]) -> None:
        if not requests:
            return

        connector = self._connector_getter()
        pipe = connector.pipeline()
        for request in requests:
            try:
                self._dispatch_request(connector, pipe, request)
            except Exception:
                logger.exception("Failed to queue buffered publish request")

        try:
            pipe.execute()
        except Exception:
            logger.exception("Failed to flush buffered Redis pipeline")

    def _publish_buffered(
        self, key: TopicKey, request: _PendingPublish, latest_only: bool = False
    ) -> None:
        now = time.monotonic()
        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = _TopicState()
                self._states[key] = state
            if latest_only:
                state.pending.clear()
            state.pending.append(request)
            while len(state.pending) > self.max_buffer_size:
                state.pending.popleft()
                state.dropped_messages += 1
            if state.dropped_messages:
                self._maybe_raise_overflow_warning(key, state)
            if self._next_flush_at is None:
                self._next_flush_at = now + self.rate_limit_s
            self._pending_event.set()
