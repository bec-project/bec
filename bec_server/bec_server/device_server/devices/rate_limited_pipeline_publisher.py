from __future__ import annotations

import enum
import heapq
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, TypeAlias

from bec_lib import messages
from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from redis.client import Pipeline

    from bec_lib.endpoints import MessageEndpoints
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger

RateLimitKey: TypeAlias = tuple[str, str]
MessageBuilder: TypeAlias = Callable[[], messages.BECMessage]


class _PublishMethod(str, enum.Enum):
    SET = "set"
    SET_AND_PUBLISH = "set_and_publish"


@dataclass
class _PendingPublish:
    """Only the latest pending message per key is retained during the cooldown window."""

    method: _PublishMethod
    topic: MessageEndpoints
    builder: MessageBuilder
    due_at: float


@dataclass
class _TopicState:
    last_sent_at: float = 0.0
    pending: _PendingPublish | None = None
    idle_expires_at: float | None = None


class RateLimitedPipelinePublisher:
    """Rate-limit Redis pipeline writes on a per-topic basis.

    The publisher sends the first update for a topic immediately and collapses
    subsequent updates during the cooldown window into the latest pending value.

    Note that the message construction is deferred until flush time, so the
    provided builder callbacks must be able to execute at an arbitrary
    later time and should not capture any state that may change between
    publish and flush.

    Args:
        connector_getter (Callable[[], RedisConnector]): Callback returning the
            Redis connector used for publishing.
        rate_limit_s (float): Minimum interval in seconds between publishes for
            the same topic and operation type.
    """

    def __init__(
        self, connector_getter: Callable[[], RedisConnector], rate_limit_s: float = 0.1
    ) -> None:
        self._connector_getter = connector_getter
        self.rate_limit_s = rate_limit_s
        self._lock = threading.Lock()
        self._states: dict[RateLimitKey, _TopicState] = {}
        self._ready: deque[_PendingPublish] = deque()
        self._pending_heap: list[tuple[float, RateLimitKey]] = []
        self._idle_heap: list[tuple[float, RateLimitKey]] = []
        self._pending_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._dispatch_pending, name="device-event-rate-limiter", daemon=True
        )
        self._thread.start()

    def publish_set_and_publish(self, topic: MessageEndpoints, builder: MessageBuilder) -> None:
        """Queue a rate-limited `set_and_publish` operation.

        Args:
            topic (MessageEndpoints): Redis endpoint to update.
            builder (MessageBuilder): Callback that builds the message at flush
                time.
        """
        self._publish_rate_limited(
            (_PublishMethod.SET_AND_PUBLISH.value, topic.endpoint),
            _PublishMethod.SET_AND_PUBLISH,
            topic,
            builder,
        )

    def publish_set(self, topic: MessageEndpoints, builder: MessageBuilder) -> None:
        """Queue a rate-limited `set` operation.

        Args:
            topic (MessageEndpoints): Redis endpoint to update.
            builder (MessageBuilder): Callback that builds the message at flush
                time.
        """
        self._publish_rate_limited(
            (_PublishMethod.SET.value, topic.endpoint), _PublishMethod.SET, topic, builder
        )

    def shutdown(self) -> None:
        """Stop the worker thread and flush any queued requests."""
        self._stop_event.set()
        self._pending_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1)
        self._flush_all_pending()

    ###################################################################################
    ################# Internal helper methods #########################################
    ###################################################################################

    def _dispatch_pending(self) -> None:
        """
        Run the worker loop that flushes pending requests when they become due.
        The loop is executed in a separate thread.
        """
        while True:
            pending_requests = self._collect_due_pending()
            if pending_requests:
                self._flush_requests(pending_requests)
                continue

            if self._stop_event.is_set():
                return

            timeout = self._next_timeout()
            self._pending_event.wait(timeout=timeout)
            self._pending_event.clear()

    def _next_timeout(self) -> float | None:
        """
        Return the wait time until the next pending request becomes due.

        Returns:
            float | None: Seconds until the next pending request, or `None` if
            nothing is pending.
        """
        now = time.monotonic()
        with self._lock:
            self._prune_idle_states_locked(now)
            next_due_at = self._peek_next_due_at_locked()
            next_idle_expiry = self._peek_next_idle_expiry_locked()
            next_wakeup = (
                min(due_at for due_at in (next_due_at, next_idle_expiry) if due_at is not None)
                if next_due_at is not None or next_idle_expiry is not None
                else None
            )
            if next_wakeup is None:
                return None
        return max(0.0, next_wakeup - now)

    def _collect_due_pending(self) -> list[_PendingPublish]:
        """
        Collect requests that are ready to be flushed.

        Returns:
            list[_PendingPublish]: Ready-to-dispatch requests collected from the
            immediate queue and any expired cooldown entries.
        """
        now = time.monotonic()
        ready: list[_PendingPublish] = []
        with self._lock:
            self._prune_idle_states_locked(now)
            while self._ready:
                ready.append(self._ready.popleft())
            while self._pending_heap:
                due_at, key = self._pending_heap[0]
                if due_at > now:
                    break
                heapq.heappop(self._pending_heap)
                state = self._states.get(key)
                if state is None or state.pending is None or state.pending.due_at != due_at:
                    continue
                ready.append(state.pending)
                state.pending = None
                # Rate limiting is based on dispatch bookkeeping time, not exact Redis execute time.
                state.last_sent_at = now
                self._mark_state_idle_locked(key, state, now + self.rate_limit_s)
        return ready

    def _flush_all_pending(self) -> None:
        """Flush all queued requests, including entries still in cooldown."""
        with self._lock:
            pending_requests = list(self._ready) + [
                state.pending for state in self._states.values() if state.pending is not None
            ]
            self._ready.clear()
            self._pending_heap.clear()
            self._idle_heap.clear()
            for state in self._states.values():
                state.pending = None
                state.idle_expires_at = None
        self._flush_requests(pending_requests)

    def _peek_next_due_at_locked(self) -> float | None:
        """Return the next valid due time while holding the internal lock."""
        while self._pending_heap:
            due_at, key = self._pending_heap[0]
            state = self._states.get(key)
            if state is not None and state.pending is not None and state.pending.due_at == due_at:
                return due_at
            heapq.heappop(self._pending_heap)
        return None

    def _peek_next_idle_expiry_locked(self) -> float | None:
        """Return the next valid idle-expiry time while holding the internal lock."""
        while self._idle_heap:
            idle_expires_at, key = self._idle_heap[0]
            state = self._states.get(key)
            if (
                state is not None
                and state.pending is None
                and state.idle_expires_at == idle_expires_at
            ):
                return idle_expires_at
            heapq.heappop(self._idle_heap)
        return None

    def _mark_state_idle_locked(
        self, key: RateLimitKey, state: _TopicState, idle_expires_at: float
    ) -> None:
        """Mark a state as idle and schedule it for lazy eviction."""
        state.idle_expires_at = idle_expires_at
        heapq.heappush(self._idle_heap, (idle_expires_at, key))

    def _prune_idle_states_locked(self, now: float) -> None:
        """
        Remove states whose cooldown has expired and that no longer have pending work.

        Args:
            now (float): Current time in seconds since an arbitrary point, as returned by `time.monotonic()`.
        """
        while self._idle_heap:
            idle_expires_at, key = self._idle_heap[0]
            if idle_expires_at > now:
                return
            heapq.heappop(self._idle_heap)
            state = self._states.get(key)
            if (
                state is None
                or state.pending is not None
                or state.idle_expires_at != idle_expires_at
            ):
                continue
            del self._states[key]

    def _dispatch_request(
        self, connector: RedisConnector, pipe: Pipeline, request: _PendingPublish
    ) -> None:
        """Queue a single request onto the Redis pipeline.

        Args:
            connector (RedisConnector): Redis connector used for publishing.
            pipe (Pipeline): Redis pipeline that accumulates the write.
            request (_PendingPublish): Pending request to enqueue.

        Raises:
            ValueError: Raised if the publish method is unsupported.
        """
        msg = request.builder()
        if request.method is _PublishMethod.SET_AND_PUBLISH:
            connector.set_and_publish(request.topic, msg, pipe=pipe)
            return
        if request.method is _PublishMethod.SET:
            connector.set(request.topic, msg, pipe=pipe)
            return
        raise ValueError(f"Unsupported rate-limited publish method {request.method}")

    def _flush_requests(self, requests: list[_PendingPublish | None]) -> None:
        """Flush a batch of queued requests through a single Redis pipeline.

        Args:
            requests (list[_PendingPublish | None]): Requests to flush. `None`
                entries are ignored.
        """
        if not requests:
            return

        connector = self._connector_getter()
        pipe = connector.pipeline()
        for request in requests:
            if request is None:
                continue
            try:
                self._dispatch_request(connector, pipe, request)
            except Exception:
                logger.exception("Failed to build or queue rate-limited device event callback")

        try:
            pipe.execute()
        except Exception:
            logger.exception("Failed to flush rate-limited device event pipeline")

    def _publish_rate_limited(
        self,
        key: RateLimitKey,
        method: _PublishMethod,
        topic: MessageEndpoints,
        builder: MessageBuilder,
    ) -> None:
        """Queue a request, collapsing updates received during the cooldown.

        Args:
            key (RateLimitKey): Rate-limit bucket key derived from operation type
                and topic.
            method (_PublishMethod): Redis operation to perform.
            topic (MessageEndpoints): Redis endpoint to update.
            builder (MessageBuilder): Callback that builds the message at flush
                time.
        """
        now = time.monotonic()

        with self._lock:
            self._prune_idle_states_locked(now)
            state = self._states.setdefault(key, _TopicState())
            if now >= state.last_sent_at + self.rate_limit_s:
                # Immediate publishes are queued separately so they cannot be overwritten
                # by newer updates before the worker drains them.
                state.last_sent_at = now
                state.pending = None
                self._mark_state_idle_locked(key, state, now + self.rate_limit_s)
                self._ready.append(
                    _PendingPublish(method=method, topic=topic, builder=builder, due_at=now)
                )
            else:

                # Check if there is already a pending update
                should_schedule = state.pending is None

                pending = _PendingPublish(
                    method=method,
                    topic=topic,
                    builder=builder,
                    due_at=state.last_sent_at + self.rate_limit_s,
                )
                state.pending = pending
                state.idle_expires_at = None

                # If there has not yet been a pending update, schedule it
                if should_schedule:
                    heapq.heappush(self._pending_heap, (pending.due_at, key))
            self._pending_event.set()
