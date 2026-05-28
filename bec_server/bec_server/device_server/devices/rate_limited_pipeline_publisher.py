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

if TYPE_CHECKING:  # pragma: no cover
    from redis.client import Pipeline

    from bec_lib.endpoints import EndpointInfo
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger

RateLimitKey: TypeAlias = tuple[str, str]
MessageBuilder: TypeAlias = Callable[[], messages.BECMessage]


class _PublishMethod(str, enum.Enum):
    SET = "set"
    SET_AND_PUBLISH = "set_and_publish"


@dataclass
class _PendingPublish:
    """Only the latest pending message per key is retained until the next shared flush."""

    method: _PublishMethod
    topic: EndpointInfo[Any]
    builder: MessageBuilder


@dataclass
class _TopicState:
    pending: _PendingPublish | None = None
    inserted_in_cooldown: int = 0
    alarm_sent: float | None = None
    recent_insert_counts: deque[int] = field(default_factory=deque)


class RateLimitedPipelinePublisher:
    """Rate-limited Redis pipeline writes with immediate first publish per key."""

    def __init__(
        self,
        connector_getter: Callable[[], RedisConnector],
        rate_limit_s: float = 0.1,
        high_update_rate_warning_hz: float = 500,
        high_update_rate_warning_cycles: int = 3,
        high_update_rate_warning_period_s: float = 30,
    ) -> None:
        """
        Create a shared-cadence coalescing publisher.

        Args:
            connector_getter (Callable[[], RedisConnector]): Callback returning the
                Redis connector used for publishing. We use a getter instead of directly
                taking the connector to avoid issues with connector lifecycle.
            rate_limit_s (float): Shared flush interval in seconds for pending updates.
            high_update_rate_warning_hz (float): Threshold update rate in Hz for
                triggering a warning alarm when updates are being rate-limited. Set to
                0 or below to disable the warning.
            high_update_rate_warning_cycles (int): Number of cooldown windows to include
                in the rolling average used for high-rate warnings. Values greater than 1
                suppress single-window spikes and only warn on sustained high rates.
            high_update_rate_warning_period_s (float): Minimum time in seconds between
                repeated high-rate alarms for the same rate-limited topic.

        """
        self._connector_getter = connector_getter
        self.rate_limit_s = rate_limit_s
        self._high_update_rate_warning_hz = high_update_rate_warning_hz
        self._high_update_rate_warning_cycles = max(1, high_update_rate_warning_cycles)
        self._high_update_rate_warning_period_s = high_update_rate_warning_period_s
        self._lock = threading.Lock()
        self._states: dict[RateLimitKey, _TopicState] = {}
        self._ready: deque[_PendingPublish] = deque()
        self._next_flush_at: float | None = None
        self._evict_next_cycle: set[RateLimitKey] = set()
        self._pending_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._dispatch_pending, name="device-event-rate-limiter", daemon=True
        )
        self._thread.start()

    def publish_set_and_publish(self, topic: EndpointInfo[Any], builder: MessageBuilder) -> None:
        """
        Queue a rate-limited `set_and_publish` operation.

        Args:
            topic (EndpointInfo[Any]): Redis endpoint to update.
            builder (MessageBuilder): Callback that builds the message at flush
                time.
        """
        self._publish_rate_limited(
            (_PublishMethod.SET_AND_PUBLISH.value, topic.endpoint),
            _PublishMethod.SET_AND_PUBLISH,
            topic,
            builder,
        )

    def publish_set(self, topic: EndpointInfo[Any], builder: MessageBuilder) -> None:
        """
        Queue a rate-limited `set` operation.

        Args:
            topic (EndpointInfo[Any]): Redis endpoint to update.
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
        Return the wait time until the next shared flush.

        Returns:
            float | None: Seconds until the next pending flush, or `None` if
            nothing is pending.
        """
        now = time.monotonic()
        with self._lock:
            if self._next_flush_at is None:
                return None
        return max(0.0, self._next_flush_at - now)

    def _collect_due_pending(self) -> list[_PendingPublish]:
        """
        Collect requests that are ready to be flushed.
        We also check if states did not receive updates during the cooldown
        and evict them after the next flush to prevent unbounded memory growth.

        Returns:
            list[_PendingPublish]: Ready-to-dispatch requests collected from the
            immediate queue and the shared pending set.
        """
        now = time.monotonic()
        ready: list[_PendingPublish] = []
        with self._lock:
            while self._ready:
                ready.append(self._ready.popleft())
            if self._next_flush_at is None or self._next_flush_at > now:
                return ready

            keys_to_evict = self._evict_next_cycle
            self._evict_next_cycle = set()
            flushed_keys: set[RateLimitKey] = set()
            for key, state in self._states.items():
                if state.pending is None:
                    continue
                ready.append(state.pending)
                state.pending = None
                self._start_cooldown_window_locked(key, state)
                flushed_keys.add(key)
            self._next_flush_at = None
            for key in keys_to_evict:
                if key in self._states and self._states[key].pending is None:
                    del self._states[key]
            self._evict_next_cycle = flushed_keys
        return ready

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

    def _start_cooldown_window_locked(self, key: RateLimitKey, state: _TopicState) -> None:
        """Reset per-key counters for a new cooldown window and warn on sustained high rates."""
        state.recent_insert_counts.append(state.inserted_in_cooldown)
        state.inserted_in_cooldown = 0
        if self.rate_limit_s <= 0 or self._high_update_rate_warning_hz <= 0:
            return

        rolling_counts = list(state.recent_insert_counts)
        if len(rolling_counts) < self._high_update_rate_warning_cycles:
            return

        inserted_rate_hz = sum(rolling_counts) / (len(rolling_counts) * self.rate_limit_s)
        if inserted_rate_hz <= self._high_update_rate_warning_hz:
            return

        now = time.monotonic()
        if (
            state.alarm_sent is not None
            and now - state.alarm_sent < self._high_update_rate_warning_period_s
        ):
            return

        state.alarm_sent = now

        self._connector_getter().raise_alarm(
            severity=Alarms.WARNING,
            info=messages.ErrorInfo(
                error_message=(
                    f"Rate-limited topic {key[1]} averaged {sum(rolling_counts) / len(rolling_counts):.1f} "
                    f"updates per {self.rate_limit_s:.3f}s cooldown window across the last "
                    f"{len(rolling_counts)} windows (~{inserted_rate_hz:.1f} Hz)."
                ),
                compact_error_message=(
                    f"High update rate on {key[1]}: rolling average ~{inserted_rate_hz:.1f} Hz exceeds "
                    f"{self._high_update_rate_warning_hz:.1f} Hz."
                ),
                exception_type="Warning",
                device=key[1],
            ),
        )

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
        topic: EndpointInfo[Any],
        builder: MessageBuilder,
    ) -> None:
        """Queue a request, collapsing updates received during the cooldown.

        Args:
            key (RateLimitKey): Rate-limit bucket key derived from operation type and topic.
            method (_PublishMethod): Redis operation to perform.
            topic (EndpointInfo[Any]): Redis endpoint.
            builder (MessageBuilder): Callback that builds the message at flush time.
        """
        now = time.monotonic()

        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = _TopicState(
                    recent_insert_counts=deque(maxlen=self._high_update_rate_warning_cycles)
                )
                self._states[key] = state
                state.inserted_in_cooldown += 1
                state.pending = None
                self._evict_next_cycle.discard(key)
                self._ready.append(_PendingPublish(method=method, topic=topic, builder=builder))
            else:
                state.inserted_in_cooldown += 1
                state.pending = _PendingPublish(method=method, topic=topic, builder=builder)
                self._evict_next_cycle.discard(key)
                if self._next_flush_at is None:
                    self._next_flush_at = now + self.rate_limit_s
            self._pending_event.set()
