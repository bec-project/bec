from __future__ import annotations

import enum
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, TypeAlias

from bec_lib import messages
from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from redis.client import Pipeline

    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger

RateLimitKey: TypeAlias = tuple[str, str]
MessageBuilder: TypeAlias = Callable[[], messages.BECMessage]
TopicMetrics: TypeAlias = dict[str, int]


class _PublishMethod(str, enum.Enum):
    SET = "set"
    SET_AND_PUBLISH = "set_and_publish"
    SEND = "send"
    XADD = "xadd"


@dataclass
class _PendingPublish:
    """A queued publish request retained until the next shared flush."""

    method: _PublishMethod
    topic: str
    msg: MessageBuilder | messages.BECMessage
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class _TopicState:
    pending: deque[_PendingPublish] = field(default_factory=deque)
    dropped_messages: int = 0


class BufferedPublisher:
    """Buffered Redis pipeline writes with immediate first publish per key."""

    def __init__(
        self,
        connector: RedisConnector,
        rate_limit_s: float = 0.1,
        max_buffered_messages: int = 50,
        log_metrics_period_s: float = 30,
    ) -> None:
        """
        Create a shared-cadence buffered publisher.

        Args:
            connector (RedisConnector): Redis connector used for publishing.
            rate_limit_s (float): Shared flush interval in seconds for pending updates.

        """
        self._connector = connector
        self.rate_limit_s = rate_limit_s
        self._max_buffered_messages = max_buffered_messages
        self._lock = threading.Lock()
        self._states: dict[RateLimitKey, _TopicState] = {}
        self._ready: deque[_PendingPublish] = deque()
        self._next_flush_at: float | None = None
        self._evict_next_cycle: set[RateLimitKey] = set()
        self._pending_event = threading.Event()
        self._stop_event = threading.Event()
        self._log_metrics_period_s = log_metrics_period_s
        self._metrics_window_started_at = time.monotonic()
        self._metrics_last_logged_at = self._metrics_window_started_at
        self._metrics_sent_messages = 0
        self._metrics_flushes = 0
        self._metrics_min_batch_size: int | None = None
        self._metrics_max_batch_size = 0
        self._metrics_dropped_messages = 0
        self._metrics_messages_by_topic: TopicMetrics = {}
        self._thread = threading.Thread(
            target=self._dispatch_pending, name="device-event-rate-limiter", daemon=True
        )
        self._thread.start()

    def execute(
        self, method: _PublishMethod | str, topic: str, builder: MessageBuilder, **kwargs: Any
    ) -> None:
        """
        Queue a buffered Redis operation.

        Args:
            method (_PublishMethod | str): Redis operation to perform.
            topic (str): Redis endpoint to update.
            builder (MessageBuilder): Callback that builds the message at flush
                time.
        """
        normalized_method = _PublishMethod(method)
        self._publish_rate_limited(
            (normalized_method.value, topic), normalized_method, topic, builder, kwargs=kwargs
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
        We also evict states that remained empty for a full flush cycle to
        prevent unbounded memory growth.

        Returns:
            list[_PendingPublish]: Ready-to-dispatch requests collected from the
            immediate queue and the shared pending set.
        """
        now = time.monotonic()
        ready: list[_PendingPublish] = []
        with self._lock:
            while self._ready:
                ready.append(self._ready.popleft())
            should_flush_all_pending = self._stop_event.is_set()
            if not should_flush_all_pending and (
                self._next_flush_at is None or self._next_flush_at > now
            ):
                return ready

            keys_to_evict = self._evict_next_cycle
            self._evict_next_cycle = set()
            flushed_keys: set[RateLimitKey] = set()
            for key, state in self._states.items():
                if not state.pending:
                    continue
                while state.pending:
                    ready.append(state.pending.popleft())
                flushed_keys.add(key)
            self._next_flush_at = None
            for key in keys_to_evict:
                if key in self._states and not self._states[key].pending:
                    del self._states[key]
            self._evict_next_cycle = flushed_keys
        return ready

    def _dispatch_request(self, request: _PendingPublish, pipe: Pipeline) -> None:
        """Queue a single request onto the Redis pipeline.

        Args:
            request (_PendingPublish): Pending request to enqueue.
            pipe (Pipeline): Redis pipeline that accumulates the write.

        Raises:
            AttributeError: If the request's method is not recognized.
        """
        msg = request.msg() if callable(request.msg) else request.msg
        getattr(self._connector._managed_connection, request.method.value)(
            request.topic, msg, pipe=pipe, **request.kwargs
        )

    def _flush_requests(self, requests: list[_PendingPublish] | None) -> None:
        """Flush a batch of queued requests through a single Redis pipeline.

        Args:
            requests (list[_PendingPublish] | None): Requests to flush. `None`
                entries are ignored.
        """
        if not requests:
            return

        pipe = self._connector._managed_connection.pipeline()
        for request in requests:
            if request is None:
                continue
            try:
                self._dispatch_request(request, pipe)
            except Exception:
                logger.exception("Failed to build or queue rate-limited device event callback")

        try:
            pipe.execute()
            self._record_flush_metrics(requests)
        except Exception:
            logger.exception("Failed to flush rate-limited device event pipeline")

    def _record_flush_metrics(self, requests: list[_PendingPublish]) -> None:
        batch_size = len(requests)
        if batch_size <= 0:
            return

        topic_counts: TopicMetrics = {}
        for request in requests:
            topic_counts[request.topic] = topic_counts.get(request.topic, 0) + 1

        with self._lock:
            self._metrics_sent_messages += batch_size
            self._metrics_flushes += 1
            for topic, count in topic_counts.items():
                self._metrics_messages_by_topic[topic] = (
                    self._metrics_messages_by_topic.get(topic, 0) + count
                )
            if self._metrics_min_batch_size is None:
                self._metrics_min_batch_size = batch_size
            else:
                self._metrics_min_batch_size = min(self._metrics_min_batch_size, batch_size)
            self._metrics_max_batch_size = max(self._metrics_max_batch_size, batch_size)

            now = time.monotonic()
            if (
                self._log_metrics_period_s <= 0
                or now - self._metrics_last_logged_at < self._log_metrics_period_s
            ):
                return

            elapsed_s = max(now - self._metrics_window_started_at, 1e-9)
            metrics_snapshot = {
                "sent_messages": self._metrics_sent_messages,
                "flushes": self._metrics_flushes,
                "batch_min": self._metrics_min_batch_size or 0,
                "batch_avg": self._metrics_sent_messages / self._metrics_flushes,
                "batch_peak": self._metrics_max_batch_size,
                "avg_message_rate_hz": self._metrics_sent_messages / elapsed_s,
                "dropped_replaced": self._metrics_dropped_messages,
            }
            top_topics = sorted(
                self._metrics_messages_by_topic.items(), key=lambda item: (-item[1], item[0])
            )[:5]
            self._metrics_window_started_at = now
            self._metrics_last_logged_at = now
            self._metrics_sent_messages = 0
            self._metrics_flushes = 0
            self._metrics_min_batch_size = None
            self._metrics_max_batch_size = 0
            self._metrics_dropped_messages = 0
            self._metrics_messages_by_topic = {}

        if metrics_snapshot["avg_message_rate_hz"] > 5:
            # We don't need to log this if the message rate is low
            top_topics_summary = ", ".join(
                f"{topic}={count / elapsed_s:.1f}Hz ({count} msgs)" for topic, count in top_topics
            )
            logger.info(
                f"BufferedPublisher sent {metrics_snapshot['sent_messages']} messages across "
                f"{metrics_snapshot['flushes']} flushes in {elapsed_s:.1f}s "
                f"(batch min/avg/peak={metrics_snapshot['batch_min']}/"
                f"{metrics_snapshot['batch_avg']:.1f}/{metrics_snapshot['batch_peak']}, "
                f"avg msg rate={metrics_snapshot['avg_message_rate_hz']:.1f} Hz, "
                f"dropped/replaced={metrics_snapshot['dropped_replaced']}, "
                f"top topics: {top_topics_summary or 'n/a'})"
            )
        try:
            self._connector.publish_metrics("buffered_publisher", metrics_snapshot, separator="_")
        except Exception:
            logger.exception("Failed to publish buffered publisher metrics")

    def _publish_rate_limited(
        self,
        key: RateLimitKey,
        method: _PublishMethod,
        topic: str,
        msg: MessageBuilder | messages.BECMessage,
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Queue a request for the next shared flush interval.

        Args:
            key (RateLimitKey): Rate-limit bucket key derived from operation type and topic.
            method (_PublishMethod): Redis operation to perform.
            topic (str): Redis endpoint.
            msg (MessageBuilder | messages.BECMessage): Message or callback that builds the message at flush time.
        """
        now = time.monotonic()
        request_kwargs = kwargs or {}
        buffer_latest_only = request_kwargs.pop("buffer_latest_only", False)
        pending_request = _PendingPublish(
            method=method, topic=topic, msg=msg, kwargs=request_kwargs
        )

        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = _TopicState()
                self._states[key] = state
                self._evict_next_cycle.discard(key)
                self._ready.append(pending_request)
            else:
                if buffer_latest_only:
                    if state.pending:
                        dropped = len(state.pending)
                        state.dropped_messages += dropped
                        self._metrics_dropped_messages += dropped
                        state.pending.clear()
                    state.pending.append(pending_request)
                    self._evict_next_cycle.discard(key)
                    if self._next_flush_at is None:
                        self._next_flush_at = now + self.rate_limit_s
                    self._pending_event.set()
                    return

                if len(state.pending) >= self._max_buffered_messages:
                    logger.warning(
                        f"Warning: Dropping message for {key[1]} due to exceeding max buffered messages ({self._max_buffered_messages})"
                    )
                    state.dropped_messages += 1
                    self._metrics_dropped_messages += 1
                    return

                state.pending.append(pending_request)
                self._evict_next_cycle.discard(key)
                if self._next_flush_at is None:
                    self._next_flush_at = now + self.rate_limit_s
            self._pending_event.set()
