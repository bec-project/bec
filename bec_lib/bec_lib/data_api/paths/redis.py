"""
Redis data path: everything BEC publishes through Redis.

One path serves all three scenarios; the scenario only selects the recipe:

* ``OPEN_SCAN``: monitored data from scan-segment events via the scan item's
  point_id-keyed live data, async data from the per-scan
  ``device_async_signal`` streams (replayed ``from_start``; a later request of the
  same stream catches up from the messages the feed already delivered).
* ``CLOSED_SCAN``: the writer-latency window only — the scan is done but its
  file is not published yet, so monitored data is served once from the scan
  item's in-memory live data. As soon as the history entry exists this path
  declines and the file path serves the scan.
* ``NO_SCAN``: scan-less device streams (``device_readback`` and
  ``device_read_configuration`` pubsub, ``device_preview``) as unindexed points.

The path owns the Redis feeds and their lifetimes (refcounted per topic).
Turning messages into ordinals is the shared :mod:`..decoding` package and
classifying sources its :mod:`..decoding.signal_info` module. All mutation
happens under the DataAPI-wide reentrant lock.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
from numbers import Real
from typing import TYPE_CHECKING, Any, Callable

import numpy as np

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import DeviceMessage

from .. import client_views
from ..data_path import BindContext, DataPath, DataSource, SourceRequest, SourceSpec
from ..decoding import (
    ASYNC_UPDATE_HANDLERS,
    decode_async_update,
    decode_live_data,
    decode_scan_point,
    decode_stream_message,
    decode_stream_points,
    device_stream_specs,
    open_scan_specs,
    parse_async_update,
    scan_memory_specs,
)
from ..locking import OwnedLock
from ..models import DeviceStream, SourceKey, SourceKind, UpdateReason
from ..scenarios import TERMINAL_SCAN_STATES, Scenario

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

logger = bec_logger.logger

#: Messages an async feed keeps for late requests while every request on it has a
#: retention cap (the device server keeps about as many in the stream).
_FEED_LOG_CAP = 1000

#: The streams whose last message Redis keeps: they have a current value, and a history.
_VALUE_STREAMS = frozenset({DeviceStream.READBACK, DeviceStream.CONFIGURATION})


def _same_message(seed: DeviceMessage, msg: Any) -> bool:
    """Whether a feed delivery is the message that was already seeded from the cache."""
    if not isinstance(msg, DeviceMessage):
        return False
    try:
        # Not ``seed == msg``: message equality also compares the metadata with ``==``,
        # which raises for array-valued entries.
        np.testing.assert_equal(seed.model_dump(), msg.model_dump())
    except AssertionError:
        return False
    return True


def _predates_seed(seed: DeviceMessage, msg: Any, entry: str) -> bool:
    """Whether a queued reading was acquired before the cached initial sample."""
    if not isinstance(msg, DeviceMessage):
        return False
    seed_timestamp = seed.signals.get(entry, {}).get("timestamp")
    timestamp = msg.signals.get(entry, {}).get("timestamp")
    return (
        isinstance(seed_timestamp, Real)
        and isinstance(timestamp, Real)
        and timestamp < seed_timestamp
    )


@dataclass
class _Feed:
    """One Redis registration, shared by the requests that read it."""

    endpoint: Any
    callback: Callable
    #: Requests holding the feed; the registration is dropped with the last one.
    refcount: int = 1
    #: Messages delivered through this feed so far. The ``from_start`` replay of an
    #: async stream happens once, on first registration.
    delivered: int = 0
    #: The delivered messages, kept in memory so that a later request catches up.
    log: collections.deque = field(default_factory=collections.deque)
    #: Whether messages are missing before the start of ``log``: the stream was already
    #: trimmed when the feed attached, or the log itself was capped.
    trimmed: bool = False


@dataclass(frozen=True)
class _Recipe:
    """How one scenario is served: its resolve / open / close steps."""

    resolve: Callable[[list[SourceKey], BindContext], list[SourceSpec] | None]
    open: Callable[[SourceRequest], None]
    close: Callable[[SourceRequest], None]


class RedisDataPath(DataPath):
    """Serves open scans, the writer-latency window and device streams from Redis."""

    data_source = DataSource.REDIS
    scenarios = frozenset({Scenario.OPEN_SCAN, Scenario.CLOSED_SCAN, Scenario.NO_SCAN})
    priority = 10

    #: Reconstruction strategy per ``DeviceAsyncUpdate.type``; subclasses may override
    #: handlers (a new wire type also needs the message schema, the writer and
    #: ``models.py``). Each handler returns the number of points inserted.
    async_update_handlers = ASYNC_UPDATE_HANDLERS

    def __init__(self, client: BECClient, lock: OwnedLock | None = None):
        super().__init__(client, lock)
        self._callback_ids: list[int | str] = []
        # OPEN_SCAN: scan_id -> open requests (fed by segments and async streams)
        self._scan_requests: dict[str, list[SourceRequest]] = {}
        # NO_SCAN: open device-stream requests
        self._stream_requests: list[SourceRequest] = []
        # Keys are ("async", scan_id, device, signal) or ("stream", stream, device, entry-or-"").
        self._feeds: dict[tuple, _Feed] = {}
        # (device, entry) -> async signal info (positive hits only)
        self._async_info_cache: dict[SourceKey, dict] = {}
        self._recipes: dict[Scenario, _Recipe] = {
            Scenario.OPEN_SCAN: _Recipe(
                self._resolve_open_scan, self._open_open_scan, self._close_open_scan
            ),
            Scenario.CLOSED_SCAN: _Recipe(
                self._resolve_closed_scan, self._open_closed_scan, self._close_closed_scan
            ),
            Scenario.NO_SCAN: _Recipe(
                self._resolve_no_scan, self._open_no_scan, self._close_no_scan
            ),
        }

    # --- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        self._callback_ids = [
            self.client.callbacks.register("scan_segment", self._on_scan_segment),
            self.client.callbacks.register("device_update", self._on_device_update),
        ]

    def _on_device_update(self, *_args, **_kwargs) -> None:
        """Invalidate cached device metadata on device-config changes."""
        with self._lock:
            self._async_info_cache.clear()

    def disconnect(self) -> None:
        for callback_id in self._callback_ids:
            self.client.callbacks.remove(callback_id)
        self._callback_ids = []
        with self._lock:
            for feed in self._feeds.values():
                self.client.connector.unregister(topics=feed.endpoint, cb=feed.callback)
            self._feeds.clear()
            self._scan_requests.clear()
            self._stream_requests.clear()
            self._async_info_cache.clear()

    # --- contract: dispatch on the scenario ----------------------------------

    def resolve(self, sources: list[SourceKey], context: BindContext) -> list[SourceSpec] | None:
        recipe = self._recipes.get(context.scenario)
        return None if recipe is None else recipe.resolve(sources, context)

    def _open(self, request: SourceRequest) -> None:
        self._recipes[request.context.scenario].open(request)

    def close(self, request: SourceRequest) -> None:
        self._recipes[request.context.scenario].close(request)

    # --- shared helpers ------------------------------------------------------

    def _scan_item(self, scan_id: str):
        return client_views.scan_item(self.client, scan_id)

    def _history_message(self, scan_id: str):
        return client_views.history_message(self.client, scan_id)

    def _ensure_feed(self, key: tuple, endpoint, callback, *, from_start: bool = False) -> bool:
        """
        Attach (or share) the feed for ``key``. Lock held.

        Returns:
            bool: True when the feed already existed — a ``from_start``
                replay already happened, so the caller must catch its
                request up by other means.
        """
        feed = self._feeds.get(key)
        if feed is not None:
            feed.refcount += 1
            return True
        if from_start:
            self.client.connector.register(endpoint, cb=callback, from_start=True)
        else:
            self.client.connector.register(endpoint, cb=callback)
        self._feeds[key] = _Feed(endpoint=endpoint, callback=callback)
        return False

    def _release_feed(self, key: tuple) -> None:
        feed = self._feeds.get(key)
        if feed is None:
            return
        feed.refcount -= 1
        if feed.refcount <= 0:
            self.client.connector.unregister(topics=feed.endpoint, cb=feed.callback)
            del self._feeds[key]

    # =========================================================================
    # OPEN_SCAN: scan segments + async signal streams
    # =========================================================================

    def _resolve_open_scan(
        self, sources: list[SourceKey], context: BindContext
    ) -> list[SourceSpec] | None:
        # Scenario routing already established this scan as OPEN_SCAN; only
        # guard against the item having vanished from storage since.
        scan_item = context.resolve_scan_item(self._scan_item)
        if scan_item is None or scan_item.status_message is None:
            return None
        return open_scan_specs(
            self.client, sources, scan_item.status_message.readout_priority, self._async_info_cache
        )

    def _open_open_scan(self, request: SourceRequest) -> None:
        scan_item = request.context.resolve_scan_item(self._scan_item)
        with self._lock:
            # Feeds acquired by this request are recorded on it, so close()
            # releases exactly those — never a feed another request holds —
            # and a request whose open failed half-way is rolled back.
            feeds: list[tuple] = request.state.setdefault("feeds", [])
            self._scan_requests.setdefault(request.scan_id, []).append(request)
            try:
                for spec in request.specs:
                    if spec.kind != SourceKind.ASYNC or not spec.available:
                        continue
                    key = self._async_feed_key(request.scan_id, spec)
                    if key in feeds:
                        # Sub-signals of one AsyncMultiSignal share a stream: one feed
                        # and one catch-up (it ingests every spec of the request).
                        continue
                    replayed = self._ensure_async_feed(request.scan_id, spec)
                    feeds.append(key)
                    if replayed:
                        # The stream was already replayed for an earlier
                        # request; replay it for this one or its backfill
                        # would be empty.
                        self._catch_up_async(request, spec)
                if scan_item is not None:
                    self._feed_monitored(scan_item, [request])
            except Exception:
                self._close_open_scan(request)
                raise
        request.notify(UpdateReason.BACKFILL)

    def _close_open_scan(self, request: SourceRequest) -> None:
        with self._lock:
            requests = self._scan_requests.get(request.scan_id, [])
            if request in requests:
                requests.remove(request)
            if not requests:
                self._scan_requests.pop(request.scan_id, None)
            for key in request.state.pop("feeds", []):
                self._release_feed(key)

    @staticmethod
    def _async_feed_key(scan_id: str, spec: SourceSpec) -> tuple:
        return ("async", scan_id, spec.device, spec.storage_name or spec.entry)

    def _ensure_async_feed(self, scan_id: str, spec: SourceSpec) -> bool:
        key = self._async_feed_key(scan_id, spec)
        endpoint = MessageEndpoints.device_async_signal(
            scan_id=scan_id, device=spec.device, signal=spec.storage_name or spec.entry
        )

        def connector_callback(msg, *, _scan_id=scan_id, _device=spec.device, _key=key):
            self._on_async_message(msg, _scan_id, _device, feed_key=_key)

        return self._ensure_feed(key, endpoint, connector_callback, from_start=True)

    def _on_scan_segment(self, content: dict, metadata: dict) -> None:
        content = content or {}
        scan_id = content.get("scan_id") or (metadata or {}).get("scan_id")
        if scan_id is None:
            return
        point_id, data = content.get("point_id"), content.get("data")
        with self._lock:
            requests = list(self._scan_requests.get(scan_id, []))
            if not requests:
                return
            if isinstance(point_id, int) and isinstance(data, dict):
                # Only the segment's own point: never re-walk the whole scan per segment.
                touched = [
                    r for r in requests if decode_scan_point(r.bundle, r.specs, point_id, data)
                ]
            else:
                scan_item = self._scan_item(scan_id)
                if scan_item is None:
                    return
                touched = self._feed_monitored(scan_item, requests)
        for request in touched:
            request.notify(UpdateReason.LIVE)

    def _feed_monitored(self, scan_item, requests: list[SourceRequest]) -> list[SourceRequest]:
        """Insert new live-data points into the requests; return those that changed."""
        return [r for r in requests if decode_live_data(r.bundle, r.specs, scan_item.live_data)]

    def _catch_up_async(self, request: SourceRequest, spec: SourceSpec) -> None:
        """
        Replay what the shared feed already delivered into one request. Lock held.

        The feed keeps its delivered messages in memory, so the replay needs no Redis
        read and does not depend on what the (trimmed) stream still holds. A message
        that is in the stream but not yet dispatched is not part of the log: it arrives
        through the feed callback, exactly once.
        """
        feed = self._feeds.get(self._async_feed_key(request.scan_id, spec))
        if feed is None:
            return
        if feed.trimmed:
            request.set_status(spec.key, stream_trimmed=True)
        for msg_obj in list(feed.log):
            self._ingest_message([request], msg_obj, spec.device)

    def _log_async_message(self, feed: _Feed, feed_key: tuple, msg_obj: DeviceMessage) -> None:
        """Keep a delivered message for late requests of the same feed. Lock held."""
        holders = [
            request
            for request in self._scan_requests.get(feed_key[1], [])
            if feed_key in request.state.get("feeds", ())
        ]
        log = feed.log
        if feed.delivered == 1 and (self._stream_index(msg_obj, feed_key[3]) or 0) > 0:
            # The first message we see is not the first of the scan: the stream had been
            # trimmed before this feed attached.
            feed.trimmed = True
            for request in holders:
                for spec in request.specs:
                    if spec.available and self._async_feed_key(request.scan_id, spec) == feed_key:
                        request.set_status(spec.key, stream_trimmed=True)
        log.append(msg_obj)
        # Requests without a cap hold the data anyway (the log only adds references);
        # when all are capped, keep a bounded tail so the log cannot outgrow them.
        caps = [request.bundle.max_points for request in holders]
        if caps and all(cap is not None for cap in caps):
            limit = max(_FEED_LOG_CAP, *caps)
            while len(log) > limit:
                log.popleft()
                feed.trimmed = True

    @staticmethod
    def _stream_index(msg_obj: DeviceMessage, signal: str) -> int | None:
        """The per-scan message index the device server stamps on async messages."""
        indices = msg_obj.metadata.get("async_indices") or {}
        index = indices.get(signal)
        if isinstance(index, int):
            return index
        values = [value for value in indices.values() if isinstance(value, int)]
        return max(values) if values else None

    def _on_async_message(
        self, msg: dict, scan_id: str, device: str, feed_key: tuple | None = None
    ) -> None:
        msg_obj = msg.get("data")
        with self._lock:
            # Count the delivery in the same critical section as its ingestion:
            # a request opened in between would otherwise replay this message
            # from the stream AND receive it from the feed (double add_slice).
            feed = self._feeds.get(feed_key) if feed_key is not None else None
            if feed is not None:
                feed.delivered += 1
                if isinstance(msg_obj, DeviceMessage):
                    self._log_async_message(feed, feed_key, msg_obj)
            if not isinstance(msg_obj, DeviceMessage):
                return
            requests = list(self._scan_requests.get(scan_id, []))
            if not requests:
                return
            touched = self._ingest_message(requests, msg_obj, device)
        for request in touched:
            request.notify(UpdateReason.LIVE)

    def _ingest_message(
        self, requests: list[SourceRequest], msg_obj: DeviceMessage, device: str
    ) -> list[SourceRequest]:
        """Decode one stream message into the requests; return those that changed. Lock held."""
        update = parse_async_update(msg_obj)  # validated once per message, not per request
        if update is None:
            return []
        touched = []
        for request in requests:
            inserted = decode_async_update(
                request.bundle,
                request.state,
                request.specs,
                msg_obj,
                update,
                device=device,
                handlers=self.async_update_handlers,
            )
            if inserted:
                touched.append(request)
        return touched

    # =========================================================================
    # CLOSED_SCAN: writer-latency window, served once from scan memory
    # =========================================================================

    def _terminal_scan_item(self, context: BindContext):
        scan_item = context.resolve_scan_item(self._scan_item)
        if scan_item is None or scan_item.status not in TERMINAL_SCAN_STATES:
            return None
        return scan_item

    def _resolve_closed_scan(
        self, sources: list[SourceKey], context: BindContext
    ) -> list[SourceSpec] | None:
        if self._history_message(context.scan_id) is not None:
            # The file is published: it is authoritative (complete async data,
            # survives scan-storage trimming) — leave the scan to the file path.
            return None
        scan_item = self._terminal_scan_item(context)
        if scan_item is None:
            return None
        return scan_memory_specs(sources, scan_item.live_data)

    def _open_closed_scan(self, request: SourceRequest) -> None:
        scan_item = self._terminal_scan_item(request.context)
        if scan_item is not None:
            with self._lock:
                decode_live_data(request.bundle, request.specs, scan_item.live_data)
        request.notify(UpdateReason.HISTORY)

    def _close_closed_scan(self, _request: SourceRequest) -> None:
        """Nothing to release: the writer-latency window is served once from scan memory."""

    # =========================================================================
    # NO_SCAN: scan-less device streams
    # =========================================================================

    def _resolve_no_scan(
        self, sources: list[SourceKey], _context: BindContext
    ) -> list[SourceSpec] | None:
        return device_stream_specs(self.client, sources)

    def _open_no_scan(self, request: SourceRequest) -> None:
        with self._lock:
            feeds: list[tuple] = request.state.setdefault("feeds", [])
            self._stream_requests.append(request)
            try:
                for spec in request.specs:
                    if spec.available:
                        feeds.append(self._ensure_stream_feed(spec))
                        self._seed_reading(request, spec)
            except Exception:
                self._close_no_scan(request)
                raise
        request.notify(UpdateReason.BACKFILL)

    def _seed_reading(self, request: SourceRequest, spec: SourceSpec) -> None:
        """
        Seed the latest cached reading for every correlation mode. Lock held.

        Readback and configuration pubsub topics store their last message.
        A stationary device need not publish after subscription, so DataAPI
        supplies that initial value. A message published between the feed
        subscription and this read is delivered by the feed as well; it is
        recognised and skipped.
        """
        if spec.stream not in _VALUE_STREAMS:
            return
        try:
            cached = self.client.connector.get(self._stream_endpoint(spec))
        except Exception as exc:  # pylint: disable=broad-except
            # The feed is attached; without the cached reading the source starts with
            # the next publication instead of the current value.
            logger.warning(f"No cached {spec.stream} of {spec.device} to start from: {exc!r}")
            return
        if isinstance(cached, DeviceMessage):
            request.state.setdefault("seeds", {})[self._stream_key(spec)] = cached
            decode_stream_message(request.bundle, spec, spec.stream, cached)

    def _close_no_scan(self, request: SourceRequest) -> None:
        with self._lock:
            if request in self._stream_requests:
                self._stream_requests.remove(request)
            for key in request.state.pop("feeds", []):
                self._release_feed(key)

    @staticmethod
    def _stream_key(spec: SourceSpec) -> tuple[str, str, str]:
        return (spec.stream or DeviceStream.READBACK, spec.device, spec.entry)

    @staticmethod
    def _stream_endpoint(spec: SourceSpec):
        if spec.stream == DeviceStream.PREVIEW:
            return MessageEndpoints.device_preview(spec.device, spec.entry)
        if spec.stream == DeviceStream.CONFIGURATION:
            return MessageEndpoints.device_read_configuration(spec.device)
        return MessageEndpoints.device_readback(spec.device)

    @staticmethod
    def _feed_key(spec: SourceSpec) -> tuple:
        """One feed per endpoint: all entries of a device share its readback message."""
        stream = spec.stream or DeviceStream.READBACK
        return ("stream", stream, spec.device, spec.entry if stream == DeviceStream.PREVIEW else "")

    def _ensure_stream_feed(self, spec: SourceSpec) -> tuple:
        key = self._feed_key(spec)

        def connector_callback(msg, *, _key=key):
            self._on_stream_message(msg, _key)

        self._ensure_feed(key, self._stream_endpoint(spec), connector_callback)
        return key

    def _on_stream_message(self, msg, key: tuple) -> None:
        stream = key[1]
        # Stream registrations (device_preview) deliver the record as a dict with the
        # message under "data"; pubsub registrations (device_readback) deliver a
        # MessageObject carrying the message in .value.
        msg_obj = msg.get("data") if isinstance(msg, dict) else msg.value
        if msg_obj is None:
            return
        with self._lock:
            targets = []
            for request in self._stream_requests:
                specs = []
                for spec in request.specs:
                    if not spec.available or self._feed_key(spec) != key:
                        continue
                    seeds = request.state.get("seeds", {})
                    seed_key = self._stream_key(spec)
                    if seed_key in seeds:
                        # Queued publications can predate the cache read. Keep
                        # the initial snapshot current until the feed catches up.
                        seed = seeds[seed_key]
                        if _predates_seed(seed, msg_obj, spec.entry):
                            continue
                        seeds.pop(seed_key)
                        if _same_message(seed, msg_obj):
                            continue
                    specs.append(spec)
                # The entries of one message form one row in a continuous group.
                if specs and decode_stream_points(request.bundle, specs, stream, msg_obj):
                    targets.append(request)
        for request in targets:
            request.notify(UpdateReason.LIVE)
