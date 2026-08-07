"""
DataAPI facade: one subscription API for live and history scan data.

A :class:`Subscription` binds a set of sources
to a scan (a concrete scan id, or ``"live"`` to follow the active scan), routes
it to the first claiming plugin (live for open scans, history for terminal
ones) and delivers immutable columnar :class:`~.models.SubscriptionUpdate`
snapshots to one callback, rate-limited backend-side with a guaranteed
trailing emission.

Threading: one reentrant lock per :class:`DataAPI` instance is shared with all
plugins and subscriptions. Callbacks may be invoked on dispatcher, timer or
worker threads — possibly with that lock held — and must not block on other
threads that use the data API. Qt consumers should hand the update to a queued
signal (see ``bec_widgets`` ``QtDataSubscription``).
"""

from __future__ import annotations

import threading
import time
import weakref
from typing import TYPE_CHECKING, Any, Callable

from bec_lib.logger import bec_logger

from .alignment import Bundle, CorrelationGroupError, partition_correlation_groups
from .device_plugin import DEVICE_SCOPE, DeviceStreamPlugin
from .history_plugin import HistoryDataPlugin
from .live_plugin import TERMINAL_SCAN_STATES, LiveDataPlugin
from .models import SourceKey, SubscriptionUpdate, UpdateReason
from .plugin_base import DataSourcePlugin, SourceRequest

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

logger = bec_logger.logger


class _WeakEventRelay:
    """
    Forwards client callback events to a subscription without keeping it
    alive; self-unregisters once the subscription is garbage collected.
    """

    def __init__(self, subscription: Subscription, handler_name: str):
        self._subscription_ref = weakref.ref(subscription)
        self._client = subscription._api.client
        self._handler_name = handler_name
        self.callback_id: int | str | None = None

    def __call__(self, *args, **kwargs) -> None:
        subscription = self._subscription_ref()
        if subscription is None:
            if self.callback_id is not None:
                self._client.callbacks.remove(self.callback_id)
                self.callback_id = None
            return
        getattr(subscription, self._handler_name)(*args, **kwargs)


class Subscription:
    """
    A live- or history-scan subscription delivering columnar updates.

    Create via :meth:`DataAPI.subscribe`. The source set is atomic: it is
    declared at creation and replaced wholesale with :meth:`set_sources`
    (one rebuild, one re-emission) — there is no incremental add/remove.
    """

    def __init__(
        self,
        api: DataAPI,
        sources: list[SourceKey],
        scan: str | None,
        callback: Callable[[SubscriptionUpdate], Any],
        min_emit_interval: float = 0.1,
        max_points: int | None = None,
        size_limit_bytes: int | None = None,
    ):
        self._api = api
        self._lock = api._lock
        self._callback = callback
        self._sources: list[SourceKey] = [tuple(s) for s in sources]
        self._follow = scan == "live"
        self._scan_id: str | None = (
            DEVICE_SCOPE if scan is None else (None if self._follow else scan)
        )
        self._min_emit_interval = min_emit_interval
        self._max_points = max_points
        self._size_limit_bytes = size_limit_bytes
        self._estimated_bytes: int | None = None
        self._size_gated = False
        self._closed = False

        self._plugin: DataSourcePlugin | None = None
        self._requests: dict[str, SourceRequest] = {}  # group label -> request
        self._specs: list = []

        # Per-group emission coalescing state.
        self._emit_state: dict[str, dict] = {}

        self._relays: list[_WeakEventRelay] = []
        if self._follow:
            for event, handler in (
                ("scan_status", "_on_scan_status"),
                ("scan_history_update", "_on_scan_history_update"),
            ):
                relay = _WeakEventRelay(self, handler)
                relay.callback_id = api.client.callbacks.register(event, relay)
                self._relays.append(relay)

    # --- public state --------------------------------------------------------

    @property
    def scan_id(self) -> str | None:
        """The currently bound scan id (``None`` while unbound)."""
        return self._scan_id

    @property
    def sources(self) -> list[SourceKey]:
        """The declared source set."""
        return list(self._sources)

    @property
    def unbound_sources(self) -> list[SourceKey]:
        """
        Sources that are declared but not currently delivering: the whole set
        while unbound, or the sources the serving plugin marked unavailable.
        """
        with self._lock:
            if not self._requests:
                return list(self._sources)
            return [spec.key for spec in self._specs if not spec.available]

    # --- source set ----------------------------------------------------------

    def set_sources(self, sources: list[SourceKey]) -> Subscription:
        """
        Atomically replace the source set.

        Args:
            sources (list[SourceKey]): New (device, entry) pairs.

        Returns:
            Subscription: This subscription, for chaining.

        Raises:
            RuntimeError: If the subscription is closed.
            CorrelationGroupError: If ``sources`` is empty.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot change sources on a closed subscription")
            self._sources = [tuple(s) for s in sources]
            if self._scan_id is not None:
                self._bind(self._scan_id, reason="rebind")
        return self

    # --- binding and routing -------------------------------------------------

    def _bind(self, scan_id: str, reason: UpdateReason) -> bool:
        """Route the subscription to a plugin for the given scan. Lock held."""
        for plugin in self._api.plugins:
            specs = plugin.resolve(self._sources, scan_id)
            if specs is None:
                continue
            self._teardown_requests()
            self._scan_id = scan_id
            self._specs = specs
            self._plugin = plugin
            spec_by_key = {spec.key: spec for spec in specs if spec.available}
            groups = (
                partition_correlation_groups(
                    [(s.key, s.kind, s.acquisition_group) for s in specs if s.available]
                )
                if spec_by_key
                else {}
            )
            self._estimated_bytes = plugin.estimate_bytes(list(spec_by_key), scan_id)
            self._size_gated = (
                self._size_limit_bytes is not None
                and self._estimated_bytes is not None
                and self._estimated_bytes > self._size_limit_bytes
            )
            for label, keys in groups.items():
                bundle = Bundle(scan_id, max_points=self._max_points)
                # Pre-register every declared source: a silent source must
                # hold back aligned_ordinals (and be reported as lagging)
                # instead of being invisible to the intersection.
                for key in keys:
                    spec = spec_by_key[key]
                    bundle.get_series(spec.device, spec.entry, spec.kind or "unindexed")
                request = SourceRequest(
                    scan_id=scan_id,
                    specs=[spec_by_key[key] for key in keys],
                    bundle=bundle,
                    notify=lambda reason, _label=label: self._notify(_label, reason),
                )
                self._requests[label] = request
                if self._size_gated:
                    # Oversized payload: nothing is read until the consumer
                    # calls confirm_size(); the estimate costs no file I/O.
                    logger.info(
                        f"Subscription to scan {scan_id} withheld: estimated "
                        f"{self._estimated_bytes} bytes exceeds the configured limit "
                        f"{self._size_limit_bytes}. Call confirm_size() to load it."
                    )
                    continue
                # The plugin owns the initial emission: synchronous backfill
                # for live scans, worker-thread completion for history reads.
                plugin.open(request)
            return True
        return False

    @property
    def size_gated(self) -> bool:
        """Whether delivery is withheld pending :meth:`confirm_size`."""
        return self._size_gated

    @property
    def estimated_bytes(self) -> int | None:
        """Estimated payload size of the bound scan, if the plugin knows it."""
        return self._estimated_bytes

    def confirm_size(self) -> Subscription:
        """
        Load a subscription that was withheld by the size limit.

        The read itself runs on the serving plugin's worker thread, so this
        returns immediately and never blocks the caller (GUI) thread.

        Returns:
            Subscription: This subscription, for chaining.
        """
        with self._lock:
            if self._closed or not self._size_gated or self._plugin is None:
                return self
            self._size_gated = False
            for request in self._requests.values():
                self._plugin.open(request)
        return self

    def _teardown_requests(self) -> None:
        if self._plugin is not None:
            for request in self._requests.values():
                self._plugin.close(request)
        self._requests.clear()
        self._specs = []
        self._plugin = None
        for state in self._emit_state.values():
            timer = state.get("timer")
            if timer is not None:
                timer.cancel()
        self._emit_state.clear()

    def _on_scan_status(self, scan_status: dict, _metadata: dict) -> None:
        with self._lock:
            if self._closed or not self._follow:
                return
            scan_id = scan_status.get("scan_id")
            status = scan_status.get("status")
            if not scan_id:
                return
            if status == "open" and scan_id != self._scan_id:
                try:
                    if not self._bind(scan_id, reason="rebind"):
                        logger.warning(f"No data source can serve scan {scan_id}; waiting.")
                except CorrelationGroupError as exc:
                    logger.warning(f"Cannot bind scan {scan_id}: {exc}")
                return
            if (
                status == "open"
                and scan_id == self._scan_id
                and any(not spec.available for spec in self._specs)
            ):
                # A source that resolved unavailable at bind time (e.g. device
                # info still settling) must not stay lost for the whole scan:
                # retry on the next status update of the same scan.
                try:
                    self._bind(scan_id, reason="rebind")
                except CorrelationGroupError as exc:  # pragma: no cover - defensive
                    logger.warning(f"Cannot rebind scan {scan_id}: {exc}")
                return
            if status in TERMINAL_SCAN_STATES and scan_id == self._scan_id:
                # Final flush of the live state; the authoritative history
                # re-route happens when the scan-history entry appears.
                for label in list(self._requests):
                    self._notify(label, "live", force=True)

    def _on_scan_history_update(self, history_msg=None, **_kwargs) -> None:
        with self._lock:
            if self._closed or not self._follow:
                return
            scan_id = getattr(history_msg, "scan_id", None)
            if scan_id is None or scan_id != self._scan_id:
                return
            try:
                self._bind(scan_id, reason="history")
            except CorrelationGroupError as exc:  # pragma: no cover - defensive
                logger.warning(f"Cannot re-route scan {scan_id} to history: {exc}")

    # --- emission ------------------------------------------------------------

    def _notify(self, label: str, reason: UpdateReason, force: bool = False) -> None:
        """Emit (or schedule) an update for one group. Called by plugins."""
        update = None
        with self._lock:
            if self._closed or self._callback is None:
                return
            request = self._requests.get(label)
            if request is None:
                return
            state = self._emit_state.setdefault(
                label, {"last": 0.0, "pending": None, "timer": None}
            )
            now = time.monotonic()
            immediate = (
                force
                or reason != "live"
                or self._min_emit_interval <= 0
                or now - state["last"] >= self._min_emit_interval
            )
            if immediate:
                update = request.bundle.build_update(reason, metadata={"group": label})
                state["last"] = now
                state["pending"] = None
                if state["timer"] is not None:
                    state["timer"].cancel()
                    state["timer"] = None
            else:
                state["pending"] = reason
                if state["timer"] is None:
                    delay = self._min_emit_interval - (now - state["last"])
                    state["timer"] = threading.Timer(
                        max(delay, 0.001), self._flush_pending, args=(label,)
                    )
                    state["timer"].daemon = True
                    state["timer"].start()
        if update is not None:
            self._callback(update)

    def _flush_pending(self, label: str) -> None:
        with self._lock:
            state = self._emit_state.get(label)
            if state is None:
                return
            state["timer"] = None
            reason = state["pending"]
            state["pending"] = None
        if reason is not None:
            self._notify(label, reason, force=True)

    # --- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the subscription and release all resources (idempotent)."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            workers = [
                request.state.get("worker")
                for request in self._requests.values()
                if request.state.get("worker") is not None
            ]
            self._teardown_requests()
            for relay in self._relays:
                if relay.callback_id is not None:
                    self._api.client.callbacks.remove(relay.callback_id)
                    relay.callback_id = None
            self._relays.clear()
            self._callback = None
        # Join worker threads outside the lock (they need it for their final
        # insert); reads are bounded, so this terminates promptly.
        for worker in workers:
            if worker.is_alive() and worker is not threading.current_thread():
                worker.join(timeout=5)

    def __del__(self):
        if getattr(self, "_closed", True):
            return
        try:
            self.close()
        except Exception:  # pragma: no cover - destructor safety
            pass

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class DataAPI:
    """
    Per-client data access facade.

    Constructing a ``DataAPI`` with a client that already owns one returns
    that instance; different clients get independent instances. Prefer
    ``client.data_api`` — the lazy client property — over constructing
    directly. The registry holds weak values: an entry lives exactly as long
    as some consumer holds the instance (which in turn keeps the client
    alive), so ``id()`` keys cannot alias across dead clients.
    """

    _instances: weakref.WeakValueDictionary[int, DataAPI] = weakref.WeakValueDictionary()

    def __new__(cls, client):
        instance = cls._instances.get(id(client))
        if instance is None:
            instance = super().__new__(cls)
            cls._instances[id(client)] = instance
        return instance

    def __init__(self, client: BECClient):
        if hasattr(self, "_initialized"):
            return
        self.client = client
        self._lock = threading.RLock()
        self.plugins: list[DataSourcePlugin] = []
        self._initialized = True
        self.register_plugin(LiveDataPlugin(client, self._lock))
        self.register_plugin(HistoryDataPlugin(client, self._lock))
        self.register_plugin(DeviceStreamPlugin(client, self._lock))

    @classmethod
    def clear_instance(cls) -> None:
        """Close and clear all per-client instances (test helper)."""
        for instance in list(cls._instances.values()):
            instance.close()
        for key in list(cls._instances.keys()):
            cls._instances.pop(key, None)

    def register_plugin(self, plugin: DataSourcePlugin) -> None:
        """
        Register a source plugin and connect its feeds.

        Args:
            plugin (DataSourcePlugin): Plugin instance; its ``priority`` class
                attribute determines routing order (lower first).
        """
        plugin._lock = self._lock
        plugin.connect()
        self.plugins.append(plugin)
        self.plugins.sort(key=lambda p: p.priority)

    def subscribe(
        self,
        sources: list[SourceKey],
        scan: str | None = "live",
        callback: Callable[[SubscriptionUpdate], Any] | None = None,
        min_emit_interval: float = 0.1,
        max_points: int | None = None,
        size_limit_bytes: int | None = None,
    ) -> Subscription:
        """
        Create a subscription delivering columnar updates for the sources.

        Args:
            sources (list[SourceKey]): (device, entry) pairs. The sources are
                partitioned automatically into correlation groups (monitored +
                "monitored"-group async form the "scan" group; async sources
                sharing a free-form group align together; everything else is
                standalone); each group emits its own updates, labelled in
                ``update.metadata["group"]``.
            scan (str | None): ``"live"`` to follow the active scan, a
                concrete scan id (open or terminal — terminal scans are served
                by the history plugin), or ``None`` for scan-less device
                streams (readback, ``"monitor_1d"``, preview signals).
            callback: Called with each :class:`SubscriptionUpdate`; may run on
                dispatcher/timer/worker threads and must not block.
            min_emit_interval (float): Backend emission coalescing interval in
                seconds for live updates (trailing emission guaranteed);
                ``0`` disables coalescing.
            max_points (int | None): Per-source retention cap; oldest points
                are dropped beyond it. Recommended for endless device-stream
                subscriptions.
            size_limit_bytes (int | None): When the serving plugin can
                estimate the payload up front (history scans) and the estimate
                exceeds this limit, nothing is read: the subscription reports
                ``size_gated`` with ``estimated_bytes`` and waits for
                :meth:`Subscription.confirm_size`.

        Returns:
            Subscription: The active subscription.

        Raises:
            ValueError: If a concrete scan id cannot be served by any plugin.
            CorrelationGroupError: If ``sources`` is empty.
        """
        subscription = Subscription(
            self,
            sources,
            scan,
            callback,
            min_emit_interval=min_emit_interval,
            max_points=max_points,
            size_limit_bytes=size_limit_bytes,
        )
        with self._lock:
            if scan == "live":
                self._bind_current_scan(subscription)
            else:
                target = DEVICE_SCOPE if scan is None else scan
                if not subscription._bind(target, reason="backfill"):
                    subscription.close()
                    raise ValueError(f"No data source can serve scan '{scan}'.")
        return subscription

    def estimate_bytes(self, sources: list[SourceKey], scan: str) -> int | None:
        """
        Estimate the payload size of a prospective subscription.

        Args:
            sources (list[SourceKey]): Sources that would be subscribed.
            scan (str): Scan id to evaluate.

        Returns:
            int | None: Estimated bytes, or ``None`` if no plugin can tell.
        """
        with self._lock:
            for plugin in self.plugins:
                specs = plugin.resolve(list(sources), scan)
                if specs is None:
                    continue
                return plugin.estimate_bytes([s.key for s in specs if s.available], scan)
        return None

    def _bind_current_scan(self, subscription: Subscription) -> None:
        queue = getattr(self.client, "queue", None)
        scan_storage = getattr(queue, "scan_storage", None)
        current = getattr(scan_storage, "current_scan_id", None)
        if not isinstance(current, (list, tuple)) or not current:
            return
        scan_id = current[0]
        if not isinstance(scan_id, str) or not scan_id:
            return
        scan_item = scan_storage.find_scan_by_ID(scan_id)
        if (
            scan_item is None
            or getattr(scan_item, "status", None) in TERMINAL_SCAN_STATES
            or getattr(scan_item, "status_message", None) is None
        ):
            return
        subscription._bind(scan_id, reason="backfill")

    def close(self) -> None:
        """Disconnect all plugins and remove this instance from the registry."""
        for plugin in self.plugins:
            plugin.disconnect()
        self.plugins.clear()
        instances = type(self)._instances
        for key, instance in list(instances.items()):
            if instance is self:
                del instances[key]
        if hasattr(self, "_initialized"):
            delattr(self, "_initialized")

    def __del__(self):
        try:
            self.close()
        except Exception:  # pragma: no cover - destructor safety
            pass
