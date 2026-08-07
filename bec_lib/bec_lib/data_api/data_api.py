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

from .alignment import Bundle, CorrelationGroupError, validate_correlation_group
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
        scan: str,
        callback: Callable[[SubscriptionUpdate], Any],
        min_emit_interval: float = 0.1,
    ):
        self._api = api
        self._lock = api._lock
        self._callback = callback
        self._sources: list[SourceKey] = [tuple(s) for s in sources]
        self._follow = scan == "live"
        self._scan_id: str | None = None if self._follow else scan
        self._min_emit_interval = min_emit_interval
        self._closed = False

        self._plugin: DataSourcePlugin | None = None
        self._request: SourceRequest | None = None
        self._bundle: Bundle | None = None

        self._last_emit = 0.0
        self._pending_reason: UpdateReason | None = None
        self._flush_timer: threading.Timer | None = None

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
            if self._request is None:
                return list(self._sources)
            return [spec.key for spec in self._request.specs if not spec.available]

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
            CorrelationGroupError: If the sources cannot form one group in the
                currently bound scan.
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
            available = [(s.key, s.kind, s.acquisition_group) for s in specs if s.available]
            if available:
                validate_correlation_group(available)
            self._teardown_request()
            self._scan_id = scan_id
            self._bundle = Bundle(scan_id)
            self._request = SourceRequest(
                scan_id=scan_id, specs=specs, bundle=self._bundle, notify=self._notify
            )
            self._plugin = plugin
            # The plugin owns the initial emission: synchronous backfill for
            # live scans, worker-thread completion for history reads.
            plugin.open(self._request)
            return True
        return False

    def _teardown_request(self) -> None:
        if self._request is not None and self._plugin is not None:
            self._plugin.close(self._request)
        self._request = None
        self._plugin = None
        self._bundle = None

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
            if status in TERMINAL_SCAN_STATES and scan_id == self._scan_id:
                # Final flush of the live state; the authoritative history
                # re-route happens when the scan-history entry appears.
                self._notify("live", force=True)

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

    def _notify(self, reason: UpdateReason, force: bool = False) -> None:
        """Emit (or schedule) an update. Called by plugins and internally."""
        update = None
        with self._lock:
            if self._closed or self._bundle is None or self._callback is None:
                return
            now = time.monotonic()
            immediate = (
                force
                or reason != "live"
                or self._min_emit_interval <= 0
                or now - self._last_emit >= self._min_emit_interval
            )
            if immediate:
                update = self._bundle.build_update(reason)
                self._last_emit = now
                self._pending_reason = None
                if self._flush_timer is not None:
                    self._flush_timer.cancel()
                    self._flush_timer = None
            else:
                self._pending_reason = reason
                if self._flush_timer is None:
                    delay = self._min_emit_interval - (now - self._last_emit)
                    self._flush_timer = threading.Timer(max(delay, 0.001), self._flush_pending)
                    self._flush_timer.daemon = True
                    self._flush_timer.start()
        if update is not None:
            self._callback(update)

    def _flush_pending(self) -> None:
        with self._lock:
            self._flush_timer = None
            reason = self._pending_reason
            self._pending_reason = None
        if reason is not None:
            self._notify(reason, force=True)

    # --- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the subscription and release all resources (idempotent)."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._flush_timer is not None:
                self._flush_timer.cancel()
                self._flush_timer = None
            self._teardown_request()
            for relay in self._relays:
                if relay.callback_id is not None:
                    self._api.client.callbacks.remove(relay.callback_id)
                    relay.callback_id = None
            self._relays.clear()
            self._callback = None

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
    that instance; different clients get independent instances.
    """

    _instances: dict[int, DataAPI] = {}

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

    @classmethod
    def clear_instance(cls) -> None:
        """Close and clear all per-client instances (test helper)."""
        for instance in list(cls._instances.values()):
            instance.close()
        cls._instances.clear()

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
        scan: str = "live",
        callback: Callable[[SubscriptionUpdate], Any] | None = None,
        min_emit_interval: float = 0.1,
    ) -> Subscription:
        """
        Create a subscription delivering columnar updates for the sources.

        Args:
            sources (list[SourceKey]): (device, entry) pairs; must form one
                correlation group (monitored + "monitored"-group async, one
                shared async group, or a single standalone source).
            scan (str): ``"live"`` to follow the active scan, or a concrete
                scan id (open or terminal — terminal scans are served by the
                history plugin).
            callback: Called with each :class:`SubscriptionUpdate`; may run on
                dispatcher/timer/worker threads and must not block.
            min_emit_interval (float): Backend emission coalescing interval in
                seconds for live updates (trailing emission guaranteed);
                ``0`` disables coalescing.

        Returns:
            Subscription: The active subscription.

        Raises:
            ValueError: If a concrete scan id cannot be served by any plugin.
            CorrelationGroupError: If the sources do not form one group.
        """
        subscription = Subscription(
            self, sources, scan, callback, min_emit_interval=min_emit_interval
        )
        with self._lock:
            if scan == "live":
                self._bind_current_scan(subscription)
            else:
                if not subscription._bind(scan, reason="backfill"):
                    subscription.close()
                    raise ValueError(f"No data source can serve scan '{scan}'.")
        return subscription

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
