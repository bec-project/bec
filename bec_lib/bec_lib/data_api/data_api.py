"""
DataAPI facade: one subscription API for live and history scan data.

The facade registers data paths, resolves sources and creates subscriptions over
live scans, history scans and scan-less device streams.

Threading: one reentrant lock per DataAPI instance is shared with all data paths
and subscriptions. Consumer callbacks run outside that lock and must not block
on other threads that use the data API.
"""

from __future__ import annotations

import collections
import importlib.metadata
import threading
import time
import weakref
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable

from bec_lib.device import ReadoutPriority
from bec_lib.logger import bec_logger

from . import client_views
from .data_bundler import (
    CorrelationGroupError,
    CorrelationMode,
    correlation_group,
    partition_correlation_groups,
)
from .data_path import (
    BindContext,
    DataPath,
    SourceRequest,
    SourceSpec,
    apply_size_gate,
    total_estimated_bytes,
)
from .decoding import open_scan_specs
from .emission import EmissionScheduler
from .locking import OwnedLock
from .models import (
    CONTINUOUS_GROUP,
    CorrelationGroup,
    SourceInfo,
    SourceKey,
    SourceKind,
    SubscriptionUpdate,
    UpdateReason,
)
from .paths import DEFAULT_DATA_PATHS
from .scenarios import DEVICE_SCOPE, ScanTarget, Scenario, classify
from .subscription import Subscription, reject_reentrant_call

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

logger = bec_logger.logger

#: Entry-point group under which installed packages expose DataPath subclasses;
#: each is registered at facade start as ``cls(client)``.
PATH_ENTRY_POINT_GROUP = "bec.data_api.paths"

#: How long the reaper waits for the workers of one released request.
_REAP_TIMEOUT = 5.0

#: Readout priorities a scan cannot change: such a device is never read per scan point.
_FIXED_READOUT = frozenset({ReadoutPriority.ASYNC, ReadoutPriority.CONTINUOUS})

# Subscription and DataAPI are cooperating private parts of the same facade.
# pylint: disable=protected-access


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
            # ``__new__`` returned the facade this client already owns.
            return
        self.client = client
        self._closed = False
        self._lock = OwnedLock()
        # Trailing coalesced emissions of every subscription of this client.
        self._scheduler = EmissionScheduler(name=f"data-api-emit-{id(client):x}")
        #: Level-2 routing tables: one priority-ordered data path list per
        #: scenario, built from each path's ``scenarios`` declaration.
        self.data_paths: dict[Scenario, list[DataPath]] = {}
        self._paths: list[DataPath] = []
        # Live subscriptions (weak): re-bound when their data path is replaced.
        self._subscriptions: weakref.WeakSet[Subscription] = weakref.WeakSet()
        # Requests released by rebinds, joined off the dispatcher and emission threads.
        self._reap_cv = threading.Condition()
        self._reap_queue: collections.deque[SourceRequest] = collections.deque()
        self._reaper: threading.Thread | None = None
        # (path class, operation) pairs whose failure was already logged in full.
        self._reported_failures: set[tuple[str, str]] = set()
        self._initialized = True
        for path_class in DEFAULT_DATA_PATHS:
            self.register_path(path_class(client, self._lock))
        self._register_discovered_paths()

    def _register_discovered_paths(self) -> None:
        """Register the data paths that installed packages expose as entry points."""
        for entry_point in importlib.metadata.entry_points(group=PATH_ENTRY_POINT_GROUP):
            try:
                path_class = entry_point.load()
                if not (isinstance(path_class, type) and issubclass(path_class, DataPath)):
                    raise TypeError(f"{path_class!r} is not a DataPath subclass")
                self.register_path(path_class(self.client))
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    f"Data path entry point {entry_point.name!r} ({entry_point.value}) "
                    "could not be registered."
                )

    @classmethod
    def clear_instance(cls) -> None:
        """Close and clear all per-client instances (test helper)."""
        for instance in list(cls._instances.values()):
            instance.close()
        for key in list(cls._instances.keys()):
            cls._instances.pop(key, None)

    @property
    def closed(self) -> bool:
        """Whether this facade and its subscriptions have been closed."""
        return self._closed

    @property
    def paths(self) -> list[DataPath]:
        """All registered data paths, in registration order."""
        return list(self._paths)

    def register_path(self, path: DataPath, replace: bool = False) -> None:
        """
        Register a data path and connect its feeds.

        Routing is two-level: the facade first classifies the scan into its
        scenario (deterministic, no priorities), then consults only the data
        paths that declare that scenario, in ascending ``priority`` order
        (lower first; ties resolve by registration order). Exactly one path
        serves a data source: a second one (typically a subclass of a core
        path) is registered with ``replace=True``, which disconnects and
        removes the current path of that source first.

        Args:
            path (DataPath): Data path instance; its ``scenarios`` class
                attribute selects the routing tables it joins, its
                ``priority`` the order within them.
            replace (bool): Swap out the registered path of the same
                ``data_source`` instead of rejecting the registration.

        Raises:
            ValueError: If the path declares no valid scenario or data
                source, or if a path of the same data source is already
                registered and ``replace`` is false — a duplicate could never
                win routing but its ``connect()`` side effects (client
                callbacks, feeds) would run and cost dispatch overhead forever.
        """
        self._ensure_open()
        reject_reentrant_call(self._lock, "DataAPI.register_path()")
        name = type(path).__name__
        scenarios = getattr(path, "scenarios", None)
        valid = {Scenario.OPEN_SCAN, Scenario.CLOSED_SCAN, Scenario.NO_SCAN}
        if not isinstance(scenarios, (set, frozenset)) or not scenarios or not scenarios <= valid:
            raise ValueError(
                f"{name} must declare the scenarios it serves "
                f"(a non-empty set of OPEN_SCAN, CLOSED_SCAN, NO_SCAN)."
            )
        source = getattr(path, "data_source", None)
        if not isinstance(source, str):
            raise ValueError(f"{name} must declare its data_source.")
        with self._lock:
            current = next((p for p in self._paths if p.data_source == source), None)
            if current is not None and not replace:
                raise ValueError(
                    f"{type(current).__name__} already serves data source '{source}'; "
                    f"register {name} with replace=True to swap it."
                )
        # connect()/disconnect() register client callbacks and feeds: never
        # under the DataAPI lock (the callback handler dispatches to our
        # handlers under its own lock).
        path.attach(self)
        path.connect()
        with self._lock:
            owner = next((p for p in self._paths if p.data_source == source), None)
            conflict = self.closed or owner is not current
        if conflict:
            # Closed meanwhile, or another registration of the same source won the race.
            path.disconnect()
            raise RuntimeError(
                f"Registering {name} for data source '{source}' raced with "
                + ("DataAPI.close()." if self.closed else "another registration.")
            )
        with self._lock:
            if current is not None:
                self._paths.remove(current)
                for table in self.data_paths.values():
                    if current in table:
                        table.remove(current)
            self._paths.append(path)
            for scenario in scenarios:
                table = self.data_paths.setdefault(scenario, [])
                table.append(path)
                table.sort(key=lambda p: p.priority)
            affected = [s for s in self._subscriptions if s._uses_path(current)] if current else []
        for subscription in affected:
            subscription._rebind_after_replace(current)
        if current is not None:
            current.disconnect()
        # The new path may serve sources that subscriptions could not bind so far.
        self._retry_unbound()

    def unregister_path(self, data_source: str) -> None:
        """
        Remove the data path of ``data_source`` and re-route the subscriptions it served.

        Args:
            data_source (str): The ``data_source`` of a registered path.

        Raises:
            ValueError: If no path of that data source is registered.
        """
        reject_reentrant_call(self._lock, "DataAPI.unregister_path()")
        with self._lock:
            path = next((p for p in self._paths if p.data_source == data_source), None)
            if path is None:
                raise ValueError(f"No data path is registered for data source '{data_source}'.")
            self._paths.remove(path)
            for table in self.data_paths.values():
                if path in table:
                    table.remove(path)
            affected = [s for s in self._subscriptions if s._uses_path(path)]
        for subscription in affected:
            subscription._rebind_after_replace(path)
        path.disconnect()

    def retry_unbound(self) -> None:
        """
        Retry subscriptions with sources no path could serve so far, and live followers
        without a scan.

        A data path calls this (outside the lock) when its set of servable sources
        grew, e.g. a new external device was announced; ``register_path`` does it too.
        """
        if self.closed:
            return
        reject_reentrant_call(self._lock, "DataAPI.retry_unbound()")
        self._retry_unbound()

    def _retry_unbound(self) -> None:
        """Re-resolve subscriptions with sources that no path could serve so far."""
        with self._lock:
            targets = [
                (subscription, subscription.scan_id)
                for subscription in list(self._subscriptions)
                if not subscription._closed
                and subscription.scan_id is not None
                and subscription.unbound_sources
            ]
            followers = [
                subscription
                for subscription in list(self._subscriptions)
                if not subscription._closed
                and subscription._follow
                and subscription.scan_id is None
            ]
        if followers:
            # A live follower created before this path existed may now have a scan to follow.
            live = self._current_live_scan()
            if live is not None:
                scan_id, scan_item = live
                with self._lock:
                    for subscription in followers:
                        if subscription._closed or subscription.scan_id is not None:
                            continue
                        try:
                            subscription._reresolve(
                                scan_id,
                                UpdateReason.REBIND,
                                scan_item=scan_item,
                                require_available=True,
                            )
                        except Exception:  # pylint: disable=broad-except
                            logger.exception(f"Binding a live follower to {scan_id!r} failed.")
                for subscription in followers:
                    subscription._flush_deferred()
        if not targets:
            return
        # Scan items are read before taking the lock (lock-order rule).
        items = {
            scan_id: self._peek_scan_item(scan_id)
            for scan_id in {scan_id for _, scan_id in targets}
            if scan_id != DEVICE_SCOPE
        }
        with self._lock:
            for subscription, scan_id in targets:
                if subscription._closed or subscription.scan_id != scan_id:
                    continue
                try:
                    subscription._reresolve(
                        scan_id, UpdateReason.REBIND, scan_item=items.get(scan_id)
                    )
                except Exception:  # pylint: disable=broad-except
                    logger.exception(f"Re-resolving subscription for scan {scan_id!r} failed.")
        for subscription, _ in targets:
            subscription._flush_deferred()

    def subscribe(
        self,
        sources: list[SourceKey] | None = None,
        scan: str | None = ScanTarget.LIVE,
        callback: Callable[[SubscriptionUpdate], Any] | None = None,
        min_emit_interval: float = 0.1,
        max_points: int | None = None,
        size_limit_bytes: int | None = None,
        progress_callback: Callable[[float], None] | None = None,
        force_source: str | None = None,
        devices: list[str] | None = None,
        view_limit_bytes: int | None = None,
        correlation: CorrelationMode | str = CorrelationMode.ORDINAL,
    ) -> Subscription:
        """
        Create a subscription delivering columnar updates for the sources.

        Args:
            sources (list[SourceKey] | None): (device, entry) pairs. The
                sources are partitioned automatically into correlation groups
                (monitored signals form the scan group; the sub-signals of
                one AsyncMultiSignal align together; everything else is
                standalone); each group emits its own updates, which carry
                their :class:`CorrelationGroup` in ``update.metadata["group"]``.
            devices (list[str] | None): Shorthand for hub signals:
                ``devices=["samx", "samy"]`` is ``sources=[("samx", "samx"),
                ("samy", "samy")]``. Exclusive with ``sources``.
            correlation (CorrelationMode): ``"ordinal"`` (default) aligns
                by ordinal within the automatic groups. ``"continuous"`` is
                for scan-less device streams (``scan=None``): all sources
                form one sample-and-hold group in which every arriving
                sample appends a row holding the latest value of every
                source — the (x, y) pairing of independent motor readbacks.
            scan (str | None): ``ScanTarget.LIVE`` (or its string ``"live"``) to follow
                the active scan (while
                none runs, the most recent scan is shown until the next one
                opens), a concrete scan id (open or terminal — terminal scans are served
                from their file, or from scan memory until it is published),
                or ``None`` for scan-less device streams (readback,
                configuration readings, preview signals).
            callback: Called with each :class:`SubscriptionUpdate`; may run on
                dispatcher/timer/worker threads and must not block.
            min_emit_interval (float): Backend emission coalescing interval in
                seconds for live updates (trailing emission guaranteed);
                ``0`` disables coalescing.
            max_points (int | None): Per-source retention cap; oldest points
                are dropped beyond it. Recommended for endless device-stream
                subscriptions.
            progress_callback: Optional callable receiving load fractions
                (0..1) while a data path reads bulk data (file reads).
                Invoked on worker threads; must be cheap and must not call
                back into the data API.
            size_limit_bytes (int | None): When the serving data path can
                estimate the payload up front (history scans) and the estimate
                exceeds this limit, nothing is read: the subscription reports
                ``size_gated`` with ``estimated_bytes`` and waits for
                :meth:`Subscription.confirm_size`.
            view_limit_bytes (int | None): A stored column (one value per row) that would
                take more memory than this, samples plus row numbers, is not held whole. The subscription delivers an overview of it,
                the extremes of every bin of rows, and the consumer asks for the rows it
                looks at with :meth:`Subscription.request_view`. The source then carries
                ``metadata["view"]``. ``None`` holds every column whole.
            force_source (str | None): Pin the subscription to one data
                source (a :class:`DataSource` value or the ``data_source``
                of a registered path), e.g. ``"file"`` to bypass Redis for a
                closed scan. Only paths of that source are consulted, for
                the whole life of the subscription: a concrete scan the
                source cannot serve raises, and a live follow requires a
                source that serves open scans (a live-follow subscription
                then stays unbound until the source can serve a scan).

        Returns:
            Subscription: The active subscription.

        Raises:
            ValueError: If a concrete scan id cannot be served by any data
                path (of the forced source), if ``force_source`` names no
                registered data source or cannot follow live scans, if
                neither or both of ``sources`` and ``devices`` are given, or
                if ``correlation`` is not defined for the requested scan.
            CorrelationGroupError: If the source list is empty.
        """
        self._ensure_open()
        reject_reentrant_call(self._lock, "DataAPI.subscribe()")
        self._check_source(force_source, scan)
        if (sources is None) == (devices is None):
            raise ValueError("subscribe() takes exactly one of 'sources' or 'devices'.")
        if devices is not None:
            sources = [(device, device) for device in devices]
        if not sources:
            raise CorrelationGroupError("A subscription needs at least one source.")
        if max_points is not None and max_points < 1:
            raise ValueError("max_points must be at least 1 (or None for unbounded).")
        try:
            correlation = CorrelationMode(correlation)
        except ValueError:
            raise ValueError(
                f"Unknown correlation '{correlation}' (ordinal or continuous)."
            ) from None
        if correlation == CorrelationMode.CONTINUOUS and scan is not None:
            raise ValueError(
                "correlation='continuous' is defined for scan-less subscriptions only."
            )
        subscription = Subscription(
            self,
            sources,
            scan,
            callback,
            min_emit_interval=min_emit_interval,
            max_points=max_points,
            size_limit_bytes=size_limit_bytes,
            progress_callback=progress_callback,
            force_source=force_source,
            correlation=correlation,
            view_limit_bytes=view_limit_bytes,
        )
        # Prefetch scan items BEFORE taking the DataAPI lock: the reads take
        # the scan-manager RLock, and acquiring it under our lock inverts the
        # dispatcher's lock order (scan lock -> DataAPI lock) into a deadlock.
        try:
            if scan == ScanTarget.LIVE:
                # Idle: show the most recent scan until the next one opens.
                live_target = self._current_live_scan() or client_views.latest_scan(self.client)
                with self._lock:
                    # A scan-status event may have bound a newer scan while the
                    # prefetch ran without the lock: never override it.
                    if live_target is not None and subscription._scan_id is None:
                        scan_id, scan_item = live_target
                        subscription._bind(
                            scan_id, reason=UpdateReason.BACKFILL, scan_item=scan_item
                        )
                subscription._flush_deferred()
            else:
                target = DEVICE_SCOPE if scan is None else scan
                prefetched = self._peek_scan_item(target) if scan is not None else None
                with self._lock:
                    bound = subscription._bind(
                        target, reason=UpdateReason.BACKFILL, scan_item=prefetched
                    )
                subscription._flush_deferred()
                if not bound:
                    if force_source is None:
                        raise ValueError(f"No data source can serve scan '{scan}'.")
                    raise ValueError(f"Data source '{force_source}' cannot serve scan '{scan}'.")
        except BaseException:
            subscription.close()
            raise
        return subscription

    def _check_source(self, force_source: str | None, scan: str | None = None) -> None:
        if force_source is None:
            return
        known = {path.data_source for path in self._paths}
        if force_source not in known:
            names = sorted(str(source) for source in known)
            raise ValueError(f"Unknown data source '{force_source}'; registered: {names}.")
        if scan == ScanTarget.LIVE and not self._candidate_paths(Scenario.OPEN_SCAN, force_source):
            raise ValueError(
                f"Data source '{force_source}' serves no open scans and cannot follow live scans."
            )

    def _candidate_paths(self, scenario: Scenario, force_source: str | None) -> list[DataPath]:
        """The scenario's routing table, restricted to the forced source if any."""
        table = self.data_paths.get(scenario, ())
        if force_source is None:
            return list(table)
        return [path for path in table if path.data_source == force_source]

    def estimate_bytes(
        self, sources: list[SourceKey], scan: str, force_source: str | None = None
    ) -> int | None:
        """
        Estimate the payload size of a prospective subscription.

        Args:
            sources (list[SourceKey]): Sources that would be subscribed.
            scan (str): Scan id to evaluate.
            force_source (str | None): Restrict the estimate to one data
                source, as :meth:`subscribe` would.

        Returns:
            int | None: Estimated bytes, or ``None`` if no data path can tell.
        """
        self._ensure_open()
        reject_reentrant_call(self._lock, "DataAPI.estimate_bytes()")
        self._check_source(force_source)
        # Scan storage takes the scan-manager RLock: read it BEFORE taking the
        # DataAPI lock (see the lock-order rule on ``Subscription._bind``).
        prefetched = self._peek_scan_item(scan) if scan else None
        with self._lock:
            context = self._bind_context(scan, prefetched, True, force_source)
            resolved = self._resolve_sources(list(sources), context, force_source)
            return total_estimated_bytes(resolved[0]) if resolved is not None else None

    def correlation_groups(
        self,
        sources: list[SourceKey],
        scan: str | None = ScanTarget.LIVE,
        correlation: CorrelationMode | str = CorrelationMode.ORDINAL,
        force_source: str | None = None,
    ) -> dict[CorrelationGroup, list[SourceKey]]:
        """
        Tell which sources can be aligned with each other — what can be plotted with what.

        Sources in the same group share an ordinal (scan point, AsyncMultiSignal message)
        and can be paired row by row; sources in different groups cannot. Nothing is
        subscribed or read.

        Args:
            sources (list[SourceKey]): The (device, entry) pairs to classify.
            scan (str | None): ``ScanTarget.LIVE`` (the running scan; without one, an
                optimistic guess from the device configuration: any device a
                scan can read per point, e.g. a ``baseline`` motor it promotes,
                counts as the scan group), a concrete scan id, or ``None`` for
                scan-less device streams.
            correlation (CorrelationMode): ``"continuous"`` puts every source in one group.
            force_source (str | None): Classify as if pinned to this data source.

        Returns:
            dict[CorrelationGroup, list[SourceKey]]: Group -> sources: ``SCAN_GROUP`` for
                the monitored signals, one ``MULTI`` group per AsyncMultiSignal and one
                ``STANDALONE`` group per source that cannot be paired.
        """
        self._ensure_open()
        reject_reentrant_call(self._lock, "DataAPI.correlation_groups()")
        sources = [tuple(source) for source in sources]
        if not sources:
            return {}
        if correlation == CorrelationMode.CONTINUOUS:
            return partition_correlation_groups(
                [(key, SourceKind.UNINDEXED, None) for key in sources], correlation=correlation
            )
        specs = self._describe_specs(sources, scan, force_source)
        return partition_correlation_groups(
            [(spec.key, spec.kind or SourceKind.UNINDEXED, spec.multi_signal) for spec in specs]
        )

    def describe(
        self,
        sources: list[SourceKey],
        scan: str | None = ScanTarget.LIVE,
        correlation: CorrelationMode | str = CorrelationMode.ORDINAL,
        force_source: str | None = None,
    ) -> dict[SourceKey, SourceInfo]:
        """
        Tell what a subscription to ``sources`` would get, without subscribing or reading.

        For every source: whether a data path serves it, which one, its kind and the
        correlation group it would be delivered in. This is the question to ask before
        offering a source in a picker or accepting it for a plot; it covers sources of
        registered data paths that BEC's device manager does not know.

        Args:
            sources (list[SourceKey]): The (device, entry) pairs to describe.
            scan (str | None): ``ScanTarget.LIVE`` (the running scan; without one, the device
                configuration plus what the data paths announce for the next scan), a
                concrete scan id, or ``None`` for scan-less device streams.
            correlation (CorrelationMode): ``"continuous"`` puts every source in one group.
            force_source (str | None): Describe as if pinned to this data source.

        Returns:
            dict[SourceKey, SourceInfo]: One entry per requested source, in request order.
        """
        self._ensure_open()
        reject_reentrant_call(self._lock, "DataAPI.describe()")
        sources = list(dict.fromkeys(tuple(source) for source in sources))
        if not sources:
            return {}
        specs = self._describe_specs(sources, scan, force_source)
        out = {}
        for spec in specs:
            if correlation == CorrelationMode.CONTINUOUS:
                group = CONTINUOUS_GROUP
            else:
                group = correlation_group(
                    spec.key, spec.kind or SourceKind.UNINDEXED, spec.multi_signal
                )
            out[spec.key] = SourceInfo(
                device=spec.device,
                entry=spec.entry,
                available=spec.available,
                kind=spec.kind,
                group=group,
                data_source=str(spec.data_source) if spec.available and spec.data_source else None,
                estimated_bytes=spec.estimated_bytes if spec.available else None,
            )
        return out

    def knows_scan(self, scan_id: str, force_source: str | None = None) -> bool:
        """
        Whether BEC or a registered data path knows the scan (open or closed).

        Args:
            scan_id (str): The scan id.
            force_source (str | None): Only ask this data source's path about scans
                that BEC itself does not know.

        Returns:
            bool: ``False`` for an id that nobody can describe.
        """
        self._ensure_open()
        reject_reentrant_call(self._lock, "DataAPI.knows_scan()")
        if not isinstance(scan_id, str) or not scan_id or scan_id == DEVICE_SCOPE:
            return False
        prefetched = self._peek_scan_item(scan_id)
        with self._lock:
            return self._classify(scan_id, prefetched, True, force_source) != Scenario.UNKNOWN

    def _describe_specs(
        self, sources: list[SourceKey], scan: str | None, force_source: str | None
    ) -> list[SourceSpec]:
        """Resolve ``sources`` for ``scan`` as a subscription would; nothing is opened."""
        if scan is None:
            scan_id, scan_item = DEVICE_SCOPE, None
        elif scan == ScanTarget.LIVE:
            live = self._current_live_scan()
            scan_id, scan_item = live if live is not None else (None, None)
        else:
            scan_id, scan_item = scan, self._peek_scan_item(scan)
        if scan_id is not None:
            with self._lock:
                context = self._bind_context(scan_id, scan_item, True, force_source)
                resolved = self._resolve_sources(sources, context, force_source)
            if resolved is not None:
                return resolved[0]
        # No scan to ask: classify from the device configuration, then let the data paths
        # announce what they would add to the next scan.
        specs = {spec.key: spec for spec in self._config_specs(sources)}
        if scan == ScanTarget.LIVE:
            with self._lock:
                for path in self._candidate_paths(Scenario.OPEN_SCAN, force_source):
                    pending = [key for key, spec in specs.items() if not spec.available]
                    if not pending:
                        break
                    try:
                        announced = path.describe_sources(pending)
                    except Exception as exc:  # pylint: disable=broad-except
                        self._path_failed(path, "describe_sources", exc)
                        continue
                    for spec in announced or ():
                        if spec.key in pending and spec.available:
                            specs[spec.key] = replace(
                                spec, path_name=type(path).__name__, data_source=path.data_source
                            )
        return [specs[key] for key in sources]

    def _config_specs(self, sources: list[SourceKey]) -> list[SourceSpec]:
        """
        Classify sources as the next scan could, from the device configuration.

        Only async and continuous devices are fixed by their configuration: a scan
        reads its motors per point whatever their configured priority (``baseline``
        motors are promoted to ``monitored``) and may override any other device's.
        Every other device therefore counts as a possible member of the scan group;
        the bound subscription's :attr:`Subscription.groups` is authoritative.
        """
        per_point = []
        for name in dict.fromkeys(device for device, _ in sources):
            device = client_views.device(self.client, name)
            if device is not None and device.readout_priority not in _FIXED_READOUT:
                per_point.append(name)
        return open_scan_specs(self.client, sources, {"monitored": per_point})

    def _bind_context(
        self,
        scan_id: str,
        scan_item: Any,
        have_item: bool,
        force_source: str | None,
        size_limit_bytes: int | None = None,
        view_limit_bytes: int | None = None,
    ) -> BindContext:
        """Build the context a scan is routed under (see ``BindContext``). Lock held."""
        return BindContext(
            scan_id=scan_id,
            scenario=self._classify(scan_id, scan_item, have_item, force_source),
            scan_item=scan_item if have_item else None,
            have_item=have_item,
            size_limit_bytes=size_limit_bytes,
            view_limit_bytes=view_limit_bytes,
        )

    def _classify(
        self, scan_id: str, scan_item: Any, have_item: bool, force_source: str | None
    ) -> Scenario:
        scenario = classify(self.client, scan_id, scan_item, have_item)
        if scenario != Scenario.UNKNOWN:
            return scenario
        candidates = sorted(self._paths, key=lambda path: path.priority)
        for path in candidates:
            if force_source is not None and path.data_source != force_source:
                continue
            try:
                described = path.describe_scan(scan_id)
            except Exception as exc:  # pylint: disable=broad-except
                self._path_failed(path, "describe_scan", exc)
                continue
            if described is None:
                continue
            if described not in (Scenario.OPEN_SCAN, Scenario.CLOSED_SCAN):
                self._path_failed(
                    path, "describe_scan", ValueError(f"invalid scan scenario {described!r}")
                )
                continue
            return described
        return Scenario.UNKNOWN

    def _resolve_sources(
        self, sources: list[SourceKey], context: BindContext, force_source: str | None
    ) -> tuple[list[SourceSpec], dict[SourceKey, DataPath]] | None:
        """Resolve each key independently, preserving priority and unavailable evidence."""
        pending = list(dict.fromkeys(sources))
        specs: dict[SourceKey, SourceSpec] = {}
        paths: dict[SourceKey, DataPath] = {}
        # Claims of paths that cannot tell without reading; kept for the keys nobody
        # with evidence serves (first path in priority order).
        provisional: dict[SourceKey, tuple[SourceSpec, DataPath]] = {}
        claimed = False
        for path in self._candidate_paths(context.scenario, force_source):
            try:
                resolved = path.resolve(pending, context)
            except Exception as exc:  # pylint: disable=broad-except
                # A failing path declines; the next path may still serve the sources.
                self._path_failed(path, "resolve", exc)
                continue
            if not resolved:
                continue  # None, or no spec at all: the path serves none of these sources
            keys = [spec.key for spec in resolved]
            if len(set(keys)) != len(keys) or not set(keys) <= set(pending):
                # A contract violation declines like an exception: other paths still serve.
                self._path_failed(
                    path,
                    "resolve",
                    ValueError("resolve must return at most one spec per requested source"),
                )
                continue
            claimed = True
            for spec in resolved:
                spec = replace(spec, path_name=type(path).__name__, data_source=path.data_source)
                if spec.available and spec.provisional:
                    provisional.setdefault(spec.key, (spec, path))
                elif spec.available:
                    specs[spec.key] = spec
                    paths[spec.key] = path
                elif spec.key not in specs or specs[spec.key].kind is None:
                    specs[spec.key] = spec
            pending = [key for key in pending if key not in paths]
            if not pending:
                break
        if not claimed:
            return None
        for key, (spec, path) in provisional.items():
            if key not in paths:
                specs[key] = spec
                paths[key] = path
        ordered = [
            specs.get(key, SourceSpec(*key, available=False)) for key in dict.fromkeys(sources)
        ]
        # A subscription's limit applies to the aggregate across adapters, not
        # independently to each path's portion. Preserve stricter adapter gates.
        if (
            context.size_limit_bytes is not None
            and (total_estimated_bytes(ordered) or 0) > context.size_limit_bytes
        ):
            apply_size_gate(ordered, context.size_limit_bytes)
        return ordered, paths

    def refresh(self, scan_id: str | None = None) -> None:
        """
        Re-route subscriptions after a data path's own catalog changed.

        A data path calls this — outside the DataAPI lock, after updating its cached
        metadata — when a scan appeared, ended or vanished, or a source became
        (un)available. Only subscriptions whose routing actually changed are re-bound,
        so unrelated subscriptions keep their data. A ``"live"`` subscription moves to an
        open ``scan_id`` if at least one of its sources is available there and either
        ``scan_id`` is the scan ``subscribe(scan="live")`` would pick now (BEC's running
        scan first, then the paths' ``current_open_scan()``) or its current binding
        serves none of its sources. Consumer callbacks may run on the calling thread.

        Args:
            scan_id (str | None): The scan whose description changed, or ``None`` for
                scan-less (``scan=None``) subscriptions.
        """
        if self.closed:
            return  # paths' background threads may still report changes after close()
        reject_reentrant_call(self._lock, "DataAPI.refresh()")
        target = DEVICE_SCOPE if scan_id is None else scan_id
        prefetched = self._peek_scan_item(scan_id) if scan_id else None
        # Followers follow the live scan, never just any scan a path describes as open.
        live = self._current_live_scan() if scan_id is not None else None
        follow_target = live is not None and live[0] == scan_id
        with self._lock:
            subscriptions = list(self._subscriptions)
            for subscription in subscriptions:
                if subscription._closed:
                    continue
                try:
                    if subscription.scan_id == target:
                        subscription._reresolve(target, UpdateReason.REBIND, scan_item=prefetched)
                    elif (
                        subscription._follow
                        and (follow_target or not subscription._serves_any())
                        and self._classify(scan_id, prefetched, True, subscription.force_source)
                        == Scenario.OPEN_SCAN
                    ):
                        subscription._reresolve(
                            scan_id,
                            UpdateReason.REBIND,
                            scan_item=prefetched,
                            require_available=True,
                        )
                except Exception:  # pylint: disable=broad-except
                    logger.exception(f"Refreshing subscription for scan {scan_id!r} failed.")
        for subscription in subscriptions:
            subscription._flush_deferred()

    def _current_live_scan(self) -> tuple[str, Any] | None:
        """
        Read the currently running scan (id, item) from the queue.

        Takes the scan-manager RLock internally — must be called WITHOUT the
        DataAPI lock held (see the lock-order rule on ``Subscription._bind``).
        When BEC runs no scan, the data paths serving open scans are asked for a
        scan they stream live (``DataPath.current_open_scan``), in priority order.
        """
        live = client_views.current_live_scan(self.client)
        if live is not None:
            return live
        with self._lock:
            for path in self.data_paths.get(Scenario.OPEN_SCAN, ()):
                try:
                    scan_id = path.current_open_scan()
                except Exception as exc:  # pylint: disable=broad-except
                    self._path_failed(path, "current_open_scan", exc)
                    continue
                if scan_id:
                    return scan_id, None
        return None

    def _peek_scan_item(self, scan_id: str) -> Any:
        """Lock-free-caller read of one scan item (same lock-order rule)."""
        return client_views.scan_item(self.client, scan_id)

    def close(self) -> None:
        """Close subscriptions, disconnect paths, and remove this facade from the registry."""
        with self._lock:
            if self.closed:
                return
            self._closed = True
            subscriptions = list(self._subscriptions)
        # Subscription.close removes callbacks and joins workers outside the
        # facade lock. Cancel requests before disconnecting their data paths.
        for subscription in subscriptions:
            try:
                subscription.close()
            except Exception:  # pylint: disable=broad-except
                logger.exception("Closing a subscription failed during DataAPI.close().")
        for path in self.paths:
            try:
                path.disconnect()
            except Exception as exc:  # pylint: disable=broad-except
                self._path_failed(path, "disconnect", exc)
        self._stop_reaper()
        self._scheduler.stop()
        self.data_paths.clear()
        self._paths.clear()
        instances = type(self)._instances
        for key, instance in list(instances.items()):
            if instance is self:
                del instances[key]

    # --- path faults and worker reaping ------------------------------------------

    def _path_failed(self, path: DataPath, operation: str, exc: BaseException) -> None:
        """Log a data path failure: in full the first time, briefly afterwards."""
        key = (type(path).__name__, operation)
        if key in self._reported_failures:
            logger.warning(f"{key[0]}.{operation}() failed again: {exc!r}")
            return
        self._reported_failures.add(key)
        logger.opt(exception=exc).error(f"{key[0]}.{operation}() failed; the path is skipped.")

    def _join_requests(
        self, requests: list[SourceRequest], timeout: float, warn: bool = True
    ) -> bool:
        """
        Join the requests' workers outside the lock, within one shared deadline.

        Returns:
            bool: ``False`` when the deadline passed before every path was joined.
        """
        deadline = time.monotonic() + timeout
        for request in requests:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                request.path.join(request, timeout=remaining)
            except Exception as exc:  # pylint: disable=broad-except
                self._path_failed(request.path, "join", exc)
        if not requests or time.monotonic() < deadline:
            return True
        if warn:
            logger.warning(
                f"Workers of {len(requests)} released data request(s) did not finish within "
                f"{timeout}s; they keep running in the background."
            )
        return False

    def _reap(self, requests: list[SourceRequest]) -> None:
        """Join released requests on the reaper thread (never on the caller's)."""
        if not requests:
            return
        with self._reap_cv:
            self._reap_queue.extend(requests)
            if self._reaper is None:
                self._reaper = threading.Thread(
                    target=self._run_reaper, name=f"data-api-reap-{id(self.client):x}", daemon=True
                )
                self._reaper.start()
            self._reap_cv.notify()

    def _run_reaper(self) -> None:
        while True:
            with self._reap_cv:
                if not self._reap_queue:
                    self._reaper = None  # idle: the next _reap() starts a new thread
                    self._reap_cv.notify_all()
                    return
                request = self._reap_queue.popleft()
            self._join_requests([request], _REAP_TIMEOUT)

    def _stop_reaper(self, timeout: float = 5.0) -> None:
        """Wait until the reaper drained its queue and its thread ended."""
        deadline = time.monotonic() + timeout
        with self._reap_cv:
            thread = self._reaper
            while self._reaper is not None and self._reaper is not threading.current_thread():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning("DataAPI.close(): released data requests are still joining.")
                    return
                self._reap_cv.wait(remaining)
        if thread is not None and thread is not threading.current_thread():
            thread.join(max(0.0, deadline - time.monotonic()))

    def _ensure_open(self) -> None:
        if self.closed:
            raise RuntimeError("DataAPI is closed.")

    def __del__(self):
        try:
            self.close()
        except Exception:  # pragma: no cover - destructor safety
            pass
