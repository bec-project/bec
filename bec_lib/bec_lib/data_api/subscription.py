"""
Subscription lifecycle, routing and rate-limited delivery of immutable snapshots.

One reentrant DataAPI lock is shared with its paths and subscriptions. Callbacks
run outside that lock; Qt consumers should hand updates to a queued signal.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Callable

from bec_lib.logger import bec_logger

from .data_bundler import (
    CorrelationGroupError,
    CorrelationMode,
    DataBundler,
    partition_correlation_groups,
)
from .data_path import BindContext, DataPath, SourceRequest, SourceSpec, total_estimated_bytes
from .locking import OwnedLock
from .models import (
    ALIGN_LATER,
    CorrelationGroup,
    SourceKey,
    SourceKind,
    SubscriptionUpdate,
    UpdateReason,
    align_ordinals,
)
from .scenarios import DEVICE_SCOPE, TERMINAL_SCAN_STATES, ScanTarget

if TYPE_CHECKING:  # pragma: no cover
    from .data_api import DataAPI

logger = bec_logger.logger

# Subscription and DataAPI are cooperating private parts of the same facade.
# pylint: disable=protected-access

#: Sentinel distinguishing "no prefetch supplied" from a prefetched None.
_NO_PREFETCH = object()

#: close(): how long to wait for a callback still running on another thread.
_INFLIGHT_TIMEOUT = 1.0
#: close(): how long the caller waits, in total, for the data paths' per-request
#: threads; threads that need longer are joined by the facade's reaper.
_JOIN_TIMEOUT = 0.25
#: Scheduler key of deliveries handed over by a notify() made with the lock held.
_HANDOFF = "__handoff__"


def lock_held(lock: OwnedLock) -> bool:
    """Whether the calling thread holds the DataAPI lock."""
    return lock.held()


def reject_reentrant_call(lock, what: str) -> None:
    """
    Refuse a facade call made with the DataAPI lock held, i.e. from inside a data path.

    Such a call registers client callbacks or joins workers under the lock, which
    inverts the dispatcher's lock order and deadlocks; failing fast makes the bug visible.
    """
    if lock_held(lock):
        raise RuntimeError(
            f"{what} was called with the DataAPI lock held (from inside a data path). "
            "Data paths must never call the DataAPI facade; derive data inside the path."
        )


@dataclass
class _EmitSlot:
    """
    Per-group coalescing state.

    Invariant: while the scheduler runs, ``pending`` is not ``None`` exactly
    when one scheduler entry exists for this subscription and group.
    """

    #: Monotonic time of the last snapshot; 0.0 makes the first one immediate.
    last: float = 0.0
    pending: UpdateReason | None = None
    #: Snapshots taken for this binding of the group (delivery order key).
    seq: int = 0
    #: Reason of the first snapshot of this binding; adopted by the first
    #: *delivered* update if that snapshot was superseded before delivery.
    first_reason: UpdateReason | None = None


@dataclass
class _Delivery:
    """One snapshot on its way to the callback (built under the lock, delivered outside)."""

    group: CorrelationGroup
    generation: int
    seq: int
    update: SubscriptionUpdate
    callback: Callable[[SubscriptionUpdate], Any]
    first_reason: UpdateReason | None
    owner: int  # ident of the thread holding the in-flight token


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
        # Keep these options consistent with the public DataAPI.subscribe signature.
        # pylint: disable-next=duplicate-code
        max_points: int | None = None,
        size_limit_bytes: int | None = None,
        progress_callback: Callable[[float], None] | None = None,
        force_source: str | None = None,
        correlation: CorrelationMode | str = CorrelationMode.ORDINAL,
    ):
        self._api = api
        self._lock = api._lock
        self._callback = callback
        self._progress_callback = progress_callback
        self._force_source = force_source
        self._correlation: CorrelationMode = correlation
        self._sources: list[SourceKey] = [tuple(s) for s in sources]
        self._follow = scan == ScanTarget.LIVE
        self._scan_id: str | None = (
            DEVICE_SCOPE if scan is None else (None if self._follow else scan)
        )
        self._min_emit_interval = min_emit_interval
        self._max_points = max_points
        self._size_limit_bytes = size_limit_bytes
        self._closed = False

        self._requests: dict[CorrelationGroup, list[SourceRequest]] = (
            {}
        )  # group -> adapter requests
        self._specs: list[SourceSpec] = []

        # Emission: per-group coalescing slots, the client-wide scheduler for
        # trailing flushes, and an in-flight delivery count per thread so
        # close() can wait for callbacks running elsewhere.
        self._scheduler = api._scheduler
        self._slots: dict[CorrelationGroup, _EmitSlot] = {}
        # Set while the bound scan resolves on no path; the next resolution re-binds.
        self._unserved = False
        self._delivery_cv = threading.Condition()
        self._inflight: dict[int, int] = {}
        # Deliveries are never made with the DataAPI lock held: entry points
        # that hold it (bind, confirm_size, terminal flush) queue snapshots
        # here and flush them after releasing. Per group, deliveries are
        # ordered and a superseded snapshot is dropped.
        self._defer_depth = 0
        self._deferred: list[_Delivery] = []
        self._warned_locked_notify = False
        self._generation = 0
        self._group_locks: dict[CorrelationGroup, threading.RLock] = {}
        self._delivered: dict[CorrelationGroup, tuple[int, int]] = {}
        # Requests released by a rebind; joined by the facade's reaper, outside the lock.
        self._retired: list[SourceRequest] = []
        # Load progress of the current binding, aggregated over its progress-reporting requests.
        self._progress_lock = threading.Lock()
        self._progress: dict[int, float] = {}
        self._progress_sent = 0.0
        api._subscriptions.add(self)

        # Bound methods only: the client's callback handler references them
        # weakly and drops the registration itself once the subscription is
        # garbage collected, so an abandoned live-follow subscription can
        # never be pinned by the client. Explicit close() still removes them.
        self._callback_ids: list[int | str] = []
        events: list[tuple[str, Callable]] = []
        if self._follow:
            events += [
                ("scan_status", self._on_scan_status),
                ("scan_history_update", self._on_scan_history_update),
            ]
        elif scan is None:
            # Device streams: a source unavailable at subscribe time (device
            # info not settled, config reload) is retried on the next update.
            events.append(("device_update", self._on_device_update))
        else:
            # Concrete scan: unavailable sources are retried and the live
            # state flushed on its own status updates; one bound from scan
            # memory in the writer-latency window is re-routed to the file
            # once it is published.
            events += [
                ("scan_status", self._on_scan_status),
                ("scan_history_update", self._on_scan_history_update),
            ]
        for event, handler in events:
            self._callback_ids.append(api.client.callbacks.register(event, handler))

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
    def generation(self) -> int:
        """Current binding revision, carried by every delivered update."""
        return self._generation

    @property
    def force_source(self) -> str | None:
        """The data source this subscription is pinned to (``None`` = any)."""
        return self._force_source

    @property
    def correlation(self) -> CorrelationMode:
        """How the sources are correlated into rows (``"ordinal"`` or ``"continuous"``)."""
        return self._correlation

    @property
    def unbound_sources(self) -> list[SourceKey]:
        """
        Sources that are declared but not currently delivering: the whole set
        while unbound, or the sources the serving data path marked unavailable.
        """
        with self._lock:
            if not self._requests:
                return list(self._sources)
            return [spec.key for spec in self._specs if not spec.available]

    def _serves_any(self) -> bool:
        """Whether the current binding delivers at least one source. Lock held."""
        return any(spec.available for spec in self._specs)

    @property
    def groups(self) -> dict[CorrelationGroup, list[SourceKey]]:
        """
        The correlation groups of the current binding: group -> sources.

        Sources of one group are aligned row by row and arrive in one update
        (``update.metadata["group"]``); sources of different groups cannot be paired.
        """
        with self._lock:
            if not self._specs:
                return {}
            return partition_correlation_groups(
                [(s.key, s.kind or SourceKind.UNINDEXED, s.multi_signal) for s in self._specs],
                correlation=self._correlation,
            )

    @property
    def routes(self) -> dict[SourceKey, str]:
        """The data source serving each bound source (unavailable sources are absent)."""
        with self._lock:
            return {spec.key: str(spec.data_source) for spec in self._specs if spec.available}

    @property
    def min_emit_interval(self) -> float:
        """Live-emission coalescing interval in seconds (0 = uncoalesced)."""
        return self._min_emit_interval

    def set_min_emit_interval(self, seconds: float) -> Subscription:
        """
        Change the live-emission coalescing interval.

        Takes effect from the next emission; an already scheduled trailing
        flush fires once with the previous delay.

        Args:
            seconds (float): New interval in seconds; 0 disables coalescing.

        Returns:
            Subscription: self, for chaining.
        """
        with self._lock:
            self._min_emit_interval = max(0.0, float(seconds))
        return self

    # --- source set ----------------------------------------------------------

    def set_sources(self, sources: list[SourceKey]) -> Subscription:
        """
        Atomically replace the source set.

        The bound scan is re-bound with the new set (first emission reason
        ``"rebind"``). If no data path can serve the new set — or the path
        fails to open it — the previous set and binding are restored and a
        ``ValueError`` is raised, so the subscription never silently serves a
        stale set.

        Args:
            sources (list[SourceKey]): New (device, entry) pairs.

        Returns:
            Subscription: This subscription, for chaining.

        Raises:
            RuntimeError: If the subscription is closed.
            CorrelationGroupError: If ``sources`` is empty.
            ValueError: If the new set cannot be served.
        """
        reject_reentrant_call(self._lock, "Subscription.set_sources()")
        if not sources:
            raise CorrelationGroupError("A subscription needs at least one source.")
        new_sources = [tuple(s) for s in sources]
        for _attempt in range(3):
            scan_id_hint = self._scan_id
            prefetched = self._api._peek_scan_item(scan_id_hint) if scan_id_hint else None
            with self._lock:
                if self._closed:
                    raise RuntimeError("Cannot change sources on a closed subscription")
                if self._scan_id != scan_id_hint:
                    continue  # a scan event re-bound meanwhile: prefetch its item instead
                previous = self._sources
                self._sources = new_sources
                scan_id = self._scan_id
                if scan_id is None:
                    return self
                error: Exception | None = None
                try:
                    bound = self._bind(scan_id, reason=UpdateReason.REBIND, scan_item=prefetched)
                except Exception as exc:  # pylint: disable=broad-except
                    bound, error = False, exc
                if not bound:
                    # Never serve a silently stale set: keep the previous set.
                    # When no path claimed the new one the previous binding is
                    # untouched; when the claiming path failed to open, _bind
                    # rolled the binding back, so re-bind the previous set.
                    self._sources = previous
                    if error is not None:
                        try:
                            restored = self._bind(
                                scan_id, reason=UpdateReason.REBIND, scan_item=prefetched
                            )
                        except Exception as restore_error:  # pylint: disable=broad-except
                            restored = False
                            logger.warning(
                                f"Restoring the previous sources of scan {scan_id} failed: "
                                f"{restore_error!r}"
                            )
                        if not restored:
                            self._teardown_requests()
                            if self._follow:
                                self._scan_id = None
                            logger.warning(
                                f"Scan {scan_id} can no longer be served; the subscription is unbound."
                            )
            self._flush_deferred()
            if error is not None:
                raise error
            if not bound:
                if self._force_source is None:
                    raise ValueError(
                        f"No data source can serve scan '{scan_id}' with the new sources."
                    )
                raise ValueError(
                    f"Data source '{self._force_source}' cannot serve scan '{scan_id}' "
                    "with the new sources."
                )
            return self
        raise RuntimeError("The bound scan kept changing while the sources were replaced.")

    # --- binding and routing -------------------------------------------------

    def _rebind_after_replace(self, old_path: DataPath) -> None:
        """Move this subscription off a data path that is being replaced."""
        scan_id = self._scan_id
        prefetched = self._api._peek_scan_item(scan_id) if scan_id else None
        with self._lock:
            if self._closed or not self._uses_path(old_path) or scan_id != self._scan_id:
                return
            try:
                bound = self._bind(scan_id, reason=UpdateReason.REBIND, scan_item=prefetched)
            except Exception as exc:  # pylint: disable=broad-except
                bound = False
                logger.warning(f"Cannot re-bind scan {scan_id} to the replacement path: {exc!r}")
            if not bound:
                self._teardown_requests()
                if self._follow:
                    self._scan_id = None
                logger.warning(
                    f"The data path serving scan {scan_id!r} was replaced by one that cannot "
                    "serve it; the subscription is unbound."
                )
        self._flush_deferred()

    def _bind(self, scan_id: str, reason: UpdateReason, scan_item: Any = _NO_PREFETCH) -> bool:
        """
        Route the subscription to a data path for the given scan. Lock held.

        ``reason`` is what the first emission of the new binding means to the
        consumer: ``"rebind"`` replaces an earlier binding (consumers reset
        per-scan state); any other reason lets the path's own first emission
        (``backfill`` / ``history``) stand.

        Lock-order rule: scan-storage accessors take the scan-manager RLock,
        which the dispatcher thread holds while waiting for this lock — so a
        thread that does not already hold it must NEVER call them while
        holding the DataAPI lock. Callers on such threads prefetch the scan
        item first and pass it via ``scan_item`` (None is a valid prefetch);
        dispatcher-side callers omit it. The item travels to the data paths in a
        :class:`BindContext`, so neither ``resolve`` nor ``open`` touches
        scan storage under this lock.
        """
        self._api._ensure_open()
        context = self._context(scan_id, scan_item)
        resolved = self._api._resolve_sources(self._sources, context, self._force_source)
        if resolved is None:
            return False
        return self._bind_resolved(scan_id, reason, context, *resolved)

    def _context(self, scan_id: str, scan_item: Any = _NO_PREFETCH) -> BindContext:
        """The bind context of ``scan_id`` (see :meth:`_bind` for the prefetch rule). Lock held."""
        have_item = scan_item is not _NO_PREFETCH
        return self._api._bind_context(
            scan_id,
            scan_item if have_item else None,
            have_item,
            self._force_source,
            self._size_limit_bytes,
        )

    def _reresolve(
        self,
        scan_id: str,
        reason: UpdateReason,
        scan_item: Any = _NO_PREFETCH,
        require_available: bool = False,
    ) -> bool:
        """
        Route to ``scan_id`` again, rebinding only if the routing changed. Lock held.

        The shared answer to every "something may have changed" event (a refresh by
        a data path, a device update, a history entry): unchanged scenario, paths
        and specs keep the binding and its data; a bound scan that no longer
        resolves is reported as an error instead of being kept silently stale.

        Args:
            scan_id (str): Scan to route to.
            reason (UpdateReason): First-update reason if a rebind happens.
            scan_item: Prefetched scan item (see :meth:`_bind`).
            require_available (bool): Only bind when at least one source is available.

        Returns:
            bool: Whether the subscription is bound to ``scan_id`` afterwards.
        """
        self._api._ensure_open()
        context = self._context(scan_id, scan_item)
        bound_here = self._scan_id == scan_id and bool(self._specs)
        if bound_here and not any(spec.size_gated for spec in self._specs):
            # The consumer already passed (or confirmed) the size gate for this scan:
            # re-routing it (e.g. live -> file when the scan ends) must not gate again.
            context = replace(context, size_limit_bytes=None)
        resolved = self._api._resolve_sources(self._sources, context, self._force_source)
        if resolved is None:
            if bound_here:
                self._mark_unserved(scan_id)
            return False
        specs, paths = resolved
        if bound_here and not self._unserved and self._same_routing(context, specs, paths):
            return True
        if require_available and not any(spec.available for spec in specs):
            return False
        return self._bind_resolved(scan_id, reason, context, specs, paths)

    def _same_routing(
        self, context: BindContext, specs: list[SourceSpec], paths: dict[SourceKey, DataPath]
    ) -> bool:
        """Whether a fresh resolution matches the current binding. Lock held."""
        requests = self._all_requests()
        current = {spec.key: request.path for request in requests for spec in request.specs}
        if paths != current:
            return False
        if any(request.context.scenario != context.scenario for request in requests):
            return False

        def routing_view(spec: SourceSpec) -> SourceSpec:
            # A confirmed size gate must not count as a change (it would re-gate).
            return replace(spec, size_gated=False, estimated_bytes=None)

        return [routing_view(s) for s in specs] == [routing_view(s) for s in self._specs]

    def _mark_unserved(self, scan_id: str) -> None:
        """Report that the bound scan is no longer served by any data path. Lock held."""
        self._unserved = True
        for request in self._all_requests():
            for spec in request.specs:
                if spec.available:
                    request.set_status(
                        spec.key, error=f"Scan {scan_id!r} is no longer served by any data path."
                    )
            self._emit_now(request, UpdateReason.LIVE)

    def _bind_resolved(
        self,
        scan_id: str,
        reason: UpdateReason,
        context: BindContext,
        specs: list[SourceSpec],
        paths: dict[SourceKey, DataPath],
    ) -> bool:
        """Replace the binding with a resolved routing (make before break). Lock held."""
        self._generation += 1
        generation = self._generation
        self._unserved = False
        # Detach the old requests first: from here on their notifications are
        # ignored, so a path flushing on close() cannot hide the new binding.
        retired = self._detach_requests()
        with self._progress_lock:
            self._progress = {}
            self._progress_sent = 0.0
        self._scan_id = scan_id
        self._specs = specs
        self._defer_depth += 1
        try:
            self._open_groups(scan_id, context, paths, reason, generation)
        except Exception:
            self._teardown_requests()
            self._release_requests(retired)
            self._scan_id = None if self._follow else scan_id
            raise
        finally:
            self._defer_depth -= 1
        # Make before break: release the old requests only after the new ones
        # are open, so a feed both use is never dropped in between.
        self._release_requests(retired)
        return True

    def _open_groups(
        self,
        scan_id: str,
        context: BindContext,
        paths: dict[SourceKey, DataPath],
        reason: UpdateReason,
        generation: int,
    ) -> None:
        """Create shared correlation bundles, then open one request per adapter/group."""
        specs = {spec.key: spec for spec in self._specs}
        for label, keys in partition_correlation_groups(
            [(s.key, s.kind or SourceKind.UNINDEXED, s.multi_signal) for s in self._specs],
            correlation=self._correlation,
        ).items():
            if self._closed or self._generation != generation:
                return
            if not any(key in paths for key in keys):
                continue
            bundle = DataBundler(
                scan_id, max_points=self._max_points, correlation=self._correlation
            )
            by_path: dict[DataPath, list[SourceSpec]] = {}
            for key in keys:
                spec = specs[key]
                bundle.get_series(spec.device, spec.entry, spec.kind or SourceKind.UNINDEXED)
                if key in paths:
                    by_path.setdefault(paths[key], []).append(spec)
            requests = [
                SourceRequest(
                    scan_id=scan_id,
                    group=label,
                    specs=path_specs,
                    bundle=bundle,
                    sink=self,
                    context=context,
                    path=path,
                    initial_reason=UpdateReason.REBIND if reason == UpdateReason.REBIND else None,
                )
                for path, path_specs in by_path.items()
            ]
            for request in requests:
                if self._progress_callback is not None and request.path.reports_progress:
                    request.progress_callback = self._progress_reporter(request, generation)
            # Publish every request before opening any of them. Synchronous
            # backfills then see all declared columns, including silent ones.
            self._requests[label] = requests
            self._slots[label] = _EmitSlot()
            for request in requests:
                if self._closed or self._generation != generation:
                    return
                request.path.open(request)

    def _all_requests(self) -> list[SourceRequest]:
        return [request for group in self._requests.values() for request in group]

    def _uses_path(self, path: DataPath) -> bool:
        return any(request.path is path for request in self._all_requests())

    def _active_request(self, request: SourceRequest) -> bool:
        return any(request is current for current in self._requests.get(request.group, ()))

    def _group_request(self, group: CorrelationGroup) -> SourceRequest | None:
        requests = self._requests.get(group)
        return requests[0] if requests else None

    @property
    def size_gated(self) -> bool:
        """Whether delivery is withheld by the serving path pending :meth:`confirm_size`."""
        with self._lock:
            return any(spec.size_gated for spec in self._specs)

    @property
    def estimated_bytes(self) -> int | None:
        """Estimated payload size of the bound scan, if the serving path knows it."""
        with self._lock:
            return total_estimated_bytes(self._specs)

    def confirm_size(self) -> Subscription:
        """
        Load a subscription that was withheld by the size limit.

        Delegates to the serving path's ``confirm_size``; a file read then
        runs on the path's worker thread, so this returns immediately and
        never blocks the caller (GUI) thread.

        Returns:
            Subscription: This subscription, for chaining.
        """
        # A request bound on a dispatcher thread carries no scan item; the
        # path's open() would read scan storage under our lock on this
        # (GUI) thread — prefetch here, outside the lock, like subscribe().
        scan_id = self._scan_id
        prefetched = self._api._peek_scan_item(scan_id) if scan_id else None
        with self._lock:
            if self._closed:
                return self
            self._defer_depth += 1
            try:
                for request in self._all_requests():
                    if not request.size_gated:
                        continue
                    if not request.context.have_item:
                        request.context = replace(
                            request.context, scan_item=prefetched, have_item=True
                        )
                    request.path.confirm_size(request)
            finally:
                self._defer_depth -= 1
        self._flush_deferred()
        return self

    def _detach_requests(self) -> list[SourceRequest]:
        """Detach the current requests from emission and return them. Lock held."""
        requests = self._all_requests()
        self._requests.clear()
        self._specs = []
        self._scheduler.cancel(self)
        self._slots.clear()
        return requests

    def _release_requests(self, requests: list[SourceRequest]) -> None:
        """Let the paths release detached requests; their workers are joined later. Lock held."""
        for request in requests:
            try:
                request.path.release(request)
            except Exception as exc:  # pylint: disable=broad-except
                self._api._path_failed(request.path, "close", exc)
        self._retired.extend(requests)

    def _teardown_requests(self) -> None:
        self._release_requests(self._detach_requests())

    def _progress_reporter(
        self, request: SourceRequest, generation: int
    ) -> Callable[[float], None]:
        """
        Per-request progress callback feeding one monotonic 0..1 for the subscription.

        The subscription's fraction is the mean over the progress-reporting requests of
        the binding, so it reaches 1.0 only when all of them are done.
        """
        with self._progress_lock:
            self._progress[id(request)] = 0.0

        def report(fraction: float) -> None:
            callback = self._progress_callback
            if callback is None or self._closed or generation != self._generation:
                return
            with self._progress_lock:
                if id(request) not in self._progress:
                    return
                current = min(1.0, max(0.0, float(fraction)))
                self._progress[id(request)] = max(self._progress[id(request)], current)
                total = sum(self._progress.values()) / len(self._progress)
                if total <= self._progress_sent:
                    return
                self._progress_sent = total
            try:
                callback(total)
            except Exception:  # pylint: disable=broad-except
                logger.exception("Subscription progress callback failed.")

        return report

    def _on_scan_status(self, scan_status: dict, _metadata: dict) -> None:
        with self._lock:
            self._handle_scan_status(scan_status)
        self._flush_deferred()

    def _handle_scan_status(self, scan_status: dict) -> None:
        if self._closed:
            return
        scan_id = scan_status.get("scan_id")
        status = scan_status.get("status")
        if not scan_id:
            return
        if scan_id != self._scan_id:
            if self._follow and status == "open":
                try:
                    if not self._bind(scan_id, reason=UpdateReason.REBIND):
                        logger.warning(f"No data source can serve scan {scan_id}; waiting.")
                except Exception as exc:  # pylint: disable=broad-except
                    # A failing data path must not take the dispatcher down;
                    # _bind rolled the binding back, the next scan re-binds.
                    logger.warning(f"Cannot bind scan {scan_id}: {exc!r}")
            return
        if status == "open" and any(not spec.available for spec in self._specs):
            # A source that resolved unavailable at bind time (e.g. device
            # info still settling) must not stay lost for the whole scan:
            # retry on the next status update of the same scan.
            try:
                self._bind(scan_id, reason=UpdateReason.REBIND)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning(f"Cannot rebind scan {scan_id}: {exc!r}")
            return
        if status in TERMINAL_SCAN_STATES:
            # Final flush of the live state; the authoritative history
            # re-route happens when the scan-history entry appears.
            for label in list(self._requests):
                request = self._group_request(label)
                if request is not None:
                    self._emit_now(request, UpdateReason.LIVE)

    def _on_device_update(self, *_args, **_kwargs) -> None:
        with self._lock:
            if self._closed or self._scan_id != DEVICE_SCOPE or not self._specs:
                return
            if not any(not spec.available for spec in self._specs):
                return
            # Re-bind (which rebuilds the bundles) only when the routing changed;
            # a permanently unknown device must not wipe the accumulated points
            # on every config event.
            try:
                self._reresolve(DEVICE_SCOPE, UpdateReason.REBIND, scan_item=None)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning(f"Cannot rebind device streams: {exc!r}")
        self._flush_deferred()

    def _on_scan_history_update(self, history_msg=None, **_kwargs) -> None:
        with self._lock:
            self._handle_scan_history_update(history_msg)
        self._flush_deferred()

    def _handle_scan_history_update(self, history_msg) -> None:
        if self._closed:
            return
        if history_msg is None or history_msg.scan_id != self._scan_id:
            return
        scan_id = history_msg.scan_id
        try:
            # Re-routes a scan served from scan memory (or live) to its file;
            # a scan already served from the file is left alone (no repeated
            # bulk reads). The history thread holds no scan-manager lock: pass
            # an explicit empty prefetch instead of reading scan storage.
            self._reresolve(scan_id, UpdateReason.HISTORY, scan_item=None)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Cannot re-route scan {scan_id} to history: {exc!r}")

    # --- emission ------------------------------------------------------------

    def request_emission(
        self,
        request: SourceRequest,
        reason: UpdateReason,
        immediate: bool = False,
        coalesce: bool = False,
    ) -> None:
        """
        Emit (or schedule) an update of one correlation group.

        Entry point of :class:`~.data_path.EmissionSink`, called by the data
        paths through ``request.notify(reason)`` on any thread. ``UpdateReason.LIVE``
        updates, and the partial updates of a load (``coalesce``), are coalesced
        per group to at most one per
        ``min_emit_interval``; the first notification in a window emits at
        once, later ones arm one trailing flush delivered by the client's
        scheduler thread. Every other notification emits synchronously. Snapshots
        are built under the lock and delivered after it is released; an
        entry point that holds the lock (bind, confirm_size, terminal flush)
        queues them and flushes once it has released it. A path that notifies
        while still holding the lock gets its delivery handed to the scheduler
        thread, so consumer code never runs under the DataAPI lock.

        Args:
            request (SourceRequest): The notifying request; a request torn
                down by a rebind is ignored.
            reason (UpdateReason): Why the consumer should be updated.
            immediate (bool): Deliver now, bypassing coalescing (a final flush).
            coalesce (bool): Coalesce this update like a ``"live"`` one.
        """
        reason = UpdateReason(reason)  # a sink may be handed the plain string
        caller_holds_lock = lock_held(self._lock)
        with self._lock:
            if self._closed or self._callback is None:
                return
            if not self._active_request(request):
                return
            slot = self._slots[request.group]
            now = time.monotonic()
            interval = self._min_emit_interval
            if slot.seq == 0 and reason != UpdateReason.LIVE and request.initial_reason is not None:
                reason = request.initial_reason  # first snapshot of a re-bound request
            if (
                not immediate
                and (reason == UpdateReason.LIVE or coalesce)
                and interval > 0
                and now - slot.last < interval
            ):
                if slot.pending is not None and not self._scheduler.stopped:
                    slot.pending = reason  # window already armed
                    return
                if self._scheduler.schedule(
                    self, request.group, slot.last + interval, self._flush_group
                ):
                    slot.pending = reason
                    return
                # Scheduler stopped (DataAPI.close): deliver rather than lose the state.
            delivery = self._snapshot(request, slot, reason, now)
            if self._defer_depth:
                self._deferred.append(delivery)
                return
            if caller_holds_lock:
                self._hand_off(delivery)
                return
        self._deliver(delivery)

    def _hand_off(self, delivery: _Delivery) -> None:
        """Deliver from the scheduler thread: the notifying path still holds the lock. Lock held."""
        if not self._warned_locked_notify:
            self._warned_locked_notify = True
            logger.warning(
                "A data path called notify() with the DataAPI lock held; the update is "
                "delivered from the scheduler thread instead. Release the lock first "
                "(request.batch() does)."
            )
        self._deferred.append(delivery)
        # A stopped scheduler means the DataAPI is closing: close() drops the queue.
        self._scheduler.schedule(self, _HANDOFF, time.monotonic(), self._flush_handoff)

    def _flush_handoff(self, _key: str) -> None:
        """Deliver the snapshots handed over by notify() calls made under the lock (scheduler)."""
        self._flush_deferred()

    def _flush_group(self, group: CorrelationGroup) -> None:
        """Deliver the trailing update of one group (scheduler thread)."""
        with self._lock:
            if self._closed or self._callback is None:
                return
            request, slot = self._group_request(group), self._slots.get(group)
            if request is None or slot is None or slot.pending is None:
                return  # emitted early or torn down meanwhile
            reason, slot.pending = slot.pending, None
            delivery = self._snapshot(request, slot, reason, time.monotonic())
            if self._defer_depth:
                self._deferred.append(delivery)
                return
        self._deliver(delivery)

    def _emit_now(self, request: SourceRequest, reason: UpdateReason) -> None:
        """Queue an immediate snapshot of one group (drops its pending trailing flush). Lock held."""
        if self._closed or self._callback is None or not self._active_request(request):
            return
        slot = self._slots.get(request.group)
        if slot is None:
            return
        self._deferred.append(self._snapshot(request, slot, reason, time.monotonic()))

    def _snapshot(
        self, request: SourceRequest, slot: _EmitSlot, reason: UpdateReason, now: float
    ) -> _Delivery:
        """Build the update and take a delivery token. Lock held."""
        if slot.pending is not None:
            slot.pending = None
            self._scheduler.cancel(self, request.group)
        slot.last = now
        if slot.seq == 0:
            slot.first_reason = reason
        slot.seq += 1
        update = replace(
            request.bundle.build_update(
                reason, metadata={"group": request.group}, defer_alignment=True
            ),
            generation=self._generation,
        )
        owner = threading.get_ident()
        with self._delivery_cv:
            self._inflight[owner] = self._inflight.get(owner, 0) + 1
        return _Delivery(
            request.group,
            self._generation,
            slot.seq,
            update,
            self._callback,
            slot.first_reason,
            owner,
        )

    def _flush_deferred(self) -> None:
        """Deliver the snapshots queued while the lock was held. Called WITHOUT the lock."""
        with self._lock:
            items, self._deferred = self._deferred, []
            retired, self._retired = self._retired, []
        for delivery in items:
            self._deliver(delivery)
        self._api._reap(retired)

    def _deliver(self, delivery: _Delivery) -> None:
        """
        Hand one snapshot to the callback, in per-group order.

        Deliveries of one group are serialized; a snapshot older than the
        last delivered one is dropped (every update is full state, so the
        newest wins). If the first snapshot of a binding was superseded that
        way, the first delivered update adopts its reason (``backfill``,
        ``rebind``, ``history``) so consumers still see the transition. A
        raising callback is logged and never alters the subscription.
        """
        me = threading.get_ident()
        with self._delivery_cv:
            lock = self._group_locks.setdefault(delivery.group, threading.RLock())
            if delivery.owner != me:
                # Built by another thread: the token follows the delivery, so a close()
                # on the building thread waits for this callback too.
                self._inflight[me] = self._inflight.get(me, 0) + 1
                depth = self._inflight.get(delivery.owner, 0) - 1
                if depth <= 0:
                    self._inflight.pop(delivery.owner, None)
                else:
                    self._inflight[delivery.owner] = depth
                delivery.owner = me
        try:
            with lock:
                key = (delivery.generation, delivery.seq)
                with self._delivery_cv:
                    last = self._delivered.get(delivery.group)
                    stale = last is not None and key <= last
                    first = last is None or last[0] != delivery.generation
                    if not stale:
                        self._delivered[delivery.group] = key
                if stale or self._closed or delivery.generation != self._generation:
                    return
                update = delivery.update
                if update.aligned_ordinals is ALIGN_LATER:
                    # Aligned here, outside the DataAPI lock, and only for
                    # snapshots that are actually delivered.
                    update = replace(update, aligned_ordinals=align_ordinals(update.sources))
                if (
                    first
                    and update.reason == UpdateReason.LIVE
                    and delivery.first_reason is not None
                ):
                    update = replace(update, reason=delivery.first_reason)
                try:
                    delivery.callback(update)
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        f"Subscription callback failed for group '{delivery.group}' "
                        f"(scan {update.scan_id!r}, reason {update.reason!r})."
                    )
        finally:
            self._end_delivery(delivery.owner)

    def _end_delivery(self, owner: int) -> None:
        with self._delivery_cv:
            depth = self._inflight.get(owner, 0) - 1
            if depth <= 0:
                self._inflight.pop(owner, None)
            else:
                self._inflight[owner] = depth
            self._delivery_cv.notify_all()

    def _await_deliveries(self, timeout: float) -> bool:
        """Wait for callbacks running on other threads. Called WITHOUT the lock."""
        me = threading.get_ident()  # a close() from inside a callback never waits for itself
        with self._delivery_cv:
            deadline = time.monotonic() + timeout
            while any(ident != me for ident in self._inflight):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        f"Subscription.close(): a callback is still running after {timeout}s."
                    )
                    return False
                self._delivery_cv.wait(remaining)
        return True

    # --- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """
        Close the subscription and release all resources (idempotent).

        No callback runs after this returns: new deliveries are refused once
        ``_closed`` is set under the lock, deliveries already in flight on
        other threads are waited for (bounded), and the data path's worker
        threads are joined outside the lock: briefly here, then by the facade's
        reaper thread if they are still inside a blocking read.
        """
        reject_reentrant_call(self._lock, "Subscription.close()")
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._teardown_requests()
            finally:
                requests, self._retired = self._retired, []
                callback_ids, self._callback_ids = self._callback_ids, []
                self._callback = None
                self._progress_callback = None
                dropped, self._deferred = self._deferred, []
        for delivery in dropped:  # queued by this thread's entry point: never delivered
            self._end_delivery(delivery.owner)
        # Never call into the client's callback handler under the DataAPI lock:
        # it holds its own lock while dispatching to our handlers, which take
        # ours — removing under our lock inverts that order into a deadlock. A
        # handler invoked in between returns at once on ``_closed``.
        for callback_id in callback_ids:
            try:
                self._api.client.callbacks.remove(callback_id)
            except Exception:  # pylint: disable=broad-except
                logger.exception(f"Removing subscription callback {callback_id!r} failed.")
        self._await_deliveries(_INFLIGHT_TIMEOUT)
        # The paths join their own threads outside the lock (a file worker needs the
        # lock for its final insert). The caller (often the GUI thread) only waits a
        # moment: a cancelled worker publishes nothing, so one still inside a blocking
        # read is left to the reaper.
        if not self._api._join_requests(requests, _JOIN_TIMEOUT, warn=False):
            self._api._reap(requests)

    def __del__(self):
        if getattr(self, "_closed", True):
            return
        try:
            if lock_held(self._lock):
                # Collected while this thread holds the DataAPI lock: release the
                # requests, but leave callbacks (weakly registered) and joins alone.
                self._closed = True
                self._teardown_requests()
                self._api._reap(self._retired)
                self._retired = []
                return
            self.close()
        except Exception:  # pragma: no cover - destructor safety
            pass

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
