"""
Data path contract of the DataAPI.

A *data path* is one way of reaching BEC data — Redis, the scan file, a REST
service, … — and it declares which :class:`~.scenarios.Scenario` values it
can serve. The facade decides the scenario first (deterministically, see
:mod:`.scenarios`), then consults that scenario's data paths in ascending
``priority`` order for each requested source. Unavailable specs fall through
to the next path. Sources served by different paths can share one correlation
bundle; each path owns only its own requests and feeds.
"""

from __future__ import annotations

import enum
import threading
import time
import weakref
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Protocol

from bec_lib.logger import bec_logger

from .data_bundler import DataBundler, SourceSeries
from .locking import OwnedLock
from .models import CorrelationGroup, DeviceStream, SourceKey, SourceKind, UpdateReason
from .scenarios import Scenario

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

    from .data_api import DataAPI

logger = bec_logger.logger


class DataSource(enum.StrEnum):
    """
    Where a data path reads from. Core paths use these values; external
    paths may declare any string (the enum is a ``str`` subclass whose
    members format as their value, so comparisons, messages and
    ``force_source`` accept both).
    """

    REDIS = "redis"
    FILE = "file"


@dataclass
class SourceSpec:
    """Resolved description of one requested source within one scan."""

    device: str
    entry: str
    kind: SourceKind | None = None
    #: Storage name of the ``AsyncMultiSignal`` this source is a sub-signal of. Its
    #: sub-signals are emitted in the same messages, so they share ordinals and one
    #: correlation group.
    multi_signal: str | None = None
    #: Name the data is stored under when it differs from ``entry`` (async
    #: signals in the scan file are keyed by their storage name).
    storage_name: str | None = None
    #: Scan-less device stream the source is read from (``"readback"``,
    #: ``"configuration"`` or ``"preview"``); ``None`` for scan data.
    stream: DeviceStream | None = None
    available: bool = True
    #: The path may have the source but cannot tell without reading (a catalog entry
    #: that does not list its datasets). A path that has evidence for the source is
    #: asked first whatever its priority; the provisional claim stands if none has.
    provisional: bool = False
    estimated_bytes: int | None = None
    #: Set by the serving path in ``resolve`` (:func:`apply_size_gate`) on the
    #: available specs when their total exceeds ``BindContext.size_limit_bytes``.
    #: :meth:`DataPath.open` withholds a gated request; :meth:`DataPath.confirm_size`
    #: clears the flag and opens it.
    size_gated: bool = False
    #: Class name of the serving data path; stamped by the facade at bind time.
    path_name: str | None = None
    #: ``DataPath.data_source`` of the serving path; stamped by the facade.
    data_source: str | None = None
    #: Data revision as the serving path sees it (e.g. a catalog version). A different
    #: revision on ``refresh()`` re-binds the source, so a path whose stored data grew or
    #: changed only bumps it; ``None`` when the path does not track revisions.
    revision: Any = None

    def __post_init__(self) -> None:
        # Data paths may pass the plain strings; the spec always holds the enum members.
        if self.kind is not None:
            self.kind = SourceKind(self.kind)
        if self.stream is not None:
            self.stream = DeviceStream(self.stream)

    @property
    def key(self) -> SourceKey:
        """Return the (device, entry) key of this spec."""
        return (self.device, self.entry)


def total_estimated_bytes(specs: Iterable[SourceSpec]) -> int | None:
    """
    Sum the estimates of the available specs.

    Returns:
        int | None: The total, or ``None`` when no spec could be estimated
            (nothing to gate on).
    """
    known = [s.estimated_bytes for s in specs if s.available and s.estimated_bytes is not None]
    return sum(known) if known else None


def apply_size_gate(specs: list[SourceSpec], limit: int | None) -> bool:
    """
    Mark the available specs ``size_gated`` when their total exceeds ``limit``.

    Always assigns the flag (``True`` or ``False``) so re-resolved specs never
    carry a stale mark; only *available* specs are stamped, so an unavailable
    source can never keep a request withheld.

    Args:
        specs (list[SourceSpec]): Specs a path is about to return from ``resolve``.
        limit (int | None): ``BindContext.size_limit_bytes``; ``None`` never gates.

    Returns:
        bool: Whether the specs were gated.
    """
    total = total_estimated_bytes(specs)
    gated = limit is not None and total is not None and total > limit
    for spec in specs:
        spec.size_gated = gated and spec.available
    return gated


@dataclass(frozen=True)
class BindContext:
    """
    What the facade already knows about a target when it routes to a path.

    Built once per bind and handed to ``resolve`` and, via
    :attr:`SourceRequest.context`, to ``open``. It carries the routing
    decision (``scenario``) and the scan item — so data paths never have to
    fetch it themselves while the facade lock is held.

    Lock-order rule: scan-storage accessors take the scan-manager RLock, which
    dispatcher threads hold while delivering data and waiting for the DataAPI
    lock. A caller on any other thread therefore reads the scan item *before*
    taking the DataAPI lock and passes it here (``have_item=True``; ``None``
    is a valid prefetch meaning "not in storage"). Dispatcher-side callers,
    which already hold the RLock, leave ``have_item=False`` and paths may
    re-enter storage through :meth:`resolve_scan_item`.
    """

    scan_id: str
    scenario: Scenario
    scan_item: Any = None
    have_item: bool = False
    #: The subscription's payload limit. A path that can estimate the payload
    #: without reading it applies the limit in ``resolve`` through
    #: :func:`apply_size_gate`; ``None`` (e.g. ``DataAPI.estimate_bytes``)
    #: never gates.
    size_limit_bytes: int | None = None
    #: A stored column larger than this is not held whole: the subscription gets an
    #: overview of it and asks for the rows it looks at (``Subscription.request_view``).
    #: ``None`` holds every column whole.
    view_limit_bytes: int | None = None

    def resolve_scan_item(self, fetch: Callable[[str], Any]) -> Any:
        """
        Return the scan item without violating the lock-order rule.

        Args:
            fetch (Callable[[str], Any]): Storage accessor, only invoked when
                no item was prefetched (dispatcher-side callers).

        Returns:
            Any: The prefetched item when ``have_item`` is set, else
                ``fetch(scan_id)``.
        """
        if self.have_item:
            return self.scan_item
        return fetch(self.scan_id)


class EmissionSink(Protocol):
    """
    Facade-side receiver of a request's "points were inserted" signal.

    The :class:`~.data_api.Subscription` implements it; tests may pass any
    object with the same method.
    """

    def request_emission(
        self,
        request: SourceRequest,
        reason: UpdateReason,
        immediate: bool = False,
        coalesce: bool = False,
    ) -> None:
        """Emit (or schedule) an update of ``request``'s correlation group."""


@dataclass(eq=False)
class SourceRequest:
    """
    One active binding of a subscription to a data path.

    The path inserts points into ``bundle`` and calls :meth:`notify`; the
    ``sink`` (the subscription) owns emission — coalescing, callback
    invocation, thread hand-off. ``group`` is the correlation group
    the update is emitted under; ``context`` is the bind context the request
    was resolved under; ``progress_callback`` receives load fractions (0..1)
    from paths that read bulk data; ``state`` is path-private storage.
    """

    scan_id: str
    group: CorrelationGroup
    specs: list[SourceSpec]
    bundle: DataBundler = field(repr=False)
    sink: EmissionSink = field(repr=False)
    context: BindContext
    progress_callback: Callable[[float], None] | None = None
    #: ``"rebind"`` when this request replaces an earlier binding of the same
    #: subscription (scan change, source change, unavailable-source retry):
    #: the sink emits the path's first snapshot under that reason so consumers
    #: reset per-scan state. ``None`` keeps the path's own reason.
    initial_reason: UpdateReason | None = None
    state: dict[str, Any] = field(default_factory=dict)
    #: Adapter owning this request; assigned by the facade at bind time.
    path: DataPath | None = field(default=None, repr=False)
    #: Set by the facade before the path's ``close()``: workers stop publishing.
    cancelled: bool = False
    #: Set together with ``cancelled``; a worker waits on it between polls.
    stop_event: threading.Event = field(default_factory=threading.Event, repr=False)
    #: Threads started with :meth:`start_worker`; joined by :meth:`DataPath.join`.
    workers: list[threading.Thread] = field(default_factory=list, repr=False)

    def start_worker(self, target: Callable[..., None], *args: Any, name: str | None = None):
        """
        Run ``target(request, *args)`` on a daemon thread that belongs to this request.

        The thread is joined by :meth:`DataPath.join`; ``target`` checks :attr:`cancelled`
        before it publishes and waits on :attr:`stop_event` between polls. Lock held
        (call it from ``_open``).

        Args:
            target (Callable[..., None]): The worker function.
            *args: Further arguments of ``target``.
            name (str | None): Thread name.

        Returns:
            threading.Thread: The started thread.
        """
        worker = threading.Thread(
            target=target, args=(self, *args), name=name or "data-api-worker", daemon=True
        )
        # A request may run several workers (a poll loop, a later history fetch).
        self.workers = [thread for thread in self.workers if thread.is_alive()] + [worker]
        worker.start()
        return worker

    def series(self, spec: SourceSpec | SourceKey, kind: SourceKind | None = None) -> SourceSeries:
        """
        Return the series of one of this request's sources (created on first use).

        The series kind defaults to the spec's kind. A path that delivers the fragments
        of an async signal calls :meth:`SourceSeries.declare` with their encoding, which
        :meth:`SourceData.as_array` and :meth:`SourceData.latest` rely on.
        Insert into it with the path lock held, then call :meth:`notify` after releasing it.

        Args:
            spec (SourceSpec | SourceKey): One of ``specs``, or its (device, entry) key.
            kind (SourceKind | None): Override of the series kind.

        Returns:
            SourceSeries: The series in this request's (possibly shared) bundle.
        """
        if not isinstance(spec, SourceSpec):
            spec = next(s for s in self.specs if s.key == tuple(spec))
        return self.bundle.get_series(
            spec.device, spec.entry, kind or spec.kind or SourceKind.UNINDEXED
        )

    def set_status(self, key: SourceKey, error: str | None = None, **metadata: Any) -> None:
        """
        Report a source's state (e.g. ``error="connection lost"``) to the consumer.

        Updates the series metadata so the next update carries it; ``error=None``
        clears a previous error. Lock held; call :meth:`notify` afterwards.

        Args:
            key (SourceKey): One of this request's sources.
            error (str | None): Error text shown to the consumer, or None to clear it.
            **metadata: Further metadata (None removes a key).
        """
        self.series(key).update_metadata(error=error, **metadata)

    def adopt(self, series: dict[SourceKey, SourceSeries]) -> None:
        """
        Publish series built off-lock (e.g. by a worker) into this request's bundle.

        Only this request's own sources are taken, so a shared bundle's partner series
        are never touched. Lock held; call :meth:`notify` afterwards.

        Args:
            series (dict[SourceKey, SourceSeries]): Finished series by source key.
        """
        own = {spec.key for spec in self.specs}
        self.bundle.adopt({key: value for key, value in series.items() if key in own})

    def notify(self, reason: UpdateReason | str, *, partial: bool = False) -> None:
        """
        Signal that points were inserted. Any thread; a feed callback must
        release the path lock before calling it (otherwise the update is handed
        to the scheduler thread and a warning is logged once).

        Args:
            reason (UpdateReason | str): Why the consumer should be updated. The plain
                string of a member (``"live"``) is accepted; anything else raises
                ValueError.
            partial (bool): More of the same load follows, for example the next block of
                a stored scan. Such updates are coalesced like ``"live"`` ones. The
                notification that ends the load is sent without it and delivered at once.
        """
        if partial:
            self.sink.request_emission(self, UpdateReason(reason), coalesce=True)
        else:
            self.sink.request_emission(self, UpdateReason(reason))

    @contextmanager
    def batch(self, reason: UpdateReason | str = UpdateReason.LIVE) -> Iterator[SourceRequest]:
        """
        Insert under the path lock and notify once after releasing it.

        Example:
            with request.batch(UpdateReason.LIVE):
                request.series(key).insert(ordinal, value, timestamp)

        Nothing is notified if the block raises.

        Args:
            reason (UpdateReason | str): The reason of the single notification.

        Yields:
            SourceRequest: This request.
        """
        if self.path is None:
            raise RuntimeError("batch() needs a request that is bound to a data path.")
        with self.path.lock:
            yield self
        self.notify(reason)

    def flush(self) -> None:
        """
        Deliver the current state now, uncoalesced, as a ``"live"`` update.

        Use it as the final flush when the external scan a path streams has ended,
        before calling ``api.refresh(scan_id)``. Any thread; never with the lock held.
        """
        self.sink.request_emission(self, UpdateReason.LIVE, immediate=True)

    @property
    def size_gated(self) -> bool:
        """Whether the serving path withheld this request at resolve time."""
        return any(spec.size_gated for spec in self.specs)

    @property
    def estimated_bytes(self) -> int | None:
        """Estimated payload of this request's available sources, if knowable."""
        return total_estimated_bytes(self.specs)


class DataPath(ABC):
    """
    Base class of DataAPI data paths.

    Subclasses declare where they read from (``data_source``), which
    scenarios they can serve (``scenarios``) and their order among the
    paths of a scenario (``priority``); the facade builds one routing table
    per scenario from these declarations at registration. Exactly one path
    serves a data source; a replacement (e.g. a subclass) is registered
    with ``register_path(path, replace=True)``.

    Construction: ``DataPath(client)``; a subclass with its own ``__init__`` calls
    ``super().__init__(client)``. Registration calls :meth:`attach`, which
    replaces the path's lock with the facade's before ``connect()`` — so never keep
    a reference to ``self.lock`` taken in ``__init__`` (the facade passes its lock
    to the default paths directly).
    """

    #: Where this path reads from. Required for registration.
    data_source: str

    #: The scenarios this path can serve. Required, non-empty.
    scenarios: frozenset[Scenario]

    #: Routing order within a scenario; lower numbers are consulted first
    #: (ties resolve by registration order).
    priority: int = 100

    #: Whether the path reports load progress through ``request.progress_callback``;
    #: the subscription averages the progress of such requests into one fraction.
    reports_progress: bool = False

    def __init__(self, client: BECClient, lock: OwnedLock | None = None):
        self.client = client
        self._api_ref: weakref.ref[DataAPI] | None = None
        #: The DataAPI lock; shared with the facade once registered.
        self._lock = lock or OwnedLock()
        self._active: list[SourceRequest] = []

    @property
    def api(self) -> DataAPI | None:
        """The DataAPI facade this path is registered with (``None`` before :meth:`attach`)."""
        return None if self._api_ref is None else self._api_ref()

    @property
    def lock(self) -> OwnedLock:
        """
        The DataAPI lock: hold it while inserting into request series.

        Never call the facade, block on I/O or notify while holding it.
        """
        return self._lock

    def attach(self, api: DataAPI) -> None:
        """
        Join a DataAPI facade (called by ``register_path`` before :meth:`connect`).

        After registration a path may call ``self.api.refresh(...)`` — outside the
        lock — when its own catalog changes (a new scan, a new source, a scan gone).
        """
        if not hasattr(self, "_active"):
            raise TypeError(
                f"{type(self).__name__}.__init__() has to call super().__init__(client)."
            )
        # Weak: client callbacks keep paths alive, and must not keep the facade alive.
        self._api_ref = weakref.ref(api)
        self._lock = api._lock  # pylint: disable=protected-access

    def current_open_scan(self) -> str | None:
        """
        Return the scan this path currently streams live, from cached state only.

        Consulted for ``subscribe(scan="live")`` when BEC itself runs no scan, so a
        live follower can bind to a scan run by an external system. Lock held.
        """
        return None

    @property
    def active_requests(self) -> list[SourceRequest]:
        """The requests this path currently feeds (opened and not yet released)."""
        return list(self._active)

    def connect(self) -> None:
        """Attach path-wide feeds (called once at registration)."""

    def disconnect(self) -> None:
        """Detach all feeds and drop all state."""

    def describe_scan(self, scan_id: str) -> Scenario | None:  # pylint: disable=unused-argument
        """Describe a scan absent from BEC storage/history, using cached metadata only.

        Return OPEN_SCAN or CLOSED_SCAN, or None for an unknown scan. Called
        with the facade lock held: network/file I/O belongs in the adapter's
        metadata worker. Call DataAPI.refresh(scan_id) after that cache changes.
        """
        return None

    @abstractmethod
    def resolve(self, sources: list[SourceKey], context: BindContext) -> list[SourceSpec] | None:
        """
        Claim or decline a scan for the given sources.

        Args:
            sources (list[SourceKey]): Requested (device, entry) pairs.
            context (BindContext): Scan id, routing decision and the scan
                item (see :meth:`BindContext.resolve_scan_item`); never read
                scan storage directly here.

        Returns:
            list[SourceSpec] | None: One spec per requested source when this
                path serves the scan — individual specs may be marked
                unavailable, and specs carry ``estimated_bytes`` when the
                size is knowable without reading data (such a path applies
                :func:`apply_size_gate` with ``context.size_limit_bytes``
                before returning) — or ``None`` when the path does not serve
                this scan. A path may answer for a subset of the sources; an
                empty list declines like ``None``.
        """

    def open(self, request: SourceRequest) -> None:
        """
        Start feeding the request, unless it was size-gated at resolve time.

        Concrete paths implement :meth:`_open`; this template withholds a
        gated request (nothing is read until :meth:`confirm_size`). Lock held.
        """
        if request.size_gated:
            logger.info(
                f"{type(self).__name__}: request '{request.group}' for scan {request.scan_id} "
                f"withheld: estimated {request.estimated_bytes} bytes exceeds the limit "
                f"{request.context.size_limit_bytes}. Call confirm_size() to load it."
            )
            return
        self._open(request)
        self._active.append(request)

    def release(self, request: SourceRequest) -> None:
        """
        Stop feeding a request: forget it, then let the path release its resources.

        Called by the facade for every request it created (opened or withheld). Lock held.
        """
        request.cancelled = True
        request.stop_event.set()
        self._active = [active for active in self._active if active is not request]
        self.close(request)

    def request_view(
        self, request: SourceRequest, key: SourceKey, start: int, stop: int, max_points: int
    ) -> bool:
        """
        Deliver the rows ``start:stop`` of a source that is held as an overview.

        Called by :meth:`Subscription.request_view` with the lock held, so it must not
        block: a path that serves views starts a worker and returns. The worker replaces
        the detail window of the source and notifies as for any other insert.

        Args:
            request (SourceRequest): The request that serves the source.
            key (SourceKey): The source.
            start (int): First row wanted.
            stop (int): One past the last row wanted.
            max_points (int): Upper bound of the points to deliver for these rows.

        Returns:
            bool: Whether the path serves this source as a view and took the request.
        """
        return False

    def confirm_size(self, request: SourceRequest) -> None:
        """
        Release a request withheld by the size gate. Lock held.

        The default clears the gate on the request's specs and opens it; a
        path with its own confirmation protocol (server-side paging, …)
        overrides this.
        """
        if not request.size_gated:
            return
        for spec in request.specs:
            spec.size_gated = False
        self.open(request)

    def join(self, request: SourceRequest, timeout: float) -> None:
        """
        Wait for the request's threads after :meth:`close`. Called by the
        facade WITHOUT the lock; the default joins the threads started with
        :meth:`SourceRequest.start_worker`, within ``timeout`` in total.
        """
        deadline = time.monotonic() + timeout
        for worker in list(request.workers):
            if worker.is_alive() and worker is not threading.current_thread():
                worker.join(timeout=max(0.0, deadline - time.monotonic()))

    def describe_sources(  # pylint: disable=unused-argument
        self, sources: list[SourceKey]
    ) -> list[SourceSpec] | None:
        """
        Describe sources this path would serve in the next scan, from cached state only.

        Consulted by ``DataAPI.describe`` and ``DataAPI.correlation_groups`` for
        ``scan="live"`` while no scan runs, for the sources BEC's device configuration
        does not know: without a scan there is nothing to ``resolve`` against. Lock held.

        Returns:
            list[SourceSpec] | None: Specs for the sources this path knows (any subset),
                or ``None`` when it knows none of them.
        """
        return None

    def report(self, error: str | None, requests: Iterable[SourceRequest] | None = None) -> None:
        """
        Show a path-wide state (e.g. ``"connection lost"``) on every source this path serves.

        ``error=None`` clears it again. Any thread; never call it with the lock held.

        Args:
            error (str | None): Error text for the consumer, or None to clear it.
            requests (Iterable[SourceRequest] | None): The requests concerned; all active
                requests by default.
        """
        with self.lock:
            touched = list(self.active_requests if requests is None else requests)
            for request in touched:
                for spec in request.specs:
                    if spec.available:
                        request.set_status(spec.key, error=error)
        for request in touched:
            request.notify(UpdateReason.LIVE)

    @abstractmethod
    def _open(self, request: SourceRequest) -> None:
        """Start feeding (backfill + live wiring, or a worker). Lock held; notify after inserting."""

    @abstractmethod
    def close(self, request: SourceRequest) -> None:
        """
        Stop feeding the request and release per-request resources. Lock held.

        Must tolerate a request that was withheld and never opened: the
        facade closes every request it created.
        """
