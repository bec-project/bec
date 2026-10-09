"""
Ordinal-keyed alignment engine.

Replaces the prototype's positional (arrival-count) pairing: every point of
every source is keyed by an ordinal (scan point_id, async ordinal, or arrival
counter), and a bundle emits a point only at ordinals present in *all* sources.
Out-of-order and gapped delivery fill holes instead of shifting pairs.
"""

from __future__ import annotations

import bisect
from typing import Any

import numpy as np

from bec_lib.logger import bec_logger

from .models import SourceData, SourceKey, SourceKind, SubscriptionUpdate, UpdateReason

logger = bec_logger.logger

#: Acquisition group promising one async emission per monitored point.
MONITORED_GROUP = "monitored"

#: A source trailing the group frontier by more than this many ordinals is
#: reported as lagging (starvation visibility).
LAG_THRESHOLD = 2


class CorrelationGroupError(ValueError):
    """Raised when a source set cannot be partitioned into groups."""


def correlation_group_label(key: SourceKey, kind: SourceKind, group: str | None) -> str:
    """
    Return the correlation-group label of one source.

    Args:
        key (SourceKey): (device, entry) of the source.
        kind (SourceKind): Source kind.
        group (str | None): Acquisition group (async sources only).

    Returns:
        str: ``"scan"`` for monitored signals and async signals in the
            "monitored" acquisition group; ``"async:<tag>"`` for async signals
            with a free-form group; a per-source ``"standalone:..."`` label for
            unindexed or ungrouped async sources (they cannot promise any
            cadence and therefore cannot be correlated with other sources).
    """
    if kind == "monitored" or (kind == "async" and group == MONITORED_GROUP):
        return "scan"
    if kind == "async" and group:
        return f"async:{group}"
    return f"standalone:{key[0]}/{key[1]}"


def partition_correlation_groups(
    specs: list[tuple[SourceKey, SourceKind, str | None]],
) -> dict[str, list[SourceKey]]:
    """
    Partition sources into correlation groups.

    Each group is aligned independently and emits its own updates; widgets
    can therefore subscribe to all their sources at once without caring how
    they correlate.

    Args:
        specs: One ``(key, kind, acquisition_group)`` triple per source.

    Returns:
        dict[str, list[SourceKey]]: Group label -> source keys, insertion
            ordered.

    Raises:
        CorrelationGroupError: If ``specs`` is empty.
    """
    if not specs:
        raise CorrelationGroupError("A subscription needs at least one source.")
    groups: dict[str, list[SourceKey]] = {}
    for key, kind, group in specs:
        groups.setdefault(correlation_group_label(key, kind, group), []).append(key)
    return groups


class SourceSeries:
    """
    Ordinal-keyed columnar store for one source within one scan.

    A point inserted at an existing ordinal overwrites it (add_slice rows grow
    in place); completeness means "no holes from ordinal 0 to the frontier".

    The columns are maintained sorted **incrementally** (the hot path — a new
    highest ordinal — is an O(1) append) and :meth:`snapshot` caches its
    result, so an emission only pays for sources that actually changed.
    """

    def __init__(self, device: str, entry: str, kind: SourceKind, max_points: int | None = None):
        self.device = device
        self.entry = entry
        self.kind = kind
        self.metadata: dict[str, Any] = {}
        self._max_points = max_points
        self._known: set[int] = set()
        self._ordinals: list[int] = []  # sorted
        self._values: list[Any] = []  # parallel to _ordinals
        self._timestamps: list[Any] = []  # parallel to _ordinals
        # Bulk representation: numpy columns at contiguous ordinals 0..n-1,
        # used for one-shot history fills (see extend_bulk).
        self._bulk_values: Any = None
        self._bulk_timestamps: Any = None
        self._arrival_counter = 0
        self._cached_snapshot: SourceData | None = None

    def extend_bulk(self, values: Any, timestamps: Any = None) -> bool:
        """
        Bulk-fill an empty series with contiguous ordinals ``0..len-1``.

        The value/timestamp columns stay numpy arrays end to end, so a bulk
        history read costs O(1) per column instead of one Python object per
        point (and holds the shared lock accordingly briefly).

        Args:
            values: Column of values; anything ``np.asarray`` accepts (ragged
                inputs are stored as an object array).
            timestamps: Optional column parallel to ``values``; dropped when
                the lengths differ.

        Returns:
            bool: ``True`` when the fill happened; ``False`` when the series
                already holds points — callers fall back to :meth:`insert`.
        """
        if self._ordinals or self._bulk_values is not None:
            return False
        try:
            values = np.atleast_1d(np.asarray(values))
        except ValueError:  # ragged rows (e.g. vlen datasets)
            values = np.asarray(list(values), dtype=object)
        n = len(values)
        if timestamps is not None:
            timestamps = np.atleast_1d(np.asarray(timestamps))
            if len(timestamps) != n:
                timestamps = None
        if timestamps is None:
            # virtual object-None column: zero-copy broadcast keeps downstream
            # length checks and "no timestamps" fallbacks behaving like the
            # per-point path without materializing n pointers
            timestamps = np.broadcast_to(np.array([None], dtype=object), (n,))
        self._bulk_values = values
        self._bulk_timestamps = timestamps
        self._arrival_counter = n
        self._cached_snapshot = None
        return True

    def _debulk(self) -> None:
        """Convert the bulk columns to the incremental representation."""
        values, timestamps = self._bulk_values, self._bulk_timestamps
        self._bulk_values = None
        self._bulk_timestamps = None
        n = len(values)
        self._ordinals = list(range(n))
        self._values = list(values)
        self._timestamps = list(timestamps)
        self._known = set(range(n))

    def insert(self, ordinal: int | None, value: Any, timestamp: Any) -> int:
        """
        Insert or overwrite one point.

        Args:
            ordinal (int | None): Correlation ordinal; ``None`` assigns the
                next arrival counter (unindexed sources).
            value: Point value.
            timestamp: Point timestamp.

        Returns:
            int: The ordinal the point was stored at.
        """
        if self._bulk_values is not None:
            self._debulk()
        if ordinal is None:
            ordinal = self._arrival_counter
        self._cached_snapshot = None
        if ordinal in self._known:
            # Overwrite in place: the ordinal set (and thus positions) is
            # unchanged, so a bisect lookup is exact.
            position = bisect.bisect_left(self._ordinals, ordinal)
            self._values[position] = value
            self._timestamps[position] = timestamp
        elif not self._ordinals or ordinal > self._ordinals[-1]:
            # Hot path: in-order arrival.
            self._known.add(ordinal)
            self._ordinals.append(ordinal)
            self._values.append(value)
            self._timestamps.append(timestamp)
        else:
            # Rare: a hole is filled late.
            self._known.add(ordinal)
            position = bisect.bisect_left(self._ordinals, ordinal)
            self._ordinals.insert(position, ordinal)
            self._values.insert(position, value)
            self._timestamps.insert(position, timestamp)
        self._arrival_counter = max(self._arrival_counter, ordinal + 1)
        if self._max_points is not None and len(self._ordinals) > self._max_points:
            # Endless device streams: keep only the newest points.
            overflow = len(self._ordinals) - self._max_points
            for old in self._ordinals[:overflow]:
                self._known.discard(old)
            del self._ordinals[:overflow]
            del self._values[:overflow]
            del self._timestamps[:overflow]
        return ordinal

    def __len__(self) -> int:
        if self._bulk_values is not None:
            return len(self._bulk_values)
        return len(self._ordinals)

    @property
    def frontier(self) -> int:
        """One past the highest stored ordinal (0 when empty)."""
        if self._bulk_values is not None:
            return len(self._bulk_values)
        return self._ordinals[-1] + 1 if self._ordinals else 0

    @property
    def complete(self) -> bool:
        """Whether every ordinal from 0 to the frontier is present."""
        if self._bulk_values is not None:
            return True
        return len(self._ordinals) == self.frontier

    @property
    def ordinals(self) -> set[int]:
        """The set of stored ordinals."""
        if self._bulk_values is not None and not self._known:
            # Materialized lazily: only the (rare) set-intersection path of
            # mixed complete/incomplete groups needs it, and for large bulk
            # series the set costs real memory.
            self._known = set(range(len(self._bulk_values)))
        return self._known

    def snapshot(self) -> SourceData:
        """
        Return an immutable columnar snapshot of this series.

        Cached: repeated calls without an intervening :meth:`insert` return
        the same object, so bundle emissions only rebuild changed sources.
        """
        if self._cached_snapshot is not None:
            return self._cached_snapshot
        if self._bulk_values is not None:
            self._cached_snapshot = SourceData(
                device=self.device,
                entry=self.entry,
                kind=self.kind,
                ordinals=np.arange(len(self._bulk_values)),
                values=self._bulk_values,
                timestamps=self._bulk_timestamps,
                complete=True,
                metadata=dict(self.metadata),
            )
            return self._cached_snapshot
        self._cached_snapshot = SourceData(
            device=self.device,
            entry=self.entry,
            kind=self.kind,
            ordinals=tuple(self._ordinals),
            values=tuple(self._values),
            timestamps=tuple(self._timestamps),
            complete=self.complete,
            metadata=dict(self.metadata),
        )
        return self._cached_snapshot

    def clear(self) -> None:
        """Drop all points and reset the arrival counter."""
        self._known.clear()
        self._ordinals.clear()
        self._values.clear()
        self._timestamps.clear()
        self._bulk_values = None
        self._bulk_timestamps = None
        self._arrival_counter = 0
        self._cached_snapshot = None


class Bundle:
    """
    The aligned series of one subscription for one scan.

    Owns one :class:`SourceSeries` per source and builds
    :class:`SubscriptionUpdate` snapshots. Detects cadence violations of the
    "monitored" acquisition-group contract (an async source running ahead of
    the monitored point counter) and logs them once per scan instead of
    silently mispairing.
    """

    def __init__(self, scan_id: str, max_points: int | None = None):
        self.scan_id = scan_id
        self.max_points = max_points
        self.series: dict[SourceKey, SourceSeries] = {}
        self._cadence_warned = False
        self._lag_warned = False

    def get_series(self, device: str, entry: str, kind: SourceKind) -> SourceSeries:
        """Return (creating if needed) the series for one source."""
        key = (device, entry)
        series = self.series.get(key)
        if series is None:
            series = SourceSeries(device, entry, kind, max_points=self.max_points)
            self.series[key] = series
        return series

    def _check_cadence(self) -> None:
        monitored_frontiers = [s.frontier for s in self.series.values() if s.kind == "monitored"]
        async_frontiers = [s.frontier for s in self.series.values() if s.kind == "async"]
        if not monitored_frontiers or not async_frontiers:
            return
        ahead = max(async_frontiers) - max(monitored_frontiers)
        if ahead > 1 and not self._cadence_warned:
            self._cadence_warned = True
            logger.warning(
                f"Scan {self.scan_id}: an async source is {ahead} updates ahead of the "
                "monitored point counter; the 'monitored' acquisition-group cadence "
                "contract appears violated. Points are joined by ordinal, not silently "
                "re-paired, but the extra updates will not align."
            )

    def build_update(
        self, reason: UpdateReason, metadata: dict[str, Any] | None = None
    ) -> SubscriptionUpdate:
        """
        Build an immutable full-state update from the current series.

        Args:
            reason (UpdateReason): Why the update is emitted.
            metadata (dict | None): Subscription-level metadata to attach.

        Returns:
            SubscriptionUpdate: Snapshot with aligned ordinals computed as the
                intersection of all sources' ordinals.
        """
        self._check_cadence()
        lagging = self._lagging_sources()
        sources = {key: series.snapshot() for key, series in self.series.items()}
        frontiers = {series.frontier for series in self.series.values()}
        all_complete = all(s.complete for s in sources.values())
        complete = all_complete and len(frontiers) <= 1
        aligned_contiguous = False
        if not sources:
            aligned_ordinals: Any = ()
        elif complete:
            # All sources hold exactly 0..frontier-1: no set work needed.
            aligned_ordinals = next(iter(sources.values())).ordinals
            aligned_contiguous = True
        elif all_complete:
            # Complete sources of different lengths intersect on the common
            # prefix — no set materialization for (large) bulk series.
            aligned_ordinals = np.arange(min(frontiers))
            aligned_contiguous = True
        else:
            aligned_ordinals = tuple(
                sorted(set.intersection(*(series.ordinals for series in self.series.values())))
            )
        update_metadata = dict(metadata or {})
        if lagging:
            update_metadata["lagging_sources"] = lagging
        return SubscriptionUpdate(
            scan_id=self.scan_id,
            reason=reason,
            sources=sources,
            aligned_ordinals=aligned_ordinals,
            complete=complete,
            aligned_contiguous=aligned_contiguous,
            metadata=update_metadata,
        )

    def _lagging_sources(self) -> list[SourceKey]:
        """
        Sources trailing the bundle frontier by more than LAG_THRESHOLD.

        A lagging (possibly silent) source no longer blocks delivery — every
        emission is a full snapshot — but it does stall ``aligned_ordinals``;
        this surfaces the responsible source(s) to consumers and the log.
        """
        if len(self.series) < 2:
            return []
        frontiers = {key: series.frontier for key, series in self.series.items()}
        max_frontier = max(frontiers.values())
        lagging = [
            key for key, frontier in frontiers.items() if max_frontier - frontier > LAG_THRESHOLD
        ]
        if lagging and not self._lag_warned:
            self._lag_warned = True
            logger.warning(
                f"Scan {self.scan_id}: source(s) {lagging} trail the bundle frontier by more "
                f"than {LAG_THRESHOLD} points; aligned emission is stalled at the slowest "
                "source. The subscription keeps delivering full snapshots."
            )
        return lagging

    def clear(self) -> None:
        """Clear all series (keeps the source registrations)."""
        for series in self.series.values():
            series.clear()
        self._cadence_warned = False
