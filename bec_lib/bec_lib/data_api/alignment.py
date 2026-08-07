"""
Ordinal-keyed alignment engine.

Replaces the prototype's positional (arrival-count) pairing: every point of
every source is keyed by an ordinal (scan point_id, async ordinal, or arrival
counter), and a bundle emits a point only at ordinals present in *all* sources.
Out-of-order and gapped delivery fill holes instead of shifting pairs.
"""

from __future__ import annotations

from typing import Any

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
    """

    def __init__(self, device: str, entry: str, kind: SourceKind, max_points: int | None = None):
        self.device = device
        self.entry = entry
        self.kind = kind
        self.metadata: dict[str, Any] = {}
        self._max_points = max_points
        self._points: dict[int, tuple[Any, Any]] = {}  # ordinal -> (value, timestamp)
        self._arrival_counter = 0

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
        if ordinal is None:
            ordinal = self._arrival_counter
        self._points[ordinal] = (value, timestamp)
        self._arrival_counter = max(self._arrival_counter, ordinal + 1)
        if self._max_points is not None and len(self._points) > self._max_points:
            # Endless device streams: keep only the newest points.
            overflow = len(self._points) - self._max_points
            for old in sorted(self._points)[:overflow]:
                del self._points[old]
        return ordinal

    def __len__(self) -> int:
        return len(self._points)

    @property
    def frontier(self) -> int:
        """One past the highest stored ordinal (0 when empty)."""
        return max(self._points) + 1 if self._points else 0

    @property
    def complete(self) -> bool:
        """Whether every ordinal from 0 to the frontier is present."""
        return len(self._points) == self.frontier

    @property
    def ordinals(self) -> set[int]:
        """The set of stored ordinals."""
        return set(self._points)

    def snapshot(self) -> SourceData:
        """Return an immutable columnar snapshot of this series."""
        ordinals = tuple(sorted(self._points))
        values = tuple(self._points[o][0] for o in ordinals)
        timestamps = tuple(self._points[o][1] for o in ordinals)
        return SourceData(
            device=self.device,
            entry=self.entry,
            kind=self.kind,
            ordinals=ordinals,
            values=values,
            timestamps=timestamps,
            complete=self.complete,
            metadata=dict(self.metadata),
        )

    def clear(self) -> None:
        """Drop all points and reset the arrival counter."""
        self._points.clear()
        self._arrival_counter = 0


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
        if sources:
            aligned: set[int] = set.intersection(*(set(s.ordinals) for s in sources.values()))
        else:
            aligned = set()
        frontiers = {series.frontier for series in self.series.values()}
        complete = all(s.complete for s in sources.values()) and len(frontiers) <= 1
        update_metadata = dict(metadata or {})
        if lagging:
            update_metadata["lagging_sources"] = lagging
        return SubscriptionUpdate(
            scan_id=self.scan_id,
            reason=reason,
            sources=sources,
            aligned_ordinals=tuple(sorted(aligned)),
            complete=complete,
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
