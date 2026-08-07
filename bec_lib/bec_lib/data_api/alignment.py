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


class CorrelationGroupError(ValueError):
    """Raised when a source set does not form one correlation group."""


def validate_correlation_group(specs: list[tuple[SourceKey, SourceKind, str | None]]) -> str:
    """
    Validate that the given sources form exactly one correlation group.

    Args:
        specs: One ``(key, kind, acquisition_group)`` triple per source.
            ``acquisition_group`` is only meaningful for async sources.

    Returns:
        str: The group label — ``"scan"`` (monitored signals plus async
            signals in the "monitored" acquisition group), ``"async:<tag>"``
            (async signals sharing a free-form group), or ``"standalone"``
            (a single source of any kind).

    Raises:
        CorrelationGroupError: If the sources span more than one group.
    """
    if not specs:
        raise CorrelationGroupError("A subscription needs at least one source.")
    if len(specs) == 1:
        return "standalone"

    labels = set()
    for key, kind, group in specs:
        if kind == "monitored" or (kind == "async" and group == MONITORED_GROUP):
            labels.add("scan")
        elif kind == "async" and group:
            labels.add(f"async:{group}")
        else:
            # Unindexed or ungrouped async sources cannot promise any cadence
            # and therefore cannot be correlated with other sources.
            labels.add(f"standalone:{key[0]}/{key[1]}")

    if len(labels) > 1:
        raise CorrelationGroupError(
            f"Sources do not form one correlation group: {sorted(labels)}. "
            "Subscribe to them separately."
        )
    return labels.pop()


class SourceSeries:
    """
    Ordinal-keyed columnar store for one source within one scan.

    A point inserted at an existing ordinal overwrites it (add_slice rows grow
    in place); completeness means "no holes from ordinal 0 to the frontier".
    """

    def __init__(self, device: str, entry: str, kind: SourceKind):
        self.device = device
        self.entry = entry
        self.kind = kind
        self.metadata: dict[str, Any] = {}
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

    def __init__(self, scan_id: str):
        self.scan_id = scan_id
        self.series: dict[SourceKey, SourceSeries] = {}
        self._cadence_warned = False

    def get_series(self, device: str, entry: str, kind: SourceKind) -> SourceSeries:
        """Return (creating if needed) the series for one source."""
        key = (device, entry)
        series = self.series.get(key)
        if series is None:
            series = SourceSeries(device, entry, kind)
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
        sources = {key: series.snapshot() for key, series in self.series.items()}
        if sources:
            aligned: set[int] = set.intersection(*(set(s.ordinals) for s in sources.values()))
        else:
            aligned = set()
        frontiers = {series.frontier for series in self.series.values()}
        complete = all(s.complete for s in sources.values()) and len(frontiers) <= 1
        return SubscriptionUpdate(
            scan_id=self.scan_id,
            reason=reason,
            sources=sources,
            aligned_ordinals=tuple(sorted(aligned)),
            complete=complete,
            metadata=dict(metadata or {}),
        )

    def clear(self) -> None:
        """Clear all series (keeps the source registrations)."""
        for series in self.series.values():
            series.clear()
        self._cadence_warned = False
