"""
Data models of the DataAPI emission contract.

Every subscription delivers exactly one shape — a full-state columnar snapshot
(:class:`SubscriptionUpdate`) holding one :class:`SourceData` per subscribed
source — for live updates, backfills and history reads alike.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

#: A subscribed source: (device_name, entry/signal name).
SourceKey = tuple[str, str]

SourceKind = Literal["monitored", "async", "unindexed"]
UpdateReason = Literal["live", "backfill", "history", "rebind"]


@dataclass(frozen=True)
class SourceData:
    """
    Columnar snapshot of one source.

    ``ordinals`` are the correlation keys of the points: the scan ``point_id``
    for monitored sources, the async ordinal (message counter or add_slice row)
    for async sources, and an arrival counter for unindexed legacy sources.
    ``values``/``timestamps`` are parallel to ``ordinals`` (sorted ascending).
    """

    device: str
    entry: str
    kind: SourceKind
    ordinals: tuple[int, ...]
    values: tuple[Any, ...]
    timestamps: tuple[Any, ...]
    complete: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> SourceKey:
        """Return the (device, entry) key of this source."""
        return (self.device, self.entry)


@dataclass(frozen=True)
class SubscriptionUpdate:
    """
    One emission of a subscription: an immutable full-state snapshot.

    ``aligned_ordinals`` lists the ordinals present in every source of the
    subscription; :meth:`aligned` returns equal-length columns restricted to
    them — the shared replacement for per-widget length-trimming code.
    ``complete`` is ``True`` when no source has known gaps and all sources
    have reached the same frontier.
    """

    scan_id: str
    reason: UpdateReason
    sources: Mapping[SourceKey, SourceData]
    aligned_ordinals: tuple[int, ...]
    complete: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def aligned(self) -> dict[SourceKey, tuple[Any, ...]]:
        """
        Return equal-length value columns for the aligned ordinals.

        Returns:
            dict[SourceKey, tuple]: For every source, the values at
                ``aligned_ordinals``, in the same order for all sources.
        """
        out: dict[SourceKey, tuple[Any, ...]] = {}
        for key, source in self.sources.items():
            index_of = {ordinal: i for i, ordinal in enumerate(source.ordinals)}
            out[key] = tuple(source.values[index_of[o]] for o in self.aligned_ordinals)
        return out

    def get(self, device: str, entry: str) -> SourceData | None:
        """
        Return the snapshot of one source, or ``None`` if not subscribed.

        Args:
            device (str): Device name.
            entry (str): Entry/signal name.
        """
        return self.sources.get((device, entry))

    def axis(
        self,
        mode: Literal["index", "timestamp", "device"] = "index",
        source: SourceKey | None = None,
    ) -> tuple[Any, ...]:
        """
        Return an x-axis column parallel to the aligned value columns.

        This replaces the per-widget x-mode resolution: ``"index"`` returns
        the aligned ordinals themselves, ``"timestamp"`` the timestamps of
        ``source`` (or of the first source), ``"device"`` the values of
        ``source``.

        Args:
            mode: Axis mode.
            source (SourceKey | None): Source supplying the axis for
                ``"timestamp"``/``"device"`` modes.

        Returns:
            tuple: Column of the same length as :meth:`aligned` columns.

        Raises:
            KeyError: If ``source`` is required but not part of the update.
        """
        if mode == "index":
            return self.aligned_ordinals
        if source is None:
            if not self.sources:
                return ()
            source = next(iter(self.sources))
        source_data = self.sources[source]
        index_of = {ordinal: i for i, ordinal in enumerate(source_data.ordinals)}
        column = source_data.values if mode == "device" else source_data.timestamps
        return tuple(column[index_of[o]] for o in self.aligned_ordinals)
