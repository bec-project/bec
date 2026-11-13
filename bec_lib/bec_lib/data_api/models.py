"""
Data models of the DataAPI emission contract.

Every subscription delivers exactly one shape — a full-state columnar snapshot
(:class:`SubscriptionUpdate`) holding one :class:`SourceData` per subscribed
source — for live updates, backfills and history reads alike.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import numpy as np

#: A subscribed source: (device_name, entry/signal name).
SourceKey = tuple[str, str]


class SourceKind(enum.StrEnum):
    """
    How the values of a source are keyed, which decides what it can be paired with.

    Like every enum of the DataAPI the members are strings: ``kind="monitored"`` is
    accepted wherever a ``SourceKind`` is, and compares equal.
    """

    #: One value per scan point, keyed by the point id.
    MONITORED = "monitored"
    #: Async signal data, keyed by the message counter of its signal.
    ASYNC = "async"
    #: No acquisition key: values in the order they arrived.
    UNINDEXED = "unindexed"


class CorrelationMode(enum.StrEnum):
    """How the sources of one subscription are paired."""

    #: By acquisition key, within the groups BEC guarantees (the default).
    ORDINAL = "ordinal"
    #: Scan-less streams sampled together: every arrival takes the latest value of all.
    CONTINUOUS = "continuous"


class GroupKind(enum.StrEnum):
    """What the sources of a :class:`CorrelationGroup` share, so that they can be paired."""

    #: The scan point: every monitored signal of a scan.
    SCAN = "scan"
    #: The message counter of one AsyncMultiSignal: its sub-signals.
    MULTI = "multi"
    #: Nothing: a single source that cannot be paired with any other.
    STANDALONE = "standalone"
    #: The sample-and-hold rows of ``CorrelationMode.CONTINUOUS``.
    CONTINUOUS = "continuous"


@dataclass(frozen=True)
class CorrelationGroup:
    """
    Identity of a correlation group: the sources that are aligned row by row and arrive
    in one update.

    It is the key of ``Subscription.groups`` and the value of ``update.metadata["group"]``
    and ``SourceInfo.group``. Compare groups with each other and read ``kind``; ``str()``
    gives a text for logs and messages, which is not meant to be parsed.
    """

    kind: GroupKind
    #: Device of a ``MULTI`` or ``STANDALONE`` group; empty otherwise.
    device: str = ""
    #: Storage name of the AsyncMultiSignal (``MULTI``) or entry of the source (``STANDALONE``).
    name: str = ""

    @classmethod
    def multi(cls, device: str, storage_name: str) -> CorrelationGroup:
        """The group of the sub-signals of one AsyncMultiSignal."""
        return cls(GroupKind.MULTI, device, storage_name)

    @classmethod
    def standalone(cls, device: str, entry: str) -> CorrelationGroup:
        """The group of a single source that cannot be paired with another."""
        return cls(GroupKind.STANDALONE, device, entry)

    def __str__(self) -> str:
        if not self.device and not self.name:
            return str(self.kind)
        return f"{self.kind}:{self.device}/{self.name}"


#: The group of the monitored signals of a scan.
SCAN_GROUP = CorrelationGroup(GroupKind.SCAN)
#: The single group of a continuous (sample-and-hold) subscription.
CONTINUOUS_GROUP = CorrelationGroup(GroupKind.CONTINUOUS)


class DeviceStream(enum.StrEnum):
    """The scan-less device stream a source is read from (``SourceSpec.stream``)."""

    READBACK = "readback"
    CONFIGURATION = "configuration"
    PREVIEW = "preview"


class AxisMode(enum.StrEnum):
    """What :meth:`SubscriptionUpdate.axis` returns as the x column."""

    #: The aligned ordinals themselves.
    INDEX = "index"
    #: The timestamps of a source.
    TIMESTAMP = "timestamp"
    #: The values of a source.
    DEVICE = "device"


class UpdateReason(enum.StrEnum):
    """
    Why a :class:`SubscriptionUpdate` was sent.

    The members are strings: ``update.reason == "history"`` keeps working, and a data
    path may pass the plain string to ``request.notify``.
    """

    #: New data of the bound scan or stream. Coalesced to ``min_emit_interval``.
    LIVE = "live"
    #: What was already there when the binding was made.
    BACKFILL = "backfill"
    #: The stored data of a finished scan.
    HISTORY = "history"
    #: The binding changed (another scan, other sources): consumers reset what they show.
    REBIND = "rebind"


#: Internal marker: ``aligned_ordinals`` is filled in later, by the delivering
#: thread outside the DataAPI lock (see ``Subscription._deliver``).
ALIGN_LATER: Any = object()


def align_ordinals(sources: Mapping[SourceKey, SourceData]) -> np.ndarray:
    """
    Return the ordinals present in every source, ascending.

    Sources holding contiguous ordinals align as a slice view (or as one
    source's own ordinal array, so its column needs no extraction); otherwise a
    presence count (dense ranges) or a binary-search filter (sparse) is used.

    Args:
        sources (Mapping[SourceKey, SourceData]): The snapshots to align.

    Returns:
        np.ndarray: The aligned ordinals (int64).
    """
    columns = [np.asarray(source.ordinals, dtype=np.int64) for source in sources.values()]
    if not columns or any(len(column) == 0 for column in columns):
        return np.empty(0, dtype=np.int64)
    lo = max(int(column[0]) for column in columns)
    hi = min(int(column[-1]) for column in columns) + 1
    if lo >= hi:
        return np.empty(0, dtype=np.int64)
    gapless = [int(column[-1]) - int(column[0]) == len(column) - 1 for column in columns]
    if all(gapless):
        for column in columns:
            if int(column[0]) == lo and int(column[-1]) == hi - 1:
                return column  # identity: this source's column needs no extraction
        reference = columns[0]
        return reference[lo - int(reference[0]) : hi - int(reference[0])]
    shortest = min(columns, key=len)
    width = hi - lo
    if width <= 4 * len(shortest):
        counts = np.zeros(width, dtype=np.int32)
        for column in columns:
            inside = column[(column >= lo) & (column < hi)]
            counts[inside - lo] += 1
        return np.flatnonzero(counts == len(columns)).astype(np.int64) + lo
    candidates = shortest[(shortest >= lo) & (shortest < hi)]
    keep = np.ones(len(candidates), dtype=bool)
    for column, contiguous in zip(columns, gapless):
        if column is shortest or contiguous:
            continue  # a contiguous column holds every ordinal in [lo, hi)
        index = np.minimum(np.searchsorted(column, candidates), len(column) - 1)
        keep &= column[index] == candidates
    return candidates[keep]


def _join_add_fragments(values: Any, raw: Any, metadata: Mapping[str, Any]) -> np.ndarray:
    """
    Join ``add`` fragments (one per message) into the acquired dataset.

    Rank-1 sources whose fragments are all scalars, or all 1-D of one length, join
    in one vectorised step; anything else is concatenated fragment by fragment.

    Args:
        values: The values as a float64 column or a list (object columns unpacked).
        raw: The snapshot's values as stored.
        metadata: Series metadata (``max_shape`` gives the rank).
    """
    rank = len(metadata.get("max_shape") or [None])
    if rank == 1:
        try:
            joined = np.asarray(values)
        except ValueError:
            joined = None  # ragged fragments
        if joined is not None and joined.dtype != object and joined.ndim in (1, 2):
            return joined.reshape(-1)
    fragments = []
    for value in raw:
        fragment = np.asarray(value)
        if fragment.ndim < rank:
            fragment = np.expand_dims(fragment, 0)
        fragments.append(fragment)
    try:
        return np.concatenate(fragments)
    except ValueError:
        rows = [row for fragment in fragments for row in fragment]
        result = np.empty(len(rows), dtype=object)
        result[:] = rows
        return result


@dataclass(frozen=True, eq=False)
class SourceData:
    """
    Columnar snapshot of one source.

    ``ordinals`` are the correlation keys of the points: the scan ``point_id``
    for monitored sources, the async ordinal (message counter or add_slice row)
    for async sources, and an arrival counter for unindexed sources
    (scan-less device streams). A source read from a scan file holds one point per
    stored row, numbered ``0..n-1``: :meth:`as_array` is the same for a running and
    a stored scan, the split into points is not.
    ``values``/``timestamps`` are parallel to ``ordinals`` (sorted ascending).
    Snapshots built by the DataAPI hold read-only numpy columns: float64 for
    float values, object arrays (exact Python values) otherwise, or the dtype
    of an adopted file array.
    """

    device: str
    entry: str
    kind: SourceKind
    ordinals: tuple[int, ...] | np.ndarray
    values: tuple[Any, ...] | np.ndarray
    timestamps: tuple[Any, ...] | np.ndarray
    complete: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)
    #: Result of :meth:`as_array`, computed on first use (a snapshot never changes).
    _array_cache: np.ndarray | None = field(default=None, init=False, repr=False, compare=False)

    @property
    def key(self) -> SourceKey:
        """Return the (device, entry) key of this source."""
        return (self.device, self.entry)

    def as_array(self) -> np.ndarray:
        """Return the reconstructed dataset, independent of transport or scan state.

        Correlation columns remain acquisition keyed in ``values``. This accessor
        joins add fragments, stacks accumulated rows, and unwraps replace state.
        Ragged rows use an object array. The result is cached for this snapshot.
        """
        if self._array_cache is not None:
            return self._array_cache
        encoding = self.metadata.get("async_update_type")
        values = self.values
        if isinstance(values, np.ndarray) and values.dtype == object:
            # np.asarray never stacks the elements of an object array.
            values = values.tolist()
        if len(values) == 0:
            result = np.array([])
        elif encoding == "replace":
            result = np.asarray(self.values[-1])
        elif encoding == "add":
            result = _join_add_fragments(values, self.values, self.metadata)
        else:
            try:
                result = np.asarray(values)
            except ValueError:
                result = np.empty(len(values), dtype=object)
                for index, value in enumerate(values):
                    result[index] = value
        object.__setattr__(self, "_array_cache", result)
        return result

    def latest(self) -> np.ndarray | None:
        """Return the latest acquired array or the reconstructed scalar series.

        Array-valued acquisitions inspect only the final fragment. A waveform can
        select its latest row without assembling every earlier frame.
        """
        if len(self.values) == 0:
            return None
        encoding = self.metadata.get("async_update_type")
        if encoding == "add" and len(self.metadata.get("max_shape") or [None]) == 1:
            return self.as_array()
        if encoding in ("add_slice", "replace"):
            return np.atleast_1d(self.values[-1])
        value = np.asarray(self.values[-1])
        if encoding == "add":
            rank = len(self.metadata.get("max_shape") or [None])
            return np.atleast_1d(value[-1] if value.ndim == rank else value)
        if value.ndim == 0:
            return self.as_array()
        return np.atleast_1d(value)


@dataclass(frozen=True)
class SourceInfo:
    """
    What the DataAPI knows about one source before subscribing (see ``DataAPI.describe``).

    ``group`` is the correlation group the source would be delivered in: sources with the
    same group can be paired row by row. An unavailable source keeps the identity a data
    path reported for it, so a missing member of a group stays visible.
    """

    device: str
    entry: str
    available: bool
    kind: SourceKind | None
    group: CorrelationGroup
    #: ``DataPath.data_source`` of the serving path; ``None`` when no path serves it.
    data_source: str | None = None
    estimated_bytes: int | None = None

    @property
    def key(self) -> SourceKey:
        """Return the (device, entry) key of this source."""
        return (self.device, self.entry)


@dataclass(frozen=True, eq=False)
class SubscriptionUpdate:  # pylint: disable=too-many-instance-attributes
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
    #: Ordinals present in every source; ``None`` computes them on construction.
    aligned_ordinals: tuple[int, ...] | np.ndarray | None
    complete: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)
    #: Binding revision assigned by Subscription. Queued consumers compare it
    #: with Subscription.generation to discard data from an earlier source set
    #: or data path, including rebindings within the same scan.
    generation: int = 0
    #: Result of :meth:`aligned` for all sources, computed on first use.
    _aligned_cache: dict[SourceKey, Any] | None = field(
        default=None, init=False, repr=False, compare=False
    )
    #: Alignment per selection of sources (see :meth:`aligned` with ``keys``).
    _selection_cache: dict[tuple, tuple] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )

    def __post_init__(self):
        if self.aligned_ordinals is None:
            object.__setattr__(self, "aligned_ordinals", align_ordinals(self.sources))

    @property
    def has_scan(self) -> bool:
        """Whether the data belongs to a scan; ``False`` for scan-less device streams."""
        return bool(self.scan_id)

    def aligned(self, keys: Iterable[SourceKey] | None = None) -> dict[SourceKey, Any]:
        """
        Return equal-length value columns for the aligned ordinals.

        The result is computed once per update and cached (updates are
        immutable snapshots), so widgets may call this freely. Columns are
        numpy arrays (views where possible) for DataAPI snapshots.

        Args:
            keys (Iterable[SourceKey] | None): Align only these sources with each other,
                e.g. the x and y of one curve, so a source that lags or never delivers
                cannot shorten the others. ``None`` aligns every source of the update.
                Keys that are not part of the update are left out.

        Returns:
            dict[SourceKey, Any]: For every source, the values at the aligned
                ordinals, in the same order for all sources.
        """
        if keys is None:
            if self._aligned_cache is not None:
                return self._aligned_cache
            out: dict[SourceKey, Any] = {}
            for key, source in self.sources.items():
                out[key] = self._column(source, source.values)
            object.__setattr__(self, "_aligned_cache", out)
            return out
        ordinals, sources = self._alignment_of(keys)
        return {key: self._column(source, source.values, ordinals) for key, source in sources}

    def ordinals(self, keys: Iterable[SourceKey] | None = None) -> np.ndarray:
        """
        Return the ordinals the columns of :meth:`aligned` belong to.

        Args:
            keys (Iterable[SourceKey] | None): The same selection as passed to :meth:`aligned`.

        Returns:
            np.ndarray: Ascending ordinals present in every selected source.
        """
        return self.aligned_ordinals if keys is None else self._alignment_of(keys)[0]

    def _alignment_of(
        self, keys: Iterable[SourceKey]
    ) -> tuple[np.ndarray, list[tuple[SourceKey, SourceData]]]:
        """Aligned ordinals of a selection of sources (cached per selection)."""
        selection = tuple(dict.fromkeys(tuple(key) for key in keys if tuple(key) in self.sources))
        cache = self._selection_cache
        if selection not in cache:
            sources = [(key, self.sources[key]) for key in selection]
            cache[selection] = (align_ordinals(dict(sources)), sources)
        return cache[selection]

    def _column(self, source: SourceData, column: Any, aligned: Any = None) -> Any:
        """Extract one source's column at the aligned ordinals."""
        ordinals = source.ordinals
        aligned = self.aligned_ordinals if aligned is None else aligned
        if ordinals is aligned:
            # Common case: the aligned ordinals ARE this source's ordinals.
            return column
        if isinstance(ordinals, np.ndarray) and isinstance(column, np.ndarray):
            n = len(aligned)
            if n == 0:
                return column[:0]
            start = int(np.searchsorted(ordinals, aligned[0]))
            stop = start + n
            if stop <= len(ordinals) and ordinals[stop - 1] == aligned[-1]:
                # aligned is a subset of this source's ordinals: a matching first
                # and last element means a contiguous run, i.e. a zero-copy view.
                return column if start == 0 and stop == len(column) else column[start:stop]
            return column[np.searchsorted(ordinals, aligned)]
        if isinstance(ordinals, tuple) and isinstance(aligned, tuple) and ordinals == aligned:
            return column
        index_of = {ordinal: i for i, ordinal in enumerate(ordinals)}
        return tuple(column[index_of[o]] for o in aligned)

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
        mode: AxisMode | str = AxisMode.INDEX,
        source: SourceKey | None = None,
        keys: Iterable[SourceKey] | None = None,
    ) -> tuple[Any, ...]:
        """
        Return an x-axis column parallel to the aligned value columns.

        This replaces the per-widget x-mode resolution: ``"index"`` returns
        the aligned ordinals themselves, ``"timestamp"`` the timestamps of
        ``source`` (or of the first source), ``"device"`` the values of
        ``source``.

        Args:
            mode (AxisMode | str): Axis mode.
            source (SourceKey | None): Source supplying the axis for
                ``"timestamp"``/``"device"`` modes.
            keys (Iterable[SourceKey] | None): The selection passed to :meth:`aligned`.

        Returns:
            tuple: Column of the same length as :meth:`aligned` columns.

        Raises:
            KeyError: If ``source`` is required but not part of the update.
            ValueError: If ``keys`` is given and ``source`` is not one of them: a column
                is only parallel to the columns it was aligned with.
        """
        mode = AxisMode(mode)
        ordinals = self.ordinals(keys)
        if mode == AxisMode.INDEX:
            return ordinals
        selection = None if keys is None else [key for key, _ in self._alignment_of(keys)[1]]
        if source is None:
            candidates = self.sources if selection is None else selection
            if not candidates:
                return ()
            source = next(iter(candidates))
        elif selection is not None and tuple(source) not in selection:
            raise ValueError(
                f"Source {tuple(source)} is not part of the selection {selection}; "
                "pass it in 'keys' as well, so that it is aligned with them."
            )
        source_data = self.sources[source]
        if mode == AxisMode.DEVICE:
            return self._column(source_data, source_data.values, ordinals)
        return self._column(source_data, source_data.timestamps, ordinals)
