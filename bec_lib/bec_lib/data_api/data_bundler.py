"""
DataBundler: the ordinal-keyed alignment engine.

Replaces the prototype's positional (arrival-count) pairing: every point of
every source is keyed by an ordinal (scan point_id, async ordinal, or arrival
counter), and a bundle emits a point only at ordinals present in *all* sources.
Out-of-order and gapped delivery fill holes instead of shifting pairs.

Two correlation policies exist:

* ``"ordinal"`` (default): sources are partitioned into groups by what their
  ordinals mean (scan points, the shared message counter of one
  AsyncMultiSignal, standalone streams) and each group aligns by ordinal
  intersection.
* ``"continuous"``: for scan-less device streams that share no ordinal at
  all (motor readbacks at independent rates). All sources form one group;
  every arriving sample is held as the source's latest value and, once every
  member has delivered once, each arrival appends one row holding the latest
  value of every member (sample-and-hold). Rows are numbered by an emission
  counter and stamped with the triggering sample's timestamp — the pairing a
  motor map needs for (x, y) positions, done once here instead of per widget.
"""

from __future__ import annotations

import threading
import weakref
from typing import Any

import numpy as np

from bec_lib.logger import bec_logger

from .models import (
    ALIGN_LATER,
    CONTINUOUS_GROUP,
    SCAN_GROUP,
    CorrelationGroup,
    CorrelationMode,
    SourceData,
    SourceKey,
    SourceKind,
    SubscriptionUpdate,
    UpdateReason,
    align_ordinals,
)

logger = bec_logger.logger

#: A source trailing the group frontier by more than this many ordinals is
#: reported as lagging (starvation visibility).
LAG_THRESHOLD = 2


class CorrelationGroupError(ValueError):
    """Raised when a source set cannot be partitioned into groups."""


def correlation_group(
    key: SourceKey, kind: SourceKind, multi_signal: str | None = None
) -> CorrelationGroup:
    """
    Return the correlation group of one source.

    Args:
        key (SourceKey): (device, entry) of the source.
        kind (SourceKind): Source kind.
        multi_signal (str | None): Storage name of the AsyncMultiSignal the
            source is a sub-signal of (async sources only).

    Returns:
        CorrelationGroup: ``SCAN_GROUP`` for monitored signals (keyed by scan point);
            a ``MULTI`` group for the sub-signals of one AsyncMultiSignal (they are
            emitted in the same messages, so their ordinals coincide); a
            ``STANDALONE`` group of its own for everything else (other async or
            unindexed sources cannot promise a shared counter and therefore cannot
            be correlated with other sources).
    """
    if kind == SourceKind.MONITORED:
        return SCAN_GROUP
    if kind == SourceKind.ASYNC and multi_signal:
        return CorrelationGroup.multi(key[0], multi_signal)
    return CorrelationGroup.standalone(key[0], key[1])


def partition_correlation_groups(
    specs: list[tuple[SourceKey, SourceKind, str | None]],
    correlation: CorrelationMode | str = CorrelationMode.ORDINAL,
) -> dict[CorrelationGroup, list[SourceKey]]:
    """
    Partition sources into correlation groups.

    Each group is aligned independently and emits its own updates; widgets
    can therefore subscribe to all their sources at once without caring how
    they correlate.

    Args:
        specs: One ``(key, kind, multi_signal)`` tuple per source; see
            :func:`correlation_group`.
        correlation (CorrelationMode): ``"ordinal"`` groups by ordinal
            meaning; ``"continuous"`` puts every source into the single
            sample-and-hold group.

    Returns:
        dict[CorrelationGroup, list[SourceKey]]: Group -> source keys, insertion
            ordered.

    Raises:
        CorrelationGroupError: If ``specs`` is empty.
    """
    if not specs:
        raise CorrelationGroupError("A subscription needs at least one source.")
    if correlation == CorrelationMode.CONTINUOUS:
        return {CONTINUOUS_GROUP: [key for key, _, _ in specs]}
    groups: dict[CorrelationGroup, list[SourceKey]] = {}
    for key, kind, multi_signal in specs:
        groups.setdefault(correlation_group(key, kind, multi_signal), []).append(key)
    return groups


#: Smallest buffer capacity of a series.
_MIN_CAPACITY = 16


def _is_float(value: Any) -> bool:
    """Whether ``value`` can live in a float64 column without changing its Python type."""
    return (
        type(value) is float or type(value) is np.float64
    )  # pylint: disable=unidiomatic-typecheck


def _fits(buffer: np.ndarray, value: Any) -> bool:
    """Whether ``value`` can be stored in ``buffer`` as one element without conversion."""
    if buffer.dtype == object:
        return True
    if buffer.ndim == 1 and buffer.dtype == np.float64:
        return _is_float(value)
    try:
        array = np.asarray(value)
    except (TypeError, ValueError):  # not array-like, or ragged
        return False
    return array.shape == buffer.shape[1:] and array.dtype == buffer.dtype


def _object_column(values: Any, capacity: int) -> np.ndarray:
    """A 1-D object buffer holding ``values`` (sequence or array) as single elements."""
    column = np.empty(capacity, dtype=object)
    items = values.tolist() if isinstance(values, np.ndarray) and values.ndim == 1 else values
    for index, value in enumerate(items):
        column[index] = value
    return column


class _SharedRowOrdinals:
    """
    One array ``0..n-1`` that every series of stored rows numbers its rows with.

    The array is held by a weak reference: it lives as long as a series or a delivered
    snapshot uses a view of it, and a longer one replaces it when more rows are asked for.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ref: weakref.ref | None = None

    def get(self, count: int) -> np.ndarray:
        """Return the ordinals ``0..count-1`` as a read-only view of the shared array."""
        with self._lock:
            shared = self._ref() if self._ref is not None else None
            if shared is None or len(shared) < count:
                shared = np.arange(count, dtype=np.int64)
                shared.flags.writeable = False
                self._ref = weakref.ref(shared)
            return shared[:count]


_shared_row_ordinals = _SharedRowOrdinals()


def row_ordinals(count: int) -> np.ndarray:
    """
    Return the ordinals ``0..count-1`` as a read-only view of one shared array.

    Every series that holds stored rows numbers them from 0, so they all use the same
    column: a scan of many rows costs one ordinal array, not one per source. The array is
    kept only as long as a series or a delivered snapshot uses it.

    Args:
        count (int): Number of rows.

    Returns:
        np.ndarray: A read-only int64 view of length ``count``.
    """
    return _shared_row_ordinals.get(count)


class SourceSeries:
    """
    Ordinal-keyed columnar store for one source within one scan.

    A point inserted at an existing ordinal overwrites it (add_slice rows grow
    in place); completeness means "no holes from ordinal 0 to the frontier".

    Storage is three numpy buffers (ordinals, values, timestamps) with a live
    region ``[lo, hi)``. Appending at the frontier is amortised O(1); retention
    advances ``lo``; :meth:`snapshot` returns read-only *views* of the live
    region in O(1), so an emission costs the same at 10 points or 10 million.

    Immutability of delivered snapshots rests on one invariant: nothing is
    ever written below ``_shared`` in the current buffers. A snapshot (or an
    adopted array) sets ``_shared`` to ``hi``; an overwrite below it first
    copies the live region into fresh buffers (copy on write).

    Values that are exactly ``float`` live in a float64 column; anything else
    (ints, bools, None, strings, arrays) in an object column, so consumers get
    the values exactly as inserted. Arrays handed to :meth:`extend` are
    adopted with their own dtype and shape (zero copy).
    """

    def __init__(self, device: str, entry: str, kind: SourceKind, max_points: int | None = None):
        self.device = device
        self.entry = entry
        self.kind = SourceKind(kind)
        self.metadata: dict[str, Any] = {}
        self._max_points = max_points
        self._ord = np.empty(0, dtype=np.int64)
        self._val: np.ndarray = np.empty(0, dtype=np.float64)
        self._ts: np.ndarray = np.empty(0, dtype=np.float64)
        self._lo = 0
        self._hi = 0
        # Positions below this were handed out (snapshot or adoption): never written again.
        self._shared = 0
        self._frontier = 0
        # Whether the live ordinals are the contiguous range first..frontier-1.
        self._gapless = True
        self._cached_snapshot: SourceData | None = None
        # Ordinals below this were discarded by the retention cap: replaying
        # them (live data re-walks) must not re-insert them.
        self._trim_watermark = 0
        # Number of points a load in progress will deliver (see adopt_rows).
        self._expected: int | None = None

    @property
    def key(self) -> SourceKey:
        """The (device, entry) key of this series."""
        return (self.device, self.entry)

    # --- buffer management ----------------------------------------------------

    def _reallocate(self, capacity: int) -> None:
        """Move the live region to the start of fresh buffers of ``capacity``."""
        live = self._hi - self._lo
        ordinals = np.empty(capacity, dtype=np.int64)
        ordinals[:live] = self._ord[self._lo : self._hi]
        values = np.empty((capacity,) + self._val.shape[1:], dtype=self._val.dtype)
        values[:live] = self._val[self._lo : self._hi]
        timestamps = np.empty(capacity, dtype=self._ts.dtype)
        timestamps[:live] = self._ts[self._lo : self._hi]
        self._ord, self._val, self._ts = ordinals, values, timestamps
        self._lo, self._hi, self._shared = 0, live, 0

    def _reserve(self, extra: int) -> None:
        """Make room for ``extra`` appended points (amortised O(1))."""
        if self._hi + extra <= len(self._ord):
            return
        live = self._hi - self._lo
        self._reallocate(max(_MIN_CAPACITY, 2 * (live + extra)))

    def _copy_on_write(self) -> None:
        """Give the live region fresh buffers (same positions) before an overwrite."""
        self._ord = self._ord.copy()
        self._val = self._val.copy()
        self._ts = self._ts.copy()
        self._shared = 0

    def _as_object(self, buffer: np.ndarray) -> np.ndarray:
        """``buffer`` as a 1-D object buffer with the same positions."""
        column = np.empty(len(buffer), dtype=object)
        live = buffer[self._lo : self._hi]
        items = live.tolist() if live.ndim == 1 else list(live)
        for offset, value in enumerate(items):
            column[self._lo + offset] = value
        # ``_shared`` stays: the other two buffers still back delivered snapshots.
        return column

    def _store(self, position: int, value: Any, timestamp: Any) -> None:
        """Write one point at ``position``, widening a column to objects if needed."""
        if not _fits(self._val, value):
            self._val = self._as_object(self._val)
        if not _fits(self._ts, timestamp):
            self._ts = self._as_object(self._ts)
        self._val[position] = value
        self._ts[position] = timestamp

    def _start(self, value: Any, timestamp: Any) -> None:
        """Choose the column types of an empty series from its first point."""
        self._ord = np.empty(_MIN_CAPACITY, dtype=np.int64)
        self._val = np.empty(_MIN_CAPACITY, dtype=np.float64 if _is_float(value) else object)
        self._ts = np.empty(_MIN_CAPACITY, dtype=np.float64 if _is_float(timestamp) else object)
        self._lo = self._hi = self._shared = 0

    def _position(self, ordinal: int) -> int | None:
        """Buffer position of a stored ordinal, or None."""
        if self._hi == self._lo or ordinal >= self._frontier:
            return None
        first = int(self._ord[self._lo])
        if ordinal < first:
            return None
        if self._gapless:
            return self._lo + ordinal - first
        live = self._ord[self._lo : self._hi]
        index = int(np.searchsorted(live, ordinal))
        if index < len(live) and live[index] == ordinal:
            return self._lo + index
        return None

    def _trim(self) -> None:
        """Apply the retention cap (keep the newest ``max_points``)."""
        if self._max_points is None:
            return
        overflow = self._hi - self._lo - self._max_points
        if overflow <= 0:
            return
        self._lo += overflow
        self._trim_watermark = max(self._trim_watermark, int(self._ord[self._lo - 1]) + 1)

    # --- writing ----------------------------------------------------------------

    def insert(self, ordinal: int | None, value: Any, timestamp: Any) -> int:
        """
        Insert or overwrite one point.

        Do not mutate ``value`` afterwards: delivered snapshots hold it as-is.

        Args:
            ordinal (int | None): Correlation ordinal; ``None`` assigns the
                next arrival counter (unindexed sources).
            value: Point value.
            timestamp: Point timestamp.

        Returns:
            int: The ordinal the point was stored at.
        """
        ordinal = self._frontier if ordinal is None else int(ordinal)
        self._cached_snapshot = None
        if ordinal >= self._frontier:
            # Hot path: in-order arrival.
            if self._hi == self._lo:
                self._start(value, timestamp)
                self._gapless = True
            elif ordinal > self._frontier:
                self._gapless = False
            self._reserve(1)
            self._store(self._hi, value, timestamp)
            self._ord[self._hi] = ordinal
            self._hi += 1
            self._frontier = ordinal + 1
            self._trim()
            return ordinal
        position = self._position(ordinal)
        if position is not None:
            # Overwrite in place: copy first if a delivered snapshot covers it.
            if position < self._shared:
                self._copy_on_write()
            self._store(position, value, timestamp)
            return ordinal
        self._fill_hole(ordinal, value, timestamp)
        return ordinal

    def _fill_hole(self, ordinal: int, value: Any, timestamp: Any) -> None:
        """Rare: a point below the frontier arrives late (rebuilds the buffers)."""
        live = self._ord[self._lo : self._hi]
        index = int(np.searchsorted(live, ordinal))
        if not _fits(self._val, value):
            self._val = self._as_object(self._val)
        if not _fits(self._ts, timestamp):
            self._ts = self._as_object(self._ts)
        values = self._val[self._lo : self._hi]
        if values.dtype == object:
            values = np.concatenate([values[:index], _object_column([value], 1), values[index:]])
        else:
            values = np.insert(values, index, value, axis=0)
        timestamps = self._ts[self._lo : self._hi]
        if timestamps.dtype == object:
            timestamps = np.concatenate(
                [timestamps[:index], _object_column([timestamp], 1), timestamps[index:]]
            )
        else:
            timestamps = np.insert(timestamps, index, timestamp)
        self._ord = np.insert(live, index, ordinal)
        self._val, self._ts = values, timestamps
        self._lo, self._hi, self._shared = 0, len(self._ord), 0
        first = int(self._ord[0])
        self._gapless = int(self._ord[-1]) - first == len(self._ord) - 1
        self._trim()

    def extend(self, values: Any, timestamps: Any = None, ordinals: Any = None) -> int:
        """
        Append a column of points (e.g. a history read or a batched stream message).

        Into an empty series the arrays are adopted as they are (zero copy); a
        chunk starting at or after the frontier is appended in one copy; any
        other chunk falls back to per-point :meth:`insert`.

        Args:
            values: Column of values; anything ``np.asarray`` accepts (ragged
                inputs are stored as an object array).
            timestamps: Optional column parallel to ``values``; dropped when
                the lengths differ.
            ordinals: Optional strictly increasing nonnegative integer column.
                Omission continues at the frontier (``0..n-1`` for an empty series).

        Returns:
            int: Number of points stored.
        """
        try:
            values = np.atleast_1d(np.asarray(values))
        except ValueError:  # ragged rows (e.g. vlen datasets)
            values = _object_column(list(values), len(values))
        n = len(values)
        if n == 0:
            return 0
        if ordinals is None:
            ordinals = np.arange(self._frontier, self._frontier + n, dtype=np.int64)
        else:
            ordinals = np.asarray(ordinals)
            if (
                ordinals.ndim != 1
                or len(ordinals) != n
                or not np.issubdtype(ordinals.dtype, np.integer)
                or np.any(ordinals < 0)
                or np.any(np.diff(ordinals) <= 0)
            ):
                raise ValueError("Ordinals must be a sorted, unique nonnegative integer column.")
            ordinals = ordinals.astype(np.int64, copy=False)
        if timestamps is not None:
            try:
                timestamps = np.atleast_1d(np.asarray(timestamps))
            except ValueError:
                timestamps = _object_column(list(timestamps), len(timestamps))
            if len(timestamps) != n or timestamps.ndim != 1:
                timestamps = None
        if timestamps is None:
            # Virtual object-None column: a zero-copy broadcast.
            timestamps = np.broadcast_to(np.array([None], dtype=object), (n,))
        self._cached_snapshot = None
        first, last = int(ordinals[0]), int(ordinals[-1])
        if self._hi == self._lo:
            # Adopt the arrays; they may be shared (e.g. a file cache): never written.
            self._ord, self._val, self._ts = ordinals, values, timestamps
            self._lo, self._hi, self._shared = 0, n, n
            self._frontier = last + 1
            self._gapless = last - first == n - 1
            self._trim()
            return n
        if first < self._frontier:
            for ordinal, value, timestamp in zip(ordinals.tolist(), values, timestamps):
                self.insert(ordinal, value, timestamp)
            return n
        if first > self._frontier or last - first != n - 1:
            self._gapless = False
        self._reserve(n)
        start, stop = self._hi, self._hi + n
        self._val = self._appended(self._val, values, start, stop)
        self._ts = self._appended(self._ts, timestamps, start, stop)
        self._ord[start:stop] = ordinals
        self._hi = stop
        self._frontier = last + 1
        self._trim()
        return n

    def adopt_rows(self, values: np.ndarray, timestamps: np.ndarray | None, count: int) -> int:
        """
        Hold the first ``count`` rows of columns that a loader fills in place (zero copy).

        A loader that knows the final size of a stored source allocates its columns once,
        writes rows into them without the lock, and calls this with the lock held whenever
        more rows are ready. Row ``i`` is ordinal ``i``. Until ``count`` reaches
        ``len(values)`` the series is not :attr:`complete`. Rows below ``count`` must not
        be written again: delivered snapshots are views of them.

        Args:
            values (np.ndarray): The value column in its final length.
            timestamps (np.ndarray | None): The parallel timestamp column, or None.
            count (int): Number of leading rows that are ready.

        Returns:
            int: Number of rows that became visible with this call.
        """
        total = len(values)
        if self._val is not values:
            if self._hi != self._lo:
                raise ValueError("adopt_rows() needs an empty series or the columns it holds.")
            if timestamps is None:
                # Virtual object-None column: a zero-copy broadcast.
                timestamps = np.broadcast_to(np.array([None], dtype=object), (total,))
            # Shared and read-only; a loader that asked for it before pays nothing here.
            self._ord = row_ordinals(total)
            self._val, self._ts = values, timestamps
            self._lo = self._hi = self._frontier = 0
            self._gapless = True
        added = count - self._frontier
        if added < 0 or count > total:
            raise ValueError(f"Cannot hold {count} rows: {self._frontier} are held of {total}.")
        self._cached_snapshot = None
        self._hi = self._shared = self._frontier = count
        self._expected = total
        self._trim()
        return added

    def _appended(
        self, buffer: np.ndarray, column: np.ndarray, start: int, stop: int
    ) -> np.ndarray:
        """
        Copy ``column`` into ``buffer`` at ``[start, stop)``.

        Returns:
            np.ndarray: The buffer that now holds the column: ``buffer`` itself, or its
                object copy when the column does not fit its dtype or row shape.
        """
        compatible = (
            buffer.dtype != object
            and column.dtype == buffer.dtype
            and column.shape[1:] == buffer.shape[1:]
        )
        if compatible:
            buffer[start:stop] = column
            return buffer
        if buffer.dtype != object:
            buffer = self._as_object(buffer)
        items = column.tolist() if column.ndim == 1 and column.dtype != object else list(column)
        for offset, value in enumerate(items):
            buffer[start + offset] = value
        return buffer

    def update_metadata(self, **values: Any) -> None:
        """
        Update the series metadata; a ``None`` value removes the key.

        Unlike writing ``metadata`` directly, this invalidates the cached snapshot,
        so a metadata-only change (an error, a reconnect) reaches the consumer.
        """
        for key, value in values.items():
            if value is None:
                self.metadata.pop(key, None)
            else:
                self.metadata[key] = value
        self._cached_snapshot = None

    def declare(self, encoding: str, max_shape: list[int | None] | None = None) -> bool:
        """
        Record how the values of this series reconstruct.

        The first declaration stamps ``async_update_type`` and ``max_shape``, which
        :meth:`SourceData.as_array` and :meth:`SourceData.latest` rely on. Later
        declarations must match it: a source whose encoding changes during a scan
        cannot be reconstructed, so the caller drops that data.

        Args:
            encoding (str): ``"add"``, ``"add_slice"``, ``"replace"`` or a path's own encoding.
            max_shape (list[int | None] | None): Shape bound of one message's value;
                ``None`` entries are unbounded.

        Returns:
            bool: ``True`` if the series uses this encoding (now or already).
        """
        max_shape = list(max_shape) if max_shape is not None else None
        if self.metadata.get("async_update_type") is None:
            self.update_metadata(async_update_type=encoding, max_shape=max_shape)
            return True
        return (
            self.metadata["async_update_type"] == encoding
            and self.metadata.get("max_shape") == max_shape
        )

    def clear(self) -> None:
        """Drop all points and reset the arrival counter."""
        # New buffers: the old ones may back delivered snapshots.
        self._ord = np.empty(0, dtype=np.int64)
        self._val = np.empty(0, dtype=np.float64)
        self._ts = np.empty(0, dtype=np.float64)
        self._lo = self._hi = self._shared = 0
        self._frontier = 0
        self._gapless = True
        self._cached_snapshot = None
        self._trim_watermark = 0
        self._expected = None

    # --- reading ----------------------------------------------------------------

    def __len__(self) -> int:
        return self._hi - self._lo

    def is_known(self, ordinal: int) -> bool:
        """Whether ``ordinal`` is held — or was held and trimmed by the retention cap."""
        if 0 <= ordinal < self._trim_watermark:
            return True
        return self._position(ordinal) is not None

    @property
    def frontier(self) -> int:
        """One past the highest stored ordinal (0 when empty)."""
        return self._frontier

    @property
    def first(self) -> int | None:
        """The lowest held ordinal (None when empty)."""
        return int(self._ord[self._lo]) if self._hi > self._lo else None

    @property
    def gapless(self) -> bool:
        """Whether the held ordinals are one contiguous range."""
        return self._gapless

    @property
    def complete(self) -> bool:
        """Whether every ordinal from 0 to the frontier is present and no load is under way."""
        if self._expected is not None and self._frontier < self._expected:
            return False
        return not self.metadata.get("error") and len(self) == self.frontier

    def snapshot(self) -> SourceData:
        """
        Return an immutable columnar snapshot of this series, in O(1).

        The columns are read-only views of the live region. Cached: repeated
        calls without an intervening write return the same object.
        """
        if self._cached_snapshot is not None:
            return self._cached_snapshot

        def view(buffer: np.ndarray) -> np.ndarray:
            column = buffer[self._lo : self._hi]
            column.flags.writeable = False
            return column

        self._shared = max(self._shared, self._hi)
        self._cached_snapshot = SourceData(
            device=self.device,
            entry=self.entry,
            kind=self.kind,
            ordinals=view(self._ord),
            values=view(self._val),
            timestamps=view(self._ts),
            complete=self.complete,
            metadata=dict(self.metadata),
        )
        return self._cached_snapshot


class _HeldSeries(SourceSeries):
    """
    Member of a continuous bundle: an insert is a *sample* — the bundle
    decides when a row is appended and stores it in every member.
    """

    def __init__(self, bundle: DataBundler, device: str, entry: str, kind: SourceKind, **kwargs):
        super().__init__(device, entry, kind, **kwargs)
        self._bundle = bundle

    def insert(self, ordinal: int | None, value: Any, timestamp: Any) -> int:
        """Take a sample; returns the appended row, or ``-1`` while held."""
        return self._bundle._sample(self, value, timestamp)

    def store_row(self, row: int, value: Any, timestamp: Any) -> None:
        """Append one sample-and-hold row (called by the bundle)."""
        SourceSeries.insert(self, row, value, timestamp)


class DataBundler:
    """
    The aligned series of one subscription for one scan.

    Owns one :class:`SourceSeries` per source and builds
    :class:`SubscriptionUpdate` snapshots. With ``correlation="continuous"``
    it implements the sample-and-hold pairing described in the module
    docstring.
    """

    def __init__(
        self,
        scan_id: str,
        max_points: int | None = None,
        correlation: CorrelationMode | str = CorrelationMode.ORDINAL,
    ):
        self.scan_id = scan_id
        self.max_points = max_points
        self.correlation = CorrelationMode(correlation)
        self.series: dict[SourceKey, SourceSeries] = {}
        self._lag_warned = False
        # continuous mode: latest sample per member and the row counter
        self._latest: dict[SourceKey, Any] = {}
        self._rows = 0

    def get_series(self, device: str, entry: str, kind: SourceKind) -> SourceSeries:
        """Return (creating if needed) the series for one source."""
        key = (device, entry)
        series = self.series.get(key)
        if series is None:
            if self.correlation == CorrelationMode.CONTINUOUS:
                series = _HeldSeries(self, device, entry, kind, max_points=self.max_points)
            else:
                series = SourceSeries(device, entry, kind, max_points=self.max_points)
            self.series[key] = series
        return series

    def adopt(self, series: dict[SourceKey, SourceSeries]) -> None:
        """
        Take over series built elsewhere (e.g. by a worker without the lock).

        A series replaces the bundle's own only when both are plain series; members
        of a continuous bundle keep their sample-and-hold type and receive the
        adopted points as samples.

        Args:
            series (dict[SourceKey, SourceSeries]): Finished series by source key.
        """
        for key, new in series.items():
            current = self.series.get(key)
            if current is None or type(current) is type(
                new
            ):  # pylint: disable=unidiomatic-typecheck
                self.series[key] = new
                continue
            snapshot = new.snapshot()
            for ordinal, value, timestamp in zip(
                snapshot.ordinals, snapshot.values, snapshot.timestamps
            ):
                current.insert(int(ordinal), value, timestamp)

    def has_sample(self, key: SourceKey) -> bool:
        """Whether a continuous member has delivered at least one sample."""
        return key in self._latest

    def sample_many(self, samples: list[tuple[SourceSeries, Any, Any]]) -> int:
        """
        Take samples of several continuous members that arrived together as one row.

        Args:
            samples (list[tuple[SourceSeries, Any, Any]]): ``(member, value, timestamp)``
                read from one message.

        Returns:
            int: The row appended, or ``-1`` while held (a member never delivered).
        """
        for series, value, _ in samples:
            self._latest[series.key] = value
        if len(self._latest) < len(self.series):
            return -1
        row = self._rows
        self._rows += 1
        timestamp = samples[-1][2] if samples else None
        for member in self.series.values():
            member.store_row(row, self._latest[member.key], timestamp)
        return row

    def _sample(self, series: _HeldSeries, value: Any, timestamp: Any) -> int:
        """Take one sample of a continuous member (see :meth:`sample_many`)."""
        return self.sample_many([(series, value, timestamp)])

    def build_update(
        self,
        reason: UpdateReason,
        metadata: dict[str, Any] | None = None,
        defer_alignment: bool = False,
    ) -> SubscriptionUpdate:
        """
        Build an immutable full-state update from the current series, in O(sources).

        Args:
            reason (UpdateReason): Why the update is emitted.
            metadata (dict | None): Subscription-level metadata to attach.
            defer_alignment (bool): Leave ``aligned_ordinals`` as
                :data:`ALIGN_LATER`; the caller aligns outside its lock.

        Returns:
            SubscriptionUpdate: Snapshot whose aligned ordinals are the
                ordinals present in every source.
        """
        lagging = self._lagging_sources()
        sources = {key: series.snapshot() for key, series in self.series.items()}
        frontiers = {series.frontier for series in self.series.values()}
        complete = all(source.complete for source in sources.values()) and len(frontiers) <= 1
        update_metadata = dict(metadata or {})
        if lagging:
            update_metadata["lagging_sources"] = lagging
        return SubscriptionUpdate(
            scan_id=self.scan_id,
            reason=reason,
            sources=sources,
            aligned_ordinals=ALIGN_LATER if defer_alignment else align_ordinals(sources),
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
        if self.correlation == CorrelationMode.CONTINUOUS:
            # Rows start once every member has delivered: a member that has
            # not is what blocks the group, and the only observable reason.
            if not self._latest:
                return []
            lagging = [key for key in self.series if key not in self._latest]
            if lagging and not self._lag_warned:
                self._lag_warned = True
                logger.warning(
                    f"Continuous group: source(s) {lagging} have not delivered a sample yet; "
                    "no rows are emitted until every member has."
                )
            return lagging
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
        self._latest.clear()
        self._rows = 0
