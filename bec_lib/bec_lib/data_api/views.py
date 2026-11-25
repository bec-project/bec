"""
View-driven loading: a large stored column is held as an overview plus the part in view.

A column of many million rows is too large to keep whole next to a plot that shows a few
thousand pixels of it. Instead the subscription holds

- an *overview*: the smallest and the largest sample of every bin of rows, built once
  while the column streams from the file, and
- a *detail window*: the rows the consumer is looking at, read from the file on request
  and reduced the same way to the resolution that was asked for (or left as they are when
  they fit).

Both are ordinary points: every kept sample has its row number as ordinal, so the source a
consumer receives is the column with gaps, coarse where nobody looks and fine in the window.
The reduction keeps the extremes of every bin, so a spike of one sample stays visible at
any zoom level. Columns that are aligned with each other (the x and y of one curve) are
reduced together: every one keeps the rows at which any of them has an extreme, so a spike
is still there after the columns were paired row by row.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

#: Points of the overview of one column (two per bin).
OVERVIEW_POINTS = 200_000


def minmax_index(values: np.ndarray, bin_rows: int) -> np.ndarray:
    """
    Positions of the smallest and the largest sample of every bin of ``bin_rows`` rows.

    Args:
        values (np.ndarray): Consecutive rows of a 1D column.
        bin_rows (int): Rows per bin; 1 or less keeps every row.

    Returns:
        np.ndarray: The positions in ``values`` that are kept, ascending.
    """
    count = len(values)
    if bin_rows <= 1 or count <= 2:
        return np.arange(count, dtype=np.int64)
    full = count // bin_rows
    # A NaN (a missing reading) is neither the smallest nor the largest sample of its bin;
    # a bin of NaN only keeps its first row.
    lows = highs = values
    if values.dtype.kind == "f":
        missing = np.isnan(values)
        lows, highs = np.where(missing, np.inf, values), np.where(missing, -np.inf, values)
    kept = []
    if full:
        shape, base = (full, bin_rows), np.arange(full, dtype=np.int64) * bin_rows
        kept += [
            base + lows[: full * bin_rows].reshape(shape).argmin(axis=1),
            base + highs[: full * bin_rows].reshape(shape).argmax(axis=1),
        ]
    if count > full * bin_rows:
        tail = slice(full * bin_rows, None)
        kept.append(
            np.array([lows[tail].argmin(), highs[tail].argmax()], dtype=np.int64) + full * bin_rows
        )
    return np.unique(np.concatenate(kept))


def minmax_decimate(
    values: np.ndarray, first_row: int, bin_rows: int
) -> tuple[np.ndarray, np.ndarray]:
    """
    Keep the smallest and the largest sample of every bin of ``bin_rows`` rows.

    Args:
        values (np.ndarray): Consecutive rows of a 1D column.
        first_row (int): Row number of ``values[0]``.
        bin_rows (int): Rows per bin; 1 or less keeps every row.

    Returns:
        tuple[np.ndarray, np.ndarray]: The row numbers of the kept samples, ascending,
            and the samples themselves.
    """
    index = minmax_index(values, bin_rows)
    if len(index) == len(values):
        return index + first_row, values  # every row is kept: no copy
    return index + first_row, values[index]


class JointReduction:
    """
    Reduces columns that share their rows to the same rows.

    The members are read block by block, and their blocks need not end on the same rows.
    What every member has read is reduced together: the rows at which any member has the
    smallest or the largest sample of a bin are kept of all of them. The members then
    pair row by row without losing an extreme, and they are complete at the same row.
    """

    def __init__(self, bin_rows: int, first_row: int = 0):
        self.bin_rows = bin_rows
        #: Rows below this one are reduced.
        self.reduced = first_row
        # Per member: the rows read so far, and the blocks that are not reduced yet.
        self._read: dict[Any, int] = {}
        self._blocks: dict[Any, list[tuple[np.ndarray, np.ndarray | None]]] = {}

    def add(self, member: Any, stop: int, values: np.ndarray, timestamps: Any = None) -> None:
        """
        Add the next rows of one member.

        Args:
            member: The key of the member.
            stop (int): One past the last row of the block; the blocks of a member follow
                each other without gaps.
            values (np.ndarray): The rows.
            timestamps: Their timestamps; dropped when they are not one per row.
        """
        values = np.asarray(values)
        if timestamps is not None:
            timestamps = np.asarray(timestamps)
            if timestamps.shape != (len(values),):
                timestamps = None
        self._blocks.setdefault(member, []).append((values, timestamps))
        self._read[member] = stop

    def drop(self, member: Any) -> None:
        """Leave a member out from now on (its load failed): the others go on without it."""
        self._read.pop(member, None)
        self._blocks.pop(member, None)

    def reduce(self) -> tuple[np.ndarray, dict[Any, tuple[np.ndarray, np.ndarray | None]]] | None:
        """
        Reduce the rows that every member has read since the last call.

        Returns:
            tuple | None: The kept row numbers and, per member, its samples and timestamps
                at those rows; ``None`` when no further row is read by every member.
        """
        if not self._read:
            return None
        count = min(self._read.values()) - self.reduced
        if count <= 0:
            return None
        columns = {member: self._take(member, count) for member in self._read}
        # The first and the last row are kept as well: what is reduced then ends on the
        # last row that was read, for every member alike.
        kept = [np.array([0, count - 1], dtype=np.int64)]
        kept += [minmax_index(values, self.bin_rows) for values, _ in columns.values()]
        index = np.unique(np.concatenate(kept))
        rows = index + self.reduced
        self.reduced += count
        return rows, {
            member: (values[index], None if timestamps is None else timestamps[index])
            for member, (values, timestamps) in columns.items()
        }

    def _take(self, member: Any, count: int) -> tuple[np.ndarray, np.ndarray | None]:
        """Take the next ``count`` unreduced rows of a member out of its blocks."""
        blocks = self._blocks[member]
        values = blocks[0][0] if len(blocks) == 1 else np.concatenate([b[0] for b in blocks])
        with_timestamps = all(b[1] is not None for b in blocks)
        timestamps = None
        if with_timestamps:
            timestamps = (
                blocks[0][1] if len(blocks) == 1 else np.concatenate([b[1] for b in blocks])
            )
        rest = (values[count:], None if timestamps is None else timestamps[count:])
        self._blocks[member] = [rest] if len(rest[0]) else []
        return values[:count], None if timestamps is None else timestamps[:count]


def is_number_column(values: Any) -> bool:
    """Whether ``values`` are numbers, one per row: what a view can be made of."""
    dtype = getattr(values, "dtype", None)
    return dtype is not None and dtype.kind in "biuf" and getattr(values, "ndim", 0) == 1


def held_bytes(rows: int, itemsize: int) -> int:
    """
    Memory of a column that is held whole: its samples and one 8 byte row number per row.

    The view limit is compared with this number, not with the size of the dataset: for
    small sample types the row numbers are the larger part.
    """
    return rows * (itemsize + 8)


def bin_rows_for(rows: int, max_points: int) -> int:
    """
    Rows per bin so that ``rows`` rows come out as at most about ``max_points`` points.

    Rows that fit are kept as they are (one row per bin); otherwise every bin gives two
    points, its smallest and its largest sample.
    """
    if rows <= max_points:
        return 1
    return -(-2 * rows // max(2, max_points))


@dataclass
class ViewWindow:
    """The rows ``start:stop`` of a column, reduced with ``bin_rows`` rows per bin."""

    start: int
    stop: int
    bin_rows: int
    rows: np.ndarray
    values: np.ndarray
    timestamps: np.ndarray | None = None


@dataclass
class ColumnView:
    """
    What is held of one large stored column: its overview and, at most, one detail window.

    The owner (a data path) fills it from its worker and changes it with the lock held.
    """

    total: int
    reference: Any = None
    file_path: str | None = None
    bin_rows: int = 1
    loaded_rows: int = 0
    window: ViewWindow | None = None
    #: Counts the detail requests; a worker publishes only if it still serves the newest.
    generation: int = 0
    # The overview: two columns of the final size, filled block by block. Points that were
    # handed out are never written again, so every delivered snapshot is a view of them.
    _rows: np.ndarray | None = field(default=None, repr=False)
    _values: np.ndarray | None = field(default=None, repr=False)
    _timestamps: np.ndarray | None = field(default=None, repr=False)
    _count: int = 0

    def __post_init__(self):
        self.bin_rows = bin_rows_for(self.total, OVERVIEW_POINTS)

    def add_block(self, start: int, stop: int, values: np.ndarray, timestamps: Any = None) -> None:
        """Reduce the rows ``start:stop`` on their own and add them to the overview."""
        values = np.asarray(values)
        index = minmax_index(values, self.bin_rows)
        kept_timestamps = None
        if timestamps is not None and np.shape(timestamps) == (len(values),):
            kept_timestamps = np.asarray(timestamps)[index]
        self.add_rows(index + start, values[index], kept_timestamps, stop)

    def add_rows(
        self, rows: np.ndarray, values: np.ndarray, timestamps: np.ndarray | None, loaded_rows: int
    ) -> None:
        """
        Add reduced rows to the overview. One worker writes; no lock needed.

        Args:
            rows (np.ndarray): The row numbers that are kept, ascending, after the held ones.
            values (np.ndarray): The samples at those rows.
            timestamps (np.ndarray | None): Their timestamps, when the file has them.
            loaded_rows (int): Rows of the column that are read up to here.
        """
        end = self._count + len(rows)
        if self._rows is None:
            capacity = max(end, 2 * (self.total // self.bin_rows + 1))
            self._values = np.empty(capacity, dtype=values.dtype)
            if timestamps is not None:
                self._timestamps = np.empty(capacity, dtype=timestamps.dtype)
            self._rows = np.empty(capacity, dtype=np.int64)  # last: columns() looks at it first
        if end > len(self._rows):  # more points than planned: the old columns stay valid
            grown = max(end, 2 * len(self._rows))

            def grow(column: np.ndarray) -> np.ndarray:
                return np.concatenate([column[: self._count], np.empty(grown, column.dtype)])

            self._rows, self._values = grow(self._rows), grow(self._values)
            if self._timestamps is not None:
                self._timestamps = grow(self._timestamps)
        self._rows[self._count : end] = rows
        self._values[self._count : end] = values
        if self._timestamps is not None:
            if timestamps is None:
                self._timestamps = None  # not every row has one: the column has none
            else:
                self._timestamps[self._count : end] = timestamps
        self._count = end
        self.loaded_rows = loaded_rows

    def columns(self) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
        """
        The held points: the overview, with the detail window in place of its rows.

        Returns:
            tuple: The row numbers, the samples and, when the file has them, the timestamps.
        """
        if self._rows is None:
            return np.empty(0, dtype=np.int64), np.empty(0), None
        rows, values = self._rows[: self._count], self._values[: self._count]
        timestamps = None if self._timestamps is None else self._timestamps[: self._count]
        window = self.window
        if window is None or len(window.rows) == 0:
            return rows, values, timestamps
        first, last = np.searchsorted(rows, [window.start, window.stop])
        if timestamps is not None and window.timestamps is not None:
            timestamps = np.concatenate([timestamps[:first], window.timestamps, timestamps[last:]])
        else:
            timestamps = None
        return (
            np.concatenate([rows[:first], window.rows, rows[last:]]),
            np.concatenate([values[:first], window.values, values[last:]]),
            timestamps,
        )

    def describe(self, points: int) -> dict:
        """What a consumer needs to know about the held points (the ``view`` metadata)."""
        window = self.window
        return {
            "total_rows": self.total,
            "loaded_rows": self.loaded_rows,
            "overview_bin": self.bin_rows,
            "window": (
                None
                if window is None
                else {"start": window.start, "stop": window.stop, "bin": window.bin_rows}
            ),
            "points": points,
        }
