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
any zoom level.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

#: Points of the overview of one column (two per bin).
OVERVIEW_POINTS = 200_000


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
    count = len(values)
    if bin_rows <= 1 or count <= 2:
        return np.arange(first_row, first_row + count, dtype=np.int64), values
    full = count // bin_rows
    kept = []
    if full:
        body = values[: full * bin_rows].reshape(full, bin_rows)
        base = np.arange(full, dtype=np.int64) * bin_rows
        kept += [base + body.argmin(axis=1), base + body.argmax(axis=1)]
    if count > full * bin_rows:
        tail = values[full * bin_rows :]
        kept.append(np.array([tail.argmin(), tail.argmax()], dtype=np.int64) + full * bin_rows)
    index = np.unique(np.concatenate(kept))
    return index + first_row, values[index]


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
    _count: int = 0

    def __post_init__(self):
        self.bin_rows = bin_rows_for(self.total, OVERVIEW_POINTS)

    def add_block(self, start: int, stop: int, values: np.ndarray) -> None:
        """Add the rows ``start:stop`` to the overview. One worker writes; no lock needed."""
        rows, kept = minmax_decimate(np.asarray(values), start, self.bin_rows)
        if self._rows is None:
            blocks = -(-self.total // max(1, stop - start))
            capacity = 2 * (self.total // self.bin_rows + 1) + 2 * (blocks + 1)
            self._rows = np.empty(capacity, dtype=np.int64)
            self._values = np.empty(capacity, dtype=kept.dtype)
        end = self._count + len(rows)
        if end > len(self._rows):  # blocks of unequal size: make room, the old columns stay valid
            grown = max(end, 2 * len(self._rows))
            self._rows = np.concatenate([self._rows[: self._count], np.empty(grown, np.int64)])
            self._values = np.concatenate(
                [self._values[: self._count], np.empty(grown, self._values.dtype)]
            )
        self._rows[self._count : end] = rows
        self._values[self._count : end] = kept
        self._count = end
        self.loaded_rows = stop

    def columns(self) -> tuple[np.ndarray, np.ndarray]:
        """The held points: the overview, with the detail window in place of its rows."""
        if self._rows is None:
            return np.empty(0, dtype=np.int64), np.empty(0)
        rows, values = self._rows[: self._count], self._values[: self._count]
        window = self.window
        if window is None or len(window.rows) == 0:
            return rows, values
        first, last = np.searchsorted(rows, [window.start, window.stop])
        return (
            np.concatenate([rows[:first], window.rows, rows[last:]]),
            np.concatenate([values[:first], window.values, values[last:]]),
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
