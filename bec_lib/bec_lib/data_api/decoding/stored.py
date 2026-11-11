"""
Stored datasets -> points: one row of a dataset is one point.

A scan file holds the finished dataset of every signal: one row per scan point for a
monitored signal, and for an async signal the rows the file writer assembled from its
updates. The rows are delivered in stored order, numbered ``0..n-1``.
"""

from __future__ import annotations

from typing import Mapping

import numpy as np

from ..data_bundler import DataBundler, SourceSeries, row_ordinals
from ..data_path import SourceSpec
from ..models import SourceKind


def decode_dataset_columns(
    bundle: DataBundler, spec: SourceSpec, data: Mapping, *, file_path: str | None = None
) -> int:
    """
    Insert the stored columns of one source; row ``i`` becomes ordinal ``i``.

    Args:
        bundle (DataBundler): Target bundle; the caller serializes access to it.
        spec (SourceSpec): The resolved source.
        data (Mapping): The stored ``value`` column and, optionally, its ``timestamp``
            column (dropped when it is not parallel to the values).
        file_path (str | None): Provenance recorded on the source.

    Returns:
        int: Number of points loaded.
    """
    values = data.get("value")
    if values is None:
        return 0
    series = bundle.get_series(spec.device, spec.entry, spec.kind or SourceKind.UNINDEXED)
    series.metadata["file_path"] = file_path
    # Explicit ordinals: row i is ordinal i also when the series already holds points.
    rows = np.shape(values)[0] if np.ndim(values) else 1
    return series.extend(values, data.get("timestamp"), ordinals=np.arange(rows, dtype=np.int64))


class StoredRows:
    """
    The stored rows of one source, loaded block by block.

    The columns are allocated once, in their final size, when the first block arrives.
    :meth:`write` copies a block into them and needs no lock. :meth:`publish` makes the
    rows written so far visible in a series without copying, so it is cheap to call with
    the lock held. Row ``i`` becomes ordinal ``i``, as in :func:`decode_dataset_columns`.
    """

    def __init__(self, file_path: str | None = None):
        self.file_path = file_path
        self.total = 0
        self.written = 0
        self._values: np.ndarray | None = None
        self._timestamps: np.ndarray | None = None
        self._ordinals: np.ndarray | None = None

    def write(self, start: int, stop: int, total: int, data: Mapping) -> None:
        """
        Copy the rows ``start:stop`` of ``total`` into the columns. No lock needed.

        Args:
            start (int): First row of the block. Blocks arrive in order, without gaps.
            stop (int): One past the last row of the block.
            total (int): Number of rows of the source.
            data (Mapping): The ``value`` rows and, optionally, their ``timestamp`` rows.
        """
        if start != self.written:
            raise ValueError(f"Rows {start}:{stop} do not follow the {self.written} rows held.")
        values = np.asarray(data["value"])
        timestamps = data.get("timestamp")
        if self._values is None:
            self.total = total
            # Created here, without the lock; publish() then finds it ready.
            self._ordinals = row_ordinals(total)
            self._values = np.empty((total,) + values.shape[1:], dtype=values.dtype)
            if timestamps is not None:
                self._timestamps = np.empty(total, dtype=np.asarray(timestamps).dtype)
        self._values[start:stop] = values
        if self._timestamps is not None:
            self._timestamps[start:stop] = timestamps
        self.written = stop

    def publish(self, series: SourceSeries) -> int:
        """
        Show the rows written so far in ``series``. The caller holds the lock.

        Args:
            series (SourceSeries): The series of the source.

        Returns:
            int: Number of rows that became visible with this call.
        """
        series.metadata["file_path"] = self.file_path
        if self._values is None:
            return 0
        return series.adopt_rows(self._values, self._timestamps, self.written)
