"""
File data path: closed scans from their scan file.

Routing decisions (``resolve``) are answered without file I/O from the
client-side scan history (``ScanHistoryMessage.stored_data_info``, which also
yields the up-front size estimate the size gate uses); the data itself is
read through :class:`~bec_lib.scan_data_container.ScanDataContainer` on a
short-lived worker thread, block by block. Every block is delivered as a
``reason="history"`` update, so a consumer sees a large scan grow while it loads;
``update.complete`` turns true with the last block.
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Iterator, Mapping

import h5py
import numpy as np

from bec_lib.logger import bec_logger
from bec_lib.scan_data_container import SignalSlab

from .. import client_views
from ..data_bundler import DataBundler, SourceSeries
from ..data_path import (
    BindContext,
    DataPath,
    DataSource,
    SourceRequest,
    SourceSpec,
    apply_size_gate,
)
from ..decoding import (
    StoredRows,
    StoredScan,
    acquisition_rows,
    decode_dataset_columns,
    stored_scan_specs,
)
from ..models import SourceKey, SourceKind, UpdateReason
from ..scenarios import Scenario
from ..views import (
    ColumnView,
    JointReduction,
    ViewWindow,
    bin_rows_for,
    held_bytes,
    is_number_column,
)

logger = bec_logger.logger


#: Scan files whose dataset list is remembered; one small dict per file.
_LISTED_FILES = 100


@functools.lru_cache(maxsize=_LISTED_FILES)
def _datasets_of_file(file_path: str | None) -> dict[str, dict[str, dict]] | None:
    """
    The datasets of a scan file as the file writer lists them: device -> signal -> shape, dtype.

    Only the structure of the file is read, once per file. ``None`` when it cannot be read.
    """
    try:
        with h5py.File(file_path, "r") as file:
            listed: dict[str, dict[str, dict]] = {}
            for device, signals in file["/entry/collection/devices"].items():
                for signal, group in signals.items() if isinstance(signals, h5py.Group) else ():
                    value = group.get("value") if isinstance(group, h5py.Group) else None
                    if isinstance(value, h5py.Dataset):
                        info = {"shape": value.shape or (1,), "dtype": value.dtype.name}
                        listed.setdefault(device, {})[signal] = info
            return listed
    except (OSError, KeyError, TypeError):
        return None


class _SourceLoad:
    """
    The load of one stored source: blocks are read without the lock and shown under it.

    The points go into a series of the worker's own bundle. The request adopts that
    series, so a block that was shown costs the consumer no copy.
    """

    def __init__(
        self,
        spec: SourceSpec,
        reference: Any,
        loaded: DataBundler,
        file_path: str,
        view_limit_bytes: int | None = None,
        max_rows: int | None = None,
    ):
        self.spec = spec
        self.done = False
        self.fraction = 0.0
        #: Set when the source is too large to hold whole: only an overview is kept.
        self.view: ColumnView | None = None
        #: Rows of the source in the file, known with its first block.
        self.total: int | None = None
        #: Whether the source is a column of numbers that is held whole, row by row as stored.
        self.numbers = False
        #: The block of a column of numbers that was read last and is not reduced yet:
        #: one past its last row, its samples and its timestamps.
        self.block: tuple[int, Any, Any] | None = None
        self._view_limit_bytes = view_limit_bytes
        self._loaded = loaded
        self._file_path = file_path
        self._series = loaded.get_series(spec.device, spec.entry, spec.kind or SourceKind.UNINDEXED)
        self._rows = StoredRows(file_path)
        self._whole: Mapping | None = None
        self._error: str | None = None
        self._reference = reference
        self._slabs: Iterator | None = None
        self._max_rows = max_rows
        self._first = 0  # first row that is read
        self._tail: list[Mapping] = []
        if reference is None:
            self._fail("Stored signal is missing.")

    @property
    def failed(self) -> bool:
        """Whether the source could not be read."""
        return self._error is not None

    def _fail(self, error: str) -> None:
        self._error = error
        self.done = True

    def read_next(self, scan_id: str) -> None:
        """Read the next block of the source. No lock held."""
        if self.done:
            return
        try:
            if not hasattr(self._reference, "read_slabs"):
                # A reference without read_slabs() delivers the source in one piece.
                self._whole = self._last_rows(self._reference.read())
                self.done = True
            else:
                if self._slabs is None:
                    self._first = self._tail_start()
                    self._slabs = self._reference.read_slabs(self._first or None)
                self._take_slab(next(self._slabs, None))
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                f"Reading {self.spec.device}/{self.spec.entry} from scan {scan_id} failed: {exc}"
            )
            self._fail(str(exc))
        if self.done:
            self.fraction = 1.0
            self.close()

    def _take_slab(self, slab: Any) -> None:
        """Take the next block of the source: for a view, or into the rows that are held."""
        if slab is None or slab.total == 0:
            # Nothing to read in rows: a scalar or an empty dataset.
            self._whole = slab.data if slab is not None else {}
            self.done = True
            return
        if self._first:
            self._take_tail(slab)
            return
        values = slab.data["value"]
        self.total = slab.total
        if slab.start == 0 and self._held_as_view(values, slab.total):
            self.view = ColumnView(slab.total, reference=self._reference, file_path=self._file_path)
        if self.view is not None:
            self.block = (slab.stop, values, slab.data.get("timestamp"))
        else:
            self._rows.write(slab.start, slab.stop, slab.total, slab.data)
            self.numbers = is_number_column(values)
            if self.numbers:
                # Held whole, but its extremes count for the views it is aligned with.
                self.block = (slab.stop, values, None)
        self.fraction = slab.stop / slab.total
        self.done = slab.stop >= slab.total

    def _take_tail(self, slab: Any) -> None:
        """Take a block of the last rows: few enough to be joined in one piece at the end."""
        self._tail.append(slab.data)
        self.fraction = (slab.stop - self._first) / (slab.total - self._first)
        self.done = slab.stop >= slab.total
        if self.done:
            self._whole = {
                name: np.concatenate([np.asarray(part[name]) for part in self._tail])
                for name in ("value", "timestamp")
                if self._tail[0].get(name) is not None
            }

    def _acquisitions(self, shape: tuple[int, ...] | None = None) -> int | None:
        """
        Rows of the stored dataset that are one acquisition each, or ``None``.

        The buffer cap counts acquisitions: it does not cut a single stored frame. A catalog
        written by an older BEC does not list the dataset; its shape in the file then decides.

        Args:
            shape (tuple[int, ...] | None): Shape of the dataset when it is known; else one
                row is read to learn it.
        """
        if not self.spec.provisional:
            return self.spec.stored_acquisitions
        if shape is None:
            slabs = self._reference.read_slabs(0, 1)
            try:
                slab = next(slabs, None)
            finally:
                getattr(slabs, "close", lambda: None)()
            if slab is None or not slab.total:
                return None
            shape = (slab.total, *np.shape(slab.data["value"])[1:])
        return acquisition_rows(tuple(shape), self.spec.acquisition_ndim)

    def _tail_start(self) -> int:
        """The first row to read: with a buffer cap, the last acquisitions of a stack."""
        if self._max_rows is None:
            return 0
        rows = self._acquisitions()
        if rows is None or rows <= self._max_rows:
            return 0
        self._series.expect(rows)  # not complete while it loads
        return rows - self._max_rows

    def _last_rows(self, data: Mapping) -> Mapping:
        """The last ``max_rows`` acquisitions of a source that was read in one piece."""
        shape = np.shape(data.get("value"))
        if self._max_rows is None or not shape or shape[0] <= self._max_rows:
            return data
        if self._acquisitions(shape) is None:
            return data
        rows = shape[0]
        self._first = rows - self._max_rows
        return {
            name: column[self._first :] if np.shape(column)[:1] == (rows,) else column
            for name, column in data.items()
        }

    def _held_as_view(self, values: Any, total: int) -> bool:
        """Whether the column is above the view limit: numbers, one per row."""
        limit = self._view_limit_bytes
        return (
            limit is not None
            and is_number_column(values)
            and held_bytes(total, values.dtype.itemsize) > limit
        )

    def publish(self, request: SourceRequest) -> None:
        """Show what was read so far in the request's bundle. Lock held."""
        if self.view is not None and self._error is None:
            request.state.setdefault("views", {})[self.spec.key] = self.view
            publish_view(request, self.spec, self.view)
            return
        if self.numbers and self._error is None:
            # A window of the views it is aligned with reads its rows while it still loads.
            request.state.setdefault("stored", {})[self.spec.key] = (self.total, self._reference)
        try:
            if self._error is not None:
                raise ValueError(self._error)
            if self._whole is not None:
                if self._whole.get("value") is None:
                    raise ValueError("Stored signal is missing.")
                decode_dataset_columns(
                    self._loaded,
                    self.spec,
                    self._whole,
                    file_path=self._file_path,
                    first=self._first,
                )
                self._whole = None
            else:
                self._rows.publish(self._series)
        except Exception as exc:  # pylint: disable=broad-except
            self.done = True
            self._series.clear()
            self._series.update_metadata(error=str(exc))
            # A column whose overview failed is not served as a view any more: a later
            # request for rows is refused, so the error stays with the source instead of
            # being replaced by the rows that could still be read.
            request.state.get("views", {}).pop(self.spec.key, None)
            request.state.get("stored", {}).pop(self.spec.key, None)
            logger.warning(f"Decoding {self.spec.key} from scan {request.scan_id} failed: {exc}")
        # A plain series is adopted by reference, so every block can be shown. The members
        # of a paired bundle take the points as samples: they get them once, at the end.
        # pylint: disable=unidiomatic-typecheck
        if self.done or type(request.series(self.spec)) is type(self._series):
            request.adopt({self.spec.key: self._series})

    def close(self) -> None:
        """Let go of the file. Any thread, any time."""
        slabs, self._slabs = self._slabs, None
        if slabs is not None and hasattr(slabs, "close"):
            slabs.close()


def publish_view(request: SourceRequest, spec: SourceSpec, view: ColumnView) -> None:
    """Put the points held of a large column into the request's bundle. Lock held."""
    rows, values, timestamps = view.columns()
    series = SourceSeries(spec.device, spec.entry, spec.kind or SourceKind.UNINDEXED)
    series.metadata["file_path"] = view.file_path
    series.metadata["view"] = view.describe(len(rows))
    if len(rows):
        series.extend(values, timestamps, ordinals=rows)
    # The overview keeps some rows of many: the column is complete when all were read.
    series.expect(view.total, rows_read=view.loaded_rows)
    request.adopt({spec.key: series})


class FileDataPath(DataPath):
    """Serves closed scans from the scan file, read on a worker thread."""

    data_source = DataSource.FILE
    scenarios = frozenset({Scenario.CLOSED_SCAN})
    priority = 50
    reports_progress = True

    # --- routing -------------------------------------------------------------

    def _history_message(self, scan_id: str):
        return client_views.history_message(self.client, scan_id)

    def catalog_entry(self, scan_id: str) -> StoredScan | None:
        """
        Return the catalog entry of a closed scan, or ``None`` when this path has none.

        The default is BEC's scan history. A subclass serving stored scans from elsewhere
        returns its own cached entry: an object with the attributes of
        :class:`~bec_lib.data_api.decoding.StoredScan`. Cached state only; lock held.

        Args:
            scan_id (str): The scan.

        Returns:
            StoredScan | None: The entry, or ``None``.
        """
        return self._history_message(scan_id)

    def resolve(self, sources: list[SourceKey], context: BindContext) -> list[SourceSpec] | None:
        msg = self.catalog_entry(context.scan_id)
        if msg is None:
            return None
        if not msg.stored_data_info and hasattr(msg, "model_copy"):
            # Written by an older BEC: the file itself says which datasets it holds.
            listed = _datasets_of_file(getattr(msg, "file_path", None))
            msg = msg.model_copy(update={"stored_data_info": listed}) if listed else msg
        specs = stored_scan_specs(self.client, sources, msg)
        # The estimate costs no file I/O (stored_data_info only); the gate is
        # decided here and enforced by DataPath.open().
        apply_size_gate(specs, context.size_limit_bytes)
        return specs

    # --- request lifecycle ---------------------------------------------------

    def _open(self, request: SourceRequest) -> None:
        msg = self.catalog_entry(request.scan_id)
        if msg is None:
            # The history entry vanished between resolve and open (registry
            # trim); nothing to read.
            for spec in request.specs:
                if spec.available:
                    request.set_status(
                        spec.key, error="History entry disappeared before it could be read."
                    )
            request.notify(UpdateReason.HISTORY)
            return
        request.start_worker(self._read_file, msg, name="data-api-history")

    def close(self, request: SourceRequest) -> None:
        # Nothing to release under the lock: the facade already set
        # ``request.cancelled``, which stops the worker before it publishes, and
        # joins the worker outside the lock (it needs the lock for its final insert).
        return

    def _load_container(self, request: SourceRequest, msg):  # pylint: disable=unused-argument
        """
        Return the stored data of a closed scan. Runs on the worker thread.

        The default reads BEC's scan file through ``client.history``. A subclass that
        fetches stored data elsewhere (a REST service, an archive) overrides only this
        method and keeps the worker, decoding, progress and cancellation. It returns
        any object whose ``devices.get(device)`` gives a mapping from storage names to
        references. A reference has ``read()``, which returns the value and timestamp
        columns. A reference that also has ``read_slabs()``, as
        :class:`~bec_lib.scan_data_container.SignalDataReference` does, is loaded and
        delivered block by block.

        Args:
            request (SourceRequest): The request being served.
            msg (ScanHistoryMessage): The scan's catalog entry, as used by ``resolve``.

        Returns:
            The container, or ``None`` when the scan is no longer available.
        """
        return self.client.history.get_by_scan_id(request.scan_id)

    def _read_file(self, request: SourceRequest, msg) -> None:
        loads: list[_SourceLoad] = []
        try:
            container = self._load_container(request, msg)
            if container is None:
                raise ValueError(f"History container for scan {request.scan_id} disappeared.")
            progress_cb = request.progress_callback
            loaded = DataBundler(request.scan_id)  # the loads apply max_buffer_size themselves
            loads = [
                _SourceLoad(
                    spec,
                    self._reference(container, spec),
                    loaded,
                    msg.file_path,
                    request.context.view_limit_bytes,
                    request.bundle.max_buffer_size,
                )
                for spec in request.specs
                if spec.available
            ]
            pending = list(loads)
            reductions: dict[int, JointReduction] = {}
            while pending:
                if request.cancelled:
                    return
                # One block of every source: reading and copying happen on this worker. A
                # column that is ahead of the ones it is reduced with waits for them.
                for load in pending:
                    reduction = reductions.get(load.total)
                    if reduction is None or not reduction.waits(load):
                        load.read_next(request.scan_id)
                grown = self._reduce_views(loads, reductions)
                with self._lock:
                    if request.cancelled:
                        return
                    # Only showing the blocks takes the shared lock. A view that has read
                    # all its rows still grows while the views it is reduced with read on.
                    for load in pending + [load for load in grown if load not in pending]:
                        load.publish(request)
                pending = [load for load in pending if not load.done]
                if not pending:
                    break
                if progress_cb is not None:
                    self._report_progress(request, progress_cb, loads)
                request.notify(UpdateReason.HISTORY, partial=True)
            if request.cancelled:
                return
            if progress_cb is not None:
                self._report_progress(request, progress_cb, loads, finished=True)
            request.notify(UpdateReason.HISTORY)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception(f"History read for scan {request.scan_id} failed.")
            with self._lock:
                if request.cancelled:
                    return
                for spec in request.specs:
                    request.series(spec).clear()
                    request.set_status(spec.key, error=str(exc))
            request.notify(UpdateReason.HISTORY)
        finally:
            for load in loads:
                load.close()

    # --- views of large columns ----------------------------------------------

    @staticmethod
    def _reduce_views(
        loads: list[_SourceLoad], reductions: dict[int, JointReduction]
    ) -> list[_SourceLoad]:
        """
        Reduce the blocks the views have read. Worker thread, no lock.

        The columns of one request with the same number of rows are aligned with each
        other. They are reduced together, to the same rows (see :class:`JointReduction`).
        A column that is held whole takes part in choosing the rows, so that its extremes
        are among the rows the views keep.

        Returns:
            list[_SourceLoad]: The loads whose view got new rows.
        """
        views = {load.total: load.view for load in loads if load.view is not None}
        for load in loads:
            block, load.block = load.block, None
            view = views.get(load.total)
            if view is None:
                continue  # no view among the columns with as many rows
            if load.total not in reductions:
                reductions[load.total] = JointReduction(view.bin_rows)
            if load.failed:
                reductions[load.total].drop(load)
            elif block is not None:
                reductions[load.total].add(load, *block)
        grown = []
        for reduction in reductions.values():
            result = reduction.reduce()
            if result is None:
                continue
            rows, columns = result
            for load, (values, timestamps) in columns.items():
                if load.view is not None:
                    load.view.add_rows(rows, values, timestamps, reduction.reduced)
                    grown.append(load)
        return grown

    def request_view(
        self, request: SourceRequest, key: SourceKey, start: int, stop: int, max_points: int
    ) -> bool:
        views: dict[SourceKey, ColumnView] = request.state.get("views", {})
        view = views.get(key)
        if view is None or request.cancelled:
            return False
        start = max(0, min(start, view.total))
        stop = max(start, min(stop, view.total))
        # The views it is aligned with show the same rows: one request moves them all.
        members = {k: v for k, v in views.items() if v.total == view.total}
        generation = max(member.generation for member in members.values()) + 1
        for member in members.values():
            member.generation = generation
        request.start_worker(
            self._read_view,
            members,
            self._partner_slabs(request, members, start, stop),
            start,
            stop,
            max_points,
            generation,
            name="data-api-view",
        )
        return True

    @staticmethod
    def _partner_slabs(
        request: SourceRequest, members: dict[SourceKey, ColumnView], start: int, stop: int
    ) -> dict[SourceKey, Iterator]:
        """
        The rows ``start:stop`` of the columns that are held whole and aligned with views.

        Their extremes count for the rows a window keeps. The rows are in memory, or still
        in the file while the column loads. Lock held.

        Args:
            request (SourceRequest): The request the views belong to.
            members (dict[SourceKey, ColumnView]): The views that share their rows.
            start (int): First row wanted.
            stop (int): One past the last row wanted.

        Returns:
            dict[SourceKey, Iterator]: Per column, the blocks of its rows.
        """
        total = next(iter(members.values())).total
        stored = request.state.get("stored", {})
        partners = {}
        for spec in request.specs:
            if not spec.available or spec.key in members:
                continue
            values = request.series(spec).snapshot().values
            rows, reference = stored.get(spec.key, (None, None))
            if is_number_column(values) and len(values) == total:
                slab = SignalSlab(start, stop, total, {"value": values[start:stop]})
                partners[spec.key] = iter([slab])
            elif rows == total:
                partners[spec.key] = reference.read_slabs(start, stop)
        return partners

    def _read_view(
        self,
        request: SourceRequest,
        members: dict[SourceKey, ColumnView],
        partners: dict[SourceKey, Iterator],
        start: int,
        stop: int,
        max_points: int,
        generation: int,
    ) -> None:
        """
        Read the rows ``start:stop`` of aligned large columns and show them. Worker thread.

        Args:
            request (SourceRequest): The request the views belong to.
            members (dict[SourceKey, ColumnView]): The views that share their rows.
            partners (dict[SourceKey, Iterator]): The blocks of the same rows of the columns
                that are held whole: they take part in choosing the rows.
            start (int): First row wanted.
            stop (int): One past the last row wanted.
            max_points (int): Upper bound of the points for these rows.
            generation (int): The request counter of the views when this read was asked for.
        """

        def superseded() -> bool:
            # closed, or a newer request took over
            return request.cancelled or any(v.generation != generation for v in members.values())

        bin_rows = bin_rows_for(stop - start, max_points)
        reduction = JointReduction(bin_rows, first_row=start)
        slabs = {key: view.reference.read_slabs(start, stop) for key, view in members.items()}
        slabs |= partners
        try:
            window = self._reduce_window(reduction, slabs, superseded)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Reading rows {start}:{stop} of {list(members)} failed: {exc}")
            return
        if window is None:
            return
        rows, columns = window
        shown = False
        with self._lock:
            if superseded():
                return
            for spec in request.specs:
                view = members.get(spec.key)
                if view is None or request.state.get("views", {}).get(spec.key) is not view:
                    continue  # the overview failed meanwhile: its error stands
                values, timestamps = columns[spec.key]
                view.window = ViewWindow(start, stop, bin_rows, rows, values, timestamps)
                publish_view(request, spec, view)
                shown = True
        if shown:
            request.notify(UpdateReason.HISTORY)

    @staticmethod
    def _reduce_window(
        reduction: JointReduction, slabs: dict[SourceKey, Iterator], superseded: Callable[[], bool]
    ) -> tuple[np.ndarray, dict[SourceKey, tuple[np.ndarray, np.ndarray | None]]] | None:
        """
        Read aligned columns block by block and reduce them together.

        Args:
            reduction (JointReduction): The reduction of the rows that are read.
            slabs (dict[SourceKey, Iterator]): Per column, the blocks of the rows to read.
            superseded (Callable[[], bool]): Whether the read is no longer wanted.

        Returns:
            tuple | None: The kept row numbers and, per column, its samples and timestamps
                at those rows; ``None`` when the read was superseded.
        """
        rows: list[np.ndarray] = [np.empty(0, dtype=np.int64)]
        parts: dict[SourceKey, list[tuple[np.ndarray, np.ndarray | None]]] = {
            key: [] for key in slabs
        }
        try:
            idle = False
            while slabs and not idle:
                idle = True  # until a view reads: one whose file ended early stalls the others
                for key in list(slabs):
                    if superseded():
                        return None
                    if reduction.waits(key):
                        continue  # ahead of the views it is reduced with
                    idle = False
                    slab = next(slabs[key], None)
                    if slab is None or slab.stop <= slab.start:
                        del slabs[key]
                        continue
                    reduction.add(key, slab.stop, slab.data["value"], slab.data.get("timestamp"))
                result = reduction.reduce()
                if result is not None:
                    rows.append(result[0])
                    for key, column in parts.items():
                        column.append(result[1][key])
        finally:
            for iterator in slabs.values():
                getattr(iterator, "close", lambda: None)()
        columns = {}
        for key, column in parts.items():
            timestamps = None
            if column and all(part[1] is not None for part in column):
                timestamps = np.concatenate([part[1] for part in column])
            values = np.concatenate([part[0] for part in column]) if column else np.empty(0)
            columns[key] = (values, timestamps)
        return np.concatenate(rows), columns

    @staticmethod
    def _report_progress(request, progress_cb, loads, finished: bool = False) -> None:
        """Report the mean fraction of the request's sources that is loaded."""
        fraction = 1.0 if finished or not loads else sum(l.fraction for l in loads) / len(loads)
        try:
            progress_cb(fraction)
        except Exception:  # pylint: disable=broad-except
            # The data is published; a failing progress consumer must not undo it.
            logger.exception(f"Progress callback failed for scan {request.scan_id}.")

    @staticmethod
    def _reference(container, spec: SourceSpec):
        """The stored reference of one source in ``container``, or None."""
        device = container.devices.get(spec.device)
        if not device:
            return None
        reference = device.get(spec.storage_name or spec.entry)
        if reference is None and spec.storage_name != spec.entry:
            reference = device.get(spec.entry)
        return reference
