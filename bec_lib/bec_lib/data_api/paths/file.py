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

from typing import Any, Iterator, Mapping

from bec_lib.logger import bec_logger

from .. import client_views
from ..data_bundler import DataBundler
from ..data_path import (
    BindContext,
    DataPath,
    DataSource,
    SourceRequest,
    SourceSpec,
    apply_size_gate,
)
from ..decoding import StoredRows, StoredScan, decode_dataset_columns, stored_scan_specs
from ..models import SourceKey, SourceKind, UpdateReason
from ..scenarios import Scenario

logger = bec_logger.logger


class _SourceLoad:
    """
    The load of one stored source: blocks are read without the lock and shown under it.

    The points go into a series of the worker's own bundle. The request adopts that
    series, so a block that was shown costs the consumer no copy.
    """

    def __init__(self, spec: SourceSpec, reference: Any, loaded: DataBundler, file_path: str):
        self.spec = spec
        self.done = False
        self.fraction = 0.0
        self._loaded = loaded
        self._file_path = file_path
        self._series = loaded.get_series(spec.device, spec.entry, spec.kind or SourceKind.UNINDEXED)
        self._rows = StoredRows(file_path)
        self._whole: Mapping | None = None
        self._error: str | None = None
        self._reference = reference
        self._slabs: Iterator | None = None
        if reference is None:
            self._fail("Stored signal is missing.")
        elif hasattr(reference, "read_slabs"):
            self._slabs = reference.read_slabs()

    def _fail(self, error: str) -> None:
        self._error = error
        self.done = True

    def read_next(self, scan_id: str) -> None:
        """Read the next block of the source. No lock held."""
        if self.done:
            return
        try:
            if self._slabs is None:
                # A reference without read_slabs() delivers the source in one piece.
                self._whole = self._reference.read()
                self.done = True
            else:
                slab = next(self._slabs, None)
                if slab is None or slab.total == 0:
                    # Nothing to read in rows: a scalar or an empty dataset.
                    self._whole = slab.data if slab is not None else {}
                    self.done = True
                else:
                    self._rows.write(slab.start, slab.stop, slab.total, slab.data)
                    self.fraction = slab.stop / slab.total
                    self.done = slab.stop >= slab.total
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                f"Reading {self.spec.device}/{self.spec.entry} from scan {scan_id} failed: {exc}"
            )
            self._fail(str(exc))
        if self.done:
            self.fraction = 1.0
            self.close()

    def publish(self, request: SourceRequest) -> None:
        """Show what was read so far in the request's bundle. Lock held."""
        try:
            if self._error is not None:
                raise ValueError(self._error)
            if self._whole is not None:
                if self._whole.get("value") is None:
                    raise ValueError("Stored signal is missing.")
                decode_dataset_columns(
                    self._loaded, self.spec, self._whole, file_path=self._file_path
                )
                self._whole = None
            else:
                self._rows.publish(self._series)
        except Exception as exc:  # pylint: disable=broad-except
            self.done = True
            self._series.clear()
            self._series.update_metadata(error=str(exc))
            logger.warning(f"Decoding {self.spec.key} from scan {request.scan_id} failed: {exc}")
        # A plain series is adopted by reference, so every block can be shown. The members
        # of a continuous bundle take the points as samples: they get them once, at the end.
        # pylint: disable=unidiomatic-typecheck
        if self.done or type(request.series(self.spec)) is type(self._series):
            request.adopt({self.spec.key: self._series})

    def close(self) -> None:
        """Let go of the file. Any thread, any time."""
        slabs, self._slabs = self._slabs, None
        if slabs is not None and hasattr(slabs, "close"):
            slabs.close()


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
            loaded = DataBundler(request.scan_id, max_points=request.bundle.max_points)
            loads = [
                _SourceLoad(spec, self._reference(container, spec), loaded, msg.file_path)
                for spec in request.specs
                if spec.available
            ]
            pending = list(loads)
            while pending:
                if request.cancelled:
                    return
                # One block of every source: reading and copying happen on this worker.
                for load in pending:
                    load.read_next(request.scan_id)
                with self._lock:
                    if request.cancelled:
                        return
                    # Only showing the blocks takes the shared lock.
                    for load in pending:
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
