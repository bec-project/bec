"""
History data plugin: serves terminal scans.

Routing decisions (``resolve``) are answered without file I/O from the
client-side scan history (``ScanHistoryMessage.stored_data_info``); the data
itself is read through :class:`~bec_lib.scan_data_container.ScanDataContainer`
(blocking h5py behind an LRU cache) on a short-lived worker thread and
delivered as one ``reason="history"`` emission. For the writer-latency window
(scan terminal but file not yet published) monitored data is served from the
scan item's in-memory live data.
"""

from __future__ import annotations

import math
import threading
from typing import TYPE_CHECKING

import numpy as np

from bec_lib.logger import bec_logger

from .models import SourceKey
from .plugin_base import DataSourcePlugin, SourceRequest, SourceSpec

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

logger = bec_logger.logger

_ASYNC_SIGNAL_CLASSES = ["AsyncSignal", "AsyncMultiSignal", "DynamicSignal"]


class HistoryDataPlugin(DataSourcePlugin):
    """Serves scans that have ended (file-backed, with an in-memory fallback)."""

    priority = 50

    def __init__(self, client: BECClient, lock: threading.RLock | None = None):
        self.client = client
        self._lock = lock or threading.RLock()

    # --- resolution ----------------------------------------------------------

    def _history_message(self, scan_id: str):
        history = getattr(self.client, "history", None)
        if history is None:
            return None
        # The message registry is the only file-I/O-free view of the history;
        # entries with unreadable files are already filtered out by ScanHistory.
        return getattr(history, "_scan_data", {}).get(scan_id)

    def _terminal_scan_item(self, scan_id: str):
        from .live_plugin import TERMINAL_SCAN_STATES

        queue = getattr(self.client, "queue", None)
        if queue is None:
            return None
        scan_item = queue.scan_storage.find_scan_by_ID(scan_id)
        if scan_item is None or getattr(scan_item, "status", None) not in TERMINAL_SCAN_STATES:
            return None
        return scan_item

    def _async_declaration(self, device: str, entry: str) -> tuple[bool, str | None, str | None]:
        """
        Best-effort async lookup from live device info (may be stale for old
        scans, in which case the dataset-shape heuristic decides).

        Returns:
            tuple: (is declared async, acquisition group, storage name). The
                storage name matters because the file writer (and therefore
                ``stored_data_info``) keys async datasets by it, not by the
                signal's ``obj_name``.
        """
        device_manager = getattr(self.client, "device_manager", None)
        if not device_manager:
            return False, None, None
        try:
            signals = device_manager.get_bec_signals(_ASYNC_SIGNAL_CLASSES)
        except Exception:  # pylint: disable=broad-except
            return False, None, None
        for dev_name, _, entry_info in signals:
            if dev_name != device or entry_info.get("obj_name") != entry:
                continue
            group = entry_info.get("acquisition_group")
            if not group:
                signal_info = entry_info.get("describe", {})
                if isinstance(signal_info, dict):
                    signal_info = signal_info.get("signal_info", {})
                if isinstance(signal_info, dict):
                    group = signal_info.get("acquisition_group")
            return True, group, entry_info.get("storage_name")
        return False, None, None

    def resolve(self, sources: list[SourceKey], scan_id: str) -> list[SourceSpec] | None:
        if not scan_id:
            # Device-scoped (scan-less) subscriptions are not ours.
            return None
        msg = self._history_message(scan_id)
        if msg is not None:
            stored = msg.stored_data_info or {}
            if not stored:
                # Older history messages carry no stored_data_info: classify
                # from the device declaration and let the file read decide.
                specs = []
                for device, entry in sources:
                    declared_async, group, storage_name = self._async_declaration(device, entry)
                    specs.append(
                        SourceSpec(
                            device=device,
                            entry=entry,
                            kind="async" if declared_async else "monitored",
                            acquisition_group=group,
                            storage_name=storage_name,
                        )
                    )
                return specs
            num_points = getattr(msg, "num_monitored_readouts", None) or getattr(
                msg, "num_points", None
            )
            specs = []
            for device, entry in sources:
                declared_async, group, storage_name = self._async_declaration(device, entry)
                stored_device = stored.get(device, {})
                stored_key, info = entry, None
                for candidate in self._stored_key_candidates(device, entry, storage_name):
                    info = stored_device.get(candidate)
                    if info is not None:
                        stored_key = candidate
                        break
                if info is None:
                    specs.append(SourceSpec(device=device, entry=entry, available=False))
                    continue
                # ScanHistoryMessage.stored_data_info values are pydantic
                # _StoredDataInfo objects, not dicts.
                shape = tuple(self._info_field(info, "shape") or ())
                # An async-signal declaration in the (possibly newer) device
                # config wins; otherwise a 1-D dataset with one row per
                # monitored readout is a monitored signal.
                is_async = declared_async or not (
                    len(shape) == 1 and num_points is not None and shape[0] == num_points
                )
                if is_async:
                    specs.append(
                        SourceSpec(
                            device=device,
                            entry=entry,
                            kind="async",
                            acquisition_group=group,
                            storage_name=stored_key,
                        )
                    )
                else:
                    specs.append(
                        SourceSpec(
                            device=device, entry=entry, kind="monitored", storage_name=stored_key
                        )
                    )
            return specs

        scan_item = self._terminal_scan_item(scan_id)
        if scan_item is not None:
            # Writer-latency window: the file is not published yet; monitored
            # data is complete in the in-memory live data.
            live_data = getattr(scan_item, "live_data", None) or {}
            specs = []
            for device, entry in sources:
                available = live_data.get(device, {}).get(entry) is not None
                specs.append(
                    SourceSpec(
                        device=device,
                        entry=entry,
                        kind="monitored" if available else None,
                        available=available,
                    )
                )
            return specs
        return None

    @staticmethod
    def _stored_key_candidates(device: str, entry: str, storage_name: str | None) -> list[str]:
        """
        Dataset-key candidates for one source in ``stored_data_info``/the file.

        Async datasets are stored under the signal's storage name. When device
        info is unavailable (old scans, config reloads in flight), the storage
        name is still derivable from the BEC naming convention
        ``obj_name == f"{device}_{storage_name}"``.
        """
        candidates = [entry]
        if storage_name and storage_name not in candidates:
            candidates.append(storage_name)
        prefix = f"{device}_"
        if entry.startswith(prefix):
            derived = entry[len(prefix) :]
            if derived and derived not in candidates:
                candidates.append(derived)
        return candidates

    @staticmethod
    def _info_field(info, name):
        if isinstance(info, dict):
            return info.get(name)
        return getattr(info, name, None)

    def estimate_bytes(self, sources: list[SourceKey], scan_id: str) -> int | None:
        """
        Estimate the total size of the requested sources from the scan history
        metadata (dataset shapes and dtypes) without touching the file.

        Args:
            sources (list[SourceKey]): Requested sources.
            scan_id (str): Identifier of the scan.

        Returns:
            int | None: Estimated size in bytes, or ``None`` when the scan has
                no stored-data metadata (e.g. writer-latency window).
        """
        msg = self._history_message(scan_id)
        if msg is None:
            return None
        stored = msg.stored_data_info or {}
        total = 0
        for device, entry in sources:
            stored_device = stored.get(device) or {}
            _, _, storage_name = self._async_declaration(device, entry)
            info = None
            for candidate in self._stored_key_candidates(device, entry, storage_name):
                info = stored_device.get(candidate)
                if info is not None:
                    break
            if info is None:
                continue
            shape = tuple(self._info_field(info, "shape") or ())
            dtype = self._info_field(info, "dtype")
            try:
                itemsize = np.dtype(dtype).itemsize if dtype else 8
            except TypeError:
                itemsize = 8
            total += int(math.prod(shape)) * itemsize if shape else itemsize
        return total

    # --- request lifecycle ---------------------------------------------------

    def open(self, request: SourceRequest) -> None:
        request.state["cancelled"] = False
        msg = self._history_message(request.scan_id)
        if msg is None:
            self._fill_from_live_data(request)
            request.notify("history")
            return
        worker = threading.Thread(
            target=self._read_file, args=(request, msg), name="data-api-history", daemon=True
        )
        request.state["worker"] = worker
        worker.start()

    def close(self, request: SourceRequest) -> None:
        # Only flag here: close() runs under the api lock, which the worker
        # needs for its final insert — joining happens in the facade, outside
        # the lock (Subscription.close).
        request.state["cancelled"] = True

    # --- data paths ----------------------------------------------------------

    def _fill_from_live_data(self, request: SourceRequest) -> None:
        scan_item = self._terminal_scan_item(request.scan_id)
        if scan_item is None:
            return
        live_data = getattr(scan_item, "live_data", None) or {}
        with self._lock:
            for spec in request.specs:
                if not spec.available:
                    continue
                signal_data = live_data.get(spec.device, {}).get(spec.entry)
                if signal_data is None:
                    continue
                series = request.bundle.get_series(spec.device, spec.entry, "monitored")
                for point_id, point in signal_data.items():
                    if isinstance(point, dict):
                        series.insert(point_id, point.get("value"), point.get("timestamp"))

    def _read_file(self, request: SourceRequest, msg) -> None:
        try:
            container = self.client.history.get_by_scan_id(request.scan_id)
            if container is None:
                logger.warning(f"History container for scan {request.scan_id} disappeared.")
                return
            progress_cb = request.state.get("progress_callback")
            available = [spec for spec in request.specs if spec.available]
            columns: dict[SourceKey, dict] = {}
            for spec_index, spec in enumerate(available):
                if request.state.get("cancelled"):
                    continue
                try:
                    device_data = container.devices.get(spec.device)
                    signal_ref = None
                    if device_data:
                        signal_ref = device_data.get(spec.storage_name or spec.entry)
                        if signal_ref is None and spec.storage_name != spec.entry:
                            signal_ref = device_data.get(spec.entry)
                    data = None
                    if signal_ref is not None:
                        if progress_cb is not None:
                            n_specs = len(available)

                            def _sub_progress(fraction, _base=spec_index, _n=n_specs):
                                progress_cb((_base + min(1.0, fraction)) / _n)

                            try:
                                data = signal_ref.read(progress=_sub_progress)
                            except TypeError:
                                # reference without chunked-read support
                                data = signal_ref.read()
                        else:
                            data = signal_ref.read()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning(
                        f"Reading {spec.device}/{spec.entry} from scan {request.scan_id} "
                        f"failed: {exc}"
                    )
                    continue
                if data is None:
                    continue
                columns[spec.key] = data

            if request.state.get("cancelled"):
                return
            with self._lock:
                if request.state.get("cancelled"):
                    return
                for spec in request.specs:
                    data = columns.get(spec.key)
                    if data is None:
                        continue
                    values = data.get("value")
                    timestamps = data.get("timestamp")
                    if values is None:
                        continue
                    series = request.bundle.get_series(
                        spec.device, spec.entry, spec.kind or "monitored"
                    )
                    series.metadata["file_path"] = getattr(msg, "file_path", None)
                    # Row i is ordinal i by writer construction (monitored:
                    # point i; async: async ordinal i). The bulk fill keeps the
                    # file's numpy columns intact — per-point inserts on large
                    # scans held the shared lock for seconds and forced an
                    # O(n) numpy->python->numpy round trip on the GUI thread.
                    if not series.extend_bulk(values, timestamps):
                        n_ts = len(timestamps) if timestamps is not None else 0
                        for i, value in enumerate(values):
                            timestamp = timestamps[i] if i < n_ts else None
                            series.insert(i, value, timestamp)
            if progress_cb is not None:
                try:
                    progress_cb(1.0)
                except Exception:  # pylint: disable=broad-except
                    pass
            request.notify("history")
        except Exception:  # pylint: disable=broad-except
            logger.exception(f"History read for scan {request.scan_id} failed.")
