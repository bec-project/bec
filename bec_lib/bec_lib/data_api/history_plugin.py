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

import threading
from typing import TYPE_CHECKING

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

    def _async_declaration(self, device: str, entry: str) -> tuple[bool, str | None]:
        """
        Best-effort async lookup from live device info (may be stale for old
        scans, in which case the dataset-shape heuristic decides).

        Returns:
            tuple[bool, str | None]: (is declared async, acquisition group).
        """
        device_manager = getattr(self.client, "device_manager", None)
        if not device_manager:
            return False, None
        try:
            signals = device_manager.get_bec_signals(_ASYNC_SIGNAL_CLASSES)
        except Exception:  # pylint: disable=broad-except
            return False, None
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
            return True, group
        return False, None

    def resolve(self, sources: list[SourceKey], scan_id: str) -> list[SourceSpec] | None:
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
                info = stored.get(device, {}).get(entry)
                if info is None:
                    specs.append(SourceSpec(device=device, entry=entry, available=False))
                    continue
                shape = tuple(info.get("shape") or ())
                # An async-signal declaration in the (possibly newer) device
                # config wins; otherwise a 1-D dataset with one row per
                # monitored readout is a monitored signal.
                declared_async, group = self._async_declaration(device, entry)
                is_async = declared_async or not (
                    len(shape) == 1 and num_points is not None and shape[0] == num_points
                )
                if is_async:
                    specs.append(
                        SourceSpec(
                            device=device, entry=entry, kind="async", acquisition_group=group
                        )
                    )
                else:
                    specs.append(SourceSpec(device=device, entry=entry, kind="monitored"))
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
            columns: dict[SourceKey, dict] = {}
            for spec in request.specs:
                if not spec.available or request.state.get("cancelled"):
                    continue
                try:
                    device_data = container.devices.get(spec.device)
                    signal_ref = device_data.get(spec.entry) if device_data else None
                    data = signal_ref.read() if signal_ref is not None else None
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
                    n_ts = len(timestamps) if timestamps is not None else 0
                    # Row i is ordinal i by writer construction (monitored:
                    # point i; async: async ordinal i).
                    for i, value in enumerate(values):
                        timestamp = timestamps[i] if i < n_ts else None
                        series.insert(i, value, timestamp)
            request.notify("history")
        except Exception:  # pylint: disable=broad-except
            logger.exception(f"History read for scan {request.scan_id} failed.")
