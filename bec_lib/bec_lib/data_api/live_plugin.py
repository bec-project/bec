"""
Live data plugin: serves open (non-terminal) scans.

Monitored sources are fed from scan-segment events via the scan item's
point_id-keyed live data; async sources are fed from the per-scan
``device_async_signal`` Redis streams (replayed ``from_start``), with
add/add_slice/replace payloads mapped onto ordinals (message counter or slice
row). All mutation happens under the DataAPI-wide reentrant lock.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import DeviceAsyncUpdate, DeviceMessage

from .models import SourceKey
from .plugin_base import DataSourcePlugin, SourceRequest, SourceSpec

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

logger = bec_logger.logger

#: Scan states after which no further live data is produced.
TERMINAL_SCAN_STATES = frozenset({"closed", "aborted", "halted", "user_completed"})

_ASYNC_SIGNAL_CLASSES = ["AsyncSignal", "AsyncMultiSignal", "DynamicSignal"]


class LiveDataPlugin(DataSourcePlugin):
    """Serves scans that are known to scan storage and not terminal."""

    priority = 10

    def __init__(self, client: BECClient, lock: threading.RLock | None = None):
        self.client = client
        self._lock = lock or threading.RLock()
        self._connect_id: int | str | None = None
        # scan_id -> open requests
        self._requests: dict[str, list[SourceRequest]] = {}
        # (scan_id, device, storage_name) -> {"endpoint","callback","refcount"}
        self._feeds: dict[tuple[str, str, str], dict[str, Any]] = {}
        # (scan_id, device, entry) -> {row_index: accumulated list}
        self._slice_rows: dict[tuple[str, str, str], dict[int, list]] = {}
        # (device, entry) -> async signal info (positive hits only)
        self._async_info_cache: dict[SourceKey, dict] = {}

    # --- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        self._connect_id = self.client.callbacks.register("scan_segment", self._on_scan_segment)

    def disconnect(self) -> None:
        if self._connect_id is not None:
            self.client.callbacks.remove(self._connect_id)
            self._connect_id = None
        with self._lock:
            for feed in self._feeds.values():
                self.client.connector.unregister(topics=feed["endpoint"], cb=feed["callback"])
            self._feeds.clear()
            self._requests.clear()
            self._slice_rows.clear()
            self._async_info_cache.clear()

    # --- resolution ----------------------------------------------------------

    def _scan_item(self, scan_id: str):
        queue = getattr(self.client, "queue", None)
        if queue is None:
            return None
        return queue.scan_storage.find_scan_by_ID(scan_id)

    def _async_signal_info(self, device: str, entry: str) -> dict | None:
        cached = self._async_info_cache.get((device, entry))
        if cached is not None:
            return cached
        if not self.client.device_manager:
            return None
        for dev_name, _, entry_info in self.client.device_manager.get_bec_signals(
            _ASYNC_SIGNAL_CLASSES
        ):
            if dev_name == device and entry_info.get("obj_name") == entry:
                self._async_info_cache[(device, entry)] = entry_info
                return entry_info
        return None

    @staticmethod
    def _acquisition_group(entry_info: dict | None) -> str | None:
        if not isinstance(entry_info, dict):
            return None
        group = entry_info.get("acquisition_group")
        if group:
            return group
        signal_info = entry_info.get("describe", {})
        if isinstance(signal_info, dict):
            signal_info = signal_info.get("signal_info", {})
        return signal_info.get("acquisition_group") if isinstance(signal_info, dict) else None

    def _is_monitored(self, device: str, entry: str, scan_item) -> bool:
        readout_priority = scan_item.status_message.readout_priority or {}
        if device not in readout_priority.get("monitored", []):
            return False
        devices = getattr(getattr(self.client, "device_manager", None), "devices", None)
        dev = devices.get(device) if hasattr(devices, "get") else None
        if dev is None:
            return entry == device
        device_info = getattr(dev, "_info", {})
        signals = device_info.get("signals", {}) if isinstance(device_info, dict) else {}
        if not isinstance(signals, dict) or not signals:
            return entry == device
        return any(info.get("obj_name") == entry for info in signals.values())

    def resolve(self, sources: list[SourceKey], scan_id: str) -> list[SourceSpec] | None:
        scan_item = self._scan_item(scan_id)
        if scan_item is None or scan_item.status_message is None:
            return None
        status = getattr(scan_item, "status", None)
        if not isinstance(status, str) or status in TERMINAL_SCAN_STATES:
            # Terminal (or malformed) scans belong to the history plugin,
            # even while the scan item is still in the storage deque.
            return None

        specs: list[SourceSpec] = []
        for device, entry in sources:
            if self._is_monitored(device, entry, scan_item):
                specs.append(SourceSpec(device=device, entry=entry, kind="monitored"))
                continue
            info = self._async_signal_info(device, entry)
            if info is not None:
                specs.append(
                    SourceSpec(
                        device=device,
                        entry=entry,
                        kind="async",
                        acquisition_group=self._acquisition_group(info),
                        storage_name=info.get("storage_name"),
                    )
                )
                continue
            specs.append(SourceSpec(device=device, entry=entry, available=False))
        return specs

    # --- request lifecycle ---------------------------------------------------

    def open(self, request: SourceRequest) -> None:
        with self._lock:
            self._requests.setdefault(request.scan_id, []).append(request)
            for spec in request.specs:
                if spec.kind == "async" and spec.available:
                    self._ensure_feed(request.scan_id, spec)
            scan_item = self._scan_item(request.scan_id)
            if scan_item is not None:
                self._feed_monitored(scan_item, [request])
        request.notify("backfill")

    def close(self, request: SourceRequest) -> None:
        with self._lock:
            requests = self._requests.get(request.scan_id, [])
            if request in requests:
                requests.remove(request)
            if not requests:
                self._requests.pop(request.scan_id, None)
            for spec in request.specs:
                if spec.kind == "async" and spec.available:
                    self._release_feed(request.scan_id, spec)
            if request.scan_id not in self._requests:
                for key in [k for k in self._slice_rows if k[0] == request.scan_id]:
                    del self._slice_rows[key]

    # --- async feeds ---------------------------------------------------------

    def _ensure_feed(self, scan_id: str, spec: SourceSpec) -> None:
        key = (scan_id, spec.device, spec.storage_name or spec.entry)
        feed = self._feeds.get(key)
        if feed is not None:
            feed["refcount"] += 1
            return
        endpoint = MessageEndpoints.device_async_signal(
            scan_id=scan_id, device=spec.device, signal=spec.storage_name or spec.entry
        )

        def connector_callback(msg, *, _scan_id=scan_id, _device=spec.device):
            self._on_async_message(msg, _scan_id, _device)

        self.client.connector.register(endpoint, cb=connector_callback, from_start=True)
        self._feeds[key] = {"endpoint": endpoint, "callback": connector_callback, "refcount": 1}

    def _release_feed(self, scan_id: str, spec: SourceSpec) -> None:
        key = (scan_id, spec.device, spec.storage_name or spec.entry)
        feed = self._feeds.get(key)
        if feed is None:
            return
        feed["refcount"] -= 1
        if feed["refcount"] <= 0:
            self.client.connector.unregister(topics=feed["endpoint"], cb=feed["callback"])
            del self._feeds[key]

    # --- data ingestion ------------------------------------------------------

    def _on_scan_segment(self, _content: dict, metadata: dict) -> None:
        scan_id = (_content or {}).get("scan_id") or (metadata or {}).get("scan_id")
        if scan_id is None:
            return
        with self._lock:
            requests = list(self._requests.get(scan_id, []))
            if not requests:
                return
            scan_item = self._scan_item(scan_id)
            if scan_item is None:
                return
            self._feed_monitored(scan_item, requests)
        for request in requests:
            request.notify("live")

    def _feed_monitored(self, scan_item, requests: list[SourceRequest]) -> None:
        live_data = getattr(scan_item, "live_data", None)
        if live_data is None:
            return
        for request in requests:
            for spec in request.specs:
                if spec.kind != "monitored" or not spec.available:
                    continue
                signal_data = live_data.get(spec.device, {}).get(spec.entry)
                if signal_data is None:
                    continue
                series = request.bundle.get_series(spec.device, spec.entry, "monitored")
                known = series.ordinals
                for point_id, point in signal_data.items():
                    if point_id in known or not isinstance(point, dict):
                        continue
                    series.insert(point_id, point.get("value"), point.get("timestamp"))

    def _on_async_message(self, msg: dict, scan_id: str, device: str) -> None:
        msg_obj = msg.get("data")
        if not isinstance(msg_obj, DeviceMessage):
            return
        metadata = dict(msg_obj.metadata or {})
        try:
            async_update = DeviceAsyncUpdate.model_validate(metadata.get("async_update", {}))
        except Exception:  # pylint: disable=broad-except
            logger.warning(f"Dropping async update with invalid async_update metadata: {metadata}")
            return
        async_indices = metadata.get("async_indices", {})

        with self._lock:
            requests = list(self._requests.get(scan_id, []))
            if not requests:
                return
            for request in requests:
                for spec in request.specs:
                    if spec.kind != "async" or spec.device != device or not spec.available:
                        continue
                    signal_data = msg_obj.signals.get(spec.entry)
                    if signal_data is None:
                        continue
                    self._ingest_async_point(
                        request, spec, scan_id, signal_data, async_update, async_indices, metadata
                    )
        for request in requests:
            request.notify("live")

    def _ingest_async_point(
        self,
        request: SourceRequest,
        spec: SourceSpec,
        scan_id: str,
        signal_data: dict,
        async_update: DeviceAsyncUpdate,
        async_indices: dict,
        metadata: dict,
    ) -> None:
        value = signal_data.get("value")
        timestamp = signal_data.get("timestamp", metadata.get("timestamp"))
        series = request.bundle.get_series(spec.device, spec.entry, "async")
        series.metadata.update(
            {
                "async_update_type": async_update.type,
                "acquisition_group": spec.acquisition_group or metadata.get("acquisition_group"),
                "max_shape": async_update.max_shape,
            }
        )

        if async_update.type == "add_slice":
            rows_key = (scan_id, spec.device, spec.entry)
            rows = self._slice_rows.setdefault(rows_key, {})
            row = async_update.index
            if row is None:
                logger.warning(
                    f"add_slice update without index for {spec.device}/{spec.entry}; dropped."
                )
                return
            if row == -1:
                row = max(rows) if rows else 0
            fragment = list(value) if isinstance(value, (list, tuple)) else [value]
            rows.setdefault(row, []).extend(fragment)
            series.insert(row, list(rows[row]), timestamp)
            return

        if async_update.type == "replace":
            # A replace source exposes one point: its current full state.
            series.insert(0, value, timestamp)
            return

        # "add": one fragment per message, keyed by the per-scan message counter.
        ordinal = async_indices.get(spec.entry)
        series.insert(ordinal if isinstance(ordinal, int) else None, value, timestamp)
