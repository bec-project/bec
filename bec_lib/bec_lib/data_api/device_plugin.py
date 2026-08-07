"""
Device stream plugin: serves scan-less, continuously updating sources.

Covers the three device-scoped feeds widgets consume outside any scan:
``device_readback`` (motor positions etc., pubsub), ``device_monitor_1d``
(monitor streams; requested with the reserved entry name ``"monitor_1d"``)
and ``device_preview`` (preview signals, requested with the preview signal
name as entry). All sources are unindexed standalone streams with
arrival-counter ordinals; subscriptions should bound them with
``max_points``.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import DeviceMessage

from .models import SourceKey
from .plugin_base import DataSourcePlugin, SourceRequest, SourceSpec

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient

logger = bec_logger.logger

#: Reserved entry name selecting a device's 1D monitor stream.
MONITOR_1D_ENTRY = "monitor_1d"

#: Device scope sentinel used as the scan id of scan-less subscriptions.
DEVICE_SCOPE = ""


class DeviceStreamPlugin(DataSourcePlugin):
    """Serves scan-less device streams (readback, monitor, preview)."""

    priority = 90

    def __init__(self, client: BECClient, lock: threading.RLock | None = None):
        self.client = client
        self._lock = lock or threading.RLock()
        self._requests: list[SourceRequest] = []
        # (stream, device, entry) -> {"endpoint","callback","refcount"}
        self._feeds: dict[tuple[str, str, str], dict[str, Any]] = {}

    # --- lifecycle -----------------------------------------------------------

    def disconnect(self) -> None:
        with self._lock:
            for feed in self._feeds.values():
                self.client.connector.unregister(topics=feed["endpoint"], cb=feed["callback"])
            self._feeds.clear()
            self._requests.clear()

    # --- resolution ----------------------------------------------------------

    def _is_preview_signal(self, device: str, entry: str) -> bool:
        devices = getattr(getattr(self.client, "device_manager", None), "devices", None)
        dev = devices.get(device) if hasattr(devices, "get") else None
        if dev is None:
            return False
        device_info = getattr(dev, "_info", {})
        signals = device_info.get("signals", {}) if isinstance(device_info, dict) else {}
        if not isinstance(signals, dict):
            return False
        for info in signals.values():
            if not isinstance(info, dict):
                continue
            if info.get("obj_name") == entry or info.get("component_name") == entry:
                return info.get("signal_class") == "PreviewSignal"
        return False

    def _device_known(self, device: str) -> bool:
        devices = getattr(getattr(self.client, "device_manager", None), "devices", None)
        return hasattr(devices, "get") and devices.get(device) is not None

    def resolve(self, sources: list[SourceKey], scan_id: str | None) -> list[SourceSpec] | None:
        if scan_id != DEVICE_SCOPE:
            return None
        specs: list[SourceSpec] = []
        for device, entry in sources:
            if entry == MONITOR_1D_ENTRY:
                stream = MONITOR_1D_ENTRY
            elif self._is_preview_signal(device, entry):
                stream = "preview"
            elif self._device_known(device):
                stream = "readback"
            else:
                specs.append(SourceSpec(device=device, entry=entry, available=False))
                continue
            specs.append(
                SourceSpec(device=device, entry=entry, kind="unindexed", storage_name=stream)
            )
        return specs

    # --- request lifecycle ---------------------------------------------------

    def open(self, request: SourceRequest) -> None:
        with self._lock:
            self._requests.append(request)
            for spec in request.specs:
                if spec.available:
                    self._ensure_feed(spec)
        request.notify("backfill")

    def close(self, request: SourceRequest) -> None:
        with self._lock:
            if request in self._requests:
                self._requests.remove(request)
            for spec in request.specs:
                if spec.available:
                    self._release_feed(spec)

    # --- feeds ---------------------------------------------------------------

    @staticmethod
    def _feed_key(spec: SourceSpec) -> tuple[str, str, str]:
        return (spec.storage_name or "readback", spec.device, spec.entry)

    def _endpoint_for(self, spec: SourceSpec):
        if spec.storage_name == MONITOR_1D_ENTRY:
            return MessageEndpoints.device_monitor_1d(spec.device)
        if spec.storage_name == "preview":
            return MessageEndpoints.device_preview(spec.device, spec.entry)
        return MessageEndpoints.device_readback(spec.device)

    def _ensure_feed(self, spec: SourceSpec) -> None:
        key = self._feed_key(spec)
        feed = self._feeds.get(key)
        if feed is not None:
            feed["refcount"] += 1
            return
        endpoint = self._endpoint_for(spec)

        def connector_callback(msg, *, _key=key):
            self._on_stream_message(msg, _key)

        self.client.connector.register(endpoint, cb=connector_callback)
        self._feeds[key] = {"endpoint": endpoint, "callback": connector_callback, "refcount": 1}

    def _release_feed(self, spec: SourceSpec) -> None:
        key = self._feed_key(spec)
        feed = self._feeds.get(key)
        if feed is None:
            return
        feed["refcount"] -= 1
        if feed["refcount"] <= 0:
            self.client.connector.unregister(topics=feed["endpoint"], cb=feed["callback"])
            del self._feeds[key]

    # --- ingestion -----------------------------------------------------------

    def _extract_point(self, stream: str, entry: str, msg_obj) -> tuple[Any, Any, dict] | None:
        metadata = dict(getattr(msg_obj, "metadata", None) or {})
        if stream == "readback":
            if not isinstance(msg_obj, DeviceMessage):
                return None
            signal_data = msg_obj.signals.get(entry)
            if signal_data is None:
                return None
            return signal_data.get("value"), signal_data.get("timestamp"), metadata
        data = getattr(msg_obj, "data", None)
        if data is None:
            return None
        return data, metadata.get("timestamp"), metadata

    def _on_stream_message(self, msg, key: tuple[str, str, str]) -> None:
        stream, device, entry = key
        # Stream registrations deliver a dict with the message under "data";
        # pubsub registrations (device_readback) deliver a MessageObject
        # carrying the message in .value.
        if isinstance(msg, dict):
            msg_obj = msg.get("data")
        else:
            msg_obj = getattr(msg, "value", msg)
        if isinstance(msg_obj, list):
            msg_obj = msg_obj[-1] if msg_obj else None
        if msg_obj is None:
            return
        with self._lock:
            targets = []
            for request in self._requests:
                for spec in request.specs:
                    if not spec.available or self._feed_key(spec) != key:
                        continue
                    point = self._extract_point(stream, entry, msg_obj)
                    if point is None:
                        continue
                    value, timestamp, metadata = point
                    series = request.bundle.get_series(device, entry, "unindexed")
                    scan_id = metadata.get("scan_id")
                    if scan_id is not None:
                        series.metadata["scan_id"] = scan_id
                    series.metadata["stream"] = stream
                    series.insert(None, value, timestamp)
                    targets.append(request)
        for request in targets:
            request.notify("live")
