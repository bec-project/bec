"""
BEC data-model knowledge shared by every data path: what a requested signal is
(signal_info) and how each payload shape maps to ordinal-keyed points (the decoders).

Layer rule: this package imports data_path, data_bundler, models, client_views and
bec_lib.messages only; never paths or data_api. Decoders never lock, never notify and never
touch the client; the caller serializes access to the target bundle (the DataAPI lock for a
shared bundle, nothing for a worker-private one). Each decoder returns the number of points
inserted.
"""

from __future__ import annotations

from .async_updates import (
    ASYNC_UPDATE_HANDLERS,
    AsyncPoint,
    AsyncUpdateHandler,
    decode_add,
    decode_add_slice,
    decode_async_message,
    decode_async_update,
    decode_replace,
    parse_async_update,
)
from .device_stream import (
    decode_stream_message,
    decode_stream_points,
    decode_stream_values,
    extract_stream_point,
)
from .scan_points import decode_live_data, decode_scan_message, decode_scan_point
from .signal_info import (
    ASYNC_SIGNAL_CLASSES,
    StoredScan,
    async_declarations,
    async_signal_info,
    device_known,
    device_signals,
    device_stream_specs,
    estimate_stored_bytes,
    is_configuration_signal,
    is_monitored_signal,
    is_preview_signal,
    multi_signal_name,
    open_scan_specs,
    recorded_points,
    scan_memory_specs,
    stored_info_field,
    stored_key_candidates,
    stored_scan_specs,
)
from .stored import StoredRows, decode_dataset_columns

__all__ = [
    "StoredScan",
    "ASYNC_UPDATE_HANDLERS",
    "AsyncPoint",
    "AsyncUpdateHandler",
    "decode_add",
    "decode_add_slice",
    "decode_async_message",
    "decode_async_update",
    "decode_replace",
    "parse_async_update",
    "decode_stream_message",
    "decode_stream_points",
    "decode_stream_values",
    "extract_stream_point",
    "decode_live_data",
    "decode_scan_message",
    "decode_scan_point",
    "ASYNC_SIGNAL_CLASSES",
    "async_declarations",
    "async_signal_info",
    "device_known",
    "device_signals",
    "device_stream_specs",
    "estimate_stored_bytes",
    "is_configuration_signal",
    "is_monitored_signal",
    "is_preview_signal",
    "multi_signal_name",
    "open_scan_specs",
    "recorded_points",
    "scan_memory_specs",
    "stored_info_field",
    "stored_key_candidates",
    "stored_scan_specs",
    "StoredRows",
    "decode_dataset_columns",
]
