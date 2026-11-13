"""Scan segments: scan_item.live_data mappings or ScanMessage;
ordinal = point_id.
"""

from __future__ import annotations

from typing import Any, Mapping

from bec_lib.messages import ScanMessage

from ..data_bundler import DataBundler
from ..data_path import SourceSpec
from ..models import SourceKind

# --- scan segments (monitored sources) ---------------------------------------


def decode_live_data(bundle: DataBundler, specs: list[SourceSpec], live_data: Mapping) -> int:
    """
    Insert the not-yet-known points of a scan item's live data.

    Args:
        bundle (DataBundler): Target bundle.
        specs (list[SourceSpec]): Request specs; only available monitored
            specs are read.
        live_data (Mapping): ``scan_item.live_data`` — ``device -> entry ->
            {point_id: {"value", "timestamp"}}``.

    Returns:
        int: Number of points inserted.
    """
    inserted = 0
    for spec in specs:
        if spec.kind != SourceKind.MONITORED or not spec.available:
            continue
        signal_data = live_data.get(spec.device, {}).get(spec.entry)
        if signal_data is None:
            continue
        series = bundle.get_series(spec.device, spec.entry, SourceKind.MONITORED)
        for point_id, point in signal_data.items():
            if series.is_known(point_id) or not isinstance(point, dict):
                continue
            series.insert(point_id, point.get("value"), point.get("timestamp"))
            inserted += 1
    return inserted


def decode_scan_message(bundle: DataBundler, specs: list[SourceSpec], msg: ScanMessage) -> int:
    """
    Insert one scan segment (``point_id`` -> one value per monitored spec).

    Args:
        bundle (DataBundler): Target bundle.
        specs (list[SourceSpec]): Request specs; only available monitored
            specs are read.
        msg (ScanMessage): The segment.

    Returns:
        int: Number of points inserted.
    """
    return decode_scan_point(bundle, specs, msg.point_id, msg.data)


def decode_scan_point(
    bundle: DataBundler, specs: list[SourceSpec], point_id: Any, data: Any
) -> int:
    """
    Insert one scan point (``device -> entry -> {"value", "timestamp"}``) of a segment.

    Points already held (or trimmed by retention) are skipped, so a segment that
    also reached the request through its backfill is not applied twice.

    Args:
        bundle (DataBundler): Target bundle.
        specs (list[SourceSpec]): Request specs; only available monitored specs are read.
        point_id: The scan point id.
        data: The segment's readings.

    Returns:
        int: Number of points inserted.
    """
    if not isinstance(point_id, int) or not isinstance(data, dict):
        return 0
    inserted = 0
    for spec in specs:
        if spec.kind != SourceKind.MONITORED or not spec.available:
            continue
        point = data.get(spec.device, {}).get(spec.entry)
        if not isinstance(point, dict):
            continue
        series = bundle.get_series(spec.device, spec.entry, SourceKind.MONITORED)
        if series.is_known(point_id):
            continue
        series.insert(point_id, point.get("value"), point.get("timestamp"))
        inserted += 1
    return inserted
