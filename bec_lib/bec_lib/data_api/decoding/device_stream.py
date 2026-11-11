"""Scan-less DeviceMessage signals or preview payloads;
ordinals are unindexed arrival positions, with held samples in continuous mode.
"""

from __future__ import annotations

from typing import Any, Mapping

from bec_lib.messages import DeviceMessage, DevicePreviewMessage

from ..data_bundler import DataBundler
from ..data_path import SourceSpec
from ..models import CorrelationMode, DeviceStream, SourceKind

# --- device streams ----------------------------------------------------------


def extract_stream_point(spec: SourceSpec, stream: str, msg: Any) -> tuple[Any, Any] | None:
    """
    Read one source's ``(value, timestamp)`` from a scan-less device-stream message.

    Args:
        spec (SourceSpec): The source (its ``entry`` selects the signal of a device message).
        stream (str): ``"readback"``, ``"configuration"`` or ``"preview"``.
        msg: ``DeviceMessage`` for readback and configuration streams,
            ``DevicePreviewMessage`` for preview streams.

    Returns:
        tuple | None: The point, or None when the message carries nothing for the source.
    """
    if stream in {DeviceStream.READBACK, DeviceStream.CONFIGURATION}:
        if not isinstance(msg, DeviceMessage):
            return None
        signal_data = msg.signals.get(spec.entry)
        if signal_data is None:
            return None
        return signal_data.get("value"), signal_data.get("timestamp")
    if not isinstance(msg, DevicePreviewMessage):
        return None
    return msg.data, msg.timestamp


def decode_stream_values(
    bundle: DataBundler,
    points: list[tuple[SourceSpec, Any, Any]],
    stream: str,
    metadata: Mapping[str, Any] | None = None,
) -> int:
    """
    Insert scan-less points that arrived together, from any transport.

    Points of one arrival form one sample-and-hold row in a continuous bundle (not one
    row per source with stale partners); otherwise each is an unindexed point.

    Args:
        bundle (DataBundler): Target bundle.
        points (list[tuple[SourceSpec, Any, Any]]): ``(spec, value, timestamp)`` per source.
        stream (str): Stream name recorded in the series metadata.
        metadata (Mapping | None): Message metadata (a ``scan_id`` is recorded).

    Returns:
        int: Number of sources that changed; a continuous member's first (held)
            sample counts as a change, so the group's lagging report becomes
            observable to the consumer.
    """
    scan_id = (metadata or {}).get("scan_id")
    samples = []
    changed = 0
    for spec, value, timestamp in points:
        series = bundle.get_series(spec.device, spec.entry, SourceKind.UNINDEXED)
        if scan_id is not None:
            series.metadata["scan_id"] = scan_id
        series.metadata["stream"] = stream
        if bundle.correlation == CorrelationMode.CONTINUOUS:
            if not bundle.has_sample(spec.key):
                changed += 1
            samples.append((series, value, timestamp))
        elif series.insert(None, value, timestamp) >= 0:
            changed += 1
    if samples and bundle.sample_many(samples) >= 0:
        changed = max(changed, len(samples))
    return changed


def decode_stream_points(
    bundle: DataBundler, specs: list[SourceSpec], stream: str, msg: Any
) -> int:
    """
    Insert the points one BEC device-stream message carries for several sources.

    Args:
        bundle (DataBundler): Target bundle.
        specs (list[SourceSpec]): Sources fed by ``msg`` (one device or one preview signal).
        stream (str): Stream name the message came from.
        msg: The message (see :func:`extract_stream_point`).

    Returns:
        int: See :func:`decode_stream_values`.
    """
    points = []
    for spec in specs:
        point = extract_stream_point(spec, stream, msg)
        if point is not None:
            points.append((spec, *point))
    return decode_stream_values(bundle, points, stream, msg.metadata if points else None)


def decode_stream_message(bundle: DataBundler, spec: SourceSpec, stream: str, msg: Any) -> int:
    """
    Insert one scan-less device-stream message as an unindexed point of one source.

    Args:
        bundle (DataBundler): Target bundle.
        spec (SourceSpec): The stream's spec (``stream`` names the stream:
            ``"readback"``, ``"configuration"`` or ``"preview"``).
        stream (str): Stream name the message came from.
        msg: The message (see :func:`extract_stream_point`).

    Returns:
        int: 1 when a point was inserted or the sample is a continuous
            member's first (held) one; 0 for an unusable message or a
            further held sample.
    """
    return 1 if decode_stream_points(bundle, [spec], stream, msg) else 0
