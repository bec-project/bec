"""
Async signal streams -> ordinal-keyed points.

Wire updates carry ``async_update`` metadata; ordinals come from async_indices,
add_slice row indices or replace's single state. How each shape maps onto
correlation ordinals is BEC-data-model knowledge and lives here, once, so a
new data path only has to fetch and forward.

The caller serializes access to the target bundle. Decoders never lock, never
notify and never touch the client. Each returns the number of points it
inserted so the caller can decide whether an update is due.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

import numpy as np

from bec_lib.logger import bec_logger
from bec_lib.messages import DeviceAsyncUpdate, DeviceMessage

from ..data_bundler import DataBundler
from ..data_path import SourceSpec
from ..models import SourceKind

logger = bec_logger.logger


# --- async signal streams ----------------------------------------------------


@dataclass
class AsyncPoint:
    """Everything one async-update handler needs to reconstruct one point."""

    spec: SourceSpec
    series: Any
    value: Any
    timestamp: Any
    update: DeviceAsyncUpdate
    indices: dict
    #: Per-request scratch storage (accumulated ``add_slice`` rows); dies
    #: with the request and never crosses subscriptions.
    state: dict


AsyncUpdateHandler = Callable[[AsyncPoint], int]


def decode_add(point: AsyncPoint) -> int:
    """One fragment per message, keyed by the per-scan message counter."""
    ordinal = point.indices.get(point.spec.entry)
    if not isinstance(ordinal, int):
        logger.warning(f"add update without async index for {point.spec.key}; dropped.")
        return 0
    point.series.insert(ordinal, point.value, point.timestamp)
    return 1


def decode_add_slice(point: AsyncPoint) -> int:
    """
    A row grows across messages.

    Each row lives in a growable buffer, and the series receives a read-only view
    of its filled part: a message costs O(fragment) instead of a copy of the whole
    row. Appends only write past the end of every view handed out so far, and a
    buffer that must grow or widen its dtype is replaced, never changed, so
    delivered snapshots stay immutable.
    """
    if len(point.update.max_shape or []) != 2 or np.ndim(point.value) != 1:
        logger.warning(f"Invalid add_slice shape for {point.spec.key}; dropped.")
        return 0
    rows = point.state.setdefault("slice_rows", {}).setdefault(point.spec.key, {})
    row = point.update.index
    if row is None:
        logger.warning(
            f"add_slice update without index for {point.spec.device}/{point.spec.entry}; dropped."
        )
        return 0
    buffer, length = rows.get(row, (None, 0))
    fragment = np.atleast_1d(np.asarray(point.value))
    row_size = point.update.max_shape[1]
    if row_size is not None:
        fragment = fragment[: max(row_size - length, 0)]
    filled = length + len(fragment)
    dtype = fragment.dtype if buffer is None else np.result_type(buffer.dtype, fragment.dtype)
    if buffer is None or filled > len(buffer) or dtype != buffer.dtype:
        grown = np.empty(max(filled, 2 * length, 16), dtype=dtype)
        if buffer is not None:
            grown[:length] = buffer[:length]
        buffer = grown
    buffer[length:filled] = fragment
    rows[row] = (buffer, filled)
    view = buffer[:filled]
    view.flags.writeable = False
    point.series.insert(row, view, point.timestamp)
    return 1


def decode_replace(point: AsyncPoint) -> int:
    """A replace source exposes one point: its current full state."""
    point.series.insert(0, point.value, point.timestamp)
    return 1


#: Reconstruction strategy per ``DeviceAsyncUpdate.type``. A path may override a
#: handler by passing ``handlers={**ASYNC_UPDATE_HANDLERS, ...}``; a *new* wire type
#: also needs ``messages.DeviceAsyncUpdate``, the file writer and the reconstruction in
#: ``models.py``. Unknown types are dropped with a warning.
ASYNC_UPDATE_HANDLERS: dict[str, AsyncUpdateHandler] = {
    "add": decode_add,
    "add_slice": decode_add_slice,
    "replace": decode_replace,
}


def parse_async_update(msg: DeviceMessage) -> DeviceAsyncUpdate | None:
    """
    Validate the ``async_update`` metadata of one stream message.

    Messages without it cannot be ordinal-mapped (an implicit arrival-counter
    path is not supported) and are skipped silently: old scans replayed
    ``from_start`` would otherwise flood the log. Invalid metadata is dropped
    with one warning per message.

    Returns:
        DeviceAsyncUpdate | None: The update, or ``None`` when the message
            must be dropped.
    """
    metadata = msg.metadata or {}
    raw_update = metadata.get("async_update")
    if raw_update is None:
        return None
    try:
        return DeviceAsyncUpdate.model_validate(raw_update)
    except ValueError:  # pydantic's ValidationError is a ValueError
        logger.warning(f"Dropping async update with invalid async_update metadata: {metadata}")
        return None


def decode_async_message(
    bundle: DataBundler,
    state: dict,
    specs: list[SourceSpec],
    msg: DeviceMessage,
    *,
    device: str,
    handlers: Mapping[str, AsyncUpdateHandler] = ASYNC_UPDATE_HANDLERS,
) -> int:
    """
    Insert one async-stream message into the matching async specs.

    Convenience for a single request: :func:`parse_async_update` followed
    by :func:`decode_async_update`. A path feeding several requests parses
    once and calls :func:`decode_async_update` per request.

    Args:
        bundle (DataBundler): Target bundle.
        state (dict): Per-request scratch storage.
        specs (list[SourceSpec]): Request specs; only available async specs
            of ``device`` are fed.
        msg (DeviceMessage): The stream message.
        device (str): Device the stream belongs to.
        handlers (Mapping[str, AsyncUpdateHandler]): Reconstruction
            strategies per update type.

    Returns:
        int: Number of points inserted.
    """
    update = parse_async_update(msg)
    if update is None:
        return 0
    return decode_async_update(bundle, state, specs, msg, update, device=device, handlers=handlers)


def decode_async_update(
    bundle: DataBundler,
    state: dict,
    specs: list[SourceSpec],
    msg: DeviceMessage,
    update: DeviceAsyncUpdate,
    *,
    device: str,
    handlers: Mapping[str, AsyncUpdateHandler] = ASYNC_UPDATE_HANDLERS,
) -> int:
    """
    Insert one stream message whose ``async_update`` was already validated.

    Args:
        bundle (DataBundler): Target bundle.
        state (dict): Per-request scratch storage.
        specs (list[SourceSpec]): Request specs; only available async specs
            of ``device`` are fed.
        msg (DeviceMessage): The stream message.
        update (DeviceAsyncUpdate): Its validated update (see
            :func:`parse_async_update`).
        device (str): Device the stream belongs to.
        handlers (Mapping[str, AsyncUpdateHandler]): Reconstruction
            strategies per update type.

    Returns:
        int: Number of points inserted.
    """
    metadata = msg.metadata or {}
    handler = handlers.get(update.type)
    indices = metadata.get("async_indices", {})
    inserted = 0
    for spec in specs:
        if spec.kind != SourceKind.ASYNC or spec.device != device or not spec.available:
            continue
        signal_data = msg.signals.get(spec.entry)
        if signal_data is None:
            continue
        if handler is None:
            logger.warning(
                f"Unknown async_update type '{update.type}' for {spec.device}/{spec.entry}; dropped."
            )
            continue
        series = bundle.get_series(spec.device, spec.entry, spec.kind or SourceKind.ASYNC)
        if not series.declare(update.type, update.max_shape):
            logger.warning(f"Acquisition encoding changed during scan for {spec.key}; dropped.")
            continue
        inserted += handler(
            AsyncPoint(
                spec=spec,
                series=series,
                value=signal_data.get("value"),
                timestamp=signal_data.get("timestamp", metadata.get("timestamp")),
                update=update,
                indices=indices,
                state=state,
            )
        )
    return inserted
