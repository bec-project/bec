"""
Shared BEC source classification for DataAPI data paths (resolve time).

Whether a (device, entry) pair is a monitored signal, an async signal (and
its AsyncMultiSignal and storage name), a preview signal or a stored
dataset is BEC-data-model knowledge — it must not fork per data path (it
already drifted once between the live and history plugins). The spec
builders at the bottom turn *evidence* — a scan status message, a history
message, a scan item's live data, the device manager — into
:class:`~.data_path.SourceSpec` lists; a data path only has to fetch the
evidence and pick the builder for its scenario.
"""

from __future__ import annotations

import collections
import math
from typing import Any, Mapping, Protocol

import numpy as np

from bec_lib.logger import bec_logger
from bec_lib.messages import ScanHistoryMessage

from .. import client_views
from ..data_bundler import MONITORED_GROUP
from ..data_path import SourceSpec
from ..models import DeviceStream, SourceKey, SourceKind

logger = bec_logger.logger

#: BEC signal classes that publish through the async signal machinery.
ASYNC_SIGNAL_CLASSES = ["AsyncSignal", "AsyncMultiSignal", "DynamicSignal"]


class StoredScan(Protocol):
    """
    What a data path has to know about a stored scan to serve it.

    BEC's ``ScanHistoryMessage`` is one; a path that serves stored scans from elsewhere
    returns its own object with these attributes (see ``FileDataPath.catalog_entry``).
    """

    #: ``device -> storage name -> info`` with ``shape`` and ``dtype``; ``None`` when the
    #: catalog does not say.
    stored_data_info: Mapping[str, Mapping[str, Any]] | None
    #: Monitored readouts of the scan, or ``None`` when unknown.
    num_monitored_readouts: int | None
    #: Where the data is stored; recorded on the sources as provenance.
    file_path: str


# --- device manager lookups --------------------------------------------------


def device_known(client, device: str) -> bool:
    """Whether the device manager knows ``device``."""
    return client_views.device(client, device) is not None


def device_signals(client, device: str) -> dict[str, dict] | None:
    """
    The ``signals`` block of one device's info.

    Returns:
        dict[str, dict] | None: Signal infos keyed by component name; an
            empty dict when the device carries no signal info, ``None`` when
            the device is unknown.
    """
    device_info = client_views.device_info(client, device)
    if device_info is None:
        return None
    signals = device_info.get("signals", {})
    return signals if isinstance(signals, dict) else {}


def is_monitored_signal(client, device: str, entry: str, readout_priority: Mapping | None) -> bool:
    """
    Whether ``entry`` is a monitored signal of ``device`` in the given scan.

    Without signal info (unknown device, or a device whose info has not
    settled yet) the hub-signal convention ``entry == device`` decides.
    """
    if device not in (readout_priority or {}).get("monitored", []):
        return False
    signals = device_signals(client, device)
    if not signals:
        return entry == device
    # A scan reads the hinted and normal signals of a device per point; configuration and
    # omitted signals are never part of a scan segment.
    return any(
        isinstance(info, dict)
        and info.get("obj_name") == entry
        and info.get("kind_str") not in ("config", "omitted")
        for info in signals.values()
    )


def is_readback_signal(client, device: str, entry: str) -> bool:
    """
    Whether ``entry`` is published in ``device``'s readback stream.

    Only evidenced signals qualify: an entry listed in the device's signal info
    (and neither a preview nor an async signal), or — without signal info yet —
    the hub signal ``entry == device``. Anything else is left to other data paths.
    """
    signals = device_signals(client, device)
    if signals is None:
        return False
    if not signals:
        return entry == device
    excluded = {"PreviewSignal", *ASYNC_SIGNAL_CLASSES}
    return any(
        isinstance(info, dict)
        and info.get("obj_name") == entry
        and info.get("signal_class") not in excluded
        for info in signals.values()
    )


def preview_component(client, device: str, entry: str) -> str | None:
    """
    The component name of the ``PreviewSignal`` of ``device`` that ``entry`` names, if any.

    A preview is published under its component name (``det/preview``), whichever of its
    names a consumer asks for: the component name or the object name (``det_preview``).
    """
    for component, info in (device_signals(client, device) or {}).items():
        if not isinstance(info, dict):
            continue
        if info.get("obj_name") == entry or info.get("component_name") == entry:
            if info.get("signal_class") != "PreviewSignal":
                return None
            return info.get("component_name") or component
    return None


def is_preview_signal(client, device: str, entry: str) -> bool:
    """Whether ``entry`` names a ``PreviewSignal`` of ``device``."""
    return preview_component(client, device, entry) is not None


def is_configuration_signal(client, device: str, entry: str) -> bool:
    """Whether BEC publishes the signal through its configuration readings."""
    for info in (device_signals(client, device) or {}).values():
        if not isinstance(info, dict):
            continue
        if info.get("obj_name") == entry or info.get("component_name") == entry:
            return info.get("kind_str") == "config"
    device_info = client_views.device_info(client, device) or {}
    if entry == device or device_info.get("device_base_class") == "signal":
        # A root signal. Its describe_configuration() repeats describe(), but the device
        # server publishes a root signal through the readback stream only.
        return False
    # A device without component declarations: its describe block tells which of its
    # entries the device server publishes with the configuration readings.
    described = device_info.get("describe_configuration", {})
    return isinstance(described, dict) and entry in described


def multi_signal_name(entry_info: dict | None) -> str | None:
    """
    The storage name shared by the sub-signals of one ``AsyncMultiSignal``.

    Returns:
        str | None: The storage name for a sub-signal of a multi-signal group,
            ``None`` for any other signal (including a single-signal group, which
            is stored under its own name).
    """
    if not isinstance(entry_info, dict) or entry_info.get("signal_class") != "AsyncMultiSignal":
        return None
    storage_name = entry_info.get("storage_name")
    if not storage_name or storage_name == entry_info.get("obj_name"):
        return None
    return storage_name


def declared_correlation_group(entry_info: dict | None) -> str | None:
    """
    The correlation group a signal declares in its signal info.

    Returns:
        str | None: ``"monitored"`` for a signal that promises one value per scan point,
            any other tag for signals that are emitted together, ``None`` without one.
    """
    described = (entry_info or {}).get("describe") or {}
    signal_info = described.get("signal_info") or {}
    # ``acquisition_group`` is the name older devices publish the same field under.
    return signal_info.get("correlation_group") or signal_info.get("acquisition_group")


# --- stored datasets ---------------------------------------------------------


def stored_key_candidates(device: str, entry: str, storage_name: str | None) -> list[str]:
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


def stored_info_field(info: Any, name: str) -> Any:
    """Read one field of a ``stored_data_info`` entry (pydantic object or dict)."""
    if isinstance(info, dict):
        return info.get(name)
    return getattr(info, name, None)


def estimate_stored_bytes(info: Any) -> int | None:
    """
    Size of one stored dataset from its metadata (shape × dtype), no file
    I/O. ``None`` when the metadata is unusable.
    """
    shape = tuple(stored_info_field(info, "shape") or ())
    dtype = stored_info_field(info, "dtype")
    try:
        itemsize = np.dtype(dtype).itemsize if dtype else 8
    except TypeError:
        itemsize = 8
    return int(math.prod(shape)) * itemsize if shape else itemsize


# --- spec builders (one per kind of evidence) --------------------------------


def open_scan_specs(
    client, sources: list[SourceKey], readout_priority: Mapping[str, list[str]] | None
) -> list[SourceSpec]:
    """
    Classify sources of an open scan from the readout priorities of its devices.

    Monitored signals come from the scan's readout priorities, async signals
    from the device manager's declarations; anything else is unavailable.

    Args:
        client: BEC client (device manager).
        sources (list[SourceKey]): Requested (device, entry) pairs.
        readout_priority (Mapping[str, list[str]] | None): Device names per priority, as
            in ``ScanStatusMessage.readout_priority``; only ``"monitored"`` is read.

    Returns:
        list[SourceSpec]: One spec per source.
    """
    specs: list[SourceSpec] = []
    # Read once per call and never kept: a device may change what its signals declare.
    declared = async_declarations(client)
    for device, entry in sources:
        info = declared.get(device, {}).get(entry)
        if info is not None:
            specs.append(
                SourceSpec(
                    device=device,
                    entry=entry,
                    kind=SourceKind.ASYNC,
                    multi_signal=multi_signal_name(info),
                    correlation_group=declared_correlation_group(info),
                    storage_name=info.get("storage_name"),
                )
            )
            continue
        if is_monitored_signal(client, device, entry, readout_priority):
            specs.append(SourceSpec(device=device, entry=entry, kind=SourceKind.MONITORED))
            continue
        specs.append(SourceSpec(device=device, entry=entry, available=False))
    return specs


def scan_memory_specs(sources: list[SourceKey], live_data: Mapping) -> list[SourceSpec]:
    """
    Classify sources of a finished scan from its in-memory live data
    (writer-latency window: only monitored data is complete there).
    """
    specs = []
    for device, entry in sources:
        available = live_data.get(device, {}).get(entry) is not None
        specs.append(
            SourceSpec(
                device=device,
                entry=entry,
                kind=SourceKind.MONITORED if available else None,
                available=available,
            )
        )
    return specs


def device_stream_specs(client, sources: list[SourceKey]) -> list[SourceSpec]:
    """
    Classify scan-less device streams: preview signals, configuration readings,
    or evidenced readback signals (see :func:`is_readback_signal`). Any other
    entry is unavailable here, so a data path further down the table can serve it.
    The stream name is recorded in ``SourceSpec.stream``.
    """
    specs: list[SourceSpec] = []
    for device, entry in sources:
        published_as = preview_component(client, device, entry)
        if published_as is not None:
            stream = DeviceStream.PREVIEW
        elif is_configuration_signal(client, device, entry):
            stream = DeviceStream.CONFIGURATION
        elif is_readback_signal(client, device, entry):
            stream = DeviceStream.READBACK
        else:
            specs.append(SourceSpec(device=device, entry=entry, available=False))
            continue
        specs.append(
            SourceSpec(
                device=device,
                entry=entry,
                kind=SourceKind.UNINDEXED,
                stream=stream,
                storage_name=published_as,
            )
        )
    return specs


def stored_scan_specs(
    client, sources: list[SourceKey], stored_scan: StoredScan
) -> list[SourceSpec]:
    """
    Classify the sources of a stored scan from its catalog entry.

    The catalog lists the stored datasets and their shapes, not how they were acquired.
    A signal the device manager declares as async keeps the classification of the live
    path, so the sub-signals of one AsyncMultiSignal pair in a stored scan as they do in
    a running one. Any other dataset with one row per recorded scan point is a monitored
    signal (row = point, see :func:`recorded_points`), and what is left is a standalone
    unindexed source.

    A catalog entry without ``stored_data_info`` (written by an older BEC) does not say
    which datasets the file holds. Its specs are ``provisional``: the read decides, and a
    data path that has evidence for a source is asked first.

    Args:
        client: BEC client (device manager).
        sources (list[SourceKey]): Requested (device, entry) pairs.
        stored_scan (StoredScan): The catalog entry of the scan.

    Returns:
        list[SourceSpec]: One spec per source.
    """
    stored = stored_scan.stored_data_info or {}
    declared = async_declarations(client)
    points = recorded_points(stored, _monitored_readouts(stored_scan), declared)
    specs = []
    for device, entry in sources:
        declaration = declared.get(device, {}).get(entry)
        storage_name = declaration.get("storage_name") if declaration is not None else None
        stored_device = stored.get(device, {})
        stored_key = next(
            (
                key
                for key in stored_key_candidates(device, entry, storage_name)
                if key in stored_device
            ),
            entry,
        )
        info = stored_device.get(stored_key)
        if info is None and stored:
            specs.append(SourceSpec(device=device, entry=entry, available=False))
            continue
        group = declared_correlation_group(declaration)
        if group == MONITORED_GROUP and (points is None or _rows(info) != points):
            # The declaration is today's; the file does not show one value per scan point,
            # so its rows cannot be paired with the points.
            group = None
        if declaration is not None:
            kind = SourceKind.ASYNC
        elif points is not None and _rows(info) == points:
            kind = SourceKind.MONITORED
        else:
            kind = SourceKind.UNINDEXED
        specs.append(
            SourceSpec(
                device=device,
                entry=entry,
                kind=kind,
                multi_signal=multi_signal_name(declaration),
                correlation_group=group,
                storage_name=stored_key,
                provisional=not stored,
                estimated_bytes=estimate_stored_bytes(info) if info is not None else None,
                stored_acquisitions=_acquisition_rows(info, declaration),
            )
        )
    return specs


def async_declarations(client) -> dict[str, dict[str, dict]]:
    """
    The async signals the device manager declares: ``device -> obj_name -> entry info``.

    Returns:
        dict[str, dict[str, dict]]: Empty when the client has no device manager.
    """
    declared: dict[str, dict[str, dict]] = {}
    if client.device_manager is None:
        return declared
    for device, _, info in client.device_manager.get_bec_signals(ASYNC_SIGNAL_CLASSES):
        if info.get("obj_name"):
            declared.setdefault(device, {})[info["obj_name"]] = info
    return declared


def recorded_points(
    stored: Mapping[str, Mapping[str, Any]],
    readouts: int | None,
    declared_async: Mapping[str, Mapping[str, dict]],
) -> int | None:
    """
    The number of scan points a stored scan recorded: the rows of its monitored signals.

    Every monitored signal is written from the same scan points, so they all have the
    same number of rows. A scan that ran to its end has one per planned readout. A scan
    that was stopped has fewer, and the catalog does not say how many: the count is then
    the number of rows most datasets share that are not async signals. A baseline signal
    has a single row and only counts when nothing has more.

    Args:
        stored (Mapping): ``device -> storage name -> info`` with the dataset shapes.
        readouts (int | None): Planned monitored readouts of the scan, if known.
        declared_async (Mapping): The declared async signals (see :func:`async_declarations`).

    Returns:
        int | None: The number of recorded points, or ``None`` when the catalog lists no
            dataset to tell from.
    """
    counts: collections.Counter[int] = collections.Counter()
    for device, entries in stored.items():
        async_names = {
            name
            for info in declared_async.get(device, {}).values()
            for name in (info.get("obj_name"), info.get("storage_name"))
        }
        for key, info in entries.items():
            rows = _rows(info)
            if rows is not None and key not in async_names:
                counts[rows] += 1
    if readouts is not None and counts[readouts]:
        return readouts
    several = {rows: count for rows, count in counts.items() if rows > 1} or counts
    if not several:
        return None
    return max(several, key=lambda rows: (several[rows], rows))


def _monitored_readouts(history_message: StoredScan) -> int | None:
    """Monitored readouts of a finished scan, or ``None`` when the entry does not say."""
    readouts = history_message.num_monitored_readouts
    if readouts is None and isinstance(history_message, ScanHistoryMessage):
        # Written before the readout count was recorded: one readout per scan point.
        readouts = history_message.num_points
    return readouts if readouts is not None and readouts > 0 else None


def _acquisition_rows(info: Any, declaration: dict | None) -> int | None:
    """
    Rows of a stored dataset that are one acquisition each, or ``None``.

    A dataset with as many dimensions as its signal declares is a single acquisition (one
    frame, stored as its rows); with one more, it is a stack of them. A dataset of a
    signal without a declaration is read as one row per scan point.
    """
    shape = tuple(stored_info_field(info, "shape") or ()) if info is not None else ()
    if not shape:
        return None
    if declaration is not None:
        ndim = ((declaration.get("describe") or {}).get("signal_info") or {}).get("ndim")
        if ndim is None or len(shape) <= ndim:
            return None
    return int(shape[0])


def _rows(info: Any) -> int | None:
    """Rows of a stored dataset from its catalog info, or ``None`` when it has no shape."""
    shape = tuple(stored_info_field(info, "shape") or ()) if info is not None else ()
    return int(shape[0]) if shape else None
