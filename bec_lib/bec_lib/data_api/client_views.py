"""
Read-only views of the BEC client state the DataAPI routes on.

This is the only module in the package that reads the client: its scan storage, its
history registry and its device manager. A client has none of them before it has started
(the attributes are ``None``); every helper reports that as "nothing known". The module
depends on nothing else in the package.

Lock-order rule: scan-storage reads take the scan-manager RLock, which the dispatcher thread
holds while waiting for the DataAPI lock. A caller that does not already hold it must call
these helpers BEFORE taking the DataAPI lock and pass the result along (see BindContext).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.client import BECClient
    from bec_lib.scan_items import ScanItem, ScanStorage

#: Scan states after which no further live data is produced.
TERMINAL_SCAN_STATES = frozenset({"closed", "aborted", "halted", "user_completed"})


def scan_storage(client: BECClient) -> ScanStorage | None:
    """Return the client's scan storage, or ``None`` before the client has started."""
    return None if client.queue is None else client.queue.scan_storage


def scan_item(client: BECClient, scan_id: str) -> ScanItem | None:
    """Return the scan item of ``scan_id`` from scan storage, or ``None``."""
    storage = scan_storage(client)
    if storage is None or not isinstance(scan_id, str) or not scan_id:
        return None
    return storage.find_scan_by_ID(scan_id)


def history_message(client: BECClient, scan_id: str) -> Any:
    """Return the registry entry of ``scan_id`` (no file I/O), or ``None``."""
    history = client.history
    if history is None or not isinstance(scan_id, str) or not scan_id:
        return None
    return history.get_message(scan_id)


def default_queue(client: BECClient) -> str | None:
    """Return the client's default scan queue (e.g. ``"primary"``), or ``None`` if unknown."""
    queue = None if client.queue is None else client.queue.get_default_scan_queue()
    return queue if isinstance(queue, str) else None


def scan_queue(item: ScanItem | None) -> str | None:
    """
    Return the scan queue a scan was submitted to, or ``None`` while it is not known.

    The queue entry of the scan holds the request it came from (``ScanQueueMessage.queue``);
    the client knows the entry from the queue updates it has seen.
    """
    queue_item = None if item is None else item.queue
    blocks = getattr(queue_item, "request_blocks", None) or []
    queue = getattr(getattr(blocks[0], "msg", None), "queue", None) if blocks else None
    return queue if isinstance(queue, str) else None


def in_queue(item: ScanItem | None, queue: str | None) -> bool:
    """
    Whether a scan belongs to ``queue``. ``None`` stands for any queue, and a scan whose
    queue is not known yet counts as in it.
    """
    name = scan_queue(item)
    return queue is None or name is None or name == queue


def _queue_scan_ids(client: BECClient, queue: str) -> list[str | None]:
    """The scan ids of the active entry of a named queue; empty when it has none."""
    queues = client.queue.queue_storage.current_scan_queue or {}
    status = queues.get(queue)
    return list(status.info[0].scan_id) if status is not None and status.info else []


def current_live_scan(client: BECClient, queue: str | None = None) -> tuple[str, ScanItem] | None:
    """
    Return ``(scan_id, scan_item)`` of the scan running in ``queue``, or ``None``.

    ``None`` is the client's default queue, the one its scans are submitted to.
    """
    storage = scan_storage(client)
    if storage is None:
        return None
    if queue is None or queue == default_queue(client):
        current = storage.current_scan_id  # the default queue's active entry
    else:
        current = _queue_scan_ids(client, queue)
    if not current:
        return None
    scan_id = current[0]  # the ids of the active queue entry; an entry may hold none yet
    if not isinstance(scan_id, str) or not scan_id:
        return None
    item = storage.find_scan_by_ID(scan_id)
    if item is None or item.status in TERMINAL_SCAN_STATES or item.status_message is None:
        return None
    return scan_id, item


def scan_start(item: ScanItem) -> float:
    """When the scan of a scan item started (its ``open`` status); 0 while that is unknown."""
    started = item.start_time or getattr(item.status_message, "timestamp", None)
    return float(started) if isinstance(started, (int, float)) else 0.0


def latest_scan(client: BECClient, queue: str | None = None) -> tuple[str, ScanItem | None] | None:
    """
    Return ``(scan_id, scan_item)`` of the most recent scan, running or finished, or ``None``.

    Only scans of ``queue`` are considered (``None``: any queue). The history registry does
    not say which queue a finished scan ran in: its entries are always considered.

    Scan storage holds the scans this client saw (with their live data, also before the
    file is written); the history registry holds finished scans, including those from
    before the client started and those it missed. The scan that started last wins; a scan
    found only in the history has no item (``None``, an explicit empty prefetch). Neither
    the scan number nor the place in the storage decides: the number starts again with a
    new experiment, and the storage is in the order the scans were queued.
    """
    started, scan_id, latest_item = -1.0, None, None
    storage = scan_storage(client)
    for item in list(storage.storage) if storage is not None else ():
        # An item is created from the queue entry; its id and status follow later.
        if not isinstance(item.scan_id, str) or not item.scan_id or item.status_message is None:
            continue
        if not in_queue(item, queue):
            continue
        if scan_start(item) >= started:
            started, scan_id, latest_item = scan_start(item), item.scan_id, item
    message = None if client.history is None else client.history.latest_message()
    if message is not None and message.scan_id != scan_id and message.start_time > started:
        scan_id, latest_item = message.scan_id, None
    return None if scan_id is None else (scan_id, latest_item)


def device(client: BECClient, name: str) -> Any:
    """Return the device named ``name``, or ``None`` when the device manager has none."""
    manager = client.device_manager
    return None if manager is None else manager.devices.get(name)


def device_info(client: BECClient, name: str) -> dict | None:
    """
    Return the info block the device server published for a device.

    Returns:
        dict | None: The block; an empty dict for a device that carries none (yet),
            ``None`` for an unknown device.
    """
    dev = device(client, name)
    if dev is None:
        return None
    info = dev._info  # pylint: disable=protected-access
    return info if isinstance(info, dict) else {}
