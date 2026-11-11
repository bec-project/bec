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

from numbers import Integral
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


def current_live_scan(client: BECClient) -> tuple[str, ScanItem] | None:
    """Return ``(scan_id, scan_item)`` of the running scan, or ``None``."""
    storage = scan_storage(client)
    if storage is None:
        return None
    current = storage.current_scan_id
    if not current:
        return None
    scan_id = current[0]  # the ids of the active queue entry; an entry may hold none yet
    if not isinstance(scan_id, str) or not scan_id:
        return None
    item = storage.find_scan_by_ID(scan_id)
    if item is None or item.status in TERMINAL_SCAN_STATES or item.status_message is None:
        return None
    return scan_id, item


def latest_scan(client: BECClient) -> tuple[str, ScanItem | None] | None:
    """
    Return ``(scan_id, scan_item)`` of the most recent scan, running or finished, or ``None``.

    Scan storage holds the scans this client saw (with their live data, also before the
    file is written); the history registry holds finished scans, including those from
    before the client started. The higher scan number wins; a scan found only in the
    history has no item (``None``, an explicit empty prefetch).
    """
    number, scan_id, latest_item = -1, None, None
    storage = scan_storage(client)
    for item in list(storage.storage) if storage is not None else ():
        # An item is created from the queue entry; its number and status follow later.
        if not isinstance(item.scan_id, str) or not item.scan_id:
            continue
        if not isinstance(item.scan_number, Integral) or item.status_message is None:
            continue
        if item.scan_number > number:
            number, scan_id, latest_item = int(item.scan_number), item.scan_id, item
    message = None if client.history is None else client.history.latest_message()
    if message is not None and message.scan_number > number:
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
