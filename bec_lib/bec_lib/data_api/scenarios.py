"""
Level-1 routing of the DataAPI: the data-access scenario.

Every subscription is in exactly one of three stable scenarios — the set is a
complete partition of (scan id, scan state) and is not expected to grow:

* ``OPEN_SCAN``  (SO): has a scan id and the scan is open  -> live data
* ``CLOSED_SCAN`` (SC): has a scan id and the scan is done -> history
* ``NO_SCAN``    (NS): no scan id -> continuous device streams

What *does* change over time is how the data of one scenario is accessed
(Redis today; BEC Atlas, REST, ... tomorrow). That volatility lives one level
below: each scenario has its own priority-ordered list of data paths
(:attr:`~bec_lib.data_api.data_api.DataAPI.data_paths`), and the first
data path that claims a scan serves it. The scenario decision itself is
deterministic and carries no priorities.

The classification is deterministic *per evaluation*, but it is re-evaluated
over a subscription's life (subscribe, scan change, source change, history
publication): a live-followed scan flips OPEN_SCAN -> CLOSED_SCAN when its
history entry appears, not merely when its status turns terminal — during the
writer-latency window the closed scan is served from scan memory (Redis path).

``UNKNOWN`` is the fourth, transient outcome: a scan id that is neither in
scan storage nor in the history registry (yet). Concrete-scan subscriptions
raise on it; live-follow subscriptions wait for the next scan event.
"""

from __future__ import annotations

import enum
from typing import Any

from . import client_views
from .client_views import TERMINAL_SCAN_STATES

#: Internal scan-id sentinel for scan-less (NO_SCAN) subscriptions. Only the
#: classifier and the facade know it; data paths are consulted only for their
#: own scenario and never need to check it.
DEVICE_SCOPE = ""


class ScanTarget(enum.StrEnum):
    """
    Named values of the ``scan`` argument, next to a concrete scan id and ``None``.

    ``subscribe(scan=ScanTarget.LIVE)`` follows the running scan and every scan after
    it. The member is a string, so ``scan="live"`` means the same.
    """

    LIVE = "live"


class Scenario(enum.Enum):
    """The three stable data-access situations (plus the transient UNKNOWN)."""

    OPEN_SCAN = "open_scan"
    CLOSED_SCAN = "closed_scan"
    NO_SCAN = "no_scan"
    UNKNOWN = "unknown"


def classify(client, scan_id: str, scan_item: Any, have_item: bool) -> Scenario:
    """
    Decide the scenario for one scan id. Deterministic; no priorities.

    Lock-order rule (inherited from the facade): callers on threads that do
    not hold the scan-manager RLock must prefetch the scan item and pass it
    via ``scan_item``/``have_item=True``; only dispatcher-side callers (which
    already own that RLock) may let the classifier read scan storage itself.

    Args:
        client: BEC client (history registry and, when allowed, scan storage).
        scan_id: Scan id to classify; the device-scope sentinel maps to NO_SCAN.
        scan_item: Prefetched scan item (None is a valid prefetch result).
        have_item: Whether ``scan_item`` is authoritative or storage may be read.

    Returns:
        Scenario: OPEN_SCAN, CLOSED_SCAN, NO_SCAN or UNKNOWN.
    """
    if scan_id == DEVICE_SCOPE:
        return Scenario.NO_SCAN
    if not have_item:
        scan_item = client_views.scan_item(client, scan_id)
    if scan_item is not None and scan_item.status_message is not None:
        status = scan_item.status
        if isinstance(status, str) and status not in TERMINAL_SCAN_STATES:
            return Scenario.OPEN_SCAN
    if client_views.history_message(client, scan_id) is not None:
        return Scenario.CLOSED_SCAN
    if scan_item is not None and scan_item.status in TERMINAL_SCAN_STATES:
        # Writer-latency window: the scan is done but its file has not been
        # published; the Redis path serves it once from the scan's live data.
        return Scenario.CLOSED_SCAN
    return Scenario.UNKNOWN
