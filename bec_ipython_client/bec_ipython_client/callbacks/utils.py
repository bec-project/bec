from __future__ import annotations

import abc
import time
import traceback
from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING, Any

from bec_lib.bec_errors import ScanInterruption, ScanRestart
from bec_lib.logger import bec_logger
from bec_lib.request_items import RequestItem

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.client import BECClient
    from bec_lib.queue_items import QueueItem
    from bec_lib.scan_items import ScanItem

logger = bec_logger.logger


class ScanRequestError(Exception):
    """Raised when the server rejects a scan request."""


class ScanState(Enum):
    """Outcome of evaluating the current scan or queue state."""

    CONTINUE = "continue"
    DONE = "done"
    WAIT = "wait"


def check_alarms(bec: BECClient) -> None:
    """Raise any pending alarms for the active client."""
    bec.alarm_handler.raise_alarms()


def evaluate_scan_state(
    *, scan_item: ScanItem | None = None, queue: QueueItem | None = None
) -> ScanState:
    """Evaluate restart, completion, and interruption state for live callbacks.

    Args:
        scan_item: Scan-centric state, used by callbacks that poll a concrete scan item.
        queue: Queue-centric state, used by callbacks that follow a queue entry directly.

    Returns:
        `ScanState.DONE` when the scan should stop cleanly.
        `ScanState.WAIT` when the callback should keep polling, typically while a restart is in
        progress and the replacement request has not arrived yet.
        `ScanState.CONTINUE` when no terminal state has been reached.

    Raises:
        ScanRestart: When a restart message is already available.
        ScanInterruption: When the scan ended in an interruption state.
    """
    current_scan = scan_item or _latest_scan(queue)
    restarted_msg = _restart_signal(scan_item=scan_item, queue=queue)
    if restarted_msg is not None:
        raise ScanRestart(new_scan_msg=restarted_msg)

    if getattr(current_scan, "status", None) == "user_completed":
        return ScanState.DONE

    if queue is not None:
        queue_status = getattr(queue, "status", None)
        if queue_status == "STOPPED" and getattr(queue, "reason", None) == "restart":
            return ScanState.WAIT
        if queue_status in ["STOPPED", "CANCELLED"]:
            raise ScanInterruption(_interruption_message(queue_status, current_scan))

    status_message = getattr(current_scan, "status_message", None)
    if status_message and getattr(status_message, "reason", None) == "user":
        raise ScanInterruption(_aborted_by_user_message(current_scan))

    if getattr(current_scan, "status", None) in {"aborted", "halted"}:
        raise ScanInterruption(_interrupted_message(current_scan))

    return ScanState.CONTINUE


def _restart_signal(
    *, scan_item: ScanItem | None = None, queue: QueueItem | None = None
) -> messages.ScanQueueMessage | None:
    """Return the restart message currently associated with the scan, if any."""
    current_scan = scan_item or _latest_scan(queue)
    restarted_msg = getattr(current_scan, "restarted_msg", None)
    if restarted_msg is not None:
        return restarted_msg
    return None


def _latest_scan(queue: QueueItem | None) -> ScanItem | None:
    """Return the latest scan item associated with a queue entry."""
    if queue is None:
        return None
    scans = getattr(queue, "scans", None) or []
    return scans[-1] if scans else None


def _scan_number(scan_item: ScanItem | None) -> int | None:
    """Return the scan number when it is present and well-typed."""
    scan_number = getattr(scan_item, "scan_number", None)
    return scan_number if isinstance(scan_number, int) else None


def _aborted_by_user_message(scan_item: ScanItem | None) -> str:
    """Build a user-facing message for a user-initiated abort."""
    scan_number = _scan_number(scan_item)
    if scan_number is None:
        return "Scan was aborted by user."
    return f"Scan {scan_number} was aborted by user."


def _interrupted_message(scan_item: ScanItem | None) -> str:
    """Build a user-facing message for a non-user interruption."""
    scan_number = _scan_number(scan_item)
    if scan_number is None:
        return "Scan was interrupted."
    return f"Scan {scan_number} was interrupted."


def _interruption_message(queue_status: str, scan_item: ScanItem | None) -> str:
    """Build a user-facing message for an interrupted queue entry."""
    if queue_status == "CANCELLED":
        return "Scan was cancelled."
    status_message = getattr(scan_item, "status_message", None)
    if status_message is not None and getattr(status_message, "reason", None) == "user":
        return _aborted_by_user_message(scan_item)
    return _interrupted_message(scan_item)


class LiveUpdatesBase(abc.ABC):
    def __init__(
        self,
        bec: BECClient,
        report_instruction: dict[str, Any] | None = None,
        request: messages.ScanQueueMessage | None = None,
        callbacks: list[Callable[..., Any]] | Callable[..., Any] | None = None,
    ) -> None:
        """Base class for live update callbacks.

        Args:
            bec: Active BEC client instance.
            report_instruction: Callback-specific report instruction payload.
            request: Scan queue request currently being processed.
            callbacks: One or more user callbacks to invoke for emitted points.
        """
        self.bec = bec
        self.request = request
        self.RID = request.metadata["RID"]
        self.scan_queue_request: RequestItem | None = None
        self.report_instruction = report_instruction
        if callbacks is None:
            self.callbacks = []
        self.callbacks = callbacks if isinstance(callbacks, list) else [callbacks]

    def wait_for_request_acceptance(self):
        scan_request = ScanRequestMixin(self.bec, self.RID)
        scan_request.wait()
        self.scan_queue_request = scan_request.scan_queue_request

    @abc.abstractmethod
    def run(self) -> None:
        """Run the live update callback."""

    def emit_point(self, data: dict[str, Any], metadata: dict[str, Any] | None = None) -> None:
        """Emit a point update to all registered user callbacks."""
        for cb in self.callbacks:
            if not cb:
                continue
            try:
                cb(data, metadata=metadata)
            except Exception:
                content = traceback.format_exc()
                logger.warning(f"Failed to run callback function: {content}")

    def _print_client_msgs_asap(self):
        """Print queued client messages marked for immediate display."""
        # pylint: disable=protected-access
        if self.scan_queue_request is None:
            return
        queue = self.scan_queue_request.queue
        if queue is None:
            return
        msgs = queue.get_client_messages(only_asap=True)
        if not msgs:
            return
        if self.bec.live_updates_config.print_client_messages is False:
            return
        for msg in msgs:
            print(queue.format_client_msg(msg))

    def _print_client_msgs_all(self):
        """Print a summary of all queued client messages."""
        # pylint: disable=protected-access
        if self.scan_queue_request is None:
            return
        queue = self.scan_queue_request.queue
        if queue is None:
            return
        msgs = queue.get_client_messages()
        if self.bec.live_updates_config.print_client_messages is False:
            return
        if not msgs:
            return
        print("------------------------")
        print("Summary of client messages")
        print("------------------------")
        # pylint: disable=protected-access
        for msg in msgs:
            print(queue.format_client_msg(msg))
        print("------------------------")


class ScanRequestMixin:
    def __init__(self, bec: BECClient, RID: str) -> None:
        """Mixin providing request-acceptance waiting helpers.

        Args:
            bec: Active BEC client instance.
            RID: Request identifier to wait for.
        """
        self.bec = bec
        self.request_storage = self.bec.queue.request_storage
        self.RID = RID
        self.scan_queue_request: RequestItem | None = None

    def _wait_for_scan_request(self) -> RequestItem:
        """Wait until the request item appears in request storage.

        Returns:
            The matching queue request item.
        """
        logger.trace("Waiting for request ID")
        start = time.time()
        while self.request_storage.find_request_by_ID(self.RID) is None:
            time.sleep(0.1)
            check_alarms(self.bec)
        logger.trace(f"Waiting for request ID finished after {time.time()-start} s.")
        return self.request_storage.find_request_by_ID(self.RID)

    def _wait_for_scan_request_decision(self) -> None:
        """Wait until the server has accepted or rejected the request."""
        logger.trace("Waiting for decision")
        start = time.time()
        while self.scan_queue_request.decision_pending:
            time.sleep(0.1)
            check_alarms(self.bec)
        logger.trace(f"Waiting for decision finished after {time.time()-start} s.")

    def wait(self) -> None:
        """Wait until the request is accepted and linked to a queue entry."""
        self.scan_queue_request = self._wait_for_scan_request()

        self._wait_for_scan_request_decision()
        check_alarms(self.bec)

        while self.scan_queue_request.accepted is None:
            time.sleep(0.1)
            check_alarms(self.bec)

        if not self.scan_queue_request.accepted[0]:
            raise ScanRequestError(
                "Scan was rejected by the server:"
                f" {self.scan_queue_request.response.content.get('message')}"
            )

        while self.scan_queue_request.queue is None:
            time.sleep(0.1)
            check_alarms(self.bec)
