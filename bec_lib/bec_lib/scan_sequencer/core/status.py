"""Checked completion and cancellation of native BEC operation handles."""

from __future__ import annotations

from bec_lib.scan_report import ScanReport

_MOVE_FAILURE_STATES = {"STOPPED", "CANCELLED", "FAILED", "ERROR", "ABORTED"}


def status_done(status: object) -> bool:
    """Observe outstanding mv/signal handles, propagating completion failures.

    Args:
        status (object): Runtime asynchronous handle, or None for a completed blocking call.

    Returns:
        bool: True after checked successful completion; False while still running.
    """
    if status is None:
        return True
    if isinstance(status, ScanReport):
        state = status.status
        if state in _MOVE_FAILURE_STATES:
            raise RuntimeError(f"Move ended with {state}")
        if state != "COMPLETED":
            return False
        status.wait(timeout=0, file_written=False)
        return True
    if not status.done:
        return False
    status.wait(timeout=0, raise_on_failure=True)
    return True


def cancel_status(status: object) -> None:
    """Cancel an outstanding move without stopping motors from a terminal move.

    Args:
        status (object): Runtime asynchronous handle, or None for a completed blocking call.
    """
    if isinstance(status, ScanReport):
        state = status.queue_item.status
        if state in _MOVE_FAILURE_STATES:
            return
        if state != "COMPLETED":
            status.cancel(queue_request=True)
        elif status.status != "COMPLETED":
            # The queue item can finish before the device reports move completion.
            status.cancel()
