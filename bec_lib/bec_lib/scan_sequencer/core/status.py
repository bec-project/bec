"""Checked completion and cancellation of native BEC operation handles."""

from __future__ import annotations

from bec_lib.scan_report import ScanReport


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
        if state in {"STOPPED", "CANCELLED", "FAILED", "ERROR", "ABORTED"}:
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
    """Use mv's existing cancellation; signal Status has no general abort API.

    Args:
        status (object): Runtime asynchronous handle, or None for a completed blocking call.
    """
    if isinstance(status, ScanReport):
        status.cancel()
