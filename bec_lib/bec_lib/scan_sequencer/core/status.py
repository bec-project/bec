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


def check_motor_available(moves: dict[str, object], name: str) -> None:
    """Release a completed motor move or reject an overlapping write.

    Args:
        moves (dict[str, object]): Outstanding move handles indexed by motor name.
        name (str): Motor targeted by the next write.
    """
    if name not in moves:
        return
    try:
        done = status_done(moves[name])
    except TimeoutError:
        # A completed queue request can still have a moving device.
        done = False
    if not done:
        raise RuntimeError(f"Motor {name!r} still has an outstanding nonblocking move")
    del moves[name]


def cancel_status(status: object) -> None:
    """Cancel outstanding moves using the existing native handle.

    Args:
        status (object): Runtime asynchronous handle, or None for a completed blocking call.
    """
    if isinstance(status, ScanReport):
        if status.status in _MOVE_FAILURE_STATES:
            return
        try:
            if status_done(status):
                return
        except TimeoutError:
            # Queue completion does not imply motor completion.
            pass
        status.cancel()
