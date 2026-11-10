"""Framework-independent device instruction errors."""

from __future__ import annotations

import traceback

from bec_lib.messages import ErrorInfo


class DisabledDeviceError(Exception):
    """A disabled or read-only device was accessed."""


class InvalidDeviceError(Exception):
    """A requested device or operation result is invalid."""


def format_error_info(exc: Exception, device: str | None) -> ErrorInfo:
    """
    Format an operation failure for publication.

    Args:
        exc (Exception): Operation failure with its traceback.
        device (str | None): Device associated with the failure.

    Returns:
        ErrorInfo: Full and compact diagnostics for the failure.
    """
    return ErrorInfo(
        error_message="".join(traceback.format_exception(exc)),
        compact_error_message="".join(traceback.format_exception(exc, limit=0)),
        exception_type=type(exc).__name__,
        device=device,
    )
