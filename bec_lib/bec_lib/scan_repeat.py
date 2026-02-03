"""
This module contains a decorator and helper functions to implement custom
repeat logic during data acquisition.
"""

import contextvars
import functools
from typing import Callable

from bec_lib.bec_errors import ScanInterruption, ScanRestart

# Tracks nesting depth per execution context
_scan_repeat_depth = contextvars.ContextVar("_scan_repeat_depth", default=0)


class TooManyScanRestarts(Exception):
    """Exception raised when the maximum number of scan restarts is exceeded."""


def scan_repeat(
    *,
    max_repeats=1,
    exc_handler: Callable[[Exception, int], bool] | None = None,
    default: bool = False,
):
    """
    Decorator to repeat a scan function with custom retry logic.

    Args:
        max_repeats (int): Maximum number of retry attempts. Default is 1, i.e., if will retry once on failure.
        exc_handler (Callable[[Exception, int], bool] | None): Optional exception handler that takes the exception and
            the current attempt number as arguments and returns True to retry or False to stop retrying. If None, only
            ScanRepeat exceptions will trigger a retry. Default is None.
        default (bool): If True, the decorated function will always be retried on exceptions unless the exception
            handler specifies otherwise. If False, only ScanRestart exceptions will trigger a retry. Default is False.
            Please note that retrying on all exceptions may lead to unintended behavior and should be used with caution.

    Returns:
        Callable: The decorated function with retry logic.

    Raises:
        TooManyScanRestarts: If the number of retry attempts exceeds max_repeats.
        ScanInterruption: If a scan interruption is raised during execution.
        Exception: Any exception raised by the decorated function that is not handled by exc_handler or is not a ScanRestart (if default is False).

    Example:
        >>> @scan_repeat(max_repeats=3)
        ... def my_scan_function():
        ...     # Scan logic here
        ...
        >>> my_scan_function()
    """

    def decorator(fcn: Callable) -> Callable:
        @functools.wraps(fcn)
        def wrapper(*args, **kwargs):
            depth = _scan_repeat_depth.get()

            # Nested call: no retry logic here
            if depth > 0:
                token = _scan_repeat_depth.set(depth + 1)
                try:
                    return fcn(*args, **kwargs)
                finally:
                    _scan_repeat_depth.reset(token)

            # Top-level call: enable retry logic
            token = _scan_repeat_depth.set(depth + 1)
            try:
                attempt = 0
                while True:
                    try:
                        return fcn(*args, **kwargs)

                    except ScanRestart:
                        attempt += 1
                        if attempt > max_repeats:
                            # pylint: disable=raise-missing-from
                            raise TooManyScanRestarts(
                                f"Maximum scan restart attempts ({max_repeats}) exceeded."
                            )
                    except (KeyboardInterrupt, ScanInterruption) as exc:
                        # Do not retry on these exceptions
                        raise exc
                    except Exception as exc:
                        attempt += 1

                        if exc_handler is not None:
                            should_retry = exc_handler(exc, attempt)
                        else:
                            should_retry = default

                        if not should_retry:
                            raise
                        if attempt > max_repeats:
                            # pylint: disable=raise-missing-from
                            raise TooManyScanRestarts(
                                f"Maximum scan restart attempts ({max_repeats}) exceeded."
                            ) from exc
            finally:
                _scan_repeat_depth.reset(token)

        return wrapper

    return decorator
