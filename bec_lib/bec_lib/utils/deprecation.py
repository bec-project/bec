"""Helpers for logging deprecation notices from BEC client APIs."""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Any, Callable


def deprecated(
    remove_in_version: str | None = None, recommendation: str | None = None, stack: int = 0
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to mark functions as deprecated.

    Args:
        remove_in_version (str | None): Optional version string indicating when the function will be removed.
        recommendation (str | None): Optional string suggesting an alternative function or approach to use.
        stack (int): Number of caller frames to traverse to identify the call site. Use 0 to omit
            the location, 1 for a direct caller, or a higher value when another wrapper intervenes.

    Returns:
        A decorator that can be applied to functions to mark them as deprecated.

    Example usage:
        @deprecated(remove_in_version="2.0", recommendation="Use new_function instead.")
        def old_function():
            pass

        # This will print:
        # "old_function is deprecated and will be removed in version 2.0. Use new_function instead"
    """
    if stack < 0:
        raise ValueError("stack must be non-negative")

    def decorator(func):
        logged_calls = set()

        @wraps(func)
        def wrapper(*args, **kwargs):
            from bec_lib.logger import bec_logger

            message = f"{func.__name__} is deprecated"
            if remove_in_version:
                message += f" and will be removed in version {remove_in_version}"
            if recommendation:
                message += f". {recommendation}"
            call_site = None
            if stack:
                frame = inspect.currentframe()
                try:
                    for _ in range(stack):
                        frame = frame.f_back if frame is not None else None
                    if frame is not None:
                        call_site = (frame.f_code.co_filename, frame.f_lineno)
                        module = frame.f_globals.get("__name__", "<unknown>")
                        message += f" (called from {module}:{frame.f_lineno})"
                finally:
                    del frame
            if call_site not in logged_calls:
                bec_logger.logger.bind(deprecation=True).warning(message)
                logged_calls.add(call_site)
            return func(*args, **kwargs)

        return wrapper

    return decorator
