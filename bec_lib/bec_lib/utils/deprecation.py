from __future__ import annotations

import warnings
from functools import wraps

warnings.formatwarning = lambda msg, *args, **kwargs: f">>> DEPRECATION: {msg}\n"


def deprecated(remove_in_version: str | None = None, recommendation: str | None = None):
    """
    Decorator to mark functions as deprecated.

    Args:
        remove_in_version (str | None): Optional version string indicating when the function will be removed.
        recommendation (str | None): Optional string suggesting an alternative function or approach to use.

    Returns:
        A decorator that can be applied to functions to mark them as deprecated.

    Example usage:
        @deprecated(remove_in_version="2.0", recommendation="Use new_function instead.")
        def old_function():
            pass

        # This will print:
        # "old_function is deprecated and will be removed in version 2.0. Use new_function instead"
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            message = f"{func.__name__} is deprecated"
            if remove_in_version:
                message += f" and will be removed in version {remove_in_version}"
            if recommendation:
                message += f". {recommendation}"
            warnings.warn(message, category=DeprecationWarning)
            return func(*args, **kwargs)

        return wrapper

    return decorator
