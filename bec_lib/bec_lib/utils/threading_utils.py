"""
Threading utility functions for the bec_lib package.
"""

import functools


def threadlocked(fcn):
    """Ensure that the thread acquires and releases the lock."""

    @functools.wraps(fcn)
    def wrapper(self, *args, **kwargs):

        with self._lock:
            return fcn(self, *args, **kwargs)

    return wrapper
