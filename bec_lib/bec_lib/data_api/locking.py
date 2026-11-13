"""The DataAPI lock: reentrant, and able to say whether the calling thread holds it."""

from __future__ import annotations

import threading


class OwnedLock:
    """
    Reentrant lock that knows its owner.

    The DataAPI never runs consumer code, calls the client's callback handler or joins a
    worker while a thread holds its lock, and it refuses facade calls made from inside a
    data path. Both need to know whether the *calling* thread holds the lock;
    ``threading.RLock`` tells that only through a private method, so the owner is
    tracked here.

    Use it like a ``threading.RLock``: as a context manager, or with ``acquire`` and
    ``release``.
    """

    __slots__ = ("_lock", "_owner", "_depth")

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # Written only by the thread that holds ``_lock``.
        self._owner: int | None = None
        self._depth = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """
        Acquire the lock; a thread that already holds it enters again.

        Args:
            blocking (bool): Wait for the lock.
            timeout (float): Longest wait in seconds when blocking; -1 waits forever.

        Returns:
            bool: Whether the lock was acquired.
        """
        acquired = self._lock.acquire(blocking, timeout)
        if acquired:
            self._owner = threading.get_ident()
            self._depth += 1
        return acquired

    def release(self) -> None:
        """Leave the lock once; the last release frees it for other threads."""
        if self._owner != threading.get_ident():
            raise RuntimeError("cannot release a lock that this thread does not hold")
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
        self._lock.release()

    def held(self) -> bool:
        """Whether the calling thread holds the lock."""
        return self._owner == threading.get_ident()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *_exc) -> None:
        self.release()
