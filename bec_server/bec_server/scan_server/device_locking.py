import threading
from contextlib import contextmanager
from typing import Dict, Iterable

from bec_lib.logger import bec_logger

logger = bec_logger.logger


class DeviceLockManager:
    """
    Manages locks for devices, identified simply as their name.
    Allows acquiring multiple item locks atomically via a context manager.
    """

    def __init__(self) -> None:
        self._locks: Dict[str, threading.RLock] = {}
        self._locks_guard = threading.RLock()

    def _get_lock(self, key: str) -> threading.RLock:
        """
        Get (or create) a lock for a given key.
        """
        with self._locks_guard:
            if key not in self._locks:
                self._locks[key] = threading.RLock()
            return self._locks[key]

    @contextmanager
    def lock(self, keys: Iterable[str], blocking: bool = True):
        """
        Context manager to lock one or more items.
        """
        keys = list(set(keys))
        try:
            if not self.acquire(*keys, blocking=blocking):
                return
            yield
        finally:
            self.release(*keys)

    def acquire(self, *keys: str, blocking: bool = True):
        logger.info(f"Locking devices: {keys}")
        with self._locks_guard:
            new_locks = []
            for key in sorted(keys):
                next_lock = self._get_lock(key)
                if not next_lock.acquire(blocking=blocking):
                    [lock.release() for lock in new_locks]
                    return False
                new_locks.append(next_lock)
        return True

    def release(self, *keys: str):
        logger.info(f"Releasing devices: {keys}")
        with self._locks_guard:
            for key in reversed(sorted(keys)):
                self._get_lock(key).release()
