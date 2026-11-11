"""The DataAPI lock: one reentrant lock, and the facade can tell whether a thread holds it."""

import threading

from bec_lib.data_api import DataAPI
from bec_lib.data_api.testing import lock_held, make_mock_client

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


def test_lock_held_tells_whether_the_calling_thread_holds_the_lock():
    lock = threading.RLock()
    assert not lock_held(lock)
    with lock:
        assert lock_held(lock)
        with lock:
            assert lock_held(lock)
        assert lock_held(lock)  # still held after the inner block
        seen = []
        other = threading.Thread(target=lambda: seen.append(lock_held(lock)))
        other.start()
        other.join()
        assert seen == [False]  # held, but not by that thread
    assert not lock_held(lock)


def test_the_facade_and_its_paths_share_one_lock():
    api = DataAPI(make_mock_client())
    try:
        assert all(path.lock is api._lock for path in api.paths)
        assert not lock_held(api._lock)
        with api._lock:
            assert lock_held(api._lock)
    finally:
        api.close()
