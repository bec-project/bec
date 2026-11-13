"""The DataAPI lock: reentrant, and it knows whether the calling thread holds it."""

import threading

import pytest

from bec_lib.data_api import DataAPI, OwnedLock
from bec_lib.data_api.testing import lock_held, make_mock_client

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


def test_owned_lock_is_reentrant_and_knows_its_owner():
    lock = OwnedLock()
    assert not lock.held()
    with lock:
        assert lock.held()
        with lock:
            assert lock.held()
        assert lock.held()  # still held after the inner block
        seen = []
        other = threading.Thread(
            target=lambda: seen.append((lock.held(), lock.acquire(blocking=False)))
        )
        other.start()
        other.join()
        assert seen == [(False, False)]  # another thread neither holds nor gets it
    assert not lock.held()


def test_owned_lock_hands_over_to_a_waiting_thread():
    lock = OwnedLock()
    entered = threading.Event()
    held_inside = []

    def worker():
        with lock:
            held_inside.append(lock.held())
            entered.set()

    with lock:
        thread = threading.Thread(target=worker)
        thread.start()
        assert not entered.wait(0.05)  # blocked while this thread holds the lock
    thread.join(2)
    assert held_inside == [True] and not lock.held()


def test_owned_lock_cannot_be_released_by_a_thread_that_does_not_hold_it():
    lock = OwnedLock()
    with pytest.raises(RuntimeError, match="does not hold"):
        lock.release()
    assert lock.acquire(timeout=1)
    errors = []

    def stranger():
        try:
            lock.release()
        except RuntimeError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=stranger)
    thread.start()
    thread.join()
    assert len(errors) == 1 and lock.held()
    lock.release()


def test_the_facade_and_its_paths_share_one_owned_lock():
    api = DataAPI(make_mock_client())
    try:
        assert isinstance(api._lock, OwnedLock)
        assert all(path.lock is api._lock for path in api.paths)
        assert not lock_held(api._lock)
        with api._lock:
            assert lock_held(api._lock)
    finally:
        api.close()
