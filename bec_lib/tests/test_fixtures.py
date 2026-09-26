"""Regression tests for the shared thread-cleanup fixture."""

from __future__ import annotations

import threading
from unittest import mock

import pytest

from bec_lib.tests.fixtures import threads_check


def test_threads_check_ignores_native_thread_dummy() -> None:
    """A native thread's bookkeeping entry must not count as a Python thread leak."""
    dummy = mock.Mock(spec=threading._DummyThread)  # pylint: disable=protected-access
    main = threading.main_thread()
    with mock.patch("threading.enumerate", side_effect=[[main], [main, dummy]]):
        check = threads_check.__wrapped__()
        next(check)
        with pytest.raises(StopIteration):
            next(check)


@pytest.mark.parametrize("daemon", [False, True])
def test_threads_check_detects_real_thread_with_dummy_name(daemon: bool) -> None:
    """Real threads must be detected regardless of their name or daemon flag."""
    release = threading.Event()
    thread = threading.Thread(target=release.wait, name="Dummy-real-worker", daemon=daemon)
    check = threads_check.__wrapped__()
    next(check)
    thread.start()
    try:
        with pytest.raises(AssertionError, match="1 threads.*Dummy-real-worker"):
            next(check)
    finally:
        release.set()
        thread.join(timeout=1)
    assert not thread.is_alive()


def test_threads_check_detects_real_thread_alongside_dummy() -> None:
    """Ignoring a dummy must not hide another thread created by the same test."""
    dummy = mock.Mock(spec=threading._DummyThread)  # pylint: disable=protected-access
    real = mock.Mock(spec=threading.Thread)
    main = threading.main_thread()
    with mock.patch("threading.enumerate", side_effect=[[main], [main, dummy, real]]):
        check = threads_check.__wrapped__()
        next(check)
        with pytest.raises(AssertionError, match="1 threads"):
            next(check)


def test_threads_check_allows_preexisting_thread() -> None:
    """A thread which predates the test is not the test's responsibility."""
    release = threading.Event()
    thread = threading.Thread(target=release.wait)
    thread.start()
    try:
        check = threads_check.__wrapped__()
        next(check)
        with pytest.raises(StopIteration):
            next(check)
    finally:
        release.set()
        thread.join(timeout=1)
    assert not thread.is_alive()


def test_threads_check_allows_joined_thread() -> None:
    """A test may create a thread if it joins it before teardown."""
    check = threads_check.__wrapped__()
    next(check)
    thread = threading.Thread(target=lambda: None)
    thread.start()
    thread.join(timeout=1)
    assert not thread.is_alive()
    with pytest.raises(StopIteration):
        next(check)
