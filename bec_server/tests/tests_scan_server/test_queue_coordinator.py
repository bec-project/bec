"""Ordering, failure isolation, expiry, and shutdown of the queue owner."""

from __future__ import annotations

import threading
from collections.abc import Iterator

import pytest

from bec_server.scan_server.scan_queue.coordinator import QueueCoordinator


@pytest.fixture
def coordinator() -> Iterator[QueueCoordinator]:
    """Join every coordinator created by these tests."""
    owner = QueueCoordinator()
    yield owner
    if owner.thread.is_alive():
        owner.begin_shutdown(lambda: None)
        owner.join()


def test_events_and_acknowledgements_follow_admission_order(coordinator: QueueCoordinator) -> None:
    observed: list[tuple[int, threading.Thread]] = []
    for value in range(50):
        coordinator.post(lambda value=value: observed.append((value, threading.current_thread())))
    assert coordinator.call(lambda: len(observed)) == 50
    assert observed == [(value, coordinator.thread) for value in range(50)]


def test_nested_owner_call_does_not_deadlock(coordinator: QueueCoordinator) -> None:
    assert (
        coordinator.call(lambda: coordinator.call(lambda: threading.current_thread()))
        is coordinator.thread
    )


def test_request_failure_is_returned_and_owner_survives(coordinator: QueueCoordinator) -> None:
    def fail() -> None:
        raise ValueError("bad request")

    with pytest.raises(ValueError, match="bad request"):
        coordinator.call(fail)
    coordinator.post(fail)
    assert coordinator.call(lambda: "alive") == "alive"


def test_cancelled_expiry_never_executes(coordinator: QueueCoordinator) -> None:
    expired = threading.Event()

    def schedule_and_cancel() -> None:
        coordinator.schedule(0, expired.set).cancel()

    coordinator.call(schedule_and_cancel)
    coordinator.call(lambda: None)
    assert not expired.is_set()


def test_expiry_runs_on_owner_even_with_pending_events(coordinator: QueueCoordinator) -> None:
    observed: list[threading.Thread] = []
    ready = threading.Event()

    def schedule() -> None:
        coordinator.schedule(0, lambda: observed.append(threading.current_thread()))
        for _ in range(20):
            coordinator.post(ready.set)

    coordinator.call(schedule)
    assert ready.wait(5)
    assert observed == [coordinator.thread]


def test_non_owner_cannot_modify_expiry_heap(coordinator: QueueCoordinator) -> None:
    with pytest.raises(RuntimeError, match="scheduled by its coordinator"):
        coordinator.schedule(0, lambda: None)


def test_shutdown_drains_accepted_requests_and_internal_completions(
    coordinator: QueueCoordinator,
) -> None:
    observed: list[str] = []
    coordinator.post(lambda: observed.append("request"))
    coordinator.begin_shutdown(lambda: observed.append("shutdown"))
    assert not coordinator.post(lambda: observed.append("rejected"))
    with pytest.raises(RuntimeError, match="shutting down"):
        coordinator.call(lambda: observed.append("rejected"))
    assert coordinator.post(lambda: observed.append("completion"), internal=True)
    coordinator.join()
    assert observed == ["request", "shutdown", "completion"]
    assert not coordinator.thread.is_alive()
    assert not coordinator.post(lambda: None, internal=True)


def test_latest_updates_coalesce_without_crossing_ordered_events(
    coordinator: QueueCoordinator,
) -> None:
    """Latest state may replace pending state, but terminal events retain their ordering."""
    entered = threading.Event()
    release = threading.Event()
    observed: list[int | str] = []

    def block() -> None:
        entered.set()
        assert release.wait(5)

    coordinator.post(block)
    assert entered.wait(5)
    try:
        coordinator.post_latest("status", observed.append, 1)
        coordinator.post_latest("status", observed.append, 2)
        coordinator.post(observed.append, "terminal")
        coordinator.post_latest("status", observed.append, 3)
        coordinator.post_latest("status", observed.append, 4)
        assert coordinator._events.qsize() == 3
    finally:
        release.set()
    coordinator.call(lambda: None)
    assert observed == [2, "terminal", 4]


def test_latest_updates_reject_closed_admission(coordinator: QueueCoordinator) -> None:
    coordinator.begin_shutdown(lambda: None)
    assert not coordinator.post_latest("status", lambda: None)


def test_call_forwards_internal_keyword_to_operation(coordinator: QueueCoordinator) -> None:
    """The admission helper must not consume keywords belonging to the operation."""
    assert coordinator.call(lambda *, internal: internal, internal=42) == 42
    assert coordinator.call_internal(lambda *, internal: internal, internal=43) == 43
