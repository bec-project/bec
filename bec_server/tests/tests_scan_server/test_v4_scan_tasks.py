"""Behavior and regression tests for the v4 scan queue and its task workers."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from types import SimpleNamespace
from typing import Any, TypeAlias, cast
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.scan_server.direct_scan_worker import DirectScanWorker, ScanControl, ScanOutcome
from bec_server.scan_server.scan_queue import (
    InstructionQueueStatus,
    QueueManager,
    ScanQueue,
    ScanQueueStatus,
)
from bec_server.scan_server.scans.scan_base import ScanBase, ScanType


def wait_for(predicate: Callable[[], bool], timeout: float = 5) -> None:
    """Wait for an asynchronous state transition with a bounded deadline."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("Expected scan task state was not reached")
        time.sleep(0.005)


class FakeScan:
    """Small v4 scan stand-in that exposes controllable lifecycle checkpoints."""

    is_scan = True

    def __init__(
        self, rid: str, parent: mock.MagicMock, core: Callable[[FakeScan], object] | None = None
    ) -> None:
        self.redis_connector = parent.connector
        self.device_manager = parent.device_manager
        self.scan_info = SimpleNamespace(
            scan_id=f"scan-{rid}",
            scan_type=ScanType.SOFTWARE_TRIGGERED,
            scan_number=None,
            dataset_number=None,
            metadata={"RID": rid},
            run_on_exception_hook=True,
            readout_priority_modification={},
            scan_report_instructions=[],
        )
        self.actions = mock.MagicMock()
        self.actions.get_owned_device_locks.return_value = []
        self.actions.get_pending_device_locks.return_value = []
        self._shutdown_event = threading.Event()
        self.started = threading.Event()
        self.finished = threading.Event()
        self.core = core
        self.cleanup_errors = []

    def prepare_scan(self) -> None:
        pass

    def open_scan(self) -> None:
        pass

    def stage(self) -> None:
        pass

    def pre_scan(self) -> None:
        pass

    def scan_core(self) -> None:
        self.started.set()
        if self.core is not None:
            self.core(self)

    def post_scan(self) -> None:
        pass

    def unstage(self) -> None:
        pass

    def close_scan(self) -> None:
        self.finished.set()

    def on_exception(self, exc: Exception) -> None:
        self.cleanup_errors.append(exc)


ScanFactory: TypeAlias = Callable[[dict[str, FakeScan]], QueueManager]


def scan_message(
    rid: str, *, queue: str = "primary", group: str | None = None
) -> messages.ScanQueueMessage:
    """Build one v4 scan request."""
    metadata = {"RID": rid}
    if group is not None:
        metadata["queue_group"] = group
    return messages.ScanQueueMessage(
        scan_type="fake_v4", parameter={"args": {}, "kwargs": {}}, queue=queue, metadata=metadata
    )


@pytest.fixture
def make_manager() -> Iterator[ScanFactory]:
    """Create queue managers with a fake v4 assembler and real task futures."""
    managers = []

    def build(scans: dict[str, FakeScan]) -> QueueManager:
        parent = mock.MagicMock()
        parent.scan_number = 0
        parent.dataset_number = 0
        parent.scan_assembler.scan_manager.scan_dict = {"fake_v4": SimpleNamespace(is_scan=True)}
        parent.scan_assembler.assemble_scan.side_effect = lambda msg, scan_id: scans[
            msg.metadata["RID"]
        ]
        for scan in scans.values():
            scan.redis_connector = parent.connector
            scan.device_manager = parent.device_manager
        manager = QueueManager(parent)
        manager.add_queue("primary")
        managers.append(manager)
        return manager

    yield build

    for manager in managers:
        manager.shutdown()


def test_worker_runs_without_queue_reference() -> None:
    parent = mock.MagicMock()
    scan = FakeScan("one", parent)
    control = ScanControl(run_on_exception_hook=True)
    worker = DirectScanWorker(
        scan=cast(ScanBase, scan),
        control=control,
        on_status=mock.MagicMock(),
        device_lock_registry=parent.device_lock_registry,
    )

    assert worker.run() == "completed"
    assert scan.finished.is_set()
    parent.device_lock_registry.release_all.assert_called_once_with("one")


def test_future_completion_retires_item_and_writes_history(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})

    manager.add_to_queue("primary", scan_message("one"))
    wait_for(lambda: not manager.queues["primary"].queue)

    queue = manager.queues["primary"]
    assert scan.finished.is_set()
    assert queue.active_task is None
    assert queue.history_queue[-1].status == InstructionQueueStatus.COMPLETED
    manager._publisher.call(lambda: None)
    cast(mock.Mock, manager.connector.lpush).assert_called_once()
    assert manager.parent.scan_number == 1


def test_tasks_are_serial_within_queue(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})

    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))
    assert not second.started.wait(0.05)

    release.set()
    assert second.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert [
        item.scan_msgs[0].metadata["RID"] for item in manager.queues["primary"].history_queue
    ] == ["first", "second"]


def test_v4_scans_run_sequentially_in_separate_items(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})

    manager.add_to_queue("primary", scan_message("first", group="group"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second", group="group"))
    assert len(manager.queues["primary"].queue) == 2
    assert not second.started.is_set()

    release.set()
    assert second.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert len(manager.queues["primary"].history_queue) == 2


def test_immediate_completions_are_retired_on_coordinator(make_manager: ScanFactory) -> None:
    scans = {str(index): FakeScan(str(index), mock.MagicMock()) for index in range(25)}
    manager = make_manager(scans)
    retirement_threads: list[threading.Thread] = []
    cast(mock.Mock, manager.connector.lpush).side_effect = (
        lambda *_args, **_kwargs: retirement_threads.append(threading.current_thread())
    )
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))
    queue.status = ScanQueueStatus.RUNNING
    wait_for(lambda: not queue.queue)
    assert all(scan.finished.is_set() for scan in scans.values())
    manager._publisher.call(lambda: None)
    assert retirement_threads == [manager._publisher.thread] * len(scans)


def test_named_queues_can_execute_concurrently(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock(), lambda scan: release.wait(5))
    manager = make_manager({"first": first, "second": second})
    manager.add_queue("secondary")

    manager.add_to_queue("primary", scan_message("first"))
    manager.add_to_queue("secondary", scan_message("second", queue="secondary"))
    assert first.started.wait(5)
    assert second.started.wait(5)

    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)
    wait_for(lambda: not manager.queues["secondary"].queue)


def test_queue_lock_holds_admission_until_release(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    lock = messages.ScanQueueLock(reason="test", identifier="hold", allow_device_instructions=False)
    manager.add_queue_lock("primary", lock)

    manager.add_to_queue("primary", scan_message("one"))
    assert not scan.started.wait(0.05)
    manager.remove_queue_lock("primary", lock)
    assert scan.started.wait(5)


def test_queue_status_change_is_serialized_by_coordinator(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))
    entered = threading.Event()
    release = threading.Event()

    def hold_coordinator() -> None:
        entered.set()
        release.wait(5)

    manager._coordinator.post(hold_coordinator)
    assert entered.wait(5)
    resume = threading.Thread(target=lambda: setattr(queue, "status", ScanQueueStatus.RUNNING))
    resume.start()
    try:
        assert not scan.started.wait(0.05)
        assert resume.is_alive()
    finally:
        release.set()
    resume.join(5)
    assert not resume.is_alive()
    assert scan.started.wait(5)


def test_pending_abort_does_not_interrupt_active_scan(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})

    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))
    manager.set_abort(request_id="second", queue="primary")
    assert not first._shutdown_event.is_set()
    assert not second.started.is_set()

    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)
    assert not second.started.is_set()


def test_halt_pending_scan_keeps_active_scan_exception_cleanup(make_manager: ScanFactory) -> None:
    release = threading.Event()

    def fail_after_halt(_scan: FakeScan) -> None:
        assert release.wait(5)
        raise ValueError("active scan failed")

    first = FakeScan("first", mock.MagicMock(), fail_after_halt)
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))

    manager.set_halt(request_id="second", queue="primary")
    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)
    assert len(first.cleanup_errors) == 1
    assert not second.started.is_set()


def test_restart_stops_original_then_dispatches_replacement(make_manager: ScanFactory) -> None:
    release = threading.Event()

    def original_core(scan: FakeScan) -> None:
        assert release.wait(5)
        scan.actions._interruption_callback()

    original = FakeScan("original", mock.MagicMock(), original_core)
    replacement = FakeScan("replacement", mock.MagicMock())
    manager = make_manager({"original": original, "replacement": replacement})
    manager.add_to_queue("primary", scan_message("original"))
    assert original.started.wait(5)

    manager.set_restart(scan_id="scan-original", queue="primary", parameter={"RID": "replacement"})
    assert not replacement.started.is_set()
    release.set()

    assert replacement.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    history = list(manager.queues["primary"].history_queue)
    assert [item.status for item in history] == [
        InstructionQueueStatus.STOPPED,
        InstructionQueueStatus.COMPLETED,
    ]


def test_abort_reaches_running_task_and_future_finishes_after_cleanup(
    make_manager: ScanFactory,
) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_abort(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert manager.queues["primary"].history_queue[-1].status == InstructionQueueStatus.STOPPED
    assert len(scan.cleanup_errors) == 1
    scan.actions._send_scan_status.assert_called_with("aborted", reason="user")


def test_clear_keeps_exception_cleanup_for_running_scan(make_manager: ScanFactory) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_clear(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)
    assert len(scan.cleanup_errors) == 1


def test_clear_discards_deferred_inserts(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)

    manager.set_abort(queue="primary")
    manager.add_to_queue("primary", scan_message("second"))
    assert manager.queues["primary"]._deferred_inserts
    manager.set_clear(queue="primary")
    release.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert not manager.queues["primary"]._deferred_inserts
    assert not second.started.is_set()


def test_halt_disables_exception_hook_for_running_scan(make_manager: ScanFactory) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_halt(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)
    assert not scan.cleanup_errors
    scan.actions._send_scan_status.assert_called_with("halted", reason="user")


def test_pause_and_continue_use_control_channel(make_manager: ScanFactory) -> None:
    proceed = threading.Event()
    checkpoint_exited = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()
        checkpoint_exited.set()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_pause(queue="primary")
    proceed.set()
    wait_for(lambda: scan.actions._send_scan_status.call_count > 0)
    assert not checkpoint_exited.is_set()

    manager.set_continue(queue="primary")
    assert checkpoint_exited.wait(5)
    wait_for(lambda: manager.queues["primary"].active_task is None)


def test_continue_resumes_item_before_dispatching_next_scan(make_manager: ScanFactory) -> None:
    observed_status: list[InstructionQueueStatus] = []
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: observed_status.append(item.status))
    manager = make_manager({"one": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))
    item = queue.queue[0]
    item.status = InstructionQueueStatus.PAUSED
    manager.set_continue(queue="primary")
    wait_for(lambda: not queue.queue)
    assert observed_status == [InstructionQueueStatus.RUNNING]


def test_paused_queue_waits_without_occupying_pool_thread(make_manager: ScanFactory) -> None:
    first = FakeScan("first", mock.MagicMock())
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    queue = manager.queues["primary"]

    def finish_and_pause() -> None:
        manager.set_deferred_pause(queue="primary")
        first.finished.set()

    first.close_scan = finish_and_pause
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("first", group="group"))
    manager.add_to_queue("primary", scan_message("second", group="group"))
    queue.status = ScanQueueStatus.RUNNING

    assert first.finished.wait(5)
    wait_for(lambda: queue.active_task is None)
    assert not second.started.is_set()
    assert queue.status == ScanQueueStatus.PAUSED

    manager.set_continue(queue="primary")
    assert second.started.wait(5)
    wait_for(lambda: not queue.queue)


def test_worker_status_callback_publishes_queue_snapshot(make_manager: ScanFactory) -> None:
    published = threading.Event()
    returned = threading.Event()
    release = threading.Event()
    publication_threads: list[threading.Thread] = []

    def core(scan: FakeScan) -> None:
        scan.actions.get_pending_device_locks.return_value = ["motor"]
        scan.actions._update_queue_info_callback()
        returned.set()
        release.wait(5)

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})

    def observe(_endpoint: Any, message: messages.ScanQueueStatusMessage) -> None:
        info = message.queue["primary"].info
        if info and info[0].request_blocks[0].pending_device_locks == ["motor"]:
            publication_threads.append(threading.current_thread())
            published.set()

    cast(mock.Mock, manager.connector.set_and_publish).side_effect = observe
    manager.add_to_queue("primary", scan_message("one"))
    try:
        assert returned.wait(5)
        assert published.wait(5)
        assert all(thread is manager._publisher.thread for thread in publication_threads)
    finally:
        release.set()


def test_worker_failure_is_reported_through_future_result(make_manager: ScanFactory) -> None:
    def fail(_scan: FakeScan) -> None:
        raise RuntimeError("scan failed")

    scan = FakeScan("one", mock.MagicMock(), fail)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert manager.queues["primary"].history_queue[-1].status == InstructionQueueStatus.STOPPED
    cast(mock.Mock, manager.connector.raise_alarm).assert_called()
    assert len(scan.cleanup_errors) == 1


def test_cleanup_hook_failure_raises_alarm(make_manager: ScanFactory) -> None:
    def fail(_scan: FakeScan) -> None:
        raise RuntimeError("scan failed")

    scan = FakeScan("one", mock.MagicMock(), fail)
    scan.on_exception = mock.Mock(side_effect=ValueError("cleanup failed"))
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    wait_for(lambda: manager.queues["primary"].active_task is None)

    alarm_info = cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs["info"]
    assert alarm_info.exception_type == "ValueError"
    assert "cleanup failed" in alarm_info.error_message
    assert manager.queues["primary"].history_queue[-1].status == InstructionQueueStatus.STOPPED


def test_removed_queue_does_not_dispatch_successor_after_late_completion(
    make_manager: ScanFactory,
) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))

    task = manager.queues["primary"].active_task
    assert task is not None
    manager.remove_queue("primary", skip_primary=False)
    assert "primary" not in manager.queues
    release.set()
    assert task.future.result(timeout=5) == "shutdown"
    assert not second.started.is_set()


def test_late_completion_cannot_change_replacement_queue(make_manager: ScanFactory) -> None:
    release = threading.Event()
    old = FakeScan("old", mock.MagicMock(), lambda scan: release.wait(5))
    new = FakeScan("new", mock.MagicMock())
    manager = make_manager({"old": old, "new": new})
    manager.add_to_queue("primary", scan_message("old"))
    assert old.started.wait(5)
    old_queue = manager.queues["primary"]
    old_task = old_queue.active_task
    assert old_task is not None

    manager.remove_queue("primary", skip_primary=False)
    replacement = threading.Thread(
        target=manager.add_to_queue, args=("primary", scan_message("new"))
    )
    replacement.start()
    assert not new.started.wait(0.05)
    release.set()
    assert old_task.future.result(timeout=5) == "shutdown"
    replacement.join(timeout=5)
    assert not replacement.is_alive()
    assert new.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert manager.queues["primary"] is not old_queue
    assert manager.queues["primary"].history_queue[-1].scan_msgs[0].metadata["RID"] == "new"


def test_shutdown_joins_independent_tasks_without_false_alarm(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock(), lambda scan: release.wait(5))
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    manager.add_to_queue("secondary", scan_message("second", queue="secondary"))
    assert first.started.wait(5) and second.started.wait(5)
    tasks = [queue.active_task for queue in manager.queues.values()]
    shutdown = threading.Thread(target=manager.shutdown)
    shutdown.start()
    wait_for(lambda: not manager.queues)
    release.set()
    shutdown.join(5)
    assert not shutdown.is_alive()
    assert all(task is not None and not task.thread.is_alive() for task in tasks)
    assert not manager._coordinator.thread.is_alive()
    cast(mock.Mock, manager.connector.raise_alarm).assert_not_called()


def test_recreation_waits_for_removed_task_without_blocking_other_queues(
    make_manager: ScanFactory,
) -> None:
    release = threading.Event()
    old = FakeScan("old", mock.MagicMock(), lambda scan: release.wait(5))
    new = FakeScan("new", mock.MagicMock())
    other = FakeScan("other", mock.MagicMock())
    manager = make_manager({"old": old, "new": new, "other": other})
    manager.add_to_queue("secondary", scan_message("old", queue="secondary"))
    assert old.started.wait(5)
    old_task = manager.queues["secondary"].active_task
    assert old_task is not None
    manager.remove_queue("secondary")
    manager.add_to_queue("secondary", scan_message("new", queue="secondary"))
    manager.add_to_queue("primary", scan_message("other"))
    assert other.started.wait(5)
    assert not new.started.is_set()
    release.set()
    assert old_task.future.result(5) == "shutdown"
    assert new.started.wait(5)


def test_idle_secondary_queue_expires_without_a_worker(
    monkeypatch: pytest.MonkeyPatch, make_manager: ScanFactory
) -> None:
    monkeypatch.setattr(ScanQueue, "AUTO_SHUTDOWN_TIME", 0.02)
    manager = make_manager({})
    manager.add_queue("secondary")
    timer = manager.queues["secondary"]._idle_expiry

    wait_for(lambda: "secondary" not in manager.queues)
    assert "primary" in manager.queues
    manager.shutdown()
    assert timer is not None
    assert not manager._coordinator.thread.is_alive()


def test_pending_cancellation_restarts_idle_queue_expiry(
    monkeypatch: pytest.MonkeyPatch, make_manager: ScanFactory
) -> None:
    monkeypatch.setattr(ScanQueue, "AUTO_SHUTDOWN_TIME", 0.02)
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    manager.add_queue("secondary")
    queue = manager.queues["secondary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("secondary", scan_message("one", queue="secondary"))
    wait_for(lambda: queue._idle_expiry is None)
    assert "secondary" in manager.queues
    manager.set_abort(request_id="one", queue="secondary")
    wait_for(lambda: "secondary" not in manager.queues)
    assert not scan.started.is_set()


def test_clear_resets_empty_paused_queue(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))

    manager.set_clear(queue="primary")

    assert queue.status == ScanQueueStatus.RUNNING
    assert not queue.queue
    assert not scan.started.is_set()


def test_order_change_ignores_queue_removed_after_guard_validation(
    make_manager: ScanFactory,
) -> None:
    manager = make_manager({})
    manager.remove_queue("primary", skip_primary=False)

    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(queue="primary", scan_id="old-scan", action="move_bottom")
    )

    assert "primary" not in manager.queues


def test_reordering_active_item_during_deferred_pause_does_not_replay_it(
    make_manager: ScanFactory,
) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))
    manager.set_deferred_pause(queue="primary")
    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(
            queue="primary", scan_id=first.scan_info.scan_id, action="move_bottom"
        )
    )

    release.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)
    assert [item.scan_msgs[0].metadata["RID"] for item in manager.queues["primary"].queue] == [
        "second"
    ]
    manager.set_continue(queue="primary")
    assert second.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert not first._shutdown_event.is_set()


def test_shutdown_joins_pending_idle_timer(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    manager.add_queue("secondary")
    timer = manager.queues["secondary"]._idle_expiry
    assert timer is not None and not timer.cancelled

    manager.shutdown()

    assert not manager._coordinator.thread.is_alive()


def test_queue_export_preserves_pending_order_and_scan_numbers(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("first", "second", "third")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("first", group="series"))
    manager.add_to_queue("primary", scan_message("second", group="series"))
    manager.add_to_queue("primary", scan_message("third"))

    snapshot = manager.export_queue()["primary"]
    assert snapshot.status == "PAUSED"
    assert len(snapshot.info) == 3
    assert [block.RID for entry in snapshot.info for block in entry.request_blocks] == [
        "first",
        "second",
        "third",
    ]
    assert [entry.scan_number for entry in snapshot.info] == [[1], [2], [3]]
    assert all(entry.active_request_block is None for entry in snapshot.info)
    assert all(not scan.started.is_set() for scan in scans.values())

    manager.send_queue_status()
    manager._publisher.call(lambda: None)
    endpoint, message = cast(mock.Mock, manager.connector.set_and_publish).call_args.args
    assert endpoint == MessageEndpoints.scan_queue_status()
    assert message.queue["primary"] == snapshot


def test_group_metadata_does_not_combine_queue_items(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("a", "b", "c")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("a", group="one"))
    manager.add_to_queue("primary", scan_message("b", group="two"))
    manager.add_to_queue("primary", scan_message("c", group="one"))

    assert len(queue.queue) == 3
    assert [[msg.metadata["RID"] for msg in item.scan_msgs] for item in queue.queue] == [
        ["a"],
        ["b"],
        ["c"],
    ]
    assert queue.get_scan("scan-c") is queue.queue[2]


@pytest.mark.parametrize(
    ("order_message", "expected"),
    [
        (messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_up"), ["a", "c", "b", "d"]),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_down"),
            ["a", "b", "d", "c"],
        ),
        (messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_top"), ["c", "a", "b", "d"]),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_bottom"),
            ["a", "b", "d", "c"],
        ),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_to", target_position=0),
            ["c", "a", "b", "d"],
        ),
        (
            messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_to", target_position=99),
            ["a", "b", "d", "c"],
        ),
    ],
)
def test_paused_queue_reorders_by_scan_id(
    make_manager: ScanFactory, order_message: messages.ScanQueueOrderMessage, expected: list[str]
) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in "abcd"}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))

    manager._handle_scan_order_change(order_message)

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == expected


def test_reordered_pending_queue_executes_in_new_order(make_manager: ScanFactory) -> None:
    executed: list[str] = []
    scans = {
        rid: FakeScan(rid, mock.MagicMock(), lambda _scan, rid=rid: executed.append(rid))
        for rid in "abc"
    }
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))
    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(scan_id="scan-c", action="move_top")
    )

    queue.status = ScanQueueStatus.RUNNING
    wait_for(lambda: not queue.queue)
    assert executed == ["c", "a", "b"]
    assert [item.status for item in queue.history_queue] == [InstructionQueueStatus.COMPLETED] * 3


def test_pending_items_can_be_removed_by_scan_or_request_id(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("first", "second", "third")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))

    queue.remove_queue_item("scan-first")
    queue.remove_queue_item_by_request_id("second")

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["third"]
    assert queue.get_scan("scan-first") is None
    assert not scans["first"].started.is_set()
    assert not scans["second"].started.is_set()


def test_abort_pending_request_publishes_cancelled_before_removing_it(
    make_manager: ScanFactory,
) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("keep", "cancel")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("keep"))
    manager.add_to_queue("primary", scan_message("cancel"))

    manager.set_abort(request_id="cancel")
    manager._publisher.call(lambda: None)

    snapshots = [
        call.args[1].queue["primary"]
        for call in cast(mock.Mock, manager.connector.set_and_publish).call_args_list
    ]
    assert any(
        any(
            entry.status == "CANCELLED" and entry.request_blocks[0].RID == "cancel"
            for entry in snapshot.info
        )
        for snapshot in snapshots
    )
    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["keep"]
    assert not scans["cancel"].started.is_set()


def test_multiple_locks_restore_previous_paused_status(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    first = messages.ScanQueueLock(reason="maintenance", identifier="first")
    second = messages.ScanQueueLock(reason="alignment", identifier="second")

    manager.add_queue_lock("primary", first)
    manager.add_queue_lock("primary", second)
    assert queue.status == ScanQueueStatus.LOCKED
    assert {lock.identifier for lock in manager.export_queue()["primary"].locks} == {
        "first",
        "second",
    }
    manager.remove_queue_lock("primary", first)
    assert queue.status == ScanQueueStatus.LOCKED
    manager.remove_queue_lock("primary", second)
    assert queue.status == ScanQueueStatus.PAUSED


def test_invalid_scan_request_raises_alarm_without_adding_item(make_manager: ScanFactory) -> None:
    manager = make_manager({})

    manager.add_to_queue("primary", scan_message("invalid"))

    assert not manager.queues["primary"].queue
    alarm = cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs
    assert alarm["info"].exception_type == "KeyError"
    assert alarm["metadata"]["RID"] == "invalid"


def test_non_scan_request_does_not_consume_scan_or_dataset_number(
    make_manager: ScanFactory,
) -> None:
    scan = FakeScan("rpc", mock.MagicMock())
    scan.is_scan = False
    scan.scan_info.scan_type = None
    scan.scan_info.scan_id = None
    manager = make_manager({"rpc": scan})
    manager.parent.scan_assembler.scan_manager.scan_dict["fake_v4"].is_scan = False

    manager.add_to_queue("primary", scan_message("rpc"))
    wait_for(lambda: not manager.queues["primary"].queue)

    assert scan.finished.is_set()
    assert scan.scan_info.scan_number is None
    assert scan.scan_info.dataset_number is None
    assert manager.parent.scan_number == 0
    assert manager.parent.dataset_number == 0


def test_stopped_head_buffers_and_then_flushes_inserts(make_manager: ScanFactory) -> None:
    release = threading.Event()
    scans = {
        "first": FakeScan("first", mock.MagicMock(), lambda _scan: release.wait(5)),
        "second": FakeScan("second", mock.MagicMock()),
    }
    manager = make_manager(scans)
    manager.add_to_queue("primary", scan_message("first"))
    assert scans["first"].started.wait(5)
    manager.set_abort(queue="primary")
    manager.add_to_queue("primary", scan_message("second"))
    queue = manager.queues["primary"]
    assert [item.scan_msgs[0].metadata["RID"] for item, _ in queue._deferred_inserts] == ["second"]

    release.set()
    wait_for(lambda: queue.active_task is None)

    assert not queue._deferred_inserts
    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["second"]
    assert not scans["second"].started.is_set()
    manager.set_continue(queue="primary")
    assert scans["second"].started.wait(5)
    wait_for(lambda: not queue.queue)


def test_insert_callback_routes_request_to_named_queue(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    message = scan_message("one", queue="secondary")

    manager._scan_queue_callback(cast(Any, SimpleNamespace(value=message)))

    assert scan.started.wait(5)
    wait_for(lambda: not manager.queues["secondary"].queue)
    assert "secondary" in manager.queues
    assert manager.queues["secondary"].history_queue[-1].scan_msgs[0] == message


def test_modification_callback_dispatches_lock_and_release(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    lock_message = messages.ScanQueueModificationMessage(
        scan_id=None, action="lock", parameter={"reason": "maintenance", "identifier": "hold"}
    )
    release_message = messages.ScanQueueModificationMessage(
        scan_id=None, action="release_lock", parameter={"identifier": "hold"}
    )

    manager._scan_queue_modification_callback(cast(Any, SimpleNamespace(value=lock_message)))
    manager._coordinator.call(lambda: None)
    assert manager.queues["primary"].status == ScanQueueStatus.LOCKED
    assert manager.export_queue()["primary"].locks[0].reason == "maintenance"
    manager._scan_queue_modification_callback(cast(Any, SimpleNamespace(value=release_message)))
    manager._coordinator.call(lambda: None)
    assert manager.queues["primary"].status == ScanQueueStatus.RUNNING
    assert not manager.queues["primary"].locks


@pytest.mark.parametrize(
    ("action", "parameter", "error"),
    [
        ("lock", None, "Missing parameter"),
        ("lock", {"identifier": "hold"}, "Missing lock reason"),
        ("lock", {"reason": "maintenance"}, "Missing lock identifier"),
        ("release_lock", None, "Missing parameter"),
        ("release_lock", {"reason": "maintenance"}, "Missing lock identifier"),
    ],
)
def test_lock_modifications_require_identifiers_and_reason(
    make_manager: ScanFactory, action: str, parameter: dict[str, str] | None, error: str
) -> None:
    manager = make_manager({})
    with pytest.raises(ValueError, match=error):
        if action == "lock":
            manager.set_lock(parameter=parameter)
        else:
            manager.set_release_lock(parameter=parameter)


def test_allowed_device_instructions_lock_runs_non_scan_but_holds_scan(
    make_manager: ScanFactory,
) -> None:
    rpc = FakeScan("rpc", mock.MagicMock())
    rpc.is_scan = False
    rpc.scan_info.scan_type = None
    rpc.scan_info.scan_id = None
    scan = FakeScan("scan", mock.MagicMock())
    manager = make_manager({"rpc": rpc, "scan": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.parent.scan_assembler.scan_manager.scan_dict["fake_v4"].is_scan = False
    manager.add_to_queue("primary", scan_message("rpc"))
    manager.parent.scan_assembler.scan_manager.scan_dict["fake_v4"].is_scan = True
    manager.add_to_queue("primary", scan_message("scan"))
    lock = messages.ScanQueueLock(
        reason="allow RPC", identifier="rpc", allow_device_instructions=True
    )

    manager.add_queue_lock("primary", lock)
    assert rpc.finished.wait(5)
    assert not scan.started.is_set()
    manager.remove_queue_lock("primary", lock)
    assert queue.status == ScanQueueStatus.PAUSED
    assert not scan.started.is_set()
    queue.status = ScanQueueStatus.RUNNING
    assert scan.started.wait(5)
    wait_for(lambda: not queue.queue)


def test_continue_does_not_unlock_a_locked_queue(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    lock = messages.ScanQueueLock(reason="maintenance", identifier="hold")
    manager.add_queue_lock("primary", lock)
    manager.add_to_queue("primary", scan_message("one"))

    manager.set_continue(queue="primary")

    assert manager.queues["primary"].status == ScanQueueStatus.LOCKED
    assert not scan.started.is_set()
    manager.remove_queue_lock("primary", lock)
    assert scan.started.wait(5)


def test_dataset_hold_reuses_dataset_number_for_next_scan(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("first", "second")}
    manager = make_manager(scans)
    manager.add_to_queue("primary", scan_message("first"))
    assert scans["first"].finished.wait(5)
    second_message = scan_message("second")
    second_message.metadata["dataset_id_on_hold"] = True
    manager.add_to_queue("primary", second_message)
    assert scans["second"].finished.wait(5)

    assert [scans[rid].scan_info.scan_number for rid in ("first", "second")] == [1, 2]
    assert [scans[rid].scan_info.dataset_number for rid in ("first", "second")] == [1, 1]
    assert manager.parent.scan_number == 2
    assert manager.parent.dataset_number == 1


@pytest.mark.parametrize("devices", [None, [], ["motor"]])
def test_stop_devices_preserves_all_none_and_selected_device_semantics(
    make_manager: ScanFactory, devices: list[str] | None
) -> None:
    manager = make_manager({})

    manager.stop_all_devices(stop_id="scan-one", devices=devices)

    endpoint, message = cast(mock.Mock, manager.connector.send).call_args.args
    assert endpoint == MessageEndpoints.stop_devices()
    assert message.value == devices
    assert message.metadata["stop_id"] == "scan-one"


def test_pending_abort_by_scan_id_removes_only_target(make_manager: ScanFactory) -> None:
    scans = {rid: FakeScan(rid, mock.MagicMock()) for rid in ("keep", "cancel")}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("keep"))
    manager.add_to_queue("primary", scan_message("cancel"))

    manager.set_abort(scan_id="scan-cancel")

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["keep"]
    assert queue.queue[0].status == InstructionQueueStatus.PENDING
    assert not scans["cancel"].started.is_set()


def test_unknown_abort_target_leaves_pending_queue_unchanged(make_manager: ScanFactory) -> None:
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))

    manager.set_abort(request_id="unknown")
    manager.set_abort(scan_id="scan-unknown")

    assert [item.scan_msgs[0].metadata["RID"] for item in queue.queue] == ["one"]
    assert queue.status == ScanQueueStatus.PAUSED
    assert not scan.started.is_set()


def test_order_change_is_ignored_while_queue_is_running(make_manager: ScanFactory) -> None:
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda _scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_to_queue("primary", scan_message("second"))

    manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(scan_id="scan-second", action="move_top")
    )

    assert [item.scan_msgs[0].metadata["RID"] for item in manager.queues["primary"].queue] == [
        "first",
        "second",
    ]
    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)


def test_user_completed_restores_queue_status_after_active_scan_stops(
    make_manager: ScanFactory,
) -> None:
    proceed = threading.Event()

    def core(scan: FakeScan) -> None:
        assert proceed.wait(5)
        scan.actions._interruption_callback()

    scan = FakeScan("one", mock.MagicMock(), core)
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)

    manager.set_user_completed(queue="primary")
    proceed.set()
    wait_for(lambda: manager.queues["primary"].active_task is None)

    assert manager.queues["primary"].status == ScanQueueStatus.RUNNING
    scan.actions._send_scan_status.assert_called_with("user_completed", reason="user")


def test_unexpected_future_exception_reports_alarm_and_runs_successor(
    make_manager: ScanFactory,
) -> None:
    first = FakeScan("first", mock.MagicMock())
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("first"))
    manager.add_to_queue("primary", scan_message("second"))
    original_run = DirectScanWorker.run

    def crash_first(
        worker: DirectScanWorker, prepare: Callable[[], None] | None = None
    ) -> ScanOutcome:
        if worker.scan.scan_info.metadata["RID"] == "first":
            raise RuntimeError("unexpected worker failure")
        return original_run(worker, prepare)

    with mock.patch.object(DirectScanWorker, "run", crash_first):
        queue.status = ScanQueueStatus.RUNNING
        wait_for(lambda: not queue.queue)

    assert [item.status for item in queue.history_queue] == [
        InstructionQueueStatus.STOPPED,
        InstructionQueueStatus.COMPLETED,
    ]
    assert second.finished.is_set()
    assert (
        cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs["info"].exception_type
        == "RuntimeError"
    )


def test_snapshot_is_immutable_and_exported_messages_are_detached(
    make_manager: ScanFactory,
) -> None:
    from dataclasses import FrozenInstanceError

    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    manager.queues["primary"].status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))
    snapshot = manager.get_snapshot()
    exported = snapshot.to_messages()
    exported["primary"].info[0].request_blocks[0].msg.metadata["RID"] = "changed"
    assert snapshot.to_messages()["primary"].info[0].request_blocks[0].RID == "one"
    assert manager.export_queue()["primary"].info[0].request_blocks[0].msg.metadata["RID"] == "one"
    with pytest.raises(FrozenInstanceError):
        setattr(snapshot, "queues", ())
    manager.set_continue()
    wait_for(lambda: not manager.queues["primary"].queue)
    assert len(snapshot.to_messages()["primary"].info) == 1


def test_future_callback_can_request_snapshot_without_deadlock(make_manager: ScanFactory) -> None:
    release = threading.Event()
    callback_finished = threading.Event()
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: release.wait(5))
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)
    task = manager.queues["primary"].active_task
    assert task is not None

    def read_snapshot(_future: Future[ScanOutcome]) -> None:
        manager.get_snapshot()
        callback_finished.set()

    task.future.add_done_callback(read_snapshot)
    release.set()
    assert callback_finished.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)


def test_removing_restrictive_lock_dispatches_work_under_remaining_permissive_lock(
    make_manager: ScanFactory,
) -> None:
    scan = FakeScan("rpc", mock.MagicMock())
    scan.is_scan = False
    scan.scan_info.scan_type = None
    scan.scan_info.scan_id = None
    manager = make_manager({"rpc": scan})
    restrictive = messages.ScanQueueLock(
        reason="hold", identifier="restrictive", allow_device_instructions=False
    )
    permissive = messages.ScanQueueLock(
        reason="hold", identifier="permissive", allow_device_instructions=True
    )
    manager.add_queue_lock("primary", restrictive)
    manager.add_queue_lock("primary", permissive)
    manager.add_to_queue("primary", scan_message("rpc"))
    assert not scan.started.is_set()
    manager.remove_queue_lock("primary", restrictive)
    assert scan.started.wait(5)
    assert manager.export_queue()["primary"].status == "LOCKED"


def test_locked_named_queue_survives_idle_expiry(
    monkeypatch: pytest.MonkeyPatch, make_manager: ScanFactory
) -> None:
    monkeypatch.setattr(ScanQueue, "AUTO_SHUTDOWN_TIME", 0)
    manager = make_manager({})
    lock = messages.ScanQueueLock(
        reason="maintenance", identifier="hold", allow_device_instructions=False
    )
    manager.add_queue_lock("secondary", lock)
    manager._coordinator.call(lambda: None)
    assert manager.export_queue()["secondary"].locks == [lock]
    assert manager.queues["secondary"]._idle_expiry is None
    manager.remove_queue_lock("secondary", lock)
    wait_for(lambda: "secondary" not in manager.queues)


def test_abort_then_continue_preserves_terminal_stop(make_manager: ScanFactory) -> None:
    release = threading.Event()
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: release.wait(5))
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)
    manager.set_abort()
    manager.set_continue()
    release.set()
    wait_for(lambda: not manager.queues["primary"].queue)
    assert not scan.finished.is_set()
    assert manager.queues["primary"].history_queue[-1].status == InstructionQueueStatus.STOPPED


def test_shutdown_rejects_requests_without_recreating_queue(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    manager.shutdown()
    with pytest.raises(RuntimeError, match="shutting down"):
        manager.add_to_queue("primary", scan_message("late"))
    manager._scan_queue_callback(cast(Any, SimpleNamespace(value=scan_message("late"))))
    assert not manager.queues
    assert manager.get_snapshot().queues == ()
    assert not manager._coordinator.thread.is_alive()
    cast(mock.Mock, manager.connector.raise_alarm).assert_not_called()


def test_pre_start_abort_skips_scan_exception_hook() -> None:
    parent = mock.MagicMock()
    scan = FakeScan("one", parent)
    control = ScanControl(run_on_exception_hook=True)
    control.stop()
    worker = DirectScanWorker(scan=cast(ScanBase, scan), control=control, on_status=mock.Mock())
    assert worker.run() == "aborted"
    scan.actions._initialize_scan.assert_not_called()
    assert not scan.cleanup_errors
    assert not scan.started.is_set()


def test_paused_queues_do_not_limit_other_queue_execution(make_manager: ScanFactory) -> None:
    paused_count = 20
    scans = {str(index): FakeScan(str(index), mock.MagicMock()) for index in range(paused_count)}
    manager = make_manager(scans)
    # Hold each task at a real control checkpoint while another queue starts.
    entered = [threading.Event() for _ in range(paused_count)]
    release = threading.Event()
    for index, scan in enumerate(scans.values()):

        def core(current: FakeScan, index: int = index) -> None:
            entered[index].set()
            release.wait(5)
            current.actions._interruption_callback()

        scan.core = core
        manager.add_to_queue(str(index), scan_message(str(index), queue=str(index)))
        assert entered[index].wait(5)
        manager.set_pause(queue=str(index))
    release.set()
    runnable = FakeScan("other", mock.MagicMock())
    runnable.redis_connector = manager.connector
    runnable.device_manager = manager.parent.device_manager
    cast(mock.Mock, manager.parent.scan_assembler.assemble_scan).side_effect = (
        lambda msg, scan_id: (
            runnable if msg.metadata["RID"] == "other" else scans[msg.metadata["RID"]]
        )
    )
    manager.add_to_queue("other", scan_message("other", queue="other"))
    assert runnable.started.wait(5)
    for index in range(paused_count):
        manager.set_continue(queue=str(index))


def test_snapshot_preserves_numpy_request_data(make_manager: ScanFactory) -> None:
    import numpy as np

    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    manager.queues["primary"].status = ScanQueueStatus.PAUSED
    request = scan_message("one")
    request.parameter["kwargs"]["positions"] = np.arange(6).reshape(3, 2)
    manager.add_to_queue("primary", request)
    snapshot = manager.get_snapshot()
    exported = snapshot.to_messages()["primary"].info[0].request_blocks[0].msg
    np.testing.assert_array_equal(
        exported.parameter["kwargs"]["positions"], request.parameter["kwargs"]["positions"]
    )
    exported.parameter["kwargs"]["positions"] = exported.parameter["kwargs"]["positions"].copy()
    exported.parameter["kwargs"]["positions"][0, 0] = 100
    fresh = snapshot.to_messages()["primary"].info[0].request_blocks[0].msg
    assert fresh.parameter["kwargs"]["positions"][0, 0] == 0


def test_slow_publication_does_not_block_pause_or_snapshot(make_manager: ScanFactory) -> None:
    release_scan = threading.Event()
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: release_scan.wait(5))
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)
    manager._publisher.call(lambda: None)
    entered = threading.Event()
    release_publisher = threading.Event()

    def block(_endpoint: Any, _message: messages.ScanQueueStatusMessage) -> None:
        entered.set()
        release_publisher.wait(5)

    cast(mock.Mock, manager.connector.set_and_publish).side_effect = block
    manager.send_queue_status()
    assert entered.wait(5)
    try:
        manager.set_pause()
        assert manager.export_queue()["primary"].info[0].status == "PAUSED"
    finally:
        release_publisher.set()
        release_scan.set()
        manager.set_continue()


def test_rejected_request_alarm_does_not_abort_running_scan(make_manager: ScanFactory) -> None:
    from bec_lib.alarm_handler import Alarms
    from bec_server.scan_server.scan_server import ScanServer

    release = threading.Event()
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: release.wait(5))
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)
    manager.add_to_queue("secondary", scan_message("invalid", queue="secondary"))
    alarm_args = cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs
    alarm = messages.AlarmMessage(
        severity=Alarms.MAJOR, info=alarm_args["info"], metadata=alarm_args["metadata"]
    )
    ScanServer._alarm_callback(
        cast(ScanServer, SimpleNamespace(queue_manager=manager)),
        cast(Any, SimpleNamespace(value=alarm)),
    )
    assert manager.export_queue()["primary"].info[0].status == "RUNNING"
    assert not scan.cleanup_errors
    release.set()


def test_registry_cannot_be_mutated_by_callers(make_manager: ScanFactory) -> None:
    manager = make_manager({})
    registry = manager.queues
    with pytest.raises(TypeError):
        cast(dict[str, ScanQueue], registry)["other"] = manager.queues["primary"]
    manager.add_queue("other")
    assert "other" not in registry


@pytest.mark.parametrize("concurrent_shutdown", [False, True])
def test_future_callback_can_request_shutdown(
    make_manager: ScanFactory, concurrent_shutdown: bool
) -> None:
    """Completion callbacks must not join their own task or contend with its joiner."""
    release = threading.Event()
    callback_done = threading.Event()
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: release.wait(5))
    manager = make_manager({"one": scan})
    manager.add_to_queue("primary", scan_message("one"))
    assert scan.started.wait(5)
    task = manager.queues["primary"].active_task
    assert task is not None

    def shutdown_callback(_future: Future[ScanOutcome]) -> None:
        manager.shutdown()
        callback_done.set()

    task.future.add_done_callback(shutdown_callback)
    caller = threading.Thread(target=manager.shutdown) if concurrent_shutdown else None
    if caller is not None:
        caller.start()
        wait_for(lambda: manager._closing)
    release.set()
    assert callback_done.wait(5)
    if caller is not None:
        caller.join(5)
        assert not caller.is_alive()
    manager.shutdown()
    assert not task.thread.is_alive()
    assert not manager._coordinator.thread.is_alive()
    assert not manager._publisher.thread.is_alive()


def test_counter_io_does_not_block_queue_owner(make_manager: ScanFactory) -> None:
    """Counter reservation can block without blocking snapshots or pause commands."""
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    entered = threading.Event()
    release = threading.Event()
    counter = 0

    def read_counter() -> int:
        entered.set()
        assert release.wait(5)
        return counter

    def write_counter(value: int) -> None:
        nonlocal counter
        counter = value

    # A property descriptor exercises actual blocking I/O on the task thread.
    parent_type = type(manager.parent)
    setattr(
        parent_type,
        "scan_number",
        property(lambda _self: read_counter(), lambda _self, value: write_counter(value)),
    )
    try:
        manager.add_to_queue("primary", scan_message("one"))
        assert entered.wait(5)
        manager.set_pause()
        assert manager.export_queue()["primary"].info[0].status == "PAUSED"
        assert not scan.started.is_set()
    finally:
        release.set()
        manager.set_continue()
        wait_for(lambda: not manager.queues["primary"].queue)
        delattr(parent_type, "scan_number")
    assert scan.scan_info.scan_number == 1


def test_history_io_does_not_block_other_queue_commands(make_manager: ScanFactory) -> None:
    """Slow history publication cannot stall the queue coordinator."""
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    entered = threading.Event()
    release = threading.Event()

    def publish_history(*_args: Any, **_kwargs: Any) -> None:
        entered.set()
        assert release.wait(5)

    cast(mock.Mock, manager.connector.lpush).side_effect = publish_history
    manager.add_to_queue("primary", scan_message("one"))
    assert entered.wait(5)
    try:
        manager.add_queue("other")
        manager.set_pause(queue="other")
        assert "other" in manager.export_queue()
    finally:
        release.set()


def test_late_redis_insert_gets_request_specific_rejection(make_manager: ScanFactory) -> None:
    """Legacy insert ingress must not silently discard requests after closure."""
    manager = make_manager({})
    manager.shutdown()
    manager._scan_queue_callback(cast(Any, SimpleNamespace(value=scan_message("late"))))
    endpoint, response = cast(mock.Mock, manager.connector.send).call_args.args
    assert endpoint == MessageEndpoints.scan_queue_request_response()
    assert response.accepted is False
    assert response.metadata["RID"] == "late"


def test_paused_startup_does_not_hold_counter_lock(make_manager: ScanFactory) -> None:
    """Pausing a task waiting for counter allocation leaves other queues runnable."""
    first = FakeScan("first", mock.MagicMock())
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager._number_lock.acquire()
    try:
        manager.add_to_queue("primary", scan_message("first"))
        task = manager.queues["primary"].active_task
        assert task is not None
        manager.set_pause()
    finally:
        manager._number_lock.release()
    try:
        manager.add_to_queue("other", scan_message("second", queue="other"))
        assert second.started.wait(5)
        assert not first.started.is_set()
    finally:
        manager.set_continue()


def test_invalid_deferred_request_is_rejected_without_stranding_successor(
    make_manager: ScanFactory,
) -> None:
    """Assembly failure during abort must not be accepted or block valid buffered requests."""
    release = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda _scan: release.wait(5))
    second = FakeScan("second", mock.MagicMock())
    manager = make_manager({"first": first, "second": second})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    try:
        manager.set_abort()
        assert manager.add_to_queue("primary", scan_message("invalid")) is False
        assert manager.add_to_queue("primary", scan_message("second")) is True
        alarm = cast(mock.Mock, manager.connector.raise_alarm).call_args.kwargs
        assert alarm["metadata"]["RID"] == "invalid"
        assert alarm["metadata"]["request_rejected"] is True
    finally:
        release.set()
        manager.set_continue()
    assert second.started.wait(5)
    wait_for(lambda: not manager.queues["primary"].queue)
    assert not manager.queues["primary"]._deferred_inserts


def test_publisher_start_failure_joins_queue_owner() -> None:
    """An initialization failure must not leave a non-daemon coordinator thread alive."""
    from bec_server.scan_server.scan_queue import manager as manager_module
    from bec_server.scan_server.scan_queue.coordinator import QueueCoordinator

    owners: list[QueueCoordinator] = []

    def start(thread_name: str = "ScanQueueCoordinator") -> QueueCoordinator:
        if thread_name == "ScanQueuePublisher":
            raise RuntimeError("cannot start publisher")
        owner = QueueCoordinator(thread_name)
        owners.append(owner)
        return owner

    with mock.patch.object(manager_module, "QueueCoordinator", side_effect=start):
        with pytest.raises(RuntimeError, match="cannot start publisher"):
            QueueManager(mock.MagicMock())
    assert len(owners) == 1
    assert not owners[0].thread.is_alive()


def test_pending_snapshots_coalesce_while_publication_is_blocked(make_manager: ScanFactory) -> None:
    """A request burst retains only the latest ordinary snapshot during a Redis stall."""
    scans = {str(index): FakeScan(str(index), mock.MagicMock()) for index in range(40)}
    manager = make_manager(scans)
    manager.queues["primary"].status = ScanQueueStatus.PAUSED
    manager._publisher.call(lambda: None)
    entered = threading.Event()
    release = threading.Event()

    def block() -> None:
        entered.set()
        assert release.wait(5)

    manager._publisher.post(block)
    assert entered.wait(5)
    try:
        for rid in scans:
            assert manager.add_to_queue("primary", scan_message(rid)) is True
        assert manager._publisher._events.qsize() == 1
        assert len(manager.export_queue()["primary"].info) == 40
    finally:
        release.set()
    manager._publisher.call(lambda: None)
    endpoint, status = cast(mock.Mock, manager.connector.set_and_publish).call_args.args
    assert endpoint == MessageEndpoints.scan_queue_status()
    assert len(status.queue["primary"].info) == 40


def test_external_scan_counter_changes_refresh_pending_estimates(make_manager: ScanFactory) -> None:
    """External counter changes eventually update both exported and published estimates."""
    scan = FakeScan("one", mock.MagicMock())
    manager = make_manager({"one": scan})
    manager.set_number_baseline(0)
    manager.queues["primary"].status = ScanQueueStatus.PAUSED
    manager.add_to_queue("primary", scan_message("one"))
    manager.parent.scan_number = 40
    wait_for(lambda: manager.export_queue()["primary"].info[0].scan_number == [41])
    manager._publisher.call(lambda: None)
    status = cast(mock.Mock, manager.connector.set_and_publish).call_args.args[1]
    assert status.queue["primary"].info[0].scan_number == [41]


def test_counter_refresh_does_not_overwrite_newer_task_reservation(
    make_manager: ScanFactory,
) -> None:
    """A stale observer read must not undo a scan number already assigned by a task."""
    release = threading.Event()
    scan = FakeScan("one", mock.MagicMock(), lambda _scan: release.wait(5))
    manager = make_manager({"one": scan})
    revision = manager._coordinator.call(lambda: manager._number_revision)
    try:
        manager.add_to_queue("primary", scan_message("one"))
        assert scan.started.wait(5)
        manager._coordinator.call(manager._apply_counter_refresh, revision, 0)
        assert manager._coordinator.call(lambda: manager._last_scan_number) == 1
    finally:
        release.set()


def test_stopped_scan_does_not_disable_coalescing_for_other_queue(
    make_manager: ScanFactory,
) -> None:
    """An already announced stop must not turn unrelated updates into terminal barriers."""
    release_scan = threading.Event()
    first = FakeScan("first", mock.MagicMock(), lambda _scan: release_scan.wait(5))
    pending = {str(index): FakeScan(str(index), mock.MagicMock()) for index in range(40)}
    manager = make_manager({"first": first, **pending})
    manager.add_to_queue("primary", scan_message("first"))
    assert first.started.wait(5)
    manager.add_queue("other")
    manager.queues["other"].status = ScanQueueStatus.PAUSED
    manager._publisher.call(lambda: None)
    entered = threading.Event()
    release_publisher = threading.Event()

    def block() -> None:
        entered.set()
        assert release_publisher.wait(5)

    manager._publisher.post(block)
    assert entered.wait(5)
    try:
        manager.set_abort()
        for rid in pending:
            assert manager.add_to_queue("other", scan_message(rid, queue="other"))
        assert manager._publisher._events.qsize() <= 3
    finally:
        release_publisher.set()
        release_scan.set()


@pytest.mark.parametrize("state_variant", ["pending", "mixed", "unassigned_running", "reordered"])
def test_bulk_snapshot_numbers_match_item_descriptions(
    make_manager: ScanFactory, state_variant: str
) -> None:
    """The linear snapshot path preserves serialized output for queue numbering edge cases."""
    from bec_lib.serialization import msgpack
    from bec_server.scan_server.scan_queue import DirectInstructionQueueItem

    scans = {str(index): FakeScan(str(index), mock.MagicMock()) for index in range(12)}
    manager = make_manager(scans)
    queue = manager.queues["primary"]
    queue.status = ScanQueueStatus.PAUSED
    for rid in scans:
        manager.add_to_queue("primary", scan_message(rid))

    def compare() -> None:
        manager._last_scan_number = 40
        for index, item in enumerate(queue.queue):
            scan = item.scans[0]
            if index % 4 == 0:
                scan.is_scan = False
                scan.scan_info.scan_type = None
                scan.scan_info.scan_id = None
            if state_variant == "mixed":
                item._status = (
                    InstructionQueueStatus.PENDING,
                    InstructionQueueStatus.RUNNING,
                    InstructionQueueStatus.PAUSED,
                    InstructionQueueStatus.COMPLETED,
                )[index % 4]
                if index % 3 == 1:
                    scan.scan_info.scan_number = 100 + index
            if state_variant == "unassigned_running" and index % 3 == 1:
                item._status = InstructionQueueStatus.RUNNING
        if state_variant == "reordered":
            queue.queue.rotate(3)
        original = [item.describe() for item in queue.queue]
        with mock.patch.object(
            DirectInstructionQueueItem,
            "scan_ids_head",
            side_effect=AssertionError("quadratic path used"),
        ):
            optimized = manager._describe_queue_items(queue)
        assert msgpack.dumps([item.model_dump() for item in optimized]) == msgpack.dumps(
            [item.model_dump() for item in original]
        )

    manager._coordinator.call(compare)
