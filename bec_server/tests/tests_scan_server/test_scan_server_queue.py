"""Channel queue integration tests with real owner, preparation, I/O and worker threads."""

from __future__ import annotations

import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.scan_server.device_lock_registry import DeviceLockRegistry
from bec_server.scan_server.queue_channels import ChannelClosed, InstructionQueueStatus, ScanReport
from bec_server.scan_server.scan_queue import QueueManager, ScanQueue


def eventually(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    assert predicate(), "Condition was not reached before timeout"


class Scenario:
    def __init__(self, *, block=False, cleanup=False, prepare=False, is_scan=True, fail=False):
        self.started = threading.Event()
        self.cleaned = threading.Event()
        self.preparing = threading.Event()
        self.release = threading.Event()
        self.cleanup_release = threading.Event()
        self.prepare_release = threading.Event()
        self.finished = threading.Event()
        self.block, self.cleanup, self.prepare, self.is_scan, self.fail = (
            block,
            cleanup,
            prepare,
            is_scan,
            fail,
        )
        self.scans = []


class FakeScan:
    """Direct lifecycle whose waits are controllable without Redis or hardware."""

    def __init__(self, parent, msg, scan_id, scenario):
        self.is_scan = scenario.is_scan
        self.scan_info = SimpleNamespace(
            metadata=dict(msg.metadata),
            scan_id=scan_id if self.is_scan else None,
            scan_number=None,
            dataset_number=None,
            scan_queue=msg.queue,
            readout_priority_modification={},
            scan_report_instructions=[],
            run_on_exception_hook=True,
        )
        self.actions = mock.Mock()
        self.actions.get_owned_device_locks.side_effect = (
            lambda: parent.device_lock_registry.get_owned_devices(msg.metadata["RID"])
        )
        self.actions.get_pending_device_locks.side_effect = (
            lambda: parent.device_lock_registry.get_pending_devices(msg.metadata["RID"])
        )
        self.scenario = scenario
        scenario.scans.append(self)

    def prepare_scan(self):
        pass

    open_scan = stage = pre_scan = post_scan = unstage = close_scan = prepare_scan

    def scan_core(self):
        self.scenario.started.set()
        while self.scenario.block and not self.scenario.release.wait(0.01):
            self.actions._interruption_callback()
        self.actions._interruption_callback()
        if self.scenario.fail:
            raise RuntimeError("scan failed")
        self.scenario.finished.set()

    def on_exception(self, exc):
        self.scenario.cleaned.set()
        while self.scenario.cleanup and not self.scenario.cleanup_release.wait(0.01):
            self.actions._interruption_callback()


@pytest.fixture
def harness():
    scenarios = {}
    parent = SimpleNamespace(
        connector=mock.MagicMock(),
        device_manager=mock.MagicMock(),
        scan_number=0,
        dataset_number=0,
        device_lock_registry=DeviceLockRegistry(),
    )
    parent.device_manager._rpc_method.side_effect = lambda _: nullcontext()

    def assemble(msg, scan_id):
        scenario = scenarios[msg.scan_type]
        scenario.preparing.set()
        if scenario.prepare:
            assert scenario.prepare_release.wait(5), "test did not unblock constructor"
        return FakeScan(parent, msg, scan_id, scenario)

    parent.scan_assembler = SimpleNamespace(
        is_direct_scan_message=lambda msg: True,
        assemble_direct_scan=assemble,
        scan_manager=SimpleNamespace(scan_dict={}),
    )
    manager = QueueManager(parent)
    reports = []
    report = manager.worker_report
    manager.worker_report = lambda value: (reports.append(value), report(value))[-1]

    def add(name, scenario=None, queue="primary", rid=None, **metadata):
        scenario = scenario or Scenario()
        scenarios[name] = scenario
        parent.scan_assembler.scan_manager.scan_dict[name] = SimpleNamespace(
            is_scan=scenario.is_scan
        )
        msg = messages.ScanQueueMessage(
            scan_type=name,
            parameter={"args": {}, "kwargs": {}},
            queue=queue,
            metadata={"RID": rid or name, **metadata},
        )
        manager.add_to_queue(queue, msg)
        return scenario

    context = SimpleNamespace(
        parent=parent, manager=manager, add=add, reports=reports, scenarios=scenarios
    )
    yield context
    for scenario in scenarios.values():
        scenario.release.set()
        scenario.cleanup_release.set()
        scenario.prepare_release.set()
    manager.shutdown(timeout=5)


def entries(harness, queue="primary"):
    return harness.manager.export_queue()[queue]["info"]


def histories(harness):
    return [
        call.args[1]
        for call in harness.parent.connector.lpush.call_args_list
        if call.args[0] == MessageEndpoints.scan_queue_history()
    ]


def lock(harness, queue="primary", allow=False):
    harness.manager.add_queue_lock(
        queue,
        messages.ScanQueueLock(identifier="hold", reason="test", allow_device_instructions=allow),
    )


def unlock(harness, queue="primary"):
    harness.manager.remove_queue_lock(
        queue, messages.ScanQueueLock(identifier="hold", reason="test")
    )


def test_each_direct_request_has_its_own_lifecycle_even_in_group(harness):
    lock(harness)
    first = harness.add("first", queue_group="group")
    second = harness.add("second", queue_group="group")
    assert len(entries(harness)) == 2
    unlock(harness)
    assert first.finished.wait(2)
    assert second.finished.wait(2)
    eventually(lambda: not entries(harness))
    harness.manager.flush()
    assert [item.status for item in histories(harness)] == ["COMPLETED", "COMPLETED"]


def test_snapshots_are_copies(harness):
    lock(harness)
    harness.add("scan")
    snapshot = harness.manager.export_queue()
    snapshot["primary"]["info"][0].request_blocks[0].msg.metadata["RID"] = "changed"
    snapshot["primary"]["locks"].clear()
    assert entries(harness)[0].request_blocks[0].RID == "scan"
    assert harness.manager.export_queue()["primary"]["locks"]


def test_pause_then_lock_then_continue_does_not_dispatch(harness):
    first = harness.add("first", Scenario(block=True))
    assert first.started.wait(2)
    second = harness.add("second")
    harness.manager.set_deferred_pause()
    lock(harness)
    first.release.set()
    eventually(lambda: len(entries(harness)) == 1)
    harness.manager.set_continue()
    assert harness.manager.export_queue()["primary"]["status"] == "LOCKED"
    assert not second.started.is_set()
    unlock(harness)
    assert harness.manager.export_queue()["primary"]["status"] == "PAUSED"
    harness.manager.set_continue()
    assert second.started.wait(2)


def test_permitted_device_instruction_bypasses_lock_but_scan_does_not(harness):
    lock(harness, allow=True)
    move = harness.add("move", Scenario(is_scan=False))
    assert move.finished.wait(2)
    scan = harness.add("scan")
    assert not scan.started.is_set()
    assert harness.parent.scan_number == 0
    unlock(harness)
    assert scan.started.wait(2)


def test_clear_retains_cleanup_identity_and_defers_new_work(harness):
    first = harness.add("first", Scenario(block=True, cleanup=True))
    assert first.started.wait(2)
    harness.manager.set_clear()
    assert first.cleaned.wait(2)
    assert entries(harness) == []
    replacement = harness.add("replacement")
    assert not replacement.started.is_set()
    first.cleanup_release.set()
    assert replacement.started.wait(2)
    eventually(lambda: not entries(harness))
    harness.manager.flush()
    assert [(item.info.request_blocks[0].RID, item.status) for item in histories(harness)] == [
        ("first", "STOPPED"),
        ("replacement", "COMPLETED"),
    ]


def test_clear_discards_deferred_insertions(harness):
    first = harness.add("first", Scenario(block=True, cleanup=True))
    assert first.started.wait(2)
    harness.manager.set_abort()
    assert first.cleaned.wait(2)
    deferred = harness.add("deferred")
    harness.manager.set_clear()
    first.cleanup_release.set()
    eventually(lambda: not entries(harness))
    harness.manager.flush()
    assert not deferred.preparing.is_set()
    assert not deferred.started.is_set()


def test_reordering_does_not_change_active_completion_target(harness):
    first = harness.add("first", Scenario(block=True))
    assert first.started.wait(2)
    second = harness.add("second")
    harness.manager.set_deferred_pause()
    first_id = entries(harness)[0].scan_id[0]
    harness.manager._handle_scan_order_change(
        messages.ScanQueueOrderMessage(scan_id=first_id, action="move_bottom", queue="primary")
    )
    assert entries(harness)[1].active_request_block.scan_id == first_id
    first.release.set()
    eventually(lambda: len(entries(harness)) == 1)
    assert entries(harness)[0].request_blocks[0].RID == "second"
    assert not second.started.is_set()
    harness.manager.set_continue()
    assert second.started.wait(2)


def test_duplicate_and_old_generation_reports_are_ignored(harness):
    first = harness.add("first", Scenario(block=True), queue="secondary")
    assert first.started.wait(2)
    report = harness.reports[-1]
    harness.manager.remove_queue("secondary")
    replacement = harness.add("replacement", Scenario(block=True), queue="secondary")
    assert replacement.started.wait(2)
    harness.manager.worker_report(ScanReport(report.token, report.request, terminal=True))
    assert entries(harness, "secondary")[0].request_blocks[0].RID == "replacement"
    assert len([h for h in histories(harness) if h.info.request_blocks[0].RID == "first"]) == 1


def test_slow_constructor_does_not_block_owner_or_other_workers(harness):
    first = harness.add("first", Scenario(block=True))
    assert first.started.wait(2)
    slow = Scenario(prepare=True)
    errors = []

    def insert():
        try:
            harness.add("slow", slow)
        except ChannelClosed as exc:
            errors.append(exc)

    thread = threading.Thread(target=insert)
    thread.start()
    try:
        assert slow.preparing.wait(2)
        harness.manager.set_halt(request_id="first")
        assert first.scans[0]._shutdown_event.wait(1)
        harness.manager.set_clear()
        assert entries(harness) == []
    finally:
        slow.prepare_release.set()
        thread.join(3)
    assert not thread.is_alive()
    assert errors


def test_restart_preparation_can_outlive_original_and_still_run(harness):
    first = harness.add("scan", Scenario(block=True), queue_group="group")
    assert first.started.wait(2)
    replacement = Scenario(prepare=True)
    harness.scenarios["scan"] = replacement
    harness.manager.set_restart()
    assert replacement.preparing.wait(2)
    first.release.set()
    eventually(lambda: not any(e.request_blocks[0].RID == "scan" for e in entries(harness)))
    replacement.prepare_release.set()
    assert replacement.started.wait(2)
    eventually(lambda: not entries(harness))
    harness.manager.flush()
    assert len(histories(harness)) == 2


def test_failure_runs_cleanup_before_alarm_and_records_terminal(harness):
    scan = harness.add("bad", Scenario(fail=True, cleanup=True))
    assert scan.cleaned.wait(2)
    harness.parent.connector.raise_alarm.assert_not_called()
    scan.cleanup_release.set()
    eventually(lambda: not entries(harness))
    harness.manager.flush()
    assert histories(harness)[0].status == "STOPPED"
    assert harness.parent.connector.raise_alarm.call_args.kwargs["metadata"]["queue_id"]


def test_scan_numbers_are_serial_across_named_queues(harness):
    one = harness.add("one", Scenario(block=True), queue="one")
    two = harness.add("two", Scenario(block=True), queue="two")
    assert one.started.wait(2) and two.started.wait(2)
    assert {one.scans[0].scan_info.scan_number, two.scans[0].scan_info.scan_number} == {1, 2}
    assert harness.parent.scan_number == 2
    one.release.set()
    two.release.set()
    eventually(lambda: not entries(harness, "one") and not entries(harness, "two"))
    harness.manager.flush()
    assert {history.queue for history in histories(harness)} == {"one", "two"}


def test_slow_publication_does_not_block_local_cancellation(harness):
    scan = harness.add("scan", Scenario(block=True))
    assert scan.started.wait(2)
    blocked, release = threading.Event(), threading.Event()

    def publish(*args, **kwargs):
        blocked.set()
        assert release.wait(3)

    harness.parent.connector.set_and_publish.side_effect = publish
    try:
        harness.manager.send_queue_status()
        assert blocked.wait(2)
        harness.manager.set_abort()
        assert scan.scans[0]._shutdown_event.wait(0.5)
        assert harness.manager.export_queue()["primary"]["status"] == "PAUSED"
    finally:
        release.set()


def test_cancelled_snapshot_survives_coalescing(harness):
    lock(harness)
    harness.add("pending")
    harness.manager.flush()
    blocked, release = threading.Event(), threading.Event()
    captured = []

    def publish(endpoint, msg):
        blocked.set()
        assert release.wait(3)
        captured.append(msg)

    harness.parent.connector.set_and_publish.side_effect = publish
    try:
        harness.manager.send_queue_status()
        assert blocked.wait(2)
        harness.manager.set_abort(request_id="pending")
    finally:
        release.set()
    harness.manager.flush()
    assert any(
        entry.status == "CANCELLED" for msg in captured for entry in msg.queue["primary"].info
    )


def test_shutdown_drains_number_allocation_continuations(harness):
    blocked, release = threading.Event(), threading.Event()
    original = harness.manager._allocate_numbers

    def allocate(*args):
        blocked.set()
        assert release.wait(3)
        return original(*args)

    harness.manager._allocate_numbers = allocate
    scan = harness.add("scan")
    assert blocked.wait(2)
    errors = []

    def shutdown():
        try:
            harness.manager.shutdown(timeout=3)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=shutdown)
    thread.start()
    eventually(lambda: harness.manager._closing)
    release.set()
    thread.join(4)
    assert not thread.is_alive() and not errors
    assert not scan.started.is_set()
    assert len(histories(harness)) == 1
    assert not any(queue.active for queue in harness.manager._retired.values())


def test_shutdown_interrupts_cleanup_without_resetting_events(harness):
    scan = harness.add("scan", Scenario(block=True, cleanup=True))
    assert scan.started.wait(2)
    harness.manager.set_abort()
    assert scan.cleaned.wait(2)
    cleanup_event = scan.scans[0]._shutdown_event
    harness.manager.shutdown(timeout=2)
    assert cleanup_event.is_set()
    assert not harness.manager._owner.is_alive()
    assert not any(worker.is_alive() for worker in harness.manager._workers)


def test_secondary_idle_expiry_and_recreation(harness, monkeypatch):
    monkeypatch.setattr(ScanQueue, "AUTO_SHUTDOWN_TIME", 0.05)
    harness.manager.add_queue("idle")
    eventually(lambda: "idle" not in harness.manager.export_queue())
    scan = harness.add("scan", queue="idle")
    assert scan.finished.wait(2)


def test_capacity_rejection_does_not_block_controls(harness, monkeypatch):
    monkeypatch.setattr(QueueManager, "MAX_PENDING_REQUESTS", 1)
    lock(harness)
    harness.add("accepted")
    with pytest.raises(RuntimeError, match="capacity"):
        harness.add("rejected")
    harness.manager.set_abort(request_id="accepted")
    assert entries(harness) == []


def test_constructor_failure_resolves_caller_and_preserves_queue(harness):
    lock(harness)
    original = harness.parent.scan_assembler.assemble_direct_scan
    harness.parent.scan_assembler.assemble_direct_scan = mock.Mock(
        side_effect=ValueError("bad input")
    )
    with pytest.raises(ValueError, match="bad input"):
        harness.add("bad")
    assert entries(harness) == []
    harness.parent.scan_assembler.assemble_direct_scan = original
    scan = harness.add("good")
    unlock(harness)
    assert scan.finished.wait(2)


def test_dataset_hold_keeps_dataset_and_advances_scan_number(harness):
    scan = harness.add("scan", Scenario(block=True), dataset_id_on_hold=True)
    assert scan.started.wait(2)
    assert scan.scans[0].scan_info.scan_number == 1
    assert scan.scans[0].scan_info.dataset_number == 0


def test_stale_alarm_cannot_abort_replacement_with_reused_rid(harness):
    scan = harness.add("scan", Scenario(block=True))
    assert scan.started.wait(2)
    old = entries(harness)[0]
    harness.manager.set_restart()
    eventually(lambda: len(scan.scans) == 2 and scan.scans[1].actions._initialize_scan.called)
    replacement = entries(harness)[0]
    harness.manager.set_abort(
        scan_id=old.scan_id[0], request_id="scan", parameter={"queue_id": old.queue_id}
    )
    assert entries(harness)[0].queue_id == replacement.queue_id
    assert entries(harness)[0].status == "RUNNING"


def test_cancelled_queued_preparation_never_constructs(harness):
    slow = Scenario(prepare=True)
    errors = []

    def add_slow():
        try:
            harness.add("slow", slow)
        except ChannelClosed as exc:
            errors.append(exc)

    slow_thread = threading.Thread(target=add_slow)
    slow_thread.start()
    assert slow.preparing.wait(2)
    queued = Scenario()
    # Use the callback ingress, which must not wait for construction.
    harness.scenarios["queued"] = queued
    harness.parent.scan_assembler.scan_manager.scan_dict["queued"] = SimpleNamespace(is_scan=True)
    msg = messages.ScanQueueMessage(
        scan_type="queued", parameter={}, queue="primary", metadata={"RID": "queued"}
    )
    harness.manager._scan_queue_callback(SimpleNamespace(value=msg))
    try:
        harness.manager.set_clear()
    finally:
        slow.prepare_release.set()
        slow_thread.join(2)
    harness.manager.shutdown(timeout=2)
    assert not queued.preparing.is_set()
    assert errors


def test_accepted_deferred_work_waits_for_preparation_credit(harness, monkeypatch):
    monkeypatch.setattr(QueueManager, "MAX_PENDING_REQUESTS", 4)
    first = harness.add("first", Scenario(block=True, cleanup=True))
    assert first.started.wait(2)
    harness.manager.set_abort()
    assert first.cleaned.wait(2)
    deferred = harness.add("deferred")
    slow = Scenario(prepare=True)
    thread = threading.Thread(target=lambda: harness.add("slow", slow, queue="secondary"))
    thread.start()
    assert slow.preparing.wait(2)
    try:
        for index in range(3):
            name = f"cancelled-{index}"
            harness.scenarios[name] = Scenario()
            harness.parent.scan_assembler.scan_manager.scan_dict[name] = SimpleNamespace(
                is_scan=True
            )
            message = messages.ScanQueueMessage(
                scan_type=name, parameter={}, queue="secondary", metadata={"RID": name}
            )
            harness.manager._scan_queue_callback(SimpleNamespace(value=message))
            harness.manager.set_abort(request_id=name, queue="secondary")
        first.cleanup_release.set()
        eventually(lambda: not entries(harness))
        assert not deferred.preparing.is_set()
        assert harness.manager._workers[0].is_alive()
    finally:
        slow.prepare_release.set()
        thread.join(2)
    assert deferred.finished.wait(2)
    harness.manager.flush()
    assert any(history.info.request_blocks[0].RID == "deferred" for history in histories(harness))


def test_unexpected_failure_holds_next_request(harness):
    first = harness.add("failure", Scenario(block=True, fail=True))
    assert first.started.wait(2)
    next_scan = harness.add("next")
    first.release.set()
    eventually(lambda: len(entries(harness)) == 1)
    assert harness.manager.export_queue()["primary"]["status"] == "PAUSED"
    assert not next_scan.started.is_set()
    harness.manager.flush()
    assert histories(harness)[0].info.reason == "alarm"
    harness.manager.set_continue()
    assert next_scan.finished.wait(2)


def test_removed_name_cannot_start_replacement_while_old_execution_is_alive(harness):
    blocked, release = threading.Event(), threading.Event()

    def uncooperative_scan(scan):
        blocked.set()
        assert release.wait(3)

    with mock.patch.object(FakeScan, "scan_core", uncooperative_scan):
        harness.add("old", queue="secondary")
        assert blocked.wait(2)
        remover = threading.Thread(target=lambda: harness.manager.remove_queue("secondary"))
        remover.start()
        try:
            eventually(lambda: "secondary" not in harness.manager.export_queue())
            with pytest.raises(RuntimeError, match="still shutting down"):
                harness.add("replacement", queue="secondary")
        finally:
            release.set()
            remover.join(3)
    assert not remover.is_alive()
    replacement = harness.add("replacement", queue="secondary")
    assert replacement.finished.wait(2)
