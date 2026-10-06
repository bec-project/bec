"""Public queue controls against real services and simulated hardware."""

from __future__ import annotations

import time
import uuid

import numpy as np
import pytest
from IPython.core.interactiveshell import InteractiveShell
from traitlets.config import Config

from bec_ipython_client.bec_magics import BECMagics
from bec_lib import messages
from bec_lib.alarm_handler import AlarmBase
from bec_lib.bec_errors import ScanAbortion
from bec_lib.endpoints import MessageEndpoints

pytestmark = pytest.mark.timeout(90)
GATE = "queue_control_gate"
TERMINAL = {"COMPLETED", "STOPPED", "CANCELLED"}


def wait_for(predicate, timeout=15):
    """Wait for an observed transition, failing instead of silently timing out."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("Queue control transition was not observed")
        time.sleep(0.02)


@pytest.fixture
def controls(bec_ipython_client_fixture):
    bec = bec_ipython_client_fixture
    original_queues = set(bec.queue.queue_storage.current_scan_queue)
    original_locks = {lock.identifier for lock in queue(bec).locks}
    bec.device_manager.config_helper.send_config_request(
        action="add",
        config={
            GATE: {
                "deviceClass": "pytest_bec_e2e.queue_control_gate.QueueControlGate",
                "deviceConfig": {},
                "readoutPriority": "monitored",
                "softwareTrigger": True,
                "enabled": True,
                "readOnly": False,
            }
        },
    )
    shell = InteractiveShell(config=Config({"HistoryManager": {"enabled": False}}))
    shell.register_magics(BECMagics(shell, bec))
    try:
        yield bec, shell
    finally:
        bec.queue.set_default_scan_queue("primary")
        owned_queues = {"primary"} | (
            set(bec.queue.queue_storage.current_scan_queue) - original_queues
        )
        # Retire work while locks still forbid dispatch; removing them first can start successors.
        for name in owned_queues:
            if name in bec.queue.queue_storage.current_scan_queue:
                reset_and_retire(bec, name)
        for name in owned_queues:
            if name not in bec.queue.queue_storage.current_scan_queue:
                continue
            retained = original_locks if name == "primary" else set()
            for lock in queue(bec, name).locks:
                if lock.identifier not in retained:
                    bec.queue.remove_queue_lock(name, lock.identifier)
            wait_for(lambda: all(lock.identifier in retained for lock in queue(bec, name).locks))
        bec.device_manager.config_helper.send_config_request(action="remove", config={GATE: {}})


def queue(bec, name="primary"):
    return bec.queue.queue_storage.current_scan_queue[name]


def submit(bec, *, steps=5, exposure=0.02, relative=False, queue_name="primary"):
    return bec.scans.line_scan(
        bec.device_manager.devices.samx,
        2,
        12,
        steps=steps,
        exp_time=exposure,
        relative=relative,
        hide_report=True,
        scan_queue=queue_name,
    )


def wait_started(report):
    def held():
        assert report.status not in TERMINAL, "Scan retired before the control could be exercised"
        return (
            report.status == "RUNNING"
            and report.scan is not None
            and len(report.scan.live_data) >= 2
            and report._client.device_manager.devices[GATE].waiting.get()
        )

    wait_for(held)


def hold_scan(bec, **kwargs):
    """Submit a real line scan that cannot finish before its control is exercised."""
    bec.device_manager.devices[GATE].arm()
    return submit(bec, **kwargs)


def release_scan(bec):
    """Release only the test latch, bypassing a queue that deliberately holds dispatch."""
    bec.connector.send(
        MessageEndpoints.device_instructions(),
        messages.DeviceInstructionMessage(
            device=GATE,
            action="rpc",
            parameter={
                "device": GATE,
                "rpc_id": str(uuid.uuid4()),
                "func": "release",
                "args": [],
                "kwargs": {},
            },
            metadata={"device_instr_id": str(uuid.uuid4())},
        ),
    )


def reset_and_retire(bec, name="primary"):
    """Clear under a dispatch lock and await task retirement, not just an empty snapshot."""
    identifier = str(uuid.uuid4())
    bec.queue.add_queue_lock(name, "test cleanup", identifier, False)
    wait_for(lambda: any(lock.identifier == identifier for lock in queue(bec, name).locks))
    items = []
    for entry in queue(bec, name).info:
        wait_for(lambda: bec.queue.queue_storage.find_queue_item_by_ID(entry.queue_id) is not None)
        items.append(bec.queue.queue_storage.find_queue_item_by_ID(entry.queue_id))
    original = bec.queue.get_default_scan_queue()
    try:
        bec.queue.set_default_scan_queue(name)
        bec.queue.request_queue_reset()
        wait_for(lambda: not queue(bec, name).info and queue(bec, name).status == "LOCKED")
        wait_for(lambda: all(item.status in TERMINAL for item in items))
    finally:
        bec.queue.set_default_scan_queue(original)
        bec.queue.remove_queue_lock(name, identifier)
        wait_for(lambda: all(lock.identifier != identifier for lock in queue(bec, name).locks))


@pytest.mark.parametrize("command, cleanup", [("abort", True), ("halt", False)])
def test_stop_and_halt_preserve_pending_successor(controls, command, cleanup):
    bec, shell = controls
    bec.scans.umv(bec.device_manager.devices.samx, 0, relative=False)
    active = hold_scan(bec, steps=30, exposure=0.1, relative=True)
    wait_started(active)
    pending = submit(bec)
    wait_for(lambda: len(queue(bec).info) == 2)
    pending_id = pending.queue_item.queue_id
    shell.run_line_magic(command, "")
    wait_for(lambda: active.status == "STOPPED" and len(queue(bec).info) == 1)
    assert queue(bec).status == "PAUSED"
    assert queue(bec).info[0].queue_id == pending_id
    assert pending.status == "PENDING"
    position = bec.device_manager.devices.samx.readback.get()
    assert np.isclose(position, 0, atol=0.1) == cleanup
    assert not bec.device_manager.devices.samx.motor_is_moving.get()
    shell.run_line_magic("resume", "")
    pending.wait(timeout=15)
    assert pending.status == "COMPLETED"


def test_deferred_pause_finishes_active_and_holds_multiple_successors(controls):
    bec, shell = controls
    active = hold_scan(bec, steps=20, exposure=0.05)
    wait_started(active)
    successors = [submit(bec), submit(bec)]
    wait_for(lambda: len(queue(bec).info) == 3)
    active_id = active.scan.scan_id
    shell.run_line_magic("deferred_pause", "")
    wait_for(lambda: queue(bec).status == "PAUSED")
    release_scan(bec)
    active.wait(timeout=15, num_points=True)
    wait_for(lambda: len(queue(bec).info) == 2)
    assert active.scan.scan_id == active_id
    assert len(active.scan.live_data) == 20
    assert all(report.status == "PENDING" for report in successors)
    shell.run_line_magic("resume", "")
    for report in successors:
        report.wait(timeout=15)


@pytest.mark.parametrize("allow_devices", [False, True])
def test_multiple_locks_do_not_erase_queue_pause(controls, allow_devices):
    bec, shell = controls
    identifiers = [str(uuid.uuid4()), str(uuid.uuid4())]
    for identifier in identifiers:
        bec.queue.add_queue_lock("primary", "e2e lock", identifier, allow_devices)
    wait_for(lambda: len(queue(bec).locks) == 2)
    reports = [submit(bec) for _ in range(3)]
    wait_for(lambda: len(queue(bec).info) == 3)
    shell.run_line_magic("deferred_pause", "")
    # Removing the first lock confirms processing of the preceding pause command.
    bec.queue.remove_queue_lock("primary", identifiers[0])
    wait_for(lambda: len(queue(bec).locks) == 1)
    assert all(report.status == "PENDING" for report in reports)
    bec.queue.remove_queue_lock("primary", identifiers[1])
    wait_for(lambda: not queue(bec).locks)
    assert queue(bec).status == "PAUSED"
    assert all(report.status == "PENDING" for report in reports)
    shell.run_line_magic("resume", "")
    for report in reports:
        report.wait(timeout=15)


@pytest.mark.parametrize(
    "action, index, position, expected",
    [
        ("move_up", 2, None, [0, 2, 1]),
        ("move_down", 0, None, [1, 0, 2]),
        ("move_top", 2, None, [2, 0, 1]),
        ("move_bottom", 0, None, [1, 2, 0]),
        ("move_to", 0, 1, [1, 0, 2]),
        ("move_up", 0, None, [0, 1, 2]),
        ("move_down", 2, None, [0, 1, 2]),
        ("move_to", 2, -99, [2, 0, 1]),
        ("move_to", 0, 99, [1, 2, 0]),
    ],
)
def test_reorder_multi_element_queue(controls, action, index, position, expected):
    bec, shell = controls
    shell.run_line_magic("deferred_pause", "")
    wait_for(lambda: queue(bec).status == "PAUSED")
    reports = [submit(bec) for _ in range(3)]
    wait_for(lambda: len(queue(bec).info) == 3)
    ids = [report.queue_item.scan_ids[0] for report in reports]
    response = bec.queue.request_queue_order_modification(
        ids[index], action, position=position, wait_for_response=True
    )
    assert response.accepted
    ordered = [ids[item] for item in expected]
    wait_for(lambda: [entry.scan_id[0] for entry in queue(bec).info] == ordered)
    assert all(report.status == "PENDING" for report in reports)
    shell.run_line_magic("resume", "")
    for report in reports:
        report.wait(timeout=15)
    assert sorted(reports, key=lambda report: report.scan.scan_number) == [
        reports[item] for item in expected
    ]


def test_restart_magic_keeps_multiple_pending_items(controls):
    bec, shell = controls
    active = hold_scan(bec, steps=20, exposure=0.05)
    wait_started(active)
    successors = [submit(bec), submit(bec)]
    wait_for(lambda: len(queue(bec).info) == 3)
    successor_ids = [report.queue_item.queue_id for report in successors]
    replacement_rid = shell.run_line_magic("restart", "")
    wait_for(lambda: active.status == "STOPPED")
    for report in successors:
        report.wait(timeout=20)
    wait_for(lambda: not queue(bec).info)
    replacement = bec.queue.request_storage.find_request_by_ID(replacement_rid)
    assert replacement is not None
    assert replacement.queue.status == "COMPLETED"
    assert replacement.queue.queue_id not in successor_ids
    assert all(report.status == "COMPLETED" for report in successors)
    assert [report.queue_item.queue_id for report in successors] == successor_ids


@pytest.mark.parametrize("reuse_rid", [False, True])
def test_restart_without_fresh_rid_leaves_active_scan_running(controls, reuse_rid):
    bec, _ = controls
    active = hold_scan(bec, steps=20, exposure=0.01)
    wait_started(active)
    request_id = active.request.requestID
    bec.connector.send(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            action="restart",
            request_id=request_id,
            parameter={"RID": request_id} if reuse_rid else {},
        ),
    )
    command_barrier(bec)
    assert active.status == "RUNNING"
    assert len(queue(bec).info) == 1
    assert queue(bec).info[0].queue_id == active.queue_item.queue_id
    assert queue(bec).status == "RUNNING"
    release_scan(bec)
    active.wait(timeout=15, num_points=True)
    assert len(active.scan.live_data) == 20


def test_hard_pause_magic_is_not_exposed(controls):
    _, shell = controls
    assert "pause" not in shell.magics_manager.magics["line"]


def test_stop_magic_stops_device_motion(controls):
    bec, shell = controls
    motor = bec.device_manager.devices.samx
    original_velocity = motor.velocity.get()
    try:
        bec.scans.umv(motor, 0, relative=False)
        motor.velocity.set(1).wait()
        bec.scans.mv(motor, 40, relative=False)
        wait_for(lambda: motor.motor_is_moving.get())
        shell.run_line_magic("stop", "")
        wait_for(lambda: not motor.motor_is_moving.get())
        assert motor.readback.get() < 40
        wait_for(lambda: not queue(bec).info)
    finally:
        bec._request_stop_all_devices()
        wait_for(lambda: not motor.motor_is_moving.get())
        reset_and_retire(bec)
        bec.queue.request_queue_continuation()
        command_barrier(bec)
        motor.velocity.set(original_velocity).wait(timeout=15)


def command_barrier(bec):
    """Observe processing of preceding modification requests, including rejected ones."""
    identifier = str(uuid.uuid4())
    bec.queue.add_queue_lock("primary", "command barrier", identifier)
    wait_for(lambda: any(lock.identifier == identifier for lock in queue(bec).locks))
    bec.queue.remove_queue_lock("primary", identifier)
    wait_for(lambda: all(lock.identifier != identifier for lock in queue(bec).locks))


def test_reset_active_scan_and_multiple_pending_items(controls):
    bec, shell = controls
    active = hold_scan(bec, steps=30, exposure=0.1)
    wait_started(active)
    pending = [submit(bec), submit(bec)]
    wait_for(lambda: len(queue(bec).info) == 3)
    shell.run_line_magic("reset", "")
    wait_for(lambda: active.status == "STOPPED")
    wait_for(lambda: all(report.status == "CANCELLED" for report in pending))
    assert not queue(bec).info
    assert queue(bec).status == "PAUSED"
    shell.run_line_magic("resume", "")
    submit(bec).wait(timeout=15)


@pytest.mark.parametrize("method", ["request_scan_abortion", "request_scan_halt"])
def test_cancel_specific_pending_item_preserves_active_and_other_pending(controls, method):
    bec, _ = controls
    active = hold_scan(bec, steps=30, exposure=0.1)
    wait_started(active)
    pending = [submit(bec), submit(bec)]
    wait_for(lambda: len(queue(bec).info) == 3)
    getattr(bec.queue, method)(request_id=pending[0].request.requestID)
    wait_for(lambda: pending[0].status == "CANCELLED")
    assert active.status == "RUNNING"
    assert pending[1].status == "PENDING"
    assert queue(bec).status == "RUNNING"
    release_scan(bec)
    active.wait(timeout=15, num_points=True)
    pending[1].wait(timeout=15)
    assert len(active.scan.live_data) == 30


@pytest.mark.parametrize(
    "method",
    [
        "request_scan_abortion",
        "request_scan_halt",
        "request_scan_restart",
        "request_scan_continuation",
        "request_scan_interruption",
        "request_set_completed",
    ],
)
def test_stale_request_control_never_affects_active_successor(controls, method):
    bec, _ = controls
    original = submit(bec)
    original.wait(timeout=15)
    successor = hold_scan(bec, steps=20, exposure=0.1)
    wait_started(successor)
    getattr(bec.queue, method)(request_id=original.request.requestID)
    command_barrier(bec)
    assert successor.status == "RUNNING"
    assert queue(bec).status == "RUNNING"
    release_scan(bec)
    successor.wait(timeout=15, num_points=True)
    assert len(successor.scan.live_data) == 20
    wait_for(lambda: not queue(bec).info)


@pytest.mark.parametrize("policy", ["paused", "locked", "append"])
def test_restart_preserves_dispatch_policy_and_requested_position(controls, policy):
    bec, shell = controls
    active = hold_scan(bec, steps=30, exposure=0.1)
    wait_started(active)
    successor = submit(bec)
    wait_for(lambda: len(queue(bec).info) == 2)
    identifier = str(uuid.uuid4())
    if policy == "paused":
        shell.run_line_magic("deferred_pause", "")
        wait_for(lambda: queue(bec).status == "PAUSED")
    elif policy == "locked":
        bec.queue.add_queue_lock("primary", "restart policy", identifier)
        wait_for(lambda: queue(bec).status == "LOCKED")
    replacement_rid = bec.queue.request_scan_restart(
        request_id=active.request.requestID, replace=policy != "append"
    )
    wait_for(lambda: active.status == "STOPPED")
    replacement = bec.queue.request_storage.find_request_by_ID(replacement_rid)
    assert replacement is not None
    if policy != "append":
        wait_for(lambda: len(queue(bec).info) == 2)
        assert successor.status == "PENDING"
        assert replacement.queue.status == "PENDING"
        assert queue(bec).status == ("PAUSED" if policy == "paused" else "LOCKED")
        assert queue(bec).info[0].queue_id == replacement.queue.queue_id
        if policy == "locked":
            bec.queue.remove_queue_lock("primary", identifier)
        else:
            shell.run_line_magic("resume", "")
    successor.wait(timeout=20)
    wait_for(lambda: replacement.queue.status == "COMPLETED")
    replacement_scan = bec.queue.scan_storage.find_scan_by_ID(replacement.queue.scan_ids[0])
    assert replacement_scan is not None
    assert (replacement_scan.scan_number < successor.scan.scan_number) == (policy != "append")


@pytest.mark.parametrize("invalid", ["running", "unknown_scan", "unknown_queue"])
def test_rejected_reorder_leaves_queue_unchanged(controls, invalid):
    bec, shell = controls
    active = hold_scan(bec, steps=30, exposure=0.1)
    wait_started(active)
    pending = [submit(bec), submit(bec)]
    wait_for(lambda: len(queue(bec).info) == 3)
    original_order = [item.queue_id for item in queue(bec).info]
    if invalid != "running":
        shell.run_line_magic("deferred_pause", "")
        wait_for(lambda: queue(bec).status == "PAUSED")
    response = bec.queue.request_queue_order_modification(
        str(uuid.uuid4()) if invalid == "unknown_scan" else pending[1].queue_item.scan_ids[0],
        "move_top",
        queue="missing" if invalid == "unknown_queue" else "primary",
        wait_for_response=True,
    )
    assert not response.accepted
    assert [item.queue_id for item in queue(bec).info] == original_order
    release_scan(bec)
    shell.run_line_magic("resume", "")
    for report in [active, *pending]:
        report.wait(timeout=15)


@pytest.mark.parametrize("allow_devices", [False, True])
def test_lock_controls_device_instruction_dispatch(controls, allow_devices):
    bec, _ = controls
    motor = bec.device_manager.devices.samx
    bec.scans.umv(motor, 0, relative=False)
    identifier = str(uuid.uuid4())
    bec.queue.add_queue_lock("primary", "device dispatch", identifier, allow_devices)
    wait_for(lambda: queue(bec).status == "LOCKED")
    move = bec.scans.mv(motor, 3, relative=False, hide_report=True)
    scan = submit(bec)
    command_barrier(bec)
    if allow_devices:
        move.wait(timeout=15)
        assert np.isclose(motor.readback.get(), 3, atol=0.1)
    else:
        assert move.status == "PENDING"
        assert np.isclose(motor.readback.get(cached=True), 0, atol=0.1)
    assert scan.status == "PENDING"
    bec.queue.remove_queue_lock("primary", identifier)
    move.wait(timeout=15)
    scan.wait(timeout=15)


def test_lock_during_scan_finishes_active_but_holds_successor(controls):
    bec, _ = controls
    active = hold_scan(bec, steps=20, exposure=0.05)
    wait_started(active)
    successor = submit(bec)
    identifier = str(uuid.uuid4())
    bec.queue.add_queue_lock("primary", "active dispatch", identifier, False)
    wait_for(lambda: queue(bec).status == "LOCKED")
    release_scan(bec)
    active.wait(timeout=15, num_points=True)
    assert len(active.scan.live_data) == 20
    assert successor.status == "PENDING"
    bec.queue.request_queue_continuation()
    command_barrier(bec)
    assert successor.status == "PENDING"
    bec.queue.remove_queue_lock("primary", identifier)
    successor.wait(timeout=15)


def test_named_queue_pause_reset_and_resume_are_isolated(controls):
    bec, _ = controls
    name = f"controls-{uuid.uuid4()}"
    identifier = str(uuid.uuid4())
    bec.queue.add_queue_lock(name, "create named queue", identifier)
    wait_for(lambda: name in bec.queue.queue_storage.current_scan_queue)
    try:
        bec.queue.request_queue_pause(name)
        bec.queue.remove_queue_lock(name, identifier)
        wait_for(lambda: queue(bec, name).status == "PAUSED")
        held = submit(bec, queue_name=name)
        submit(bec).wait(timeout=15)
        assert held.status == "PENDING"
        bec.queue.set_default_scan_queue(name)
        bec.queue.request_queue_reset()
        wait_for(lambda: held.status == "CANCELLED")
        assert queue(bec).status == "RUNNING"
        successor = submit(bec, queue_name=name)
        bec.queue.request_queue_continuation(name)
        successor.wait(timeout=15)
    finally:
        bec.queue.set_default_scan_queue("primary")
        if name in bec.queue.queue_storage.current_scan_queue:
            reset_and_retire(bec, name)
        bec.queue.remove_queue_lock(name, identifier)


def test_abort_immediately_followed_by_resume_runs_successor_after_cleanup(controls):
    bec, shell = controls
    motor = bec.device_manager.devices.samx
    bec.scans.umv(motor, 0, relative=False)
    active = hold_scan(bec, steps=30, exposure=0.1, relative=True)
    wait_started(active)
    successor = submit(bec, relative=True)
    wait_for(lambda: len(queue(bec).info) == 2)
    shell.run_line_magic("abort", "")
    shell.run_line_magic("resume", "")
    wait_for(lambda: active.status == "STOPPED")
    successor.wait(timeout=15)
    assert len(successor.scan.live_data) == 5
    # The relative successor must start from the original position restored by cleanup.
    assert np.isclose(
        successor.scan.live_data[0].content["data"]["samx"]["samx"]["value"], 2, atol=0.1
    )


def test_stop_magic_stops_multiple_moving_devices(controls):
    bec, shell = controls
    motors = [bec.device_manager.devices.samx, bec.device_manager.devices.samy]
    velocities = [motor.velocity.get() for motor in motors]
    try:
        bec.scans.umv(motors[0], 0, motors[1], 0, relative=False)
        for motor in motors:
            motor.velocity.set(1).wait()
        bec.scans.mv(motors[0], 40, motors[1], 40, relative=False)
        wait_for(lambda: all(motor.motor_is_moving.get() for motor in motors))
        shell.run_line_magic("stop", "")
        wait_for(lambda: all(not motor.motor_is_moving.get() for motor in motors))
        assert all(motor.readback.get() < 40 for motor in motors)
        wait_for(lambda: not queue(bec).info)
    finally:
        bec._request_stop_all_devices()
        wait_for(lambda: all(not motor.motor_is_moving.get() for motor in motors))
        reset_and_retire(bec)
        bec.queue.request_queue_continuation()
        command_barrier(bec)
        for motor, velocity in zip(motors, velocities):
            motor.velocity.set(velocity).wait(timeout=15)


def test_device_error_holds_pending_successor_until_explicit_recovery(controls):
    bec, shell = controls
    motor = bec.device_manager.devices.samx
    original_limits = motor.limits
    try:
        motor.limits = [-50, 50]
        shell.run_line_magic("deferred_pause", "")
        wait_for(lambda: queue(bec).status == "PAUSED")
        failed = bec.scans.line_scan(
            motor, -520, 5, steps=5, exp_time=0.01, relative=False, hide_report=True
        )
        successor = submit(bec)
        wait_for(lambda: len(queue(bec).info) == 2)
        shell.run_line_magic("resume", "")
        wait_for(lambda: failed.status == "STOPPED" and len(queue(bec).info) == 1)
        assert queue(bec).status == "PAUSED"
        assert successor.status == "PENDING"
        wait_for(lambda: bool(bec.alarm_handler.alarms_stack))
        assert any(alarm.alarm_type == "LimitError" for alarm in bec.alarm_handler.alarms_stack)
        with pytest.raises((AlarmBase, ScanAbortion)):
            failed.wait(timeout=5)
        bec.alarm_handler.clear()
        shell.run_line_magic("resume", "")
        successor.wait(timeout=15)
    finally:
        reset_and_retire(bec)
        bec.alarm_handler.clear()
        motor.limits = original_limits


def test_legacy_hard_pause_request_is_ignored_by_server(controls):
    bec, _ = controls
    active = hold_scan(bec, steps=20, exposure=0.1)
    wait_started(active)
    bec.connector.send(
        MessageEndpoints.scan_queue_modification_request(),
        messages.ScanQueueModificationMessage(
            action="pause", request_id=active.request.requestID, parameter={}
        ),
    )
    command_barrier(bec)
    assert active.status == "RUNNING"
    assert queue(bec).status == "RUNNING"
    release_scan(bec)
    active.wait(timeout=15, num_points=True)
    assert len(active.scan.live_data) == 20


def test_user_completed_performs_cleanup_and_continues_dispatch(controls):
    bec, _ = controls
    motor = bec.device_manager.devices.samx
    bec.scans.umv(motor, 0, relative=False)
    active = hold_scan(bec, steps=30, exposure=0.1, relative=True)
    wait_started(active)
    successor = submit(bec, relative=True)
    wait_for(lambda: len(queue(bec).info) == 2)
    bec.queue.request_set_completed(request_id=active.request.requestID)
    wait_for(lambda: active.status == "STOPPED")
    successor.wait(timeout=15)
    assert active.scan.status == "user_completed"
    assert np.isclose(
        successor.scan.live_data[0].content["data"]["samx"]["samx"]["value"], 2, atol=0.1
    )


def test_device_communication_failure_preserves_pending_successor(controls):
    bec, shell = controls
    name = f"queue_failure_{uuid.uuid4().hex[:8]}"
    bec.device_manager.config_helper.send_config_request(
        action="add",
        config={
            name: {
                "deviceClass": "ophyd_devices.sim.sim_test_devices.SimPositionerWithCommFailure",
                "deviceConfig": {"limits": [-100, 100], "tolerance": 0.1},
                "readoutPriority": "baseline",
                "enabled": True,
                "readOnly": False,
            }
        },
    )
    try:
        motor = bec.device_manager.devices[name]
        motor.fails.set(1).wait()
        shell.run_line_magic("deferred_pause", "")
        wait_for(lambda: queue(bec).status == "PAUSED")
        failed = bec.scans.line_scan(
            motor, -5, 5, steps=5, exp_time=0.01, relative=False, hide_report=True
        )
        successor = submit(bec)
        wait_for(lambda: len(queue(bec).info) == 2)
        shell.run_line_magic("resume", "")
        wait_for(lambda: failed.status == "STOPPED" and len(queue(bec).info) == 1)
        assert queue(bec).status == "PAUSED"
        assert successor.status == "PENDING"
        wait_for(
            lambda: any(alarm.alarm.info.device == name for alarm in bec.alarm_handler.alarms_stack)
        )
        with pytest.raises((AlarmBase, ScanAbortion)):
            failed.wait(timeout=5)
        bec.alarm_handler.clear()
        shell.run_line_magic("resume", "")
        successor.wait(timeout=15)
    finally:
        reset_and_retire(bec)
        bec.alarm_handler.clear()
        bec.device_manager.config_helper.send_config_request(action="remove", config={name: {}})


def test_resume_while_locked_clears_pause_but_cannot_release_lock(controls):
    bec, shell = controls
    shell.run_line_magic("deferred_pause", "")
    wait_for(lambda: queue(bec).status == "PAUSED")
    identifier = str(uuid.uuid4())
    bec.queue.add_queue_lock("primary", "resume policy", identifier, False)
    wait_for(lambda: queue(bec).status == "LOCKED")
    pending = [submit(bec), submit(bec)]
    wait_for(lambda: len(queue(bec).info) == 2)
    shell.run_line_magic("resume", "")
    command_barrier(bec)
    assert queue(bec).status == "LOCKED"
    assert all(report.status == "PENDING" for report in pending)
    bec.queue.remove_queue_lock("primary", identifier)
    for report in pending:
        report.wait(timeout=15)
    assert queue(bec).status == "RUNNING"


def test_cleanup_retires_held_work_before_releasing_test_lock(controls):
    bec, shell = controls
    active = hold_scan(bec, steps=20, exposure=0.01)
    wait_started(active)
    identifier = str(uuid.uuid4())
    bec.queue.add_queue_lock("primary", "failed test cleanup", identifier, False)
    wait_for(lambda: queue(bec).status == "LOCKED")
    pending = submit(bec)
    wait_for(lambda: len(queue(bec).info) == 2)
    # Exercise the cleanup helper while the original scan is still held, as on an assertion failure.
    reset_and_retire(bec)
    assert active.status == "STOPPED"
    assert pending.status == "CANCELLED"
    assert not queue(bec).info
    assert queue(bec).status == "LOCKED"
    bec.queue.remove_queue_lock("primary", identifier)
    wait_for(lambda: not queue(bec).locks)
    assert queue(bec).status == "PAUSED"
    shell.run_line_magic("resume", "")
    submit(bec).wait(timeout=15)
