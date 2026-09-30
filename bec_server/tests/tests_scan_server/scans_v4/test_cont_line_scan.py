import threading
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

from bec_lib import messages
from bec_server.scan_server.device_lock_registry import DeviceLockRegistry
from bec_server.scan_server.direct_scan_worker import DirectScanWorker
from bec_server.scan_server.errors import DeviceInstructionError, ScanAbortion, UserScanInterruption
from bec_server.scan_server.scan_queue import InstructionQueueStatus
from bec_server.scan_server.tests.scan_fixtures import MockCustomDevice
from bec_server.scan_server.tests.scan_hook_tests import (
    PREMOVE_HOOK_TESTS,
    assert_close_scan_waits_for_baseline_and_closes,
    assert_pre_scan_called,
    assert_prepare_scan_reads_baseline_devices,
    assert_scan_open_called,
    assert_stage_all_devices_called,
    assert_unstage_all_devices_called,
    run_scan_tests,
)

CONT_LINE_DEFAULT_HOOK_TESTS = [
    ("prepare_scan", [assert_prepare_scan_reads_baseline_devices]),
    ("open_scan", [assert_scan_open_called]),
    ("stage", [assert_stage_all_devices_called]),
    ("pre_scan", [assert_pre_scan_called]),
    ("unstage", [assert_unstage_all_devices_called]),
    ("close_scan", [assert_close_scan_waits_for_baseline_and_closes]),
    *PREMOVE_HOOK_TESTS,
]


def _assemble_cont_line_scan(
    v4_scan_assembler,
    device_manager,
    *,
    start=-1.0,
    stop=1.0,
    steps=3,
    exp_time=0.1,
    relative=False,
    velocity=1.0,
    acceleration=2.0,
    precision=3,
):
    device_info = {
        "signals": {
            "readback": {"obj_name": "samx", "kind_str": "hinted", "describe": {"precision": 3}},
            "velocity": {
                "obj_name": "samx_velocity",
                "kind_str": "config",
                "describe": {"precision": 3},
            },
            "acceleration": {
                "obj_name": "samx_acceleration",
                "kind_str": "config",
                "describe": {"precision": 3},
            },
        }
    }
    custom_samx = MockCustomDevice(
        "samx",
        device_info=device_info,
        signal_read_values={
            "samx": 0.0,
            "samx_velocity": velocity,
            "samx_acceleration": acceleration,
        },
        precision=precision,
    )
    device_manager.add_device(custom_samx, replace=True)
    return v4_scan_assembler(
        "cont_line_scan", "samx", start, stop, steps=steps, exp_time=exp_time, relative=relative
    )


@pytest.mark.parametrize(("hook_name", "hook_tests"), CONT_LINE_DEFAULT_HOOK_TESTS)
def test_cont_line_scan_default_hooks(
    v4_scan_assembler, device_manager, nth_done_status_mock, hook_name, hook_tests
):
    scan = _assemble_cont_line_scan(
        v4_scan_assembler,
        device_manager,
        start=-1.0,
        stop=1.0,
        steps=3,
        exp_time=0.1,
        relative=False,
    )

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_cont_line_scan_prepare_scan_updates_scan_info(v4_scan_assembler, device_manager):
    scan = _assemble_cont_line_scan(
        v4_scan_assembler,
        device_manager,
        start=-1.0,
        stop=1.0,
        steps=3,
        exp_time=0.1,
        relative=False,
    )

    scan.prepare_scan()

    assert np.array_equal(scan.positions, np.array([[-1.0], [0.0], [1.0]]))
    assert scan.scan_info.num_points == 3
    assert scan.offset == 1.0


def test_cont_line_scan_example_custom_device_manager_integration(
    v4_scan_assembler, device_manager
):
    custom_samx = MockCustomDevice(
        "samx",
        device_info={
            "signals": {
                "readback": {
                    "obj_name": "samx",
                    "kind_str": "hinted",
                    "describe": {"precision": 3},
                },
                "velocity": {
                    "obj_name": "samx_velocity",
                    "kind_str": "config",
                    "describe": {"precision": 3},
                },
                "acceleration": {
                    "obj_name": "samx_acceleration",
                    "kind_str": "config",
                    "describe": {"precision": 3},
                },
            }
        },
        signal_read_values={"samx": 2.5, "samx_velocity": 1.0, "samx_acceleration": 2.0},
    )
    device_manager.add_device(custom_samx, replace=True)

    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
    )

    assert scan.device is custom_samx


def test_mock_custom_device_supports_generated_signal_values():
    custom_samx = MockCustomDevice(
        "samx",
        device_info={
            "signals": {
                "readback": {
                    "obj_name": "samx",
                    "kind_str": "hinted",
                    "describe": {"precision": 3},
                },
                "velocity": {
                    "obj_name": "samx_velocity",
                    "kind_str": "config",
                    "describe": {"precision": 3},
                },
            }
        },
        signal_read_values={"samx": iter([0.0, 0.5]), "samx_velocity": iter([1.0, 2.0])},
    )

    assert custom_samx.read()["samx"]["value"] == 0.0
    assert custom_samx.read()["samx"]["value"] == 0.5

    assert custom_samx.velocity.get() == 1.0
    assert custom_samx.read_configuration()["samx_velocity"]["value"] == 2.0

    custom_samx.set_signal_value("velocity", 5.0)
    assert custom_samx.velocity.get() == 5.0


def test_cont_line_scan_at_each_point_triggers_and_reads(v4_scan_assembler):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
    )
    scan.components.trigger_and_read = mock.MagicMock()

    scan.at_each_point()

    scan.components.trigger_and_read.assert_called_once_with()


def test_cont_line_scan_scan_core_moves_and_reads_at_matching_positions(
    v4_scan_assembler, device_manager
):
    scan = _assemble_cont_line_scan(
        v4_scan_assembler,
        device_manager,
        start=-1.0,
        stop=1.0,
        steps=3,
        exp_time=0.1,
        relative=False,
    )
    scan.prepare_scan()
    start_status = SimpleNamespace(wait=mock.MagicMock())
    end_status = SimpleNamespace(done=False, wait=mock.MagicMock())
    scan.actions.set = mock.MagicMock(side_effect=[start_status, end_status])
    read_values = iter(
        [{"samx": {"value": -1.0}}, {"samx": {"value": 0.0}}, {"samx": {"value": 1.0}}]
    )
    scan.device.read = mock.MagicMock(side_effect=lambda **kwargs: next(read_values))
    scan.at_each_point = mock.MagicMock()

    scan.scan_core()

    assert scan.actions.set.call_args_list == [
        mock.call(scan.device, -2.0, wait=True),
        mock.call(scan.device, 1.0, wait=False),
    ]
    end_status.wait.assert_called_once_with()
    assert scan.at_each_point.call_count == 3


@pytest.fixture
def cont_line_scan_core(v4_scan_assembler, device_manager):
    scan = _assemble_cont_line_scan(v4_scan_assembler, device_manager)
    scan.prepare_scan()
    status = scan.actions._create_status(name="continuous_move")
    scan.actions.set = mock.MagicMock(return_value=status)
    scan.at_each_point = mock.MagicMock()
    return scan, status


@pytest.mark.parametrize("interruption", ["abort", "shutdown"])
@pytest.mark.parametrize("reading", [None, {}, {"samx": {"value": -2.0}}])
def test_cont_line_scan_interruption_releases_worker_locks(
    cont_line_scan_core, interruption, reading
):
    scan, _ = cont_line_scan_core
    registry = DeviceLockRegistry()
    scan.device_manager.parent.device_lock_registry = registry
    request_id = scan.scan_info.metadata["RID"]
    queue = SimpleNamespace(
        parent=scan.device_manager.parent,
        exit_info=("aborted", "user"),
        stopped=False,
        active_request_block=True,
        run_on_exception_hook=False,
    )
    worker = SimpleNamespace(
        status=InstructionQueueStatus.RUNNING,
        signal_event=threading.Event(),
        current_instruction_queue_item=queue,
        device_manager=scan.device_manager,
        parent=scan.device_manager.parent,
        connector=scan.redis_connector,
    )
    direct_worker = DirectScanWorker(worker=worker)

    def interrupt_during_read(**kwargs):
        assert scan.device.read.call_count == 1, "Polling continued after interruption"
        assert "samx" in registry.get_owned_devices(request_id)
        scan._shutdown_event.set()
        if interruption == "shutdown":
            worker.signal_event.set()
        else:
            worker.status = InstructionQueueStatus.STOPPED
        return reading

    scan.device.read = mock.MagicMock(side_effect=interrupt_during_read)
    with mock.patch("bec_server.scan_server.direct_scan_worker.SCAN_SEQUENCE", ["scan_core"]):
        if interruption == "abort":
            with pytest.raises(UserScanInterruption) as exc_info:
                direct_worker.run(scan)
            assert exc_info.value.exit_info == queue.exit_info
        else:
            direct_worker.run(scan)

    assert scan.device.read.call_count == 1
    scan.at_each_point.assert_not_called()
    assert registry.get_owned_devices(request_id) == []
    assert registry.acquire_many("next-request", ["samx"]) == ["samx"]


@pytest.mark.parametrize("reading", [None, {}, {"samx": {"value": -2.0}}])
def test_cont_line_scan_aborts_when_motion_finishes_before_target(cont_line_scan_core, reading):
    scan, status = cont_line_scan_core
    status.set_done()
    scan.device.read = mock.MagicMock(
        side_effect=[reading, AssertionError("Polling continued after motion completed")]
    )

    with pytest.raises(ScanAbortion, match="completed before reaching acquisition point 1"):
        scan.scan_core()

    scan.at_each_point.assert_not_called()


def test_cont_line_scan_propagates_failed_motion(cont_line_scan_core):
    scan, status = cont_line_scan_core
    status.set_failed(
        messages.ErrorInfo(
            error_message="motor failed",
            compact_error_message="motor failed",
            exception_type="RuntimeError",
            device="samx",
        )
    )
    scan.device.read = mock.MagicMock(side_effect=AssertionError("Read after motion failure"))

    with pytest.raises(DeviceInstructionError, match="motor failed"):
        scan.scan_core()

    scan.at_each_point.assert_not_called()


def test_cont_line_scan_accepts_final_position_when_motion_is_done(cont_line_scan_core):
    scan, status = cont_line_scan_core
    scan._point_index = len(scan.positions) - 1
    status.set_done()
    scan.device.read = mock.MagicMock(return_value={"samx": {"value": scan.positions[-1][0]}})

    scan.scan_core()

    scan.at_each_point.assert_called_once_with()
    assert scan._point_index == len(scan.positions)


@pytest.mark.parametrize("reading", [None, {}, {"samx": {}}, {"samx": {"value": -2.0}}])
def test_cont_line_scan_bounds_wait_for_missing_or_stale_readback(cont_line_scan_core, reading):
    scan, _ = cont_line_scan_core
    clock = SimpleNamespace(now=0.0)

    def read_stalled_motor(**kwargs):
        assert scan.device.read.call_count <= 2, "Unbounded readback polling"
        if scan.device.read.call_count == 2:
            clock.now = 30.0
        return reading

    scan.device.read = mock.MagicMock(side_effect=read_stalled_motor)
    with (
        mock.patch("bec_server.scan_server.scans.cont_line_scan.time") as scan_time,
        mock.patch.object(scan._shutdown_event, "wait", return_value=False) as poll_wait,
    ):
        scan_time.monotonic.side_effect = lambda: clock.now
        with pytest.raises(ScanAbortion, match="Timed out.*acquisition point 1"):
            scan.scan_core()

    poll_wait.assert_called_once()
    assert 0 < poll_wait.call_args.args[0] <= 0.01
    scan.at_each_point.assert_not_called()


def test_cont_line_scan_preserves_skipped_point_error(cont_line_scan_core):
    scan, _ = cont_line_scan_core
    scan.device.read = mock.MagicMock(return_value={"samx": {"value": 0.0}})

    with pytest.raises(ScanAbortion, match="Skipped point 1"):
        scan.scan_core()

    scan.at_each_point.assert_not_called()


def test_cont_line_scan_polls_fast_enough_for_narrow_tolerance(cont_line_scan_core):
    scan, status = cont_line_scan_core
    scan.positions = np.array([[0.0]])
    scan.offset = 0.001
    scan.atol = 0.00006
    clock = SimpleNamespace(now=0.0)

    def read_moving_motor(**kwargs):
        clock.now += 0.00005
        return {"samx": {"value": -scan.offset + scan.motor_velocity * clock.now}}

    def advance_clock(delay):
        clock.now += delay

    scan.device.read = mock.MagicMock(side_effect=read_moving_motor)
    scan.at_each_point.side_effect = status.set_done
    with (
        mock.patch("bec_server.scan_server.scans.cont_line_scan.time") as scan_time,
        mock.patch.object(scan._shutdown_event, "wait", side_effect=advance_clock),
    ):
        scan_time.monotonic.side_effect = lambda: clock.now
        scan.scan_core()

    scan.at_each_point.assert_called_once_with()


def test_cont_line_scan_timeout_allows_slow_motion_and_resets_per_point(cont_line_scan_core):
    scan, status = cont_line_scan_core
    scan.motor_velocity = 0.01
    clock = SimpleNamespace(now=0.0)
    readings = iter([(10, None), (100, -1.0), (210, None), (250, 0.0), (360, None), (400, 1.0)])

    def read_slow_motor(**kwargs):
        clock.now, position = next(readings)
        if position == 1.0:
            status.set_done()
        return {"samx": {"value": position}}

    scan.device.read = mock.MagicMock(side_effect=read_slow_motor)
    with (
        mock.patch("bec_server.scan_server.scans.cont_line_scan.time") as scan_time,
        mock.patch.object(scan._shutdown_event, "wait", return_value=False),
    ):
        scan_time.monotonic.side_effect = lambda: clock.now
        scan.scan_core()

    assert scan.at_each_point.call_count == 3


def test_cont_line_scan_prepare_scan_raises_when_motor_too_fast(v4_scan_assembler, device_manager):
    scan = _assemble_cont_line_scan(
        v4_scan_assembler,
        device_manager,
        start=-1.0,
        stop=1.0,
        steps=3,
        exp_time=10.0,
        relative=False,
        velocity=1.0,
        acceleration=2.0,
    )

    with pytest.raises(ScanAbortion, match="moving too fast"):
        scan.prepare_scan()


def test_cont_line_scan_post_scan_moves_back_when_relative(v4_scan_assembler, nth_done_status_mock):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=True
    )
    completion_status = nth_done_status_mock(resolve_after=2)
    scan.start_positions = [1.5]
    scan.actions.complete_all_devices = mock.MagicMock(return_value=completion_status)
    scan.components.move_and_wait = mock.MagicMock()

    scan.post_scan()

    scan.actions.complete_all_devices.assert_called_once_with(wait=False)
    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
    assert completion_status.wait_calls == 1


def test_cont_line_scan_on_exception_moves_back_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=True
    )
    scan.start_positions = [1.5]
    scan.components.move_and_wait = mock.MagicMock()

    scan.on_exception(RuntimeError("boom"))

    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
