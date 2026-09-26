from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

from bec_lib import messages
from bec_server.scan_server.errors import DeviceInstructionError, ScanAbortion
from bec_server.scan_server.scans.scan_status import ScanStatus
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
    scan_assembler,
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
    return scan_assembler(
        "cont_line_scan", "samx", start, stop, steps=steps, exp_time=exp_time, relative=relative
    )


@pytest.mark.parametrize(("hook_name", "hook_tests"), CONT_LINE_DEFAULT_HOOK_TESTS)
def test_cont_line_scan_default_hooks(
    scan_assembler, device_manager, nth_done_status_mock, hook_name, hook_tests
):
    scan = _assemble_cont_line_scan(
        scan_assembler, device_manager, start=-1.0, stop=1.0, steps=3, exp_time=0.1, relative=False
    )

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_cont_line_scan_prepare_scan_updates_scan_info(scan_assembler, device_manager):
    scan = _assemble_cont_line_scan(
        scan_assembler, device_manager, start=-1.0, stop=1.0, steps=3, exp_time=0.1, relative=False
    )

    scan.prepare_scan()

    assert np.array_equal(scan.positions, np.array([[-1.0], [0.0], [1.0]]))
    assert scan.scan_info.num_points == 3
    assert scan.offset == 1.0


def test_cont_line_scan_example_custom_device_manager_integration(scan_assembler, device_manager):
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

    scan = scan_assembler(
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


def test_cont_line_scan_at_each_point_triggers_and_reads(scan_assembler):
    scan = scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
    )
    scan.components.trigger_and_read = mock.MagicMock()

    scan.at_each_point()

    scan.components.trigger_and_read.assert_called_once_with()


def test_cont_line_scan_scan_core_moves_and_reads_at_matching_positions(
    scan_assembler, device_manager
):
    scan = _assemble_cont_line_scan(
        scan_assembler, device_manager, start=-1.0, stop=1.0, steps=3, exp_time=0.1, relative=False
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


@pytest.mark.parametrize("reading", [None, {}, {"samx": {"value": -2.0}}])
def test_cont_line_scan_scan_core_aborts_when_shutdown_is_set(
    scan_assembler, device_manager, reading
):
    scan = _assemble_cont_line_scan(scan_assembler, device_manager)
    scan.prepare_scan()
    status = SimpleNamespace(done=False, wait=mock.MagicMock())
    scan.actions.set = mock.MagicMock(return_value=status)
    scan.at_each_point = mock.MagicMock()

    def interrupt_during_read(**kwargs):
        if scan.device.read.call_count > 1:
            raise AssertionError("Polling continued after shutdown")
        scan._shutdown_event.set()
        return reading

    scan.device.read = mock.MagicMock(side_effect=interrupt_during_read)

    with pytest.raises(ScanAbortion, match="Continuous scan interrupted"):
        scan.scan_core()

    scan.device.read.assert_called_once_with(cached=True)
    scan.at_each_point.assert_not_called()
    status.wait.assert_not_called()


def test_cont_line_scan_scan_core_propagates_failed_motion(scan_assembler, device_manager):
    scan = _assemble_cont_line_scan(scan_assembler, device_manager)
    scan.prepare_scan()
    handler = mock.Mock()
    status = ScanStatus(handler, is_container=True, shutdown_event=scan._shutdown_event)
    motion = ScanStatus(handler, shutdown_event=scan._shutdown_event)
    status.add_status(motion)
    scan.actions.set = mock.MagicMock(return_value=status)
    scan.at_each_point = mock.MagicMock()
    error_info = messages.ErrorInfo(
        error_message="Motor fault",
        compact_error_message="Motor fault",
        exception_type="RuntimeError",
    )

    def fail_during_read(**kwargs):
        if scan.device.read.call_count > 1:
            raise AssertionError("Polling continued after motion failed")
        motion.set_failed(error_info)
        return {"samx": {"value": -2.0}}

    scan.device.read = mock.MagicMock(side_effect=fail_during_read)

    with pytest.raises(DeviceInstructionError, match="Motor fault"):
        scan.scan_core()

    scan.device.read.assert_called_once_with(cached=True)
    scan.at_each_point.assert_not_called()
    assert not scan._shutdown_event.is_set()


@pytest.mark.parametrize("reading", [None, {}, "short"])
def test_cont_line_scan_scan_core_aborts_when_motion_finishes_short(
    scan_assembler, device_manager, reading
):
    scan = _assemble_cont_line_scan(scan_assembler, device_manager)
    scan.prepare_scan()
    scan._point_index = len(scan.positions) - 1
    if reading == "short":
        reading = {"samx": {"value": scan.positions[-1][0] - 5 * scan.atol}}
    status = ScanStatus(mock.Mock(), shutdown_event=scan._shutdown_event)
    scan.actions.set = mock.MagicMock(return_value=status)
    scan.at_each_point = mock.MagicMock()

    def finish_during_read(**kwargs):
        if scan.device.read.call_count > 3:
            raise AssertionError("Polling continued after motion finished")
        if not status.done:
            status.set_done()
        return reading

    scan.device.read = mock.MagicMock(side_effect=finish_during_read)

    with mock.patch.object(scan._shutdown_event, "wait", return_value=False) as wait:
        with pytest.raises(ScanAbortion, match="Motion finished before point 3 was reached"):
            scan.scan_core()

    wait.assert_called_once_with(0.1)
    assert scan.device.read.call_count == 3
    scan.at_each_point.assert_not_called()


@pytest.mark.parametrize("completion", ["before_read", "during_read"])
def test_cont_line_scan_scan_core_reads_final_position_after_motion_finishes(
    scan_assembler, device_manager, completion
):
    scan = _assemble_cont_line_scan(scan_assembler, device_manager)
    scan.prepare_scan()
    scan._point_index = len(scan.positions) - 1
    status = ScanStatus(mock.Mock(), shutdown_event=scan._shutdown_event)
    if completion == "before_read":
        status.set_done()
    scan.actions.set = mock.MagicMock(return_value=status)
    scan.at_each_point = mock.MagicMock()

    def final_readback(**kwargs):
        if scan.device.read.call_count > 2:
            raise AssertionError("Polling continued after the final point")
        if not status.done:
            # This read started before completion and may still contain an older value.
            status.set_done()
            return {"samx": {"value": scan.positions[-1][0] - 5 * scan.atol}}
        return {"samx": {"value": 1.0}}

    scan.device.read = mock.MagicMock(side_effect=final_readback)

    scan.scan_core()

    scan.at_each_point.assert_called_once_with()
    assert scan._point_index == len(scan.positions)
    assert scan.device.read.call_count == (1 if completion == "before_read" else 2)


@pytest.mark.parametrize("reading", [None, {}, "short"])
def test_cont_line_scan_scan_core_retries_delayed_final_readback(
    scan_assembler, device_manager, reading
):
    scan = _assemble_cont_line_scan(scan_assembler, device_manager)
    scan.prepare_scan()
    scan._point_index = len(scan.positions) - 1
    if reading == "short":
        reading = {"samx": {"value": scan.positions[-1][0] - 5 * scan.atol}}
    status = ScanStatus(mock.Mock(), shutdown_event=scan._shutdown_event)
    status.set_done()
    scan.actions.set = mock.MagicMock(return_value=status)
    scan.at_each_point = mock.MagicMock()
    scan.device.read = mock.MagicMock(side_effect=lambda **kwargs: reading)

    def publish_final_readback(timeout):
        nonlocal reading
        reading = {"samx": {"value": scan.positions[-1][0]}}
        return False

    with mock.patch.object(
        scan._shutdown_event, "wait", side_effect=publish_final_readback
    ) as wait:
        scan.scan_core()

    wait.assert_called_once_with(0.1)
    assert scan.device.read.call_count == 2
    scan.at_each_point.assert_called_once_with()
    assert scan._point_index == len(scan.positions)


def test_cont_line_scan_scan_core_aborts_during_readback_retry(scan_assembler, device_manager):
    scan = _assemble_cont_line_scan(scan_assembler, device_manager)
    scan.prepare_scan()
    status = ScanStatus(mock.Mock(), shutdown_event=scan._shutdown_event)
    status.set_done()
    scan.actions.set = mock.MagicMock(return_value=status)
    scan.at_each_point = mock.MagicMock()
    scan.device.read = mock.MagicMock(return_value=None)

    def abort_during_wait(timeout):
        scan._shutdown_event.set()
        return True

    with mock.patch.object(scan._shutdown_event, "wait", side_effect=abort_during_wait) as wait:
        with pytest.raises(ScanAbortion, match="Continuous scan interrupted"):
            scan.scan_core()

    wait.assert_called_once_with(0.1)
    scan.device.read.assert_called_once_with(cached=True)
    scan.at_each_point.assert_not_called()


def test_cont_line_scan_prepare_scan_raises_when_motor_too_fast(scan_assembler, device_manager):
    scan = _assemble_cont_line_scan(
        scan_assembler,
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


def test_cont_line_scan_post_scan_moves_back_when_relative(scan_assembler, nth_done_status_mock):
    scan = scan_assembler("cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=True)
    completion_status = nth_done_status_mock(resolve_after=2)
    scan.start_positions = [1.5]
    scan.actions.complete_all_devices = mock.MagicMock(return_value=completion_status)
    scan.components.move_and_wait = mock.MagicMock()

    scan.post_scan()

    scan.actions.complete_all_devices.assert_called_once_with(wait=False)
    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
    assert completion_status.wait_calls == 1


def test_cont_line_scan_on_exception_moves_back_when_relative(scan_assembler):
    scan = scan_assembler("cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=True)
    scan.start_positions = [1.5]
    scan.components.move_and_wait = mock.MagicMock()

    scan.on_exception(RuntimeError("boom"))

    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
