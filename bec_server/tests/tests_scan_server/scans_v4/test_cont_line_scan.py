from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

from bec_server.scan_server.errors import ScanAbortion
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
        "_v4_cont_line_scan", "samx", start, stop, steps=steps, exp_time=exp_time, relative=relative
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
        "_v4_cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
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
        "_v4_cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
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
    end_status = SimpleNamespace(wait=mock.MagicMock())
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
        "_v4_cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=True
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
        "_v4_cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=True
    )
    scan.start_positions = [1.5]
    scan.components.move_and_wait = mock.MagicMock()

    scan.on_exception(RuntimeError("boom"))

    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
