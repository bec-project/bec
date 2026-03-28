from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

from bec_server.scan_server.errors import ScanAbortion

from .scan_test_utils import (
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


def _configure_cont_line_device(scan, velocity=1.0, acceleration=2.0, precision=3):
    scan.device.velocity = SimpleNamespace(get=mock.MagicMock(return_value=velocity))
    scan.device.acceleration = SimpleNamespace(get=mock.MagicMock(return_value=acceleration))
    scan.device._precision = precision


@pytest.mark.parametrize(("hook_name", "hook_tests"), CONT_LINE_DEFAULT_HOOK_TESTS)
def test_cont_line_scan_default_hooks(
    v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests
):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
    )
    _configure_cont_line_device(scan)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_cont_line_scan_prepare_scan_updates_scan_info(v4_scan_assembler):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
    )
    _configure_cont_line_device(scan)

    scan.prepare_scan()

    assert np.array_equal(scan.positions, np.array([[-1.0], [0.0], [1.0]]))
    assert scan.scan_info.num_points == 3
    assert scan.offset == 1.0


def test_cont_line_scan_at_each_point_triggers_and_reads(v4_scan_assembler):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
    )
    scan.components.trigger_and_read = mock.MagicMock()

    scan.at_each_point()

    scan.components.trigger_and_read.assert_called_once_with()


def test_cont_line_scan_scan_core_moves_and_reads_at_matching_positions(v4_scan_assembler):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=0.1, relative=False
    )
    _configure_cont_line_device(scan)
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


def test_cont_line_scan_prepare_scan_raises_when_motor_too_fast(v4_scan_assembler):
    scan = v4_scan_assembler(
        "cont_line_scan", "samx", -1.0, 1.0, steps=3, exp_time=10.0, relative=False
    )
    _configure_cont_line_device(scan, velocity=1.0, acceleration=2.0)

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
