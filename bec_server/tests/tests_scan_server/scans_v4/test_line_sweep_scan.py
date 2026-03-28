from unittest import mock

import numpy as np
import pytest

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
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

LINE_SWEEP_DEFAULT_HOOK_TESTS = [
    ("prepare_scan", [assert_prepare_scan_reads_baseline_devices]),
    ("open_scan", [assert_scan_open_called]),
    ("stage", [assert_stage_all_devices_called]),
    ("pre_scan", [assert_pre_scan_called]),
    ("unstage", [assert_unstage_all_devices_called]),
    ("close_scan", [assert_close_scan_waits_for_baseline_and_closes]),
    *PREMOVE_HOOK_TESTS,
]


def _device_readback_message(device_name: str, value: float) -> MessageObject:
    endpoint = MessageEndpoints.device_readback(device_name)
    return MessageObject(
        topic=endpoint.endpoint,
        value=messages.DeviceMessage(signals={device_name: {"value": value}}),
    )


@pytest.mark.parametrize(("hook_name", "hook_tests"), LINE_SWEEP_DEFAULT_HOOK_TESTS)
def test_line_sweep_scan_default_hooks(
    v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests
):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=True)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_line_sweep_scan_prepare_scan_updates_scan_info(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_line_sweep_scan",
        "samx",
        -5.0,
        5.0,
        exp_time=0.2,
        frames_per_trigger=3,
        max_update=0.4,
        relative=False,
    )

    scan.prepare_scan()

    assert np.array_equal(scan.positions, np.array([[-5.0], [5.0]]))
    assert scan.scan_info.num_points == 0
    assert scan.scan_info.exp_time == 0.2
    assert scan.scan_info.frames_per_trigger == 3
    assert scan.max_update == 0.4
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": 0, "show_table": False}}
    ]


def test_line_sweep_scan_at_each_point_triggers_and_reads(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=False)
    scan.components.trigger_and_read = mock.MagicMock()

    scan.at_each_point()

    scan.components.trigger_and_read.assert_called_once_with()


def test_line_sweep_scan_scan_core_moves_and_reads_until_done(
    v4_scan_assembler, nth_done_status_mock
):
    scan = v4_scan_assembler(
        "_v4_line_sweep_scan", "samx", -5.0, 5.0, min_update=0.1, relative=False
    )
    scan.prepare_scan()
    done_status = nth_done_status_mock(resolve_after=4)
    scan.device.set = mock.MagicMock(return_value=done_status)
    scan.at_each_point = mock.MagicMock()
    scan.redis_connector.unregister = mock.MagicMock()

    def register_readback(endpoint, cb):
        assert endpoint == MessageEndpoints.device_readback("samx")
        cb(_device_readback_message("samx", 1.0))

    scan.redis_connector.register = mock.MagicMock(side_effect=register_readback)
    with mock.patch("bec_server.scan_server.scans.line_sweep_scan.time.sleep") as sleep_mock:
        scan.scan_core()

    scan.device.set.assert_called_once_with(5.0)
    scan.redis_connector.register.assert_called_once_with(
        MessageEndpoints.device_readback("samx"), cb=scan._device_readback_callback
    )
    scan.redis_connector.unregister.assert_called_once_with(
        MessageEndpoints.device_readback("samx"), cb=scan._device_readback_callback
    )
    scan.at_each_point.assert_called_once_with()
    sleep_mock.assert_called_once_with(0.1)


def test_line_sweep_scan_scan_core_coalesces_multiple_readback_updates(
    v4_scan_assembler, nth_done_status_mock
):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=False)
    scan.prepare_scan()
    done_status = nth_done_status_mock(resolve_after=2)
    scan.device.set = mock.MagicMock(return_value=done_status)
    scan.at_each_point = mock.MagicMock()
    scan.redis_connector.unregister = mock.MagicMock()

    def register_readback(endpoint, cb):
        assert endpoint == MessageEndpoints.device_readback("samx")
        cb(_device_readback_message("samx", 1.0))
        cb(_device_readback_message("samx", 2.0))

    scan.redis_connector.register = mock.MagicMock(side_effect=register_readback)

    scan.scan_core()

    scan.at_each_point.assert_called_once_with()


def test_line_sweep_scan_scan_core_reads_final_pending_update(
    v4_scan_assembler, nth_done_status_mock
):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=False)
    scan.prepare_scan()
    done_status = nth_done_status_mock(resolve_after=2)
    scan.device.set = mock.MagicMock(return_value=done_status)
    scan.at_each_point = mock.MagicMock()
    scan.redis_connector.unregister = mock.MagicMock()

    def register_readback(endpoint, cb):
        assert endpoint == MessageEndpoints.device_readback("samx")
        cb(_device_readback_message("samx", 1.0))

    scan.redis_connector.register = mock.MagicMock(side_effect=register_readback)

    scan.scan_core()

    scan.at_each_point.assert_called_once_with()


def test_line_sweep_scan_scan_core_waits_for_event_when_no_update(
    v4_scan_assembler, nth_done_status_mock
):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=False)
    scan.prepare_scan()
    done_status = nth_done_status_mock(resolve_after=2)
    scan.device.set = mock.MagicMock(return_value=done_status)
    scan.at_each_point = mock.MagicMock()
    scan.redis_connector.unregister = mock.MagicMock()
    wait_calls = []

    def wait(timeout):
        wait_calls.append(timeout)
        return False

    scan._readback_update_event.wait = mock.MagicMock(side_effect=wait)
    scan.redis_connector.register = mock.MagicMock()

    scan.scan_core()

    scan._readback_update_event.wait.assert_called_once_with(timeout=0.05)
    assert wait_calls == [0.05]
    scan.at_each_point.assert_not_called()


def test_line_sweep_scan_scan_core_triggers_read_when_max_update_expires(
    v4_scan_assembler, nth_done_status_mock
):
    scan = v4_scan_assembler(
        "_v4_line_sweep_scan", "samx", -5.0, 5.0, max_update=0.1, relative=False
    )
    scan.prepare_scan()
    done_status = nth_done_status_mock(resolve_after=3)
    scan.device.set = mock.MagicMock(return_value=done_status)
    scan.at_each_point = mock.MagicMock()
    scan.redis_connector.unregister = mock.MagicMock()
    scan.redis_connector.register = mock.MagicMock()
    scan._readback_update_event.wait = mock.MagicMock(side_effect=[False, False])

    with mock.patch(
        "bec_server.scan_server.scans.line_sweep_scan.time.time",
        side_effect=[100.0, 100.05, 100.2, 100.2],
    ):
        scan.scan_core()

    assert scan._readback_update_event.wait.call_args_list == [
        mock.call(timeout=0.05),
        mock.call(timeout=0.05),
    ]
    scan.at_each_point.assert_called_once_with()


def test_line_sweep_scan_device_readback_callback_sets_update_event(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=False)
    assert scan._readback_update_event.is_set() is False

    scan._device_readback_callback(_device_readback_message("samx", 2.0))

    assert scan._readback_update_event.is_set() is True


def test_line_sweep_scan_post_scan_moves_back_when_relative(
    v4_scan_assembler, nth_done_status_mock
):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=True)
    completion_status = nth_done_status_mock(resolve_after=2)
    scan.start_positions = [1.0]
    scan.actions.complete_all_devices = mock.MagicMock(return_value=completion_status)
    scan.components.move_and_wait = mock.MagicMock()

    scan.post_scan()

    scan.actions.complete_all_devices.assert_called_once_with(wait=False)
    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
    assert completion_status.wait_calls == 1


def test_line_sweep_scan_on_exception_moves_back_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_line_sweep_scan", "samx", -5.0, 5.0, relative=True)
    scan.start_positions = [1.0]
    scan.components.move_and_wait = mock.MagicMock()

    scan.on_exception(RuntimeError("boom"))

    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
