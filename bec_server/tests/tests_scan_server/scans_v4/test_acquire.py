from unittest import mock

import pytest

from bec_server.scan_server.tests.scan_hook_tests import (
    assert_close_scan_waits_for_baseline_and_closes,
    assert_pre_scan_called,
    assert_prepare_scan_reads_baseline_devices,
    assert_scan_open_called,
    assert_stage_all_devices_called,
    assert_unstage_all_devices_called,
    run_scan_tests,
)

ACQUIRE_DEFAULT_HOOK_TESTS = [
    ("prepare_scan", [assert_prepare_scan_reads_baseline_devices]),
    ("open_scan", [assert_scan_open_called]),
    ("stage", [assert_stage_all_devices_called]),
    ("pre_scan", [assert_pre_scan_called]),
    ("unstage", [assert_unstage_all_devices_called]),
    ("close_scan", [assert_close_scan_waits_for_baseline_and_closes]),
]


@pytest.mark.parametrize(("hook_name", "hook_tests"), ACQUIRE_DEFAULT_HOOK_TESTS)
def test_acquire_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler("acquire", exp_time=0.2, burst_at_each_point=3)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_acquire_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler("acquire", exp_time=0.2, burst_at_each_point=3)

    scan.prepare_scan()

    assert scan.scan_info.num_points == 1
    assert scan.scan_info.num_monitored_readouts == 3
    assert scan.scan_info.positions.size == 0
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": 3, "show_table": False}}
    ]


def test_acquire_scan_core_triggers_and_reads_for_each_burst(v4_scan_assembler):
    scan = v4_scan_assembler("acquire", exp_time=0.2, burst_at_each_point=3)
    scan.at_each_point = mock.MagicMock()

    scan.scan_core()

    assert scan.at_each_point.call_count == 3


def test_acquire_at_each_point_triggers_and_reads(v4_scan_assembler):
    scan = v4_scan_assembler("acquire", exp_time=0.2, burst_at_each_point=3)
    scan.components.trigger_and_read = mock.MagicMock()

    scan.at_each_point()

    scan.components.trigger_and_read.assert_called_once_with()


def test_acquire_post_scan_completes_all_devices(v4_scan_assembler):
    scan = v4_scan_assembler("acquire", exp_time=0.2, burst_at_each_point=3)
    scan.actions.complete_all_devices = mock.MagicMock()

    scan.post_scan()

    scan.actions.complete_all_devices.assert_called_once_with()
