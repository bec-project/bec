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

TIME_SCAN_DEFAULT_HOOK_TESTS = [
    ("prepare_scan", [assert_prepare_scan_reads_baseline_devices]),
    ("open_scan", [assert_scan_open_called]),
    ("stage", [assert_stage_all_devices_called]),
    ("pre_scan", [assert_pre_scan_called]),
    ("unstage", [assert_unstage_all_devices_called]),
    ("close_scan", [assert_close_scan_waits_for_baseline_and_closes]),
]


@pytest.mark.parametrize(("hook_name", "hook_tests"), TIME_SCAN_DEFAULT_HOOK_TESTS)
def test_time_scan_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler("_v4_time_scan", 3, 1.5, exp_time=0.2)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_time_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_time_scan", 3, 1.5, exp_time=0.2)

    scan.prepare_scan()

    assert scan.scan_info.num_points == 3
    assert scan.scan_info.positions.size == 0
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": 3, "show_table": False}}
    ]


def test_time_scan_scan_core_triggers_reads_and_waits_between_points(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_time_scan", 3, 1.5, exp_time=0.2)
    scan.at_each_point = mock.MagicMock()

    with mock.patch("bec_server.scan_server.scans.time_scan.time.sleep") as sleep_mock:
        scan.scan_core()

    assert scan.at_each_point.call_count == 3
    assert sleep_mock.call_args_list == [mock.call(1.3), mock.call(1.3)]


def test_time_scan_at_each_point_triggers_and_reads(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_time_scan", 3, 1.5, exp_time=0.2)
    scan.components.trigger_and_read = mock.MagicMock()

    scan.at_each_point()

    scan.components.trigger_and_read.assert_called_once_with()


def test_time_scan_post_scan_completes_all_devices(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_time_scan", 3, 1.5, exp_time=0.2)
    scan.actions.complete_all_devices = mock.MagicMock()

    scan.post_scan()

    scan.actions.complete_all_devices.assert_called_once_with()
