from unittest import mock


def assert_prepare_scan_reads_baseline_devices(scan):
    baseline_status = mock.MagicMock()
    scan.actions.read_baseline_devices = mock.MagicMock(return_value=baseline_status)

    scan.prepare_scan()

    scan.actions.read_baseline_devices.assert_called_once_with(wait=False)
    assert scan._baseline_readout_status is baseline_status


def assert_prepare_scan_starts_premove_move(scan):
    premove_status = mock.MagicMock()
    scan.actions.set = mock.MagicMock(return_value=premove_status)

    scan.prepare_scan()

    assert scan.actions.set.call_count >= 1
    assert any(call.kwargs.get("wait") is False for call in scan.actions.set.call_args_list)
    assert scan._premove_motor_status is premove_status


def assert_scan_open_called(scan):
    scan.actions.open_scan = mock.MagicMock()

    scan.open_scan()

    scan.actions.open_scan.assert_called_once_with()


def assert_stage_all_devices_called(scan):
    scan.actions.stage_all_devices = mock.MagicMock()

    scan.stage()

    scan.actions.stage_all_devices.assert_called_once_with()


def assert_pre_scan_called(scan):
    scan._premove_motor_status = mock.MagicMock()
    scan.actions.pre_scan_all_devices = mock.MagicMock()

    scan.pre_scan()

    scan.actions.pre_scan_all_devices.assert_called_once_with()


def assert_pre_scan_waits_for_premove(scan):
    premove_status = mock.MagicMock()
    scan._premove_motor_status = premove_status
    scan.actions.pre_scan_all_devices = mock.MagicMock()

    scan.pre_scan()

    premove_status.wait.assert_called_once_with()
    scan.actions.pre_scan_all_devices.assert_called_once_with()


def assert_unstage_all_devices_called(scan):
    scan.actions.unstage_all_devices = mock.MagicMock()

    scan.unstage()

    scan.actions.unstage_all_devices.assert_called_once_with()


def assert_close_scan_waits_for_baseline_and_closes(scan, nth_done_status_mock):
    baseline_status = nth_done_status_mock(resolve_after=2)
    scan._baseline_readout_status = baseline_status
    scan.actions.close_scan = mock.MagicMock()
    scan.actions.check_for_unchecked_statuses = mock.MagicMock()

    scan.close_scan()

    assert baseline_status.wait_calls == 1
    scan.actions.close_scan.assert_called_once_with()
    scan.actions.check_for_unchecked_statuses.assert_called_once_with()


def assert_scan_core_delegates_to_step_scan(scan):
    scan.prepare_scan()
    scan.components.step_scan = mock.MagicMock()

    scan.scan_core()

    scan.components.step_scan.assert_called_once()
    args, kwargs = scan.components.step_scan.call_args
    assert args == (scan.motors, scan.scan_info.positions)
    assert kwargs["at_each_point"] == scan.at_each_point
    if "last_positions" in kwargs:
        assert (kwargs["last_positions"] == scan.positions[0]).all()


def assert_post_scan_waits_for_completion_and_moves_back_when_relative(scan, nth_done_status_mock):
    completion_status = nth_done_status_mock(resolve_after=3)
    scan.relative = True
    scan.start_positions = [1.2, -0.7]
    scan.actions.complete_all_devices = mock.MagicMock(return_value=completion_status)
    scan.components.move_and_wait = mock.MagicMock()

    scan.post_scan()

    scan.actions.complete_all_devices.assert_called_once_with(wait=False)
    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
    assert completion_status.wait_calls == 1


DEFAULT_HOOK_TESTS = [
    ("prepare_scan", [assert_prepare_scan_reads_baseline_devices]),
    ("open_scan", [assert_scan_open_called]),
    ("stage", [assert_stage_all_devices_called]),
    ("pre_scan", [assert_pre_scan_called]),
    ("unstage", [assert_unstage_all_devices_called]),
    ("close_scan", [assert_close_scan_waits_for_baseline_and_closes]),
]


PREMOVE_HOOK_TESTS = [
    ("prepare_scan", [assert_prepare_scan_starts_premove_move]),
    ("pre_scan", [assert_pre_scan_waits_for_premove]),
]


STANDARD_STEP_SCAN_TESTS = [
    ("scan_core", [assert_scan_core_delegates_to_step_scan]),
    ("post_scan", [assert_post_scan_waits_for_completion_and_moves_back_when_relative]),
]


def run_scan_tests(scan, tests, nth_done_status_mock=None):
    for test_name, assertions in tests:
        for assertion in assertions:
            if test_name in {"close_scan", "post_scan"}:
                assertion(scan, nth_done_status_mock)
            else:
                assertion(scan)
