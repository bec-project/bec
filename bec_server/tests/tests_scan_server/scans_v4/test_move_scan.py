from unittest import mock

import pytest


@pytest.mark.parametrize(
    ("hook_name",),
    [("open_scan",), ("stage",), ("pre_scan",), ("post_scan",), ("unstage",), ("close_scan",)],
)
def test_move_scan_default_noop_hooks_do_not_raise(v4_scan_assembler, hook_name):
    scan = v4_scan_assembler("mv", "samx", 1.5, "samy", -2.0)

    getattr(scan, hook_name)()


def test_move_scan_prepare_scan_registers_required_response_devices(v4_scan_assembler):
    scan = v4_scan_assembler("mv", "samx", 1.5, "samy", -2.0)
    scan.actions.add_device_with_required_response = mock.MagicMock()

    scan.prepare_scan()

    scan.actions.add_device_with_required_response.assert_called_once_with(scan.motors)


def test_move_scan_scan_core_sets_absolute_targets_without_wait(v4_scan_assembler):
    scan = v4_scan_assembler("mv", "samx", 1.5, "samy", -2.0)
    scan.actions.set = mock.MagicMock()

    scan.scan_core()

    scan.actions.set.assert_called_once_with(scan.motors, [1.5, -2.0], wait=False)


def test_move_scan_scan_core_sets_relative_targets_without_wait(v4_scan_assembler):
    scan = v4_scan_assembler("mv", "samx", 1.5, "samy", -2.0, relative=True)
    scan.components.get_start_positions = mock.MagicMock(return_value=[0.5, 3.0])
    scan.actions.set = mock.MagicMock()

    scan.scan_core()

    scan.components.get_start_positions.assert_called_once_with(scan.motors)
    scan.actions.set.assert_called_once_with(scan.motors, [2.0, 1.0], wait=False)
