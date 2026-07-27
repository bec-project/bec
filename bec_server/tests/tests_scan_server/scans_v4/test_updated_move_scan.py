from unittest import mock

import pytest


@pytest.mark.parametrize(
    ("hook_name",),
    [
        ("prepare_scan",),
        ("open_scan",),
        ("stage",),
        ("pre_scan",),
        ("post_scan",),
        ("unstage",),
        ("close_scan",),
    ],
)
def test_updated_move_scan_default_noop_hooks_do_not_raise(v4_scan_assembler, hook_name):
    scan = v4_scan_assembler("umv", "samx", 1.5, "samy", -2.0, relative=False)

    getattr(scan, hook_name)()


def test_updated_move_scan_scan_core_adds_readback_and_moves_to_absolute_targets(v4_scan_assembler):
    scan = v4_scan_assembler("umv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.scan_info.metadata["RID"] = "rid-123"
    scan.components.get_start_positions = mock.MagicMock(return_value=[0.5, 3.0])
    scan.actions.add_scan_report_instruction_readback = mock.MagicMock()
    scan.components.move_and_wait = mock.MagicMock()

    scan.scan_core()

    scan.components.get_start_positions.assert_called_once_with(scan.motors)
    scan.actions.add_scan_report_instruction_readback.assert_called_once_with(
        devices=scan.motors, start=[0.5, 3.0], stop=[1.5, -2.0], request_id="rid-123"
    )
    scan.components.move_and_wait.assert_called_once_with(scan.motors, [1.5, -2.0])


def test_updated_move_scan_scan_core_adds_readback_and_moves_to_relative_targets(v4_scan_assembler):
    scan = v4_scan_assembler("umv", "samx", 1.5, "samy", -2.0, relative=True)
    scan.scan_info.metadata["RID"] = "rid-123"
    scan.components.get_start_positions = mock.MagicMock(return_value=[0.5, 3.0])
    scan.actions.add_scan_report_instruction_readback = mock.MagicMock()
    scan.components.move_and_wait = mock.MagicMock()

    scan.scan_core()

    scan.components.get_start_positions.assert_called_once_with(scan.motors)
    scan.actions.add_scan_report_instruction_readback.assert_called_once_with(
        devices=scan.motors, start=[0.5, 3.0], stop=[2.0, 1.0], request_id="rid-123"
    )
    scan.components.move_and_wait.assert_called_once_with(scan.motors, [2.0, 1.0])
