import numpy as np
import pytest

from bec_server.scan_server.tests.scan_hook_tests import (
    DEFAULT_HOOK_TESTS,
    PREMOVE_HOOK_TESTS,
    STANDARD_STEP_SCAN_TESTS,
    run_scan_tests,
)


@pytest.mark.parametrize(
    ("hook_name", "hook_tests"),
    [*DEFAULT_HOOK_TESTS, *PREMOVE_HOOK_TESTS, *STANDARD_STEP_SCAN_TESTS],
)
def test_line_scan_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler("line_scan", "samx", -1.0, 1.0, "samy", -2.0, 2.0, steps=5)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_line_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler("line_scan", "samx", -1.0, 1.0, "samy", -2.0, 2.0, steps=5)

    scan.prepare_scan()

    expected_positions = np.array([[-1.0, -2.0], [-0.5, -1.0], [0.0, 0.0], [0.5, 1.0], [1.0, 2.0]])
    assert np.array_equal(scan.positions, expected_positions)
    assert scan.scan_info.num_points == 5
    assert np.array_equal(scan.scan_info.positions, expected_positions)
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": 5, "show_table": False}}
    ]


def test_line_scan_prepare_scan_offsets_positions_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler(
        "line_scan", "samx", -1.0, 1.0, "samy", -2.0, 2.0, steps=5, relative=True
    )
    scan.components.get_start_positions = lambda motors: [2.0, 3.0]

    scan.prepare_scan()

    expected_positions = np.array([[1.0, 1.0], [1.5, 2.0], [2.0, 3.0], [2.5, 4.0], [3.0, 5.0]])
    assert scan.start_positions == [2.0, 3.0]
    assert np.array_equal(scan.positions, expected_positions)
