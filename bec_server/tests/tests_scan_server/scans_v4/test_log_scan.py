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
def test_log_scan_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler(
        "_v4_log_scan", "samx", 0.1, 10.0, "samy", 0.01, 1.0, steps=3, relative=False
    )

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_log_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_log_scan", "samx", 0.1, 10.0, "samy", 0.01, 1.0, steps=3, relative=False
    )

    scan.prepare_scan()

    middle_progress = (np.sqrt(10) - 1) / 9
    expected_positions = np.array(
        [[0.1, 0.01], [0.1 + middle_progress * 9.9, 0.01 + middle_progress * 0.99], [10.0, 1.0]]
    )
    assert np.allclose(scan.positions, expected_positions)
    assert scan.scan_info.num_points == 3
    assert np.allclose(scan.scan_info.positions, expected_positions)
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": 3, "show_table": False}}
    ]


def test_log_scan_prepare_scan_offsets_positions_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_log_scan", "samx", -1.0, 1.0, "samy", 0.0, 1.0, steps=3, relative=True
    )
    scan.components.get_start_positions = lambda motors: [2.0, 3.0]

    scan.prepare_scan()

    middle_progress = (np.sqrt(10) - 1) / 9
    expected_positions = np.array(
        [[1.0, 3.0], [1.0 + middle_progress * 2.0, 3.0 + middle_progress], [3.0, 4.0]]
    )
    assert scan.start_positions == [2.0, 3.0]
    assert np.allclose(scan.positions, expected_positions)
