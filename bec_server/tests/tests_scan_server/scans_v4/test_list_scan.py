import numpy as np
import pytest

from .scan_test_utils import (
    DEFAULT_HOOK_TESTS,
    PREMOVE_HOOK_TESTS,
    STANDARD_STEP_SCAN_TESTS,
    run_scan_tests,
)


@pytest.mark.parametrize(
    ("hook_name", "hook_tests"),
    [*DEFAULT_HOOK_TESTS, *PREMOVE_HOOK_TESTS, *STANDARD_STEP_SCAN_TESTS],
)
def test_list_scan_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler("list_scan", "samx", [0, 1, 2], "samy", [3, 4, 5], relative=False)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_list_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler("list_scan", "samx", [0, 1, 2], "samy", [3, 4, 5], relative=False)

    scan.prepare_scan()

    expected_positions = np.array([[0.0, 3.0], [1.0, 4.0], [2.0, 5.0]])
    assert np.array_equal(scan.positions, expected_positions)
    assert scan.scan_info.num_points == 3
    assert np.array_equal(scan.scan_info.positions, expected_positions)


def test_list_scan_prepare_scan_offsets_positions_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler("list_scan", "samx", [0, 1, 2], "samy", [3, 4, 5], relative=True)
    scan.components.get_start_positions = lambda motors: [1.0, -1.0]

    scan.prepare_scan()

    expected_positions = np.array([[1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])
    assert scan.start_positions == [1.0, -1.0]
    assert np.array_equal(scan.positions, expected_positions)


def test_list_scan_raises_for_different_lengths(v4_scan_assembler):
    with pytest.raises(ValueError, match="equal length"):
        v4_scan_assembler("list_scan", "samx", [0, 1], "samy", [0, 1, 2], relative=False)
