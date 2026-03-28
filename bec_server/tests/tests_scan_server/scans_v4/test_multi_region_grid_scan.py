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
def test_multi_region_grid_scan_default_hooks(
    v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests
):
    scan = v4_scan_assembler(
        "_v4_multi_region_grid_scan",
        "samx",
        "samy",
        regions=[[(-3.0, -1.0, 2), (-2.0, 2.0, 3)], [(1.0, 3.0, 2), (-2.0, 2.0, 3)]],
        snaked=True,
        relative=False,
    )

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_multi_region_grid_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_multi_region_grid_scan",
        "samx",
        "samy",
        regions=[[(-3.0, -1.0, 2), (-2.0, 2.0, 3)], [(1.0, 3.0, 2), (-2.0, 2.0, 3)]],
        snaked=True,
        relative=False,
    )

    scan.prepare_scan()

    expected_positions = np.array(
        [
            [-3.0, -2.0],
            [-3.0, 0.0],
            [-3.0, 2.0],
            [-1.0, 2.0],
            [-1.0, 0.0],
            [-1.0, -2.0],
            [1.0, -2.0],
            [1.0, 0.0],
            [1.0, 2.0],
            [3.0, 2.0],
            [3.0, 0.0],
            [3.0, -2.0],
        ]
    )
    assert np.allclose(scan.positions, expected_positions)
    assert scan.scan_info.num_points == len(expected_positions)
    assert np.allclose(scan.scan_info.positions, expected_positions)
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": len(expected_positions), "show_table": False}}
    ]


def test_multi_region_grid_scan_prepare_scan_offsets_positions_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_multi_region_grid_scan",
        "samx",
        "samy",
        regions=[[(-3.0, -1.0, 2), (-2.0, 2.0, 3)], [(1.0, 3.0, 2), (-2.0, 2.0, 3)]],
        snaked=True,
        relative=True,
    )
    scan.components.get_start_positions = lambda motors: [1.0, -1.0]

    scan.prepare_scan()

    expected_positions = np.array(
        [
            [-2.0, -3.0],
            [-2.0, -1.0],
            [-2.0, 1.0],
            [0.0, 1.0],
            [0.0, -1.0],
            [0.0, -3.0],
            [2.0, -3.0],
            [2.0, -1.0],
            [2.0, 1.0],
            [4.0, 1.0],
            [4.0, -1.0],
            [4.0, -3.0],
        ]
    )
    assert scan.start_positions == [1.0, -1.0]
    assert np.allclose(scan.positions, expected_positions)


def test_multi_region_grid_scan_prepare_scan_rejects_empty_region_list(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_multi_region_grid_scan", "samx", "samy", regions=[], snaked=True, relative=False
    )

    with pytest.raises(ValueError, match="at least one paired region"):
        scan.prepare_scan()
