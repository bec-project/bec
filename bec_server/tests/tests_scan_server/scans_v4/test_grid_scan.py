from unittest import mock

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
def test_grid_scan_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler("grid_scan", "samx", -1.0, 1.0, 3, "samy", -2.0, 2.0, 5, snaked=True)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_grid_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler("grid_scan", "samx", -1.0, 1.0, 3, "samy", -2.0, 2.0, 5, snaked=True)

    scan.prepare_scan()

    assert isinstance(scan.positions, np.ndarray)
    assert scan.positions.shape == (15, 2)
    assert scan.scan_info.num_points == 15
    assert np.array_equal(scan.scan_info.positions, scan.positions)
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": 15, "show_table": False}}
    ]
