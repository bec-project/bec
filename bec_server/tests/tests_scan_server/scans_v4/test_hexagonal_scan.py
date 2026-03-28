import numpy as np
import pytest

from bec_server.scan_server.scans import position_generators
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
def test_hexagonal_scan_default_hooks(
    v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests
):
    scan = v4_scan_assembler(
        "_v4_hexagonal_scan", "samx", -1.0, 1.0, 1.0, "samy", -1.0, 1.0, 1.0, relative=False
    )

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_hexagonal_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_hexagonal_scan", "samx", -1.0, 1.0, 1.0, "samy", -1.0, 1.0, 1.0, relative=False
    )

    scan.prepare_scan()

    expected_positions = position_generators.hex_grid_2d(
        [(-1.0, 1.0, 1.0), (-1.0, 1.0, 1.0)], snaked=True
    )
    assert np.array_equal(scan.positions, expected_positions)
    assert scan.scan_info.num_points == len(expected_positions)
    assert np.array_equal(scan.scan_info.positions, expected_positions)


def test_hexagonal_scan_prepare_scan_offsets_positions_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler(
        "_v4_hexagonal_scan", "samx", -1.0, 1.0, 1.0, "samy", -1.0, 1.0, 1.0, relative=True
    )
    scan.components.get_start_positions = lambda motors: [5.0, -2.0]

    scan.prepare_scan()

    expected_positions = position_generators.hex_grid_2d(
        [(-1.0, 1.0, 1.0), (-1.0, 1.0, 1.0)], snaked=True
    ) + [5.0, -2.0]
    assert scan.start_positions == [5.0, -2.0]
    assert np.array_equal(scan.positions, expected_positions)
