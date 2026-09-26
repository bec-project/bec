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
def test_round_scan_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler("round_scan", "samx", "samy", 0.0, 2.0, 2, 3, relative=False)

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_round_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler(
        "round_scan", "samx", "samy", 0.0, 2.0, 2, 3, relative=False, burst_at_each_point=2
    )

    scan.prepare_scan()

    np.testing.assert_allclose(np.linalg.norm(scan.positions, axis=1), [1] * 3 + [2] * 6)
    assert scan.scan_info.num_points == 9
    assert scan.scan_info.num_monitored_readouts == 18
    assert np.array_equal(scan.scan_info.positions, scan.positions)


def test_round_scan_prepare_scan_offsets_positions_when_relative(v4_scan_assembler):
    scan = v4_scan_assembler(
        "round_scan", "samx", "samy", 0.0, 2.0, 2, 3, relative=True, center_1=2.0, center_2=3.0
    )
    scan.components.get_start_positions = lambda motors: [1.0, -1.0]

    scan.prepare_scan()

    assert scan.start_positions == [1.0, -1.0]
    np.testing.assert_allclose(
        np.linalg.norm(scan.positions - [3.0, 2.0], axis=1), [1] * 3 + [2] * 6
    )


@pytest.mark.parametrize("inner_radius, outer_radius", [(2.0, 1.0), (1.0, 1.0)])
@pytest.mark.parametrize("relative", [False, True])
def test_round_scan_rejects_invalid_radii_without_moving(
    v4_scan_assembler, inner_radius, outer_radius, relative
):
    scan = v4_scan_assembler(
        "round_scan", "samx", "samy", inner_radius, outer_radius, 2, 3, relative=relative
    )

    with mock.patch.object(scan.actions, "set") as move:
        with pytest.raises(ValueError, match="0 <= inner_radius < outer_radius") as exc_info:
            scan.prepare_scan()
        scan.on_exception(exc_info.value)

    move.assert_not_called()
