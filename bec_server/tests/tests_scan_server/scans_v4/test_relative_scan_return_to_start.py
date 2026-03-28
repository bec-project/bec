from unittest import mock

import pytest

RELATIVE_SCAN_CASES = [
    ("_v4_cont_line_scan", ("samx", -1.0, 1.0), {"steps": 3, "exp_time": 0.1, "relative": True}),
    ("_v4_fermat_scan", ("samx", -1.0, 1.0, "samy", -2.0, 2.0), {"step": 0.5, "relative": True}),
    (
        "_v4_grid_scan",
        ("samx", -1.0, 1.0, 3, "samy", -2.0, 2.0, 5),
        {"snaked": True, "relative": True},
    ),
    ("_v4_hexagonal_scan", ("samx", -1.0, 1.0, 1.0, "samy", -1.0, 1.0, 1.0), {"relative": True}),
    ("_v4_line_scan", ("samx", -1.0, 1.0, "samy", -2.0, 2.0), {"steps": 5, "relative": True}),
    ("_v4_list_scan", ("samx", [0, 1, 2], "samy", [3, 4, 5]), {"relative": True}),
    ("_v4_log_scan", ("samx", 0.1, 1.0, "samy", 0.01, 1.0), {"steps": 3, "relative": True}),
    ("_v4_line_sweep_scan", ("samx", -5.0, 5.0), {"relative": True}),
    (
        "_v4_multi_region_grid_scan",
        ("samx", "samy"),
        {
            "regions": [((-5.0, -1.0, 5), (-4.0, 0.0, 5)), ((1.0, 5.0, 3), (-4.0, 0.0, 5))],
            "snaked": True,
            "relative": True,
        },
    ),
    (
        "_v4_multi_region_line_scan",
        ("samx",),
        {"regions": [(-5.0, -2.0, 4), (-1.0, 6.0, 4)], "relative": True},
    ),
    (
        "_v4_round_roi_scan",
        ("samx", -3.0, 3.0, "samy", -2.0, 2.0),
        {"shell_spacing": 1.0, "pos_in_first_ring": 4, "relative": True},
    ),
    ("_v4_round_scan", ("samx", "samy", 0.0, 2.0, 2, 3), {"relative": True}),
]


@pytest.mark.parametrize(("scan_type", "scan_args", "scan_kwargs"), RELATIVE_SCAN_CASES)
def test_relative_v4_scan_on_exception_moves_back_to_start(
    v4_scan_assembler, scan_type, scan_args, scan_kwargs
):
    scan = v4_scan_assembler(scan_type, *scan_args, **scan_kwargs)
    scan.start_positions = [float(index + 1) for index, _motor in enumerate(scan.motors)]
    scan.components.move_and_wait = mock.MagicMock()

    scan.on_exception(RuntimeError("boom"))

    scan.components.move_and_wait.assert_called_once_with(scan.motors, scan.start_positions)
