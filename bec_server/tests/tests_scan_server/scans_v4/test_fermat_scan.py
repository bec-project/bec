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
def test_fermat_scan_default_hooks(v4_scan_assembler, nth_done_status_mock, hook_name, hook_tests):
    scan = v4_scan_assembler(
        "fermat_scan", "samx", -1.0, 1.0, "samy", -2.0, 2.0, step=0.5, relative=False
    )

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


def test_fermat_scan_prepare_scan_updates_scan_info_and_queue(v4_scan_assembler):
    scan = v4_scan_assembler(
        "fermat_scan", "samx", -1.0, 1.0, "samy", -2.0, 2.0, step=0.5, relative=False
    )

    scan.prepare_scan()

    assert isinstance(scan.positions, np.ndarray)
    assert scan.positions.shape[1] == 2
    assert scan.scan_info.num_points == len(scan.positions)
    assert np.array_equal(scan.scan_info.positions, scan.positions)
    assert scan.scan_info.scan_report_instructions == [
        {"scan_progress": {"points": len(scan.positions), "show_table": False}}
    ]

    read_messages = [
        entry["msg"]
        for entry in scan._test.connector.message_sent
        if getattr(entry["msg"], "action", None) == "read"
    ]
    assert len(read_messages) == 1
    assert read_messages[0].device == ["samz"]
    assert read_messages[0].metadata["readout_priority"] == "baseline"
