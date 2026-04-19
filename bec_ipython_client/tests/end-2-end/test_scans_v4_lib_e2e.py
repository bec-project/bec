from __future__ import annotations

import time

import numpy as np
import pytest

from bec_server.scan_server.scans import position_generators


def _get_v4_scan_runner(bec, scan_name: str):
    return getattr(bec.scans, f"_v4_{scan_name}")


def _run_v4_scan(
    bec, scan_name: str, *args, timeout: float = 60, wait_for_num_points: bool = True, **kwargs
):
    bec.metadata.update({"unit_test": f"test_v4_{scan_name}_lib"})
    status = _get_v4_scan_runner(bec, scan_name)(*args, **kwargs)
    status.wait(timeout=timeout, num_points=wait_for_num_points, file_written=False)
    return status


def _assert_device_position(device, target: float):
    current = device.read(cached=True)[device.full_name]["value"]
    tolerance = device._config["deviceConfig"].get("tolerance", 0.05)
    assert np.isclose(current, target, atol=tolerance)


def _resolve_scan_args(scan_args: tuple, dev):
    resolved_args = []
    for arg in scan_args:
        if isinstance(arg, str) and arg.startswith("dev."):
            resolved_args.append(getattr(dev, arg.removeprefix("dev.")))
            continue
        resolved_args.append(arg)
    return tuple(resolved_args)


def _wait_for_live_data_count(bec, status, expected_count: int, timeout: float = 5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        bec.callbacks.poll()
        if len(status.scan.live_data) >= expected_count:
            return
        time.sleep(0.1)


def _wait_for_scan_status(status, expected_status: str, timeout: float = 10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if status.status == expected_status:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for scan status {expected_status!r}.")


def _wait_for_queue_status(bec, queue_name: str, expected_status: str, timeout: float = 10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        current_status = bec.queue.queue_storage.current_scan_queue[queue_name].status
        if current_status == expected_status:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for queue status {expected_status!r}.")


@pytest.mark.timeout(120)
@pytest.mark.parametrize(
    ("scan_name", "scan_args", "scan_kwargs", "expected_num_points", "expected_num_readouts"),
    [
        ("acquire", (), {"exp_time": 0.01, "burst_at_each_point": 3}, 1, 3),
        ("line_scan", ("dev.samx", -1, 1), {"steps": 4, "exp_time": 0.01, "relative": False}, 4, 4),
        (
            "grid_scan",
            ("dev.samx", -1, 1, 3, "dev.samy", -1, 1, 2),
            {"exp_time": 0.01, "relative": False},
            6,
            6,
        ),
        (
            "list_scan",
            ("dev.samx", [0, 0.5, 1.0], "dev.samy", [0, -0.5, -1.0]),
            {"exp_time": 0.01, "relative": False},
            3,
            3,
        ),
        ("log_scan", ("dev.samx", 1, 10), {"steps": 4, "exp_time": 0.01, "relative": False}, 4, 4),
        (
            "fermat_scan",
            ("dev.samx", -1, 1, "dev.samy", -1, 1),
            {"step": 1.0, "exp_time": 0.01, "relative": False},
            len(position_generators.fermat_spiral_pos(-1, 1, -1, 1, step=1.0)),
            len(position_generators.fermat_spiral_pos(-1, 1, -1, 1, step=1.0)),
        ),
        (
            "hexagonal_scan",
            ("dev.samx", -1, 1, 1, "dev.samy", -1, 1, 1),
            {"exp_time": 0.01, "relative": False},
            len(position_generators.hex_grid_2d([(-1, 1, 1), (-1, 1, 1)], snaked=True)),
            len(position_generators.hex_grid_2d([(-1, 1, 1), (-1, 1, 1)], snaked=True)),
        ),
        (
            "multi_region_line_scan",
            ("dev.samx",),
            {"regions": [(-1, 0, 2), (1, 2, 2)], "exp_time": 0.01, "relative": False},
            len(position_generators.multi_region_line_positions([(-1, 0, 2), (1, 2, 2)])),
            len(position_generators.multi_region_line_positions([(-1, 0, 2), (1, 2, 2)])),
        ),
        (
            "multi_region_grid_scan",
            ("dev.samx", "dev.samy"),
            {
                "regions": [((-1, 0, 2), (-1, 0, 2)), ((1, 2, 2), (1, 2, 2))],
                "exp_time": 0.01,
                "relative": False,
            },
            len(
                position_generators.multi_region_grid_positions(
                    [((-1, 0, 2), (-1, 0, 2)), ((1, 2, 2), (1, 2, 2))], snaked=True
                )
            ),
            len(
                position_generators.multi_region_grid_positions(
                    [((-1, 0, 2), (-1, 0, 2)), ((1, 2, 2), (1, 2, 2))], snaked=True
                )
            ),
        ),
        (
            "round_scan",
            ("dev.samx", "dev.samy", 0.0, 2.0, 2, 3),
            {"exp_time": 0.01, "relative": False},
            len(
                position_generators.round_scan_positions(
                    inner_radius=0.0,
                    outer_radius=2.0,
                    number_of_rings=2,
                    points_in_first_ring=3,
                )
            ),
            len(
                position_generators.round_scan_positions(
                    inner_radius=0.0,
                    outer_radius=2.0,
                    number_of_rings=2,
                    points_in_first_ring=3,
                )
            ),
        ),
        (
            "round_roi_scan",
            ("dev.samx", -1.0, 1.0, "dev.samy", -1.0, 1.0),
            {"dr": 1.0, "nth": 3, "exp_time": 0.01, "relative": False},
            len(
                position_generators.get_round_roi_scan_positions(
                    motor_1_start=-1.0,
                    motor_1_stop=1.0,
                    motor_2_start=-1.0,
                    motor_2_stop=1.0,
                    radial_step=1.0,
                    points_in_first_shell=3,
                )
            ),
            len(
                position_generators.get_round_roi_scan_positions(
                    motor_1_start=-1.0,
                    motor_1_stop=1.0,
                    motor_2_start=-1.0,
                    motor_2_stop=1.0,
                    radial_step=1.0,
                    points_in_first_shell=3,
                )
            ),
        ),
        ("time_scan", (), {"points": 3, "interval": 0.05, "exp_time": 0.01}, 3, 3),
    ],
)
def test_v4_fixed_point_scans_lib(
    bec_client_lib, scan_name, scan_args, scan_kwargs, expected_num_points, expected_num_readouts
):
    bec = bec_client_lib
    dev = bec.device_manager.devices
    resolved_args = _resolve_scan_args(scan_args, dev)

    status = _run_v4_scan(bec, scan_name, *resolved_args, **scan_kwargs)

    assert status.scan is not None
    assert status.scan.num_points == expected_num_points
    assert status.scan.num_monitored_readouts == expected_num_readouts
    assert len(status.scan.live_data) == expected_num_readouts


@pytest.mark.timeout(120)
def test_v4_mv_scan_lib(bec_client_lib):
    bec = bec_client_lib
    dev = bec.device_manager.devices

    status = _run_v4_scan(bec, "mv", dev.samx, 1.5, dev.samy, -1.5, relative=False)
    status.wait(timeout=30)

    _assert_device_position(dev.samx, 1.5)
    _assert_device_position(dev.samy, -1.5)


@pytest.mark.timeout(120)
def test_v4_umv_scan_lib(bec_client_lib):
    bec = bec_client_lib
    dev = bec.device_manager.devices

    status = _run_v4_scan(bec, "umv", dev.samx, -1.0, dev.samy, 1.0, relative=False)
    status.wait(timeout=30)

    _assert_device_position(dev.samx, -1.0)
    _assert_device_position(dev.samy, 1.0)


@pytest.mark.timeout(120)
def test_v4_cont_line_scan_lib(bec_client_lib):
    bec = bec_client_lib
    dev = bec.device_manager.devices
    original_velocity = dev.samx.velocity.get()
    try:
        dev.samx.velocity.set(1).wait()
        status = _run_v4_scan(
            bec, "cont_line_scan", dev.samx, 0.0, 0.2, steps=3, exp_time=0.01, relative=False
        )
    finally:
        dev.samx.velocity.set(original_velocity).wait()

    assert status.scan is not None
    assert status.scan.num_points == 3
    assert len(status.scan.live_data) == 3


@pytest.mark.timeout(120)
def test_v4_cont_line_fly_scan_lib(bec_client_lib):
    bec = bec_client_lib
    dev = bec.device_manager.devices
    original_velocity = dev.samx.velocity.get()
    try:
        dev.samx.velocity.set(1).wait()
        status = _run_v4_scan(
            bec,
            "cont_line_fly_scan",
            dev.samx,
            0.0,
            0.2,
            exp_time=0.01,
            relative=False,
            wait_for_num_points=False,
        )
    finally:
        dev.samx.velocity.set(original_velocity).wait()

    assert status.scan is not None
    assert len(status.scan.live_data) > 0


@pytest.mark.timeout(120)
def test_v4_line_sweep_scan_lib(bec_client_lib):
    bec = bec_client_lib
    dev = bec.device_manager.devices
    original_velocity = dev.samx.velocity.get()
    try:
        dev.samx.velocity.set(1).wait()
        dev.samx.limits = [-50, 50]
        status = _run_v4_scan(
            bec,
            "line_sweep_scan",
            dev.samx,
            -5.0,
            5.0,
            min_update=0.01,
            relative=False,
            wait_for_num_points=False,
        )
    finally:
        dev.samx.velocity.set(original_velocity).wait()

    assert status.scan is not None
    _wait_for_live_data_count(bec, status, expected_count=1)
    assert len(status.scan.live_data) > 0


@pytest.mark.timeout(120)
def test_v4_scan_lib_stop_resolves_cleanly(bec_client_lib):
    bec = bec_client_lib
    status = _get_v4_scan_runner(bec, "time_scan")(points=100, interval=0.2, exp_time=0.01)

    time.sleep(0.5)
    status.cancel()

    _wait_for_scan_status(status, "STOPPED", timeout=15)
    assert status.status == "STOPPED"
    _wait_for_queue_status(bec, "primary", "PAUSED", timeout=15)

    bec.queue.request_scan_continuation()
    _wait_for_queue_status(bec, "primary", "RUNNING", timeout=15)
