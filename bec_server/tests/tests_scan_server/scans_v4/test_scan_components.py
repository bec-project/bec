from unittest import mock

import numpy as np
import pytest

from bec_server.scan_server.errors import LimitError


def test_move_and_wait_only_moves_changed_motors(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.actions.set = mock.MagicMock()

    scan.components.move_and_wait(
        scan.motors, np.array([1.0, 2.5]), last_positions=np.array([1.0, 2.0])
    )

    scan.actions.set.assert_called_once_with([scan.dev["samy"]], [2.5], wait=True)


def test_move_and_wait_skips_set_when_positions_do_not_change(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.actions.set = mock.MagicMock()

    scan.components.move_and_wait(
        scan.motors, np.array([1.0, 2.0]), last_positions=np.array([1.0, 2.0])
    )

    scan.actions.set.assert_not_called()


def test_trigger_and_read_waits_triggers_and_reads(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.scan_info.exp_time = 0.2
    scan.scan_info.frames_per_trigger = 3
    scan.scan_info.settling_time = 0.1
    scan.scan_info.settling_time_after_trigger = 0.4
    scan.actions.trigger_all_devices = mock.MagicMock()
    scan.actions.read_monitored_devices = mock.MagicMock()

    with mock.patch("bec_server.scan_server.scans.scan_components.time.sleep") as sleep_mock:
        scan.components.trigger_and_read()

    sleep_mock.assert_has_calls([mock.call(0.1), mock.call(0.4)])
    scan.actions.trigger_all_devices.assert_called_once()
    assert scan.actions.trigger_all_devices.call_args.kwargs["min_wait"] == pytest.approx(0.6)
    scan.actions.read_monitored_devices.assert_called_once_with()


def test_step_scan_reuses_previous_position_across_points_and_bursts(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.scan_info.burst_at_each_point = 2
    at_each_point = mock.MagicMock()
    positions = np.array([[1.0, 2.0], [3.0, 4.0]])

    scan.components.step_scan(scan.motors, positions, at_each_point=at_each_point)

    assert at_each_point.call_count == 4
    first_args, first_kwargs = at_each_point.call_args_list[0]
    assert first_args[0] == scan.motors
    np.testing.assert_allclose(first_args[1], positions[0])
    assert first_kwargs["last_positions"] is None

    np.testing.assert_allclose(at_each_point.call_args_list[1].args[1], positions[0])
    np.testing.assert_allclose(
        at_each_point.call_args_list[1].kwargs["last_positions"], positions[0]
    )
    np.testing.assert_allclose(at_each_point.call_args_list[2].args[1], positions[1])
    np.testing.assert_allclose(
        at_each_point.call_args_list[2].kwargs["last_positions"], positions[0]
    )
    np.testing.assert_allclose(at_each_point.call_args_list[3].args[1], positions[1])
    np.testing.assert_allclose(
        at_each_point.call_args_list[3].kwargs["last_positions"], positions[1]
    )


def test_step_scan_at_each_point_moves_then_triggers(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.components.move_and_wait = mock.MagicMock()
    scan.components.trigger_and_read = mock.MagicMock()
    pos = np.array([1.0, 2.0])
    last_positions = np.array([0.5, 1.5])

    scan.components.step_scan_at_each_point(scan.motors, pos, last_positions=last_positions)

    scan.components.move_and_wait.assert_called_once_with(
        scan.motors, pos, last_positions=last_positions
    )
    scan.components.trigger_and_read.assert_called_once_with()


def test_get_start_positions_supports_motor_names_and_instances(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.dev["samx"]._value = 1.25
    scan.dev["samy"]._value = -3.5

    start_positions = scan.components.get_start_positions(["samx", scan.dev["samy"]])

    assert start_positions == [1.25, -3.5]


def test_optimize_trajectory_uses_corridor_defaults(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    positions = np.array([[0.0, 1.0], [1.0, 0.0]])
    optimized = np.array([[1.0, 0.0], [0.0, 1.0]])
    scan.components._path_optimizer.optimize_corridor = mock.MagicMock(return_value=optimized)

    result = scan.components.optimize_trajectory(
        positions, optimization_type="corridor", corridor_size=4, num_iterations=7
    )

    scan.components._path_optimizer.optimize_corridor.assert_called_once_with(
        positions,
        num_iterations=7,
        corridor_size=4,
        fast_axis=1,
        first_corridor_direction=1,
        snaked=True,
    )
    np.testing.assert_allclose(result, optimized)


def test_optimize_trajectory_passes_first_direction_for_corridor(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    positions = np.array([[0.0, 1.0], [1.0, 0.0]])
    scan.components._path_optimizer.optimize_corridor = mock.MagicMock(return_value=positions)

    scan.components.optimize_trajectory(
        positions,
        optimization_type="corridor",
        fast_axis=0,
        first_direction=-1,
        snaked=True,
        corridor_size=2,
        num_iterations=3,
    )

    scan.components._path_optimizer.optimize_corridor.assert_called_once_with(
        positions,
        num_iterations=3,
        fast_axis=0,
        first_corridor_direction=-1,
        snaked=True,
        corridor_size=2,
    )


def test_optimize_trajectory_passes_first_direction_when_first_axis_is_corridor_axis(
    v4_scan_assembler,
):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    positions = np.array([[0.0, 1.0], [1.0, 0.0]])
    scan.components._path_optimizer.optimize_corridor = mock.MagicMock(return_value=positions)

    scan.components.optimize_trajectory(
        positions,
        optimization_type="corridor",
        fast_axis=1,
        first_direction=1,
        snaked=True,
        corridor_size=2,
        num_iterations=3,
    )

    scan.components._path_optimizer.optimize_corridor.assert_called_once_with(
        positions,
        num_iterations=3,
        fast_axis=1,
        first_corridor_direction=1,
        snaked=True,
        corridor_size=2,
    )


@pytest.mark.parametrize(
    ("optimization_type", "optimizer_name"),
    [("shell", "optimize_shell"), ("nearest", "optimize_nearest_neighbor")],
)
def test_optimize_trajectory_dispatches_to_other_optimizers(
    v4_scan_assembler, optimization_type, optimizer_name
):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    positions = np.array([[0.0, 1.0], [1.0, 0.0]])
    optimized = np.array([[1.0, 0.0], [0.0, 1.0]])
    optimizer = mock.MagicMock(return_value=optimized)
    setattr(scan.components._path_optimizer, optimizer_name, optimizer)

    result = scan.components.optimize_trajectory(
        positions, optimization_type=optimization_type, num_iterations=4
    )

    if optimization_type == "shell":
        optimizer.assert_called_once_with(positions, num_iterations=4)
    else:
        optimizer.assert_called_once_with(positions)
    np.testing.assert_allclose(result, optimized)


def test_optimize_trajectory_rejects_unknown_optimization_type(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)

    with pytest.raises(ValueError, match="Invalid optimization type"):
        scan.components.optimize_trajectory(np.array([[0.0, 1.0]]), optimization_type="bad")


def test_check_limits_accepts_positions_inside_motor_limits(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)

    scan.components.check_limits(scan.motors, np.array([[0.0, -1.0], [1.0, 2.0]]))


def test_check_limits_ignores_motors_without_configured_limits(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)
    scan.dev["samx"]._limits = (5.0, 5.0)

    scan.components.check_limits(scan.motors, np.array([[100.0, -1.0], [200.0, 2.0]]))


def test_check_limits_raises_limit_error_for_out_of_bounds_position(v4_scan_assembler):
    scan = v4_scan_assembler("_v4_mv", "samx", 1.5, "samy", -2.0, relative=False)

    with pytest.raises(LimitError, match="Target position 12.0"):
        scan.components.check_limits(scan.motors, np.array([[12.0, 0.0]]))
