import numpy as np

from bec_server.scan_server.scans import position_generators


def test_rotate_points_rotates_2d_positions():
    points = np.array([[1.0, 0.0], [0.0, 2.0]])

    rotated = position_generators.rotate_points(points, np.pi / 2)

    assert np.allclose(rotated, [[0.0, 1.0], [-2.0, 0.0]])


def test_rotate_points_rotates_around_custom_center():
    points = np.array([[2.0, 1.0]])

    rotated = position_generators.rotate_points(points, np.pi / 2, center=(1.0, 1.0))

    assert np.allclose(rotated, [[1.0, 2.0]])


def test_line_scan_positions_generates_linear_trajectory():
    positions = position_generators.line_scan_positions([(-1.0, 1.0), (-2.0, 2.0)], steps=5)

    assert np.allclose(positions, [[-1.0, -2.0], [-0.5, -1.0], [0.0, 0.0], [0.5, 1.0], [1.0, 2.0]])


def test_log_scan_positions_generates_log_trajectory():
    positions = position_generators.log_scan_positions([(1.0, 100.0), (10.0, 1000.0)], steps=3)
    middle_progress = (np.sqrt(10) - 1) / 9

    assert np.allclose(
        positions,
        [
            [1.0, 10.0],
            [1.0 + middle_progress * 99.0, 10.0 + middle_progress * 990.0],
            [100.0, 1000.0],
        ],
    )


def test_log_scan_positions_spans_distance_for_zero_crossing_ranges():
    positions = position_generators.log_scan_positions([(0.0, 10.0), (-5.0, 5.0)], steps=3)
    middle_progress = (np.sqrt(10) - 1) / 9

    assert np.allclose(
        positions,
        [[0.0, -5.0], [middle_progress * 10.0, -5.0 + middle_progress * 10.0], [10.0, 5.0]],
    )


def test_log_scan_positions_supports_reverse_ranges():
    positions = position_generators.log_scan_positions([(10.0, 0.0)], steps=3)
    middle_progress = (np.sqrt(10) - 1) / 9

    assert np.allclose(positions, [[10.0], [10.0 - middle_progress * 10.0], [0.0]])


def test_oscillating_positions_cycles_back_and_forth():
    generator = position_generators.oscillating_positions([1.0, 2.0, 3.0])

    values = [next(generator) for _ in range(7)]

    assert values == [1.0, 2.0, 3.0, 2.0, 1.0, 2.0, 3.0]


def test_oscillating_positions_repeats_single_value():
    generator = position_generators.oscillating_positions([5.0])

    values = [next(generator) for _ in range(4)]

    assert values == [5.0, 5.0, 5.0, 5.0]


def test_oscillating_positions_can_repeat_turning_points():
    generator = position_generators.oscillating_positions(
        [1.0, 2.0, 3.0], repeat_turning_points=True
    )

    values = [next(generator) for _ in range(9)]

    assert values == [1.0, 1.0, 2.0, 3.0, 3.0, 2.0, 1.0, 1.0, 2.0]


def test_multi_region_line_positions_concatenates_regions():
    positions = position_generators.multi_region_line_positions([(-5.0, -2.0, 4), (-1.0, 6.0, 4)])

    assert np.allclose(
        positions, [[-5.0], [-4.0], [-3.0], [-2.0], [-1.0], [1.33333333], [3.66666667], [6.0]]
    )


def test_multi_region_grid_positions_builds_snaked_grid():
    positions = position_generators.multi_region_grid_positions(
        [((-3.0, -1.0, 2), (-2.0, 2.0, 3)), ((1.0, 3.0, 2), (-2.0, 2.0, 3))], snaked=True
    )

    assert np.allclose(
        positions,
        [
            [-3.0, -2.0],
            [-1.0, -2.0],
            [-1.0, 0.0],
            [-3.0, 0.0],
            [-3.0, 2.0],
            [-1.0, 2.0],
            [1.0, -2.0],
            [3.0, -2.0],
            [3.0, 0.0],
            [1.0, 0.0],
            [1.0, 2.0],
            [3.0, 2.0],
        ],
    )


def test_multi_region_grid_positions_rejects_empty_region_list():
    with np.testing.assert_raises_regex(ValueError, "at least one paired region"):
        position_generators.multi_region_grid_positions([])


def test_spiral_positions_starts_at_center_and_stays_in_bounds():
    positions = position_generators.spiral_positions(
        x_center=2.0, y_center=-1.0, x_range=4.0, y_range=6.0, dr=0.5, nth=8
    )

    assert len(positions) > 1
    assert np.allclose(positions[0], [2.0, -1.0])
    assert np.all(positions[:, 0] >= 0.0)
    assert np.all(positions[:, 0] <= 4.0)
    assert np.all(positions[:, 1] >= -4.0)
    assert np.all(positions[:, 1] <= 2.0)


def test_spiral_positions_supports_tilt():
    untilted = position_generators.spiral_positions(
        x_center=0.0, y_center=0.0, x_range=6.0, y_range=6.0, dr=0.5, nth=8, tilt=0.0
    )
    tilted = position_generators.spiral_positions(
        x_center=0.0, y_center=0.0, x_range=6.0, y_range=6.0, dr=0.5, nth=8, tilt=np.pi / 4
    )

    assert len(untilted) == len(tilted)
    assert np.allclose(untilted[0], tilted[0])
    assert not np.allclose(untilted[1], tilted[1])


def test_fermat_spiral_positions_are_centered_in_requested_box():
    positions = position_generators.fermat_spiral_pos(10.0, 14.0, -3.0, 1.0, step=0.5, center=True)

    assert len(positions) > 0
    assert np.allclose(positions[0], [12.0, -1.0])
    assert np.all(positions[:, 0] >= 10.0)
    assert np.all(positions[:, 0] <= 14.0)
    assert np.all(positions[:, 1] >= -3.0)
    assert np.all(positions[:, 1] <= 1.0)
