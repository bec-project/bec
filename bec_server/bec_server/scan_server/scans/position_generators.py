from __future__ import annotations

from collections.abc import Iterator, Sequence

import numpy as np


def rotate_points(
    points: np.ndarray, angle: float, center: tuple[float, float] | None = None
) -> np.ndarray:
    """
    Rotate 2D points around a center.

    Args:
        points (np.ndarray): Array of shape ``(N, 2)`` containing x/y positions.
        angle (float): Rotation angle in radians.
        center (tuple[float, float] | None): Optional center of rotation. If omitted,
            the points are rotated around the origin.

    Returns:
        np.ndarray: Rotated points with the same shape as the input.
    """
    if points.size == 0 or angle == 0:
        return points

    center_array = np.zeros(2, dtype=float) if center is None else np.asarray(center, dtype=float)
    rotation = np.array(
        [[np.cos(angle), -np.sin(angle)], [np.sin(angle), np.cos(angle)]], dtype=float
    )
    return (points - center_array) @ rotation.T + center_array


def _filter_points_in_box(
    points: np.ndarray, x_center: float, y_center: float, x_range: float, y_range: float
) -> np.ndarray:
    """Keep points inside the centered rectangular scan bounds."""
    if points.size == 0:
        return points.reshape(0, 2)

    half_x = x_range / 2
    half_y = y_range / 2
    mask = (
        (points[:, 0] >= x_center - half_x)
        & (points[:, 0] <= x_center + half_x)
        & (points[:, 1] >= y_center - half_y)
        & (points[:, 1] <= y_center + half_y)
    )
    return points[mask]


def spiral_positions(
    x_center: float,
    y_center: float,
    x_range: float,
    y_range: float,
    dr: float,
    nth: float,
    dr_y: float | None = None,
    tilt: float = 0.0,
) -> np.ndarray:
    """
    Generate an Archimedean spiral scan trajectory.

    The spiral is centered at ``(x_center, y_center)`` and clipped to the rectangular
    region defined by ``x_range`` and ``y_range``.
    """
    if dr <= 0:
        raise ValueError("dr must be positive")
    if nth <= 0:
        raise ValueError("nth must be positive")

    dr_y = dr if dr_y is None else dr_y
    if dr_y <= 0:
        raise ValueError("dr_y must be positive")

    half_x = x_range / 2
    half_y = y_range / 2
    max_radius = max(half_x / dr, half_y / dr_y)
    max_theta = 2 * np.pi * max_radius
    dtheta = 2 * np.pi / nth

    theta = np.arange(0.0, max_theta + dtheta, dtheta, dtype=float)
    x = (dr / (2 * np.pi)) * theta * np.cos(theta)
    y = (dr_y / (2 * np.pi)) * theta * np.sin(theta)
    points = np.column_stack((x, y))
    points = rotate_points(points, tilt)
    points[:, 0] += x_center
    points[:, 1] += y_center

    return _filter_points_in_box(points, x_center, y_center, x_range, y_range)


def line_scan_positions(
    axes: list[tuple[float, float]], steps: int, endpoint: bool = True
) -> np.ndarray:
    """
    Generate linearly spaced positions for one or more axes.

    Args:
        axes (list[tuple[float, float]]): Sequence of ``(start, stop)`` pairs, one per axis.
        steps (int): Number of points to generate along the trajectory.
        endpoint (bool): If True, include the stop value in the generated positions.

    Returns:
        np.ndarray: Array of shape ``(steps, len(axes))`` containing the scan positions.
    """
    if steps <= 0:
        raise ValueError("steps must be positive")

    axis_positions = [
        np.linspace(start, stop, steps, dtype=float, endpoint=endpoint) for start, stop in axes
    ]
    return np.column_stack(axis_positions)


def log_scan_positions(axes: list[tuple[float, float]], steps: int) -> np.ndarray:
    """
    Generate positions with logarithmically increasing step sizes.

    The logarithmic spacing is applied to the normalized distance between each
    ``start`` and ``stop`` pair, not to the absolute position values. This means
    ranges may include zero or cross zero.

    Args:
        axes (list[tuple[float, float]]): Sequence of ``(start, stop)`` pairs, one per axis.
        steps (int): Number of points to generate along the trajectory.

    Returns:
        np.ndarray: Array of shape ``(steps, len(axes))`` containing the scan positions.

    Raises:
        ValueError: If ``steps`` is not positive.
    """
    if steps <= 0:
        raise ValueError("steps must be positive")

    # Log spacing from 0 to 1
    log_progress = (np.logspace(0, 1, steps, dtype=float) - 1) / 9

    axis_positions = []
    for start, stop in axes:
        axis_positions.append(start + log_progress * (stop - start))
    return np.column_stack(axis_positions)


def oscillating_positions(
    values: Sequence[float], repeat_turning_points: bool = False
) -> Iterator[float]:
    """
    Yield values indefinitely in a back-and-forth pattern.

    For a single value, the same value is yielded repeatedly. For multiple values,
    the sequence is traversed to the end and then back toward the beginning. By default
    the turning points are not repeated twice in a row, but this can be enabled with
    ``repeat_turning_points``.

    Args:
        values (Sequence[float]): Ordered values to oscillate through.
        repeat_turning_points (bool): If ``True``, repeat the end points before
            reversing direction. Default is ``False``.

    Yields:
        float: The next value in the oscillating sequence.

    Raises:
        ValueError: If ``values`` is empty.

    Examples:
        Call ``next(...)`` on the returned generator to retrieve the next value
        in the oscillating sequence:

        >>> pos_generator = oscillating_positions([600.0, 620.0, 640.0])
        >>> for _ in range(6):
        ...     value = next(pos_generator)
        ...     print(value)
        600.0
        620.0
        640.0
        620.0
        600.0
        620.0
    """
    if not values:
        raise ValueError("values must contain at least one position")

    if len(values) == 1:
        while True:
            yield float(values[0])

    index = 0
    direction = 1
    repeated_turning_point = False
    while True:
        yield float(values[index])
        if index == len(values) - 1:
            if repeat_turning_points and not repeated_turning_point:
                direction = -1
                repeated_turning_point = True
            else:
                direction = -1
                index += direction
                repeated_turning_point = False
        elif index == 0:
            if repeat_turning_points and not repeated_turning_point:
                direction = 1
                repeated_turning_point = True
            else:
                direction = 1
                index += direction
                repeated_turning_point = False
        else:
            index += direction
            repeated_turning_point = False


def _region_points(start: float, stop: float, steps: int) -> np.ndarray:
    """
    Generate positions for one inclusive scan region.

    Args:
        start (float): Region start position.
        stop (float): Region stop position.
        steps (int): Number of points in the region, including start and stop.

    Returns:
        np.ndarray: 1D array of positions covering the region in scan order.
    """
    if steps <= 0:
        raise ValueError("steps must be positive")
    return np.linspace(start, stop, steps, dtype=float)


def multi_region_line_positions(regions: list[tuple[float, float, int]]) -> np.ndarray:
    """
    Generate a 1D trajectory across multiple disjoint scan regions.

    Args:
        regions (list[tuple[float, float, int]]): Sequence of ``(start, stop, steps)``
            region definitions.

    Returns:
        np.ndarray: Array of shape ``(N, 1)`` containing the concatenated positions.
    """
    if not regions:
        raise ValueError("regions must contain at least one region")

    concatenated = []
    for start, stop, steps in regions:
        region_values = _region_points(start, stop, steps)
        if concatenated and np.isclose(concatenated[-1], region_values[0]):
            region_values = region_values[1:]
        concatenated.extend(region_values.tolist())
    return np.asarray(concatenated, dtype=float)[:, np.newaxis]


def multi_region_grid_positions(
    regions: list[tuple[tuple[float, float, int], tuple[float, float, int]]], snaked: bool = True
) -> np.ndarray:
    """
    Generate multiple rectangular sub-grids from paired scan regions.

    Args:
        regions (list[tuple[tuple[float, float, int], tuple[float, float, int]]]): Sequence
            of paired region definitions. Each entry contains one
            ``(start, stop, steps)`` tuple for the first motor and one for the second motor.
        snaked (bool): If ``True``, reverse traversal of the second axis on alternating positions
            within each sub-grid.

    Returns:
        np.ndarray: Array of shape ``(N, 2)`` containing the concatenated scan positions.
    """
    if not regions:
        raise ValueError("regions must contain at least one paired region")

    positions: list[list[float]] = []
    for region1, region2 in regions:
        axis1_positions = _region_points(*region1)
        axis2_positions = _region_points(*region2)

        for index, value1 in enumerate(axis1_positions):
            current_axis2 = (
                axis2_positions[::-1] if snaked and (index % 2 == 1) else axis2_positions
            )
            for value2 in current_axis2:
                positions.append([value1, value2])

    return np.asarray(positions, dtype=float)


def nd_grid_positions(axes: list[tuple[float, float, int]], snaked: bool = True) -> np.ndarray:
    """
    Generate N-dimensional grid positions.
    It creates a grid of positions for N dimensions, with optional snaking behavior.

    snaked==True:
        ->->->->-
        -<-<-<-<-
        ->->->->-
    snaked==False:
        ->->->->-
        ->->->->-
        ->->->->-

    Args:
        axes (list of tuples): list of tuples (start, stop, step) for each axis
        snaked (bool, optional): If True, the grid is generated in a "snaked"
            pattern across all dimensions.

    Returns:
        np.ndarray: shape (num_points, N)
    """
    _axes_arrays = []
    for start, stop, step in axes:
        if step <= 0:
            raise ValueError("Step size must be positive")
        _axes_arrays.append(np.linspace(start, stop, step, dtype=float))

    def _get_positions_recursively(current_axes):
        if len(current_axes) == 1:
            return [[v] for v in current_axes[0]]

        positions = []
        for i, val in enumerate(current_axes[0]):
            sub_positions = _get_positions_recursively(current_axes[1:])
            if snaked and (i % 2 == 1):
                sub_positions.reverse()
            positions.extend([[val] + sp for sp in sub_positions])
        return positions

    return np.array(_get_positions_recursively(_axes_arrays))


def fermat_spiral_pos(
    m1_start: float,
    m1_stop: float,
    m2_start: float,
    m2_stop: float,
    step: float = 1,
    spiral_type: float = 0,
    center: bool = False,
) -> np.ndarray:
    """
    fermat_spiral_pos calculates and returns the positions for a Fermat spiral scan.

    Args:
        m1_start (float): start position motor 1
        m1_stop (float): end position motor 1
        m2_start (float): start position motor 2
        m2_stop (float): end position motor 2
        step (float, optional): Step size. Defaults to 1.
        spiral_type (float, optional): Angular offset in radians that determines the shape of the spiral.
        A spiral with spiral_type=2 is the same as spiral_type=0. Defaults to 0.
        center (bool, optional): Add a center point. Defaults to False.

    Returns:
        np.ndarray: calculated positions in the form [[m1, m2], ...]
    """
    if step <= 0:
        raise ValueError("step must be positive")

    phi = np.pi * (3 - np.sqrt(5)) + spiral_type * np.pi
    start_index = 0 if center else 1

    x_center = (m1_start + m1_stop) / 2
    y_center = (m2_start + m2_stop) / 2
    x_range = abs(m1_stop - m1_start)
    y_range = abs(m2_stop - m2_start)
    half_x = x_range / 2
    half_y = y_range / 2

    radial_scale = step / np.sqrt(np.pi)
    max_index = max(1, int(np.ceil((max(half_x, half_y) / radial_scale) ** 2)) * 2)

    points = []
    for ii in range(start_index, max_index + 1):
        radius = radial_scale * np.sqrt(ii)
        x = x_center + radius * np.cos(ii * phi)
        y = y_center + radius * np.sin(ii * phi)
        if not (m1_start <= x <= m1_stop or m1_stop <= x <= m1_start):
            continue
        if not (m2_start <= y <= m2_stop or m2_stop <= y <= m2_start):
            continue
        points.append((x, y))

    return np.asarray(points, dtype=float)


def round_scan_positions(
    inner_radius: float,
    outer_radius: float,
    number_of_rings: int,
    points_in_first_ring: int,
    center_1: float = 0,
    center_2: float = 0,
) -> np.ndarray:
    """
    Calculate positions for a circular shell scan.

    Args:
        inner_radius (float): inner radius
        outer_radius (float): outer radius
        number_of_rings (int): number of radii
        points_in_first_ring (int): number of angles in the inner ring
        center_1 (float, optional): center position for axis 1. Defaults to 0.
        center_2 (float, optional): center position for axis 2. Defaults to 0.

    Returns:
        np.ndarray: calculated positions in the form [[x, y], ...]
    """
    positions = []
    radius_step = (inner_radius - outer_radius) / number_of_rings
    for ring_index in range(1, number_of_rings + 2):
        radius = inner_radius + ring_index * radius_step
        points_on_ring = points_in_first_ring * ring_index
        angular_step = 2 * np.pi / points_on_ring
        positions.extend(
            [
                (
                    radius * np.sin(point_index * angular_step) + center_1,
                    radius * np.cos(point_index * angular_step) + center_2,
                )
                for point_index in range(points_on_ring)
            ]
        )
    positions_array = np.array(positions, dtype=float)
    return positions_array


def get_round_roi_scan_positions(
    motor_1_start: float,
    motor_1_stop: float,
    motor_2_start: float,
    motor_2_stop: float,
    radial_step: float,
    points_in_first_shell: int,
    center_1: float = 0,
    center_2: float = 0,
):
    """
    Calculate round scan positions clipped to a rectangular region of interest.

    The circular shells are centered around ``center_1`` / ``center_2``. The center does
    not need to be inside the rectangular ROI defined by the motor start/stop
    bounds.

    Args:
        motor_1_start (float): start position of the ROI for motor 1
        motor_1_stop (float): stop position of the ROI for motor 1
        motor_2_start (float): start position of the ROI for motor 2
        motor_2_stop (float): stop position of the ROI for motor 2
        radial_step (float): radial shell spacing
        points_in_first_shell (int): number of angles in the first shell
        center_1 (float, optional): center position for motor 1. Defaults to 0.
        center_2 (float, optional): center position for motor 2. Defaults to 0.

    Returns:
        np.ndarray: calculated positions in the form [[x, y], ...]
    """
    motor_1_min, motor_1_max = sorted((motor_1_start, motor_1_stop))
    motor_2_min, motor_2_max = sorted((motor_2_start, motor_2_stop))
    corners = [
        (motor_1_min, motor_2_min),
        (motor_1_min, motor_2_max),
        (motor_1_max, motor_2_min),
        (motor_1_max, motor_2_max),
    ]
    max_radius = max(
        np.hypot(motor_1_position - center_1, motor_2_position - center_2)
        for motor_1_position, motor_2_position in corners
    )

    positions = []
    number_of_shells = 1 + int(np.ceil(max_radius / radial_step))
    for shell_index in range(1, number_of_shells + 2):
        radius = shell_index * radial_step
        points_on_shell = points_in_first_shell * shell_index
        angular_step = 2 * np.pi / points_on_shell
        for point_index in range(points_on_shell):
            angle = point_index * angular_step
            local_position = np.array(
                [[radius * np.cos(angle), radius * np.sin(angle)]], dtype=float
            )
            motor_1_offset, motor_2_offset = local_position[0]
            motor_1_position = motor_1_offset + center_1
            motor_2_position = motor_2_offset + center_2
            if not (
                motor_1_min <= motor_1_position <= motor_1_max
                and motor_2_min <= motor_2_position <= motor_2_max
            ):
                continue
            positions.append((motor_1_position, motor_2_position))
    return np.array(positions, dtype=float)


def hex_grid_2d(axes: list[tuple[float, float, float]], snaked: bool = True) -> np.ndarray:
    """
    Generate a 2D hexagonal grid clipped to (start, stop) bounds.

    Args:
        axes: [(x_start, x_stop, x_step),
            (y_start, y_stop, y_step)]
            x_step = horizontal spacing between columns
            y_step = vertical spacing between rows
        snaked: if True, reverse direction on alternate rows to minimize travel distance

    Returns:
        np.ndarray of shape (N, 2)
    """
    if len(axes) != 2:
        raise ValueError("2D hex grid requires exactly 2 dimensions")

    (x0, x1, sx), (y0, y1, sy) = axes

    points = []

    # Number of rows needed
    n_rows = int(np.ceil((y1 - y0) / sy)) + 2

    for row in range(n_rows):
        y = y0 + row * sy

        # Alternate row offset - shift by half the x step
        x_offset = (sx / 2) if (row % 2) else 0.0

        # Number of columns needed
        n_cols = int(np.ceil((x1 - x0) / sx)) + 2

        row_points = []
        for col in range(n_cols):
            x = x0 + x_offset + col * sx

            if x0 <= x <= x1 and y0 <= y <= y1:
                row_points.append((x, y))

        # Reverse every other row if snaking is enabled
        if snaked and (row % 2 == 1):
            row_points.reverse()

        points.extend(row_points)

    return np.asarray(points, dtype=float)
