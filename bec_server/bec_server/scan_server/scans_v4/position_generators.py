from __future__ import annotations

import numpy as np


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
    m1_start, m1_stop, m2_start, m2_stop, step=1, spiral_type=0, center=False
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
    positions = []
    phi = 2 * np.pi * ((1 + np.sqrt(5)) / 2.0) + spiral_type * np.pi

    start = int(not center)

    length_axis1 = abs(m1_stop - m1_start)
    length_axis2 = abs(m2_stop - m2_start)
    n_max = int(length_axis1 * length_axis2 * 3.2 / step / step)

    for ii in range(start, n_max):
        radius = step * 0.57 * np.sqrt(ii)
        if abs(radius * np.sin(ii * phi)) > length_axis1 / 2:
            continue
        if abs(radius * np.cos(ii * phi)) > length_axis2 / 2:
            continue
        positions.extend([(radius * np.sin(ii * phi), radius * np.cos(ii * phi))])
    return np.array(positions)


def round_scan_positions(
    r_in: float, r_out: float, nr: int, nth: int, cenx=0, ceny=0
) -> np.ndarray:
    """
    round_scan_positions calculates and returns the positions for a round scan.

    Args:
        r_in (float): inner radius
        r_out (float): outer radius
        nr (int): number of radii
        nth (int): number of angles in the inner ring
        cenx (int, optional): center in x. Defaults to 0.
        ceny (int, optional): center in y. Defaults to 0.

    Returns:
        np.ndarray: calculated positions in the form [[x, y], ...]

    """
    positions = []
    dr = (r_in - r_out) / nr
    for ir in range(1, nr + 2):
        rr = r_in + ir * dr
        dth = 2 * np.pi / (nth * ir)
        positions.extend(
            [
                (rr * np.sin(ith * dth) + cenx, rr * np.cos(ith * dth) + ceny)
                for ith in range(nth * ir)
            ]
        )
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
