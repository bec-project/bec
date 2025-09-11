from typing import Literal

import numpy as np
from pydantic import BaseModel
from scipy.spatial import cKDTree  # type: ignore


class PathQualityStats(BaseModel):
    max_jump: float
    avg_step: np.floating | float
    std_step: np.floating | float
    total_length: float

    model_config = {"arbitrary_types_allowed": True}


class PathOptimizerMixin:
    def get_radius(self, pos: np.ndarray) -> np.ndarray:
        """
        Calculate the radial distance from the origin for each position

        Args:
            pos (np.ndarray): Array of positions
        Returns:
            np.ndarray: Radial distances
        """

        return np.sqrt(np.abs(pos[:, 0]) ** 2 + np.abs(pos[:, 1]) ** 2)

    def optimize_corridor(
        self,
        positions: np.ndarray,
        corridor_size: float | None = None,
        sort_axis: int = 1,
        num_iterations: int = 1,
        preferred_direction: int | None = None,
        corridor_estimation: Literal["density", "median_distance"] = "median_distance",
    ):
        """
        Optimize positions using a corridor-based approach.

        Note: This method is designed for 2D positions. If higher dimensions are provided,
        the positions are returned unmodified.

        Args:
            positions (np.ndarray): Array of positions
            corridor_size (float, optional): Width of each corridor. Defaults to None (auto-estimated).
            sort_axis (int, optional): Axis along which to create corridors (0 or 1). Defaults to 1.
            num_iterations (int, optional): Number of corridor sizes to try. Defaults to 1.
            preferred_direction (int | None, optional): Preferred direction for the primary axis (1 or -1).
                If None, alternates direction for each corridor.
            corridor_estimation (str, optional): Method for estimating corridor size if not provided.
                Options are "density" or "median_distance". Defaults to "density".

        Returns:
            np.ndarray: Optimized positions
        """
        if positions.ndim != 2 or positions.shape[1] < 2:
            return positions

        # Estimate corridor size if needed
        if corridor_size is None:
            if corridor_estimation == "density":
                corridor_size = self._corridor_estimate_density(positions)
            elif corridor_estimation == "median_distance":
                corridor_size = self._corridor_estimate_median_distance(positions)
            else:
                raise ValueError(
                    f"Invalid corridor_estimation method: {corridor_estimation}. "
                    'Choose "density" or "median_distance".'
                )

        axis_vals = positions[:, sort_axis]
        sec_axis = int(not sort_axis)

        best_length = np.inf
        best_path = positions

        if num_iterations > 1:
            corridor_sizes = corridor_size * np.logspace(-0.7, 0.7, num_iterations)
        else:
            corridor_sizes = [corridor_size]

        for cs in corridor_sizes:
            if cs <= 0:
                continue

            # Integer corridor binning
            min_val = axis_vals.min()
            bin_idx = ((axis_vals - min_val) / cs).astype(int)
            n_steps = bin_idx.max() + 1

            index_sorted: list[np.ndarray] = []

            for step in range(n_steps):
                block = np.where(bin_idx == step)[0]
                if block.size == 0:
                    continue

                # Sort within corridor along secondary axis
                block = block[np.argsort(positions[block, sec_axis])]

                # Direction handling
                if preferred_direction is not None:
                    if preferred_direction < 0:
                        block = block[::-1]
                else:
                    # Alternate direction between corridors
                    if step % 2 == 0:
                        block = block[::-1]

                index_sorted.append(block)

            if not index_sorted:
                continue

            path_idx = np.concatenate(index_sorted)
            path = positions[path_idx]
            path_length = self.get_path_length(path)

            if path_length < best_length:
                best_length = path_length
                best_path = path

        return best_path

    def _corridor_estimate_density(self, positions: np.ndarray) -> float:
        """
        Estimate corridor size based on point density.

        Args:
            positions (np.ndarray): Array of positions

        Returns:
            float: Estimated corridor size
        """
        dims = [
            abs(min(positions[:, 0]) - max(positions[:, 0])),
            abs(min(positions[:, 1]) - max(positions[:, 1])),
        ]
        density = np.sqrt(len(positions) / (dims[1] * dims[0]))
        corridor_size = 2 / density
        return corridor_size

    def _corridor_estimate_median_distance(
        self, positions: np.ndarray, factor: float = 1.5
    ) -> float:
        """
        Estimate corridor size based on median nearest neighbor distance.

        Args:
            positions (np.ndarray): Array of positions
            factor (float): Scaling factor for the median distance

        Returns:
            float: Estimated corridor size
        """
        tree = cKDTree(positions[:, :2])
        dists, _ = tree.query(positions[:, :2], k=2)  # k=1 is itself
        nn = dists[:, 1]
        return factor * np.median(nn)

    def optimize_shell(
        self,
        pos: np.ndarray,
        offset: float | None = None,
        dr: float | None = None,
        num_iterations: int = 3,
    ):
        """Optimize a path through a set of positions by sorting them in concentric shells.

        Args:
            pos (np.ndarray): Array of positions
            offset (float, optional): Offset for the first shell. Defaults to None (auto-estimated).
            dr (float, optional): Width of each shell. Defaults to None (auto-estimated).
            num_iterations (int, optional): Number of parameter variations to try. Defaults to 3.

        Returns:
            np.ndarray: Optimized positions
        """
        # Handle edge cases
        if pos.ndim != 2 or pos.shape[1] < 2 or len(pos) < 2:
            return pos

        # Calculate radii once
        radii = self.get_radius(pos)
        max_rad = np.max(radii)

        # Set default dr if not provided
        if dr is None:
            dr_default = self._estimate_shell_width(pos, radii, max_rad)
        else:
            dr_default = dr

        # Set default offset if not provided
        if offset is None:
            offset_default = 1
        else:
            offset_default = offset

        # Try different parameter combinations if num_iterations > 1
        best_path = None
        best_score = float("inf")  # Lower is better

        if num_iterations > 1:
            # Try variations of dr
            dr_values = np.linspace(dr_default * 0.7, dr_default * 1.3, num_iterations)
            offset_values = [offset_default]
        else:
            dr_values = [dr_default]
            offset_values = [offset_default]

        for current_dr in dr_values:
            for current_offset in offset_values:
                optimized_path = self._optimize_shell_with_params(
                    pos, radii, current_offset, current_dr, max_rad
                )

                # Analyze the quality of this path
                quality = self.analyze_path_quality(optimized_path)

                # Score based on maximum jump and total path length
                # We want to minimize both the largest jump and the total path length
                score = quality.max_jump * 0.7 + quality.total_length * 0.3

                if score < best_score:
                    best_score = score
                    best_path = optimized_path

        return best_path if best_path is not None else pos

    def _estimate_shell_width(self, pos: np.ndarray, radii: np.ndarray, max_rad: float) -> float:
        """
        Estimate optimal shell width based on point distribution characteristics.

        Uses scale-invariant approach based on:
        1. Typical spacing between points (nearest neighbor distances)
        2. Radial distribution of points
        3. Overall point density

        Args:
            pos (np.ndarray): Array of positions
            radii (np.ndarray): Pre-computed radii for all positions
            max_rad (float): Maximum radius

        Returns:
            float: Estimated shell width
        """
        n_points = len(pos)

        # Get nearest neighbor distances to understand typical point spacing
        tree = cKDTree(pos[:, :2])
        k_neighbors = min(5, n_points)  # Use up to 5 nearest neighbors
        dists, _ = tree.query(pos[:, :2], k=k_neighbors)

        # Analyze the distribution of nearest neighbor distances
        # Using 75th percentile is more robust than mean/median for varying densities
        typical_spacing = np.percentile(dists[:, 1:], 75)

        # Estimate reasonable number of shells based on point density
        # More points → can afford more shells for finer optimization
        # Use logarithmic scaling to avoid too many/few shells
        target_shells = np.sqrt(n_points)

        # Calculate shell width to achieve target number of shells
        estimated_dr = max_rad / target_shells

        # Use the larger of: 2x typical spacing or the calculated shell width
        # This ensures shells contain enough points while respecting point spacing
        return max(typical_spacing * 2.0, estimated_dr)

    def _optimize_shell_with_params(
        self, pos: np.ndarray, radii: np.ndarray, offset: float, dr: float, max_rad: float
    ) -> np.ndarray:
        """
        Helper function to optimize a path with specific parameters.

        Args:
            pos (np.ndarray): Array of positions
            radii (np.ndarray): Pre-computed radii for all positions
            offset (float): Offset for the first shell
            dr (float): Width of each shell
            max_rad (float): Maximum radius

        Returns:
            np.ndarray: Optimized positions
        """
        # Calculate number of shells needed
        nsteps = int(np.floor(max_rad / dr) + int(bool(np.mod(max_rad, dr))))

        shells = []
        shell_radius_min = -offset * dr
        last_end_angle = 0.0  # Track the ending angle of the previous shell

        # Process each shell
        for shell_idx in range(nsteps + 2):
            shell_radius_max = shell_radius_min + dr

            # Find points in the current shell
            shell_mask = (radii >= shell_radius_min) & (radii < shell_radius_max)
            shell_points = pos[shell_mask]

            if len(shell_points) > 0:
                # Calculate angles for the current shell
                angles = np.arctan2(shell_points[:, 0], shell_points[:, 1])

                # For smooth transitions, start sorting from where the previous shell ended
                if shell_idx > 0 and len(shells) > 0 and len(shells[-1]) > 0:
                    # Rotate angles to start from the last ending angle
                    shifted_angles = (angles - last_end_angle) % (2 * np.pi)
                    sorted_indices = np.argsort(shifted_angles)
                    shell_sorted = shell_points[sorted_indices]

                    # Update last_end_angle based on the actual last point in this shell
                    last_point = shell_sorted[-1]
                    last_end_angle = np.arctan2(last_point[0], last_point[1])
                else:
                    # For the first shell, just sort by angle
                    sorted_indices = np.argsort(angles)
                    shell_sorted = shell_points[sorted_indices]

                    if len(shell_sorted) > 0:
                        last_point = shell_sorted[-1]
                        last_end_angle = np.arctan2(last_point[0], last_point[1])

                shells.append(shell_sorted)

            shell_radius_min = shell_radius_max

        # Handle any remaining points beyond the last shell
        remaining_mask = radii >= shell_radius_min
        remaining_points = pos[remaining_mask]

        if len(remaining_points) > 0:
            angles = np.arctan2(remaining_points[:, 0], remaining_points[:, 1])

            if len(shells) > 0 and len(shells[-1]) > 0:
                # Continue from the last shell's ending angle
                shifted_angles = (angles - last_end_angle) % (2 * np.pi)
                remaining_sorted = remaining_points[np.argsort(shifted_angles)]
            else:
                # Just sort by angle if there's no previous shell
                remaining_sorted = remaining_points[np.argsort(angles)]

            shells.append(remaining_sorted)

        # Flatten the shells into a single array
        if not shells:
            return pos  # Return original if something went wrong

        result = np.vstack([shell for shell in shells if len(shell) > 0])

        # Validate that we haven't lost any points
        if len(result) != len(pos):
            return pos  # Return original positions if something went wrong

        return result

    def get_path_length(self, pos: np.ndarray) -> float:
        """
        Calculate the total length of a path defined by a sequence of positions.
        Args:
            pos (np.ndarray): Array of positions
        Returns:
            float: Total path length
        """
        path_length = 0
        for ii in range(len(pos) - 1):
            path_length += np.sqrt(np.sum(abs(pos[ii + 1] - pos[ii]) ** 2))
        return path_length

    def analyze_path_quality(self, pos: np.ndarray) -> PathQualityStats:
        """Analyze the quality of a path by looking at the distribution of step sizes.

        Args:
            pos (np.ndarray): Array of positions

        Returns:
            dict: Dictionary with statistics about the path quality
        """
        if len(pos) < 2:
            return PathQualityStats(max_jump=0.0, avg_step=0.0, std_step=0.0, total_length=0.0)

        steps = []
        for ii in range(len(pos) - 1):
            step = np.sqrt(np.sum(abs(pos[ii + 1] - pos[ii]) ** 2))
            steps.append(step)

        return PathQualityStats(
            max_jump=np.max(steps),
            avg_step=np.mean(steps),
            std_step=np.std(steps),
            total_length=np.sum(steps),
        )

    def optimize_nearest_neighbor(
        self, positions: np.ndarray, start_index: int | None = None
    ) -> np.ndarray:
        """
        Optimize path by always moving to the nearest unvisited point.

        This is a greedy algorithm that provides good results for many scenarios
        with relatively low computational complexity.

        Args:
            positions (np.ndarray): Array of positions
            start_index (int | None, optional): Index of the starting point. If None,
                the algorithm will start from the point closest to the origin.

        Returns:
            np.ndarray: Optimized positions
        """
        if len(positions) <= 1:
            return positions

        n_points = len(positions)

        # Use a copy to avoid modifying the original
        positions_copy = positions.copy()

        # Start from the point closest to the origin if not specified
        if start_index is None:
            # Find the point closest to the origin
            distances_to_origin = np.sum(positions_copy**2, axis=1)
            start_index = int(np.argmin(distances_to_origin))

        # Initialize the path with the starting point
        path = [positions_copy[start_index]]

        # Keep track of visited indices
        remaining_indices = set(range(n_points))
        remaining_indices.remove(start_index)

        # Current position is the starting point
        current_pos = positions_copy[start_index]

        # Add points to the path one by one
        while remaining_indices:
            # Calculate distances to all remaining points
            remaining_positions = positions_copy[list(remaining_indices)]

            # Calculate distances from current position to all remaining points
            distances = np.sqrt(np.sum((remaining_positions - current_pos) ** 2, axis=1))

            # Find the closest point
            closest_idx = np.argmin(distances)

            # Get the actual index in the original array
            original_idx = list(remaining_indices)[closest_idx]

            # Update current position
            current_pos = positions_copy[original_idx]

            # Add to path and remove from remaining
            path.append(current_pos)
            remaining_indices.remove(original_idx)

        return np.array(path)
