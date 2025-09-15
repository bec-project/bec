import numpy as np


class PathOptimizerMixin:
    def get_radius(self, pos):
        return np.sqrt(np.abs(pos[:, 0]) ** 2 + np.abs(pos[:, 1]) ** 2)

    def optimize_corridor(self, positions, corridor_size=None, sort_axis=1, num_iterations=1, preferred_direction=None):
        """Optimize positions using a corridor-based approach.
        
        Args:
            positions (np.ndarray): Array of positions
            corridor_size (float, optional): Width of each corridor. Defaults to None (auto-estimated).
            sort_axis (int, optional): Axis along which to create corridors (0 or 1). Defaults to 1.
            num_iterations (int, optional): Number of corridor sizes to try. Defaults to 1.
            preferred_direction (int, optional): Preferred direction for the primary axis (1 or -1).
                If None, alternates direction for each corridor.
                
        Returns:
            np.ndarray: Optimized positions
        """
        if len(positions[0]) < 2:
            return positions
        if corridor_size is None:
            dims = [
                abs(min(positions[:, 0]) - max(positions[:, 0])),
                abs(min(positions[:, 1]) - max(positions[:, 1])),
            ]
            density = np.sqrt(len(positions) / (dims[1] * dims[0]))
            corridor_size = 2 / density

        result = [np.inf, []]
        if num_iterations > 1:
            corridor_sizes = np.linspace(corridor_size / 2, corridor_size * 2, num_iterations)
        else:
            corridor_sizes = [corridor_size]
        for corridor_size_iter in corridor_sizes:
            index_sorted = []

            n_steps = int(
                np.round(
                    np.abs(max(positions[:, sort_axis]) - min(positions[:, sort_axis]))
                    / corridor_size_iter
                )
            )
            steps = list(
                np.floor(
                    np.linspace(min(positions[:, sort_axis]), max(positions[:, sort_axis]), n_steps)
                )
            )
            # print(steps, n_steps, positions)
            steps.append(np.inf)
            
            # If preferred_direction is specified, we'll use it to determine corridor traversal
            for step in range(n_steps):
                block = np.where(positions[:, sort_axis] >= steps[step])
                block = block[0][np.where(positions[:, sort_axis][block[0]] < steps[step + 1])]
                
                # Sort the secondary axis
                secondary_axis = int(not sort_axis)
                block = block[np.argsort(positions[:, secondary_axis][block])]
                
                # If preferred_direction is specified, always traverse in that direction
                # Otherwise, alternate directions as before
                if preferred_direction is not None:
                    if preferred_direction < 0:  # Negative direction
                        block = block[::-1]
                elif step % 2 == 0:  # Alternating if no preference
                    block = block[::-1]
                    
                index_sorted.append(block)
                
            path_length = self.get_path_length(positions[np.concatenate(index_sorted), :])
            if path_length < result[0]:
                result = [path_length, positions[np.concatenate(index_sorted), :]]
        return result[1]

    def optimize_shell(self, pos, offset=None, dr=None, num_iterations=3):
        """Optimize a path through a set of positions by sorting them in concentric shells.

        Args:
            pos (np.ndarray): Array of positions
            offset (float, optional): Offset for the first shell. Defaults to None (auto-estimated).
            dr (float, optional): Width of each shell. Defaults to None (auto-estimated).
            num_iterations (int, optional): Number of parameter variations to try. Defaults to 3.

        Returns:
            np.ndarray: Optimized positions
        """
        # Auto-estimate parameters if not provided
        max_rad_id = np.where(self.get_radius(pos) == np.max(self.get_radius(pos)))
        max_rad = self.get_radius(pos[max_rad_id])[0]

        # Calculate x and y dimensions for rectangular ROIs
        x_range = abs(max(pos[:, 0]) - min(pos[:, 0]))
        y_range = abs(max(pos[:, 1]) - min(pos[:, 1]))

        # Estimate area of the points distribution, considering possible rectangular ROIs
        area = max(np.pi * max_rad**2, x_range * y_range)
        density = len(pos) / area

        # Set default dr if not provided
        if dr is None:
            dr_default = max_rad / max(10, min(20, int(np.sqrt(len(pos) / 10))))
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
                optimized_path = self._optimize_shell_with_params(pos, current_offset, current_dr)

                # Analyze the quality of this path
                quality = self.analyze_path_quality(optimized_path)

                # Score based on maximum jump and total path length
                # We want to minimize both the largest jump and the total path length
                score = quality["max_jump"] * 0.7 + quality["total_length"] * 0.3

                if score < best_score:
                    best_score = score
                    best_path = optimized_path

        return best_path if best_path is not None else pos

    def _optimize_shell_with_params(self, pos, offset, dr):
        """Helper function to optimize a path with specific parameters.

        Args:
            pos (np.ndarray): Array of positions
            offset (float): Offset for the first shell
            dr (float): Width of each shell

        Returns:
            np.ndarray: Optimized positions
        """
        max_rad_id = np.where(self.get_radius(pos) == np.max(self.get_radius(pos)))
        max_rad = self.get_radius(pos[max_rad_id])[0]

        sub_groups = []
        nsteps = int(np.floor(max_rad / dr) + int(bool(np.mod(max_rad, dr))))
        sub_rad_prev = -offset * dr
        last_end_angle = 0  # Keep track of the last ending angle

        for i in range(nsteps + 2):
            temp = np.where(self.get_radius(pos) < dr + sub_rad_prev)
            temp = temp[0][np.where(self.get_radius(pos[temp[0]]) >= sub_rad_prev)]
            temp_pos = pos[temp]

            if len(temp_pos) > 0:
                # Calculate angles for the current shell
                angles = np.arctan2(temp_pos[:, 0], (temp_pos[:, 1]))

                # For smooth transitions between shells, start sorting from where the previous shell ended
                if i > 0 and len(sub_groups) > 0 and len(sub_groups[-1]) > 0:
                    # Find the closest starting point to the last point in the previous shell
                    prev_end_point = sub_groups[-1][-1]

                    # Calculate angle of the last point in previous shell
                    last_end_angle = np.arctan2(prev_end_point[0], prev_end_point[1])

                    # Rotate angles to make the closest point to last_end_angle the starting point
                    shifted_angles = (angles - last_end_angle) % (2 * np.pi)
                    pos_sort = temp_pos[np.argsort(shifted_angles)]
                else:
                    # For the first shell, just sort by angle
                    pos_sort = temp_pos[np.argsort(angles)]

                sub_groups.append(pos_sort)
            else:
                sub_groups.append(np.array([]))

            sub_rad_prev += dr

        temp = np.where(self.get_radius(pos) >= sub_rad_prev)
        temp = temp[0][np.where(self.get_radius(pos[temp[0]]) >= sub_rad_prev)]
        temp_pos = pos[temp]

        if len(temp_pos) > 0 and len(sub_groups) > 0 and len(sub_groups[-1]) > 0:
            # Handle the last shell with the same continuity approach
            prev_end_point = sub_groups[-1][-1]
            last_end_angle = np.arctan2(prev_end_point[0], prev_end_point[1])

            angles = np.arctan2(temp_pos[:, 0], (temp_pos[:, 1]))
            shifted_angles = (angles - last_end_angle) % (2 * np.pi)
            pos_sort = temp_pos[np.argsort(shifted_angles)]
        else:
            # Just sort by angle if there's no previous shell
            angles = np.arctan2(temp_pos[:, 0], (temp_pos[:, 1]))
            pos_sort = temp_pos[np.argsort(angles)]

        sub_groups.append(pos_sort)

        # Flatten the sub-groups into a single array of positions
        posi = []
        for i in range(len(sub_groups)):
            if len(sub_groups[i]) > 0:
                for j in range(len(sub_groups[i])):
                    posi.extend([sub_groups[i][j]])

        # Validate that we haven't lost any points
        if len(posi) != len(pos):
            return pos  # Return original positions if something went wrong

        return np.asarray(posi)

    def get_path_length(self, pos):
        path_length = 0
        for ii in range(len(pos) - 1):
            path_length += np.sqrt(np.sum(abs(pos[ii + 1] - pos[ii]) ** 2))
        return path_length

    def analyze_path_quality(self, pos):
        """Analyze the quality of a path by looking at the distribution of step sizes.

        Returns:
            dict: Dictionary with statistics about the path quality
        """
        if len(pos) < 2:
            return {"max_jump": 0, "avg_step": 0, "std_step": 0, "total_length": 0}

        steps = []
        for ii in range(len(pos) - 1):
            step = np.sqrt(np.sum(abs(pos[ii + 1] - pos[ii]) ** 2))
            steps.append(step)

        return {
            "max_jump": np.max(steps),
            "avg_step": np.mean(steps),
            "std_step": np.std(steps),
            "total_length": np.sum(steps),
        }

    def optimize_nearest_neighbor(self, positions, start_index=None):
        """Optimize path by always moving to the nearest unvisited point.

        This is a greedy algorithm that provides good results for many scenarios
        with relatively low computational complexity.

        Args:
            positions (np.ndarray): Array of positions
            start_index (int, optional): Index of the starting point. If None,
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
        
    def optimize_preferred_direction(self, positions, preferred_directions=None, axis_weights=None):
        """Optimize path considering preferred movement directions for motors.
        
        This algorithm considers both the distance between points and preferred 
        directions of movement for different motors. It uses a modified nearest 
        neighbor approach that penalizes movements against preferred directions.
        
        Args:
            positions (np.ndarray): Array of positions
            preferred_directions (list, optional): List of preferred directions for each axis.
                [1, 1] means prefer positive direction for both axes,
                [-1, 1] means prefer negative direction for first axis, positive for second.
                Defaults to [1, 1] if None.
            axis_weights (list, optional): Weights for each axis, determining how strongly
                to enforce the preferred direction. Higher values mean stronger preference.
                Defaults to [1, 1] if None.
                
        Returns:
            np.ndarray: Optimized positions
        """
        if len(positions) <= 1:
            return positions
            
        n_points = len(positions)
        n_dims = positions.shape[1]
        
        # Use a copy to avoid modifying the original
        positions_copy = positions.copy()
        
        # Set default preferred directions and weights if not provided
        if preferred_directions is None:
            preferred_directions = np.ones(n_dims)
        else:
            preferred_directions = np.array(preferred_directions)
            
        if axis_weights is None:
            axis_weights = np.ones(n_dims)
        else:
            axis_weights = np.array(axis_weights)
            
        # Normalize direction vectors to -1 or 1
        preferred_directions = np.sign(preferred_directions)
        
        # Find a good starting point: one at the "beginning" of the space 
        # based on preferred directions
        start_indices = []
        for i in range(n_dims):
            if preferred_directions[i] > 0:
                # If preferred direction is positive, start from the minimum value
                start_indices.append(np.argmin(positions_copy[:, i]))
            else:
                # If preferred direction is negative, start from the maximum value
                start_indices.append(np.argmax(positions_copy[:, i]))
                
        # Pick the axis with the highest weight to determine starting point
        primary_axis = np.argmax(axis_weights)
        start_index = start_indices[primary_axis]
        
        # Initialize the path with the starting point
        path = [positions_copy[start_index]]
        
        # Keep track of visited indices
        remaining_indices = set(range(n_points))
        remaining_indices.remove(start_index)
        
        # Current position is the starting point
        current_pos = positions_copy[start_index]
        
        # Add points to the path one by one
        while remaining_indices:
            # Calculate distances and direction penalties for all remaining points
            remaining_positions = positions_copy[list(remaining_indices)]
            
            # Calculate distances from current position to all remaining points
            distances = np.sqrt(np.sum((remaining_positions - current_pos)**2, axis=1))
            
            # Calculate direction penalties
            direction_vectors = remaining_positions - current_pos
            
            # Apply penalty for movements against preferred direction
            penalties = np.zeros(len(remaining_indices))
            for i in range(n_dims):
                # For each axis, if the movement is against the preferred direction,
                # add a penalty weighted by the axis weight
                movement_sign = np.sign(direction_vectors[:, i])
                against_preferred = movement_sign != preferred_directions[i]
                penalties += against_preferred * axis_weights[i] * np.abs(direction_vectors[:, i])
            
            # Combine distance and direction penalty to get total cost
            # Scale penalties to be comparable to distances
            max_dist = np.max(distances)
            if max_dist > 0:
                normalized_penalties = penalties / max_dist
                costs = distances + normalized_penalties
            else:
                costs = distances
            
            # Find the point with the lowest cost
            best_idx = np.argmin(costs)
            
            # Get the actual index in the original array
            original_idx = list(remaining_indices)[best_idx]
            
            # Update current position
            current_pos = positions_copy[original_idx]
            
            # Add to path and remove from remaining
            path.append(current_pos)
            remaining_indices.remove(original_idx)
        
        return np.array(path)
