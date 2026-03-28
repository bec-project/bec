from __future__ import annotations

import time
from typing import TYPE_CHECKING, Callable, Literal

import numpy as np

from bec_lib.device import DeviceBase
from bec_server.scan_server.errors import LimitError
from bec_server.scan_server.path_optimization import PathOptimizerMixin

if TYPE_CHECKING:
    from bec_server.scan_server.scans.scan_base import ScanBase


class ScanComponents:
    """
    Class to handle the components for the scan logic.
    The components are reusable building blocks for the scan logic,
    such as step scans or grid scans. They use the ScanStubs to
    execute the scan logic.
    """

    def __init__(self, scan: ScanBase):
        self._scan = scan
        self._actions = scan.actions
        self._redis_connector = scan.redis_connector
        self._device_manager = scan.device_manager
        self._dev = self._device_manager.devices if self._device_manager else None
        self._path_optimizer = PathOptimizerMixin()

    def move_and_wait(
        self,
        motors: list[str | DeviceBase] | list[str] | list[DeviceBase],
        positions: np.ndarray | list[float],
        last_positions: np.ndarray | None = None,
    ):
        """
        Move the given motors to the given positions and wait for the movement to complete.
        If last_positions is provided, only the motors with changed positions will be moved.

        Args:
            motors (list[str | DeviceBase] | list[str] | list[DeviceBase]): List of motor names or device instances to move.
            positions (np.ndarray | list[float]): Array or list of positions to move to, shape (len(motors),).
            last_positions (np.ndarray, optional): Array of last positions, shape (len(motors),).
                If provided, only motors with changed positions will be moved. Defaults to None.
        """
        motors_to_move = []
        positions_to_move = []
        for motor_index, motor in enumerate(motors):
            if last_positions is not None:
                if np.isclose(positions[motor_index], last_positions[motor_index]):
                    continue
            motors_to_move.append(motor)
            positions_to_move.append(positions[motor_index])

        if motors_to_move:
            self._actions.set(motors_to_move, positions_to_move, wait=True)

    def trigger_and_read(self):
        """
        Trigger the devices and start the readout. This is typically used for step scans after the motors have been moved to the next position.

        The logic is as follows:
            1. Let the system settle before triggering
            2. Trigger the devices
            3. Let the system settle after the trigger
            4. Start the readout

        """
        # Let the system settle before triggering
        time.sleep(self._scan.scan_info.settling_time)
        trigger_time = self._scan.scan_info.exp_time * self._scan.scan_info.frames_per_trigger

        # Trigger the devices
        self._actions.trigger_all_devices(min_wait=trigger_time)

        # Let the system settle after the trigger
        time.sleep(self._scan.scan_info.settling_time_after_trigger)

        # Start the readout
        self._actions.read_monitored_devices()

    def step_scan(
        self,
        motors: list[str | DeviceBase] | list[str] | list[DeviceBase],
        positions: np.ndarray,
        at_each_point: (
            Callable[[list[str | DeviceBase], np.ndarray, np.ndarray | None], None] | None
        ) = None,
        last_positions: np.ndarray | None = None,
    ):
        """
        Execute a step scan with the given positions. It is the core scan logic
        for most step scans.

        Args:
            motors (list[str | DeviceBase] | list[str] | list[DeviceBase]): List of motor names or device instances to move.
            positions (np.ndarray): Array of positions to move to, shape (num_points, len(motors)).
            at_each_point (Callable[[list[str | DeviceBase], np.ndarray, np.ndarray | None], None], optional): Function to call at each point. Defaults to None.
            last_positions (np.ndarray, optional): Array of last positions, shape (num_points, len(motors)). If provided, only motors with changed positions will be moved. Defaults to None.
        """
        at_each_point = at_each_point or self.step_scan_at_each_point
        for pos in positions:
            for _ in range(self._scan.scan_info.burst_at_each_point):
                at_each_point(motors, pos, last_positions=last_positions)
                last_positions = pos.copy()

    def step_scan_at_each_point(
        self,
        motors: list[str | DeviceBase] | list[str] | list[DeviceBase],
        pos: np.ndarray,
        last_positions: np.ndarray | None = None,
    ):
        """
        Execute a step scan at each point. This is the core logic that is executed at each point of the step scan.
        It is separated from the step_scan method to allow scan hooks to override the logic.

        The logic is as follows:
            1. Move the motors to the next position without waiting for each motor to complete
            2. Wait for each motor to complete
            3. Let the system settle before triggering
            4. Trigger the devices
            5. Let the system settle after the trigger
            6. Start the readout

        Args:
            motors (list[str | DeviceBase] | list[str] | list[DeviceBase]): List of motor names or device instances to move.
            pos (np.ndarray): Array of positions to move to, shape (len(motors),).
            last_positions (np.ndarray, optional): Array of last positions, shape (len(motors),).
                If provided, only motors with changed positions will be moved. Defaults to None.
        """
        self.move_and_wait(motors, pos, last_positions=last_positions)
        self.trigger_and_read()

    def get_start_positions(
        self, motors: list[str | DeviceBase] | list[str] | list[DeviceBase]
    ) -> list[float]:
        """
        Get the current position of the given motors. This can be used to make the positions relative to the current position of the motors.

        Args:
            motors (list[str | DeviceBase] | list[str] | list[DeviceBase]): List of motor names or device instances.

        Returns:
            list[float]: List of current positions of the motors.
        """
        start_positions = []
        for motor in motors:
            if isinstance(motor, str):
                obj = self._dev[motor]
            else:
                obj = motor
            val = obj.read()
            start_positions.append(val[obj.full_name].get("value"))
        return start_positions

    def optimize_trajectory(
        self,
        positions: np.ndarray,
        optimization_type: Literal["corridor", "shell", "nearest"] = "corridor",
        primary_axis: int = 1,
        preferred_directions: list[int] | None = None,
        corridor_size: int | None = None,
        num_iterations: int = 5,
    ) -> np.ndarray:
        """
        Optimize the trajectory of the scan by reordering the positions. This can help to minimize the movement time of the motors.
        The optimization can be done in different ways, depending on the optimization_type parameter:
            - "corridor": optimize the trajectory in a corridor-like way, where the scan moves back and forth along the primary axis. This is typically a good choice for grid scans. If preferred_directions are provided, the optimizer will try to optimize the trajectory in a way that minimizes the movement in the non-preferred direction.
            - "shell": optimize the trajectory in a shell-like way, where the scan moves in a spiral from the outside to the inside. This is typically a good choice for round scans.
            - "nearest": optimize the trajectory by always moving to the nearest next point. This is typically a good choice for random scans.

        Args:
            positions (np.ndarray): Array of positions to optimize, shape (num_points, num_motors).
            optimization_type (str, optional): Type of optimization to perform. Defaults to "corridor".
            primary_axis (int, optional): Primary axis for corridor optimization. Defaults to 1.
            preferred_directions (list[int] | None, optional): List of preferred directions for the non-primary axes. Each entry should be -1, 0, or 1, indicating the preferred direction of movement along that axis. The length of the list should be equal to the number of non-primary axes. Defaults to None, which means no preferred directions.
            corridor_size (int | None, optional): Size of the corridor for corridor optimization. Defaults to None, which means the default corridor size will be used.
        Returns:
            np.ndarray: Optimized array of positions, shape (num_points, num_motors).
        """

        if optimization_type == "corridor":
            if preferred_directions is None or len(preferred_directions) == 0:
                positions = self._path_optimizer.optimize_corridor(
                    positions,
                    num_iterations=num_iterations,
                    corridor_size=corridor_size,
                    sort_axis=primary_axis,
                )
            else:
                preferred_direction = (
                    preferred_directions[primary_axis]
                    if len(preferred_directions) > primary_axis
                    else None
                )
                positions = self._path_optimizer.optimize_corridor(
                    positions,
                    num_iterations=num_iterations,
                    sort_axis=primary_axis,
                    preferred_direction=preferred_direction,
                    corridor_size=corridor_size,
                )

        elif optimization_type == "shell":
            positions = self._path_optimizer.optimize_shell(
                positions, num_iterations=num_iterations
            )
        elif optimization_type == "nearest":
            positions = self._path_optimizer.optimize_nearest_neighbor(positions)
        else:
            raise ValueError(f"Invalid optimization type: {optimization_type}")
        return positions

    def check_limits(
        self, motors: list[str | DeviceBase] | list[str] | list[DeviceBase], positions: np.ndarray
    ):
        """
        Check if the given positions for the given motors are within the limits of the motors.
        If not, raise a LimitError.

        Args:
            motors (list[str | DeviceBase] | list[str] | list[DeviceBase]): List of motor names or device instances.
            positions (np.ndarray): Array of positions to check, shape (num_points, len(motors)).

        Raises:
            LimitError: If any of the positions are out of limits for the corresponding motor.
        """
        for motor_index, motor in enumerate(motors):
            if isinstance(motor, str):
                low_limit, high_limit = self._dev[motor].limits
            else:
                low_limit, high_limit = motor.limits
            if low_limit >= high_limit:
                # if both limits are the same or low > high, no limits are set
                continue
            for pos in positions[:, motor_index]:
                if not low_limit <= pos <= high_limit:
                    raise LimitError(
                        f"Target position {pos} for motor {motor} is out of limits ({low_limit}, {high_limit})",
                        device=motor if isinstance(motor, str) else motor.full_name,
                    )
