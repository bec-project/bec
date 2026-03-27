"""
Grid Scan.

Scan procedure:
    - prepare_scan
    - open_scan
    - stage
    - pre_scan
    - scan_core
        - at_each_point (optionally called by scan_core)
    - post_scan
    - unstage
    - close_scan
    - on_exception (called if any exception is raised during the scan)
"""

from __future__ import annotations

import time
from typing import Annotated

import numpy as np

from bec_lib.device import DeviceBase
from bec_lib.logger import bec_logger
from bec_server.scan_server.errors import ScanAbortion
from bec_server.scan_server.scans import ScanArgType, unpack_scan_args
from bec_server.scan_server.scans_v4 import (
    ScanBase,
    ScanType,
    Units,
    bundle_args,
    position_generators,
    scan_hook,
)

logger = bec_logger.logger


class GridScan(ScanBase):

    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = ScanType.HARDWARE_TRIGGERED

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    scan_name = "owis_grid"

    # arg_input and arg_bundle_size are only relevant for scans that accept an arbitrary number of motor / position arguments (e.g. line scans, grid scans).
    # For scans with a fixed set of parameters (e.g. Fermat spiral), these can be simply removed.

    gui_config = {
        "Motor Parameters": [
            "motor_fly",
            "start_fly",
            "stop_fly",
            "motor_step",
            "start_step",
            "stop_step",
        ],
        "Scan Parameters": ["exp_time", "relative"],
    }

    def __init__(
        self,
        motor_fly: DeviceBase,
        start_fly: Annotated[float, "motor_fly"],
        stop_fly: Annotated[float, "motor_fly"],
        interval_fly: int,
        motor_step: DeviceBase,
        start_step: Annotated[float, "motor_step"],
        stop_step: Annotated[float, "motor_step"],
        interval_step: int,
        exp_time: Annotated[float, Units.s],
        relative: bool,
        enforce_positive_to_negative: bool = True,
        **kwargs,
    ):
        """
        Scan two or more motors in a grid. This scan will always enforce scanning from positive to negative

        Args:
            motor_fly (DeviceBase): Motor to be moved in fly mode.
            start_fly (float): Start position for the fly motor.
            stop_fly (float): Stop position for the fly motor.
            interval_fly (int): Interval for the fly motor.
            motor_step (DeviceBase): Motor to be moved in step mode.
            start_step (float): Start position for the step motor.
            stop_step (float): Stop position for the step motor.
            interval_step (int): Interval for the step motor.
            exp_time (float): Exposure time at each point in seconds.
            relative (bool): Whether the start and stop positions are relative to the current position of the motors.

        Returns:
            ScanReport

        Examples:
            >>> scans.owis_grid(
            ...     motor_fly=dev.fly_motor,
            ...     start_fly=0,
            ...     stop_fly=10,
            ...     motor_step=dev.step_motor,
            ...     start_step=0,
            ...     stop_step=5,
            ...     exp_time=0.1,
            ...     relative=False,
            ... )

        """
        super().__init__(**kwargs)
        if enforce_positive_to_negative:
            if start_fly < stop_fly:
                start_fly, stop_fly = stop_fly, start_fly
            if start_step < stop_step:
                start_step, stop_step = stop_step, start_step
        self.interval_step = interval_step
        self.interval_fly = interval_fly
        self.exp_time = exp_time
        self.motor_fly = motor_fly
        self.motor_fly_start = start_fly
        self.motor_fly_stop = stop_fly
        self.motor_fly_step = self._calculate_step(start_fly, stop_fly, interval_fly)
        self.motor_step = motor_step
        self.motor_step_start = start_step
        self.motor_step_stop = stop_step
        self.motor_step_step = self._calculate_step(start_step, stop_step, interval_step)
        self.motors = [motor_fly, motor_step]
        self.relative = relative

        self.shutter_additional_width = 0.15
        self.sign = 1
        self.add_pre_move_time = 0.0

        self.high_velocity = None
        self.high_acc_time = None
        self.base_velocity = None
        self.target_velocity = None
        self.acc_time = None
        self.premove_distance = None

        # Store fly motor velocity
        self.motor_fly_initial_velocity = None
        # Store initial shutter to open delay
        self.shutter_to_open_delay = None

        # Update the default scan info with provided parameters.
        self.update_scan_info(
            exp_time=exp_time, relative=relative, frames_per_trigger=self.interval_fly
        )

    ####
    # Scan hooks
    ####

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """
        self.positions = position_generators.nd_grid_positions(
            [
                (self.motor_fly_start, self.motor_fly_stop, self.motor_fly_step),
                (self.motor_step_start, self.motor_step_stop, self.motor_step_step),
            ],
            snaked=False,
        )

        if self.relative:
            self.start_positions = self.components.get_start_positions(self.motors)
            self.positions += self.start_positions

        self.components.check_limits(self.motors, self.positions)

        # Check if mcs is there TODO
        self.actions.add_scan_report_instruction_device_progress(device="mcs")

        self.update_scan_info(positions=self.positions, num_points=len(self.positions))

        self._baseline_readout_status = self.actions.read_baseline_devices(wait=False)

        self._compute_scan_params()

    @scan_hook
    def open_scan(self):
        """
        Open the scan.
        This step must call self.actions.open_scan() to ensure that a new scan is
        opened. Make sure to prepare the scan metadata before, either in
        prepare_scan() or in open_scan() itself and call self.update_scan_info(...)
        to update the scan metadata if needed.
        """
        self.actions.open_scan()

    @scan_hook
    def stage(self):
        """
        Stage the devices for the upcoming scan. The stage logic is typically
        implemented on the device itself (i.e. by the device's stage method).
        However, if there are any additional steps that need to be executed before
        staging the devices, they can be implemented here.
        """
        self.actions.stage_all_devices()

    @scan_hook
    def pre_scan(self):
        """
        Pre-scan steps to be executed before the main scan logic.
        This is typically the last chance to prepare the devices before the core scan
        logic is executed. For example, this is a good place to initialize time-criticial
        devices, e.g. devices that have a short timeout.
        The pre-scan logic is typically implemented on the device itself.
        """
        fly_start, step_start = self.positions[0][0] - self.premove_distance, self.positions[0][1]
        self.components.move_and_wait(self.motors, [fly_start, step_start])
        self.actions.pre_scan()

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
        """
        # Avoid moving motors for the first point, we already moved in pre_scan
        last_positions = self.positions[0][0] - self.premove_distance, self.positions[0][1]
        for pos in self.positions[:: self.interval_fly]:
            fly_pos, step_pos = pos
            self.components.move_and_wait(
                self.motors,
                [fly_pos - self.premove_distance, step_pos],
                last_positions=last_positions,
            )
            self.motor_fly.velocity.set(self.target_velocity).wait()
            self.at_each_point(target_pos_fly)
            self.motor_fly.velocity.set(self.motor_fly_initial_velocity).wait()
            last_positions = None  # Only relevant for the first point in the first line, as we already moved in pre_scan hook.

    @scan_hook
    def at_each_point(self, target_pos_fly: float):
        """
        Logic to be executed at each point during the scan. This is called by the step_scan method at each point.

        Args:
            target_pos_fly (float): Target position of the fly motor.

        """
        status_flyer = self.motor_fly.set(target_pos_fly)
        status_ddg = self.ddg1.trigger()
        while not status_flyer.done:
            status = self.actions.read_monitored_devices(wait=False)
            time.sleep(1)
            status.wait()
        if not status_ddg.done:
            raise ScanAbortion(
                f"DDG1 trigger did not complete successfully during the motion of the fly motor {self.motor_fly}."
            )

    @scan_hook
    def post_scan(self):
        """
        Post-scan steps to be executed after the main scan logic.
        """
        status = self.actions.complete_all_devices(wait=False)
        self._reset_device_settings()

        if self.relative:
            # Move the motors back to their starting position
            self.components.move_and_wait(self.motors, self.start_positions)
        status.wait()

    @scan_hook
    def unstage(self):
        """Unstage the scan by executing post-scan steps."""
        self.actions.unstage_all_devices()

    @scan_hook
    def close_scan(self):
        """Close the scan."""
        if self._baseline_readout_status is not None:
            self._baseline_readout_status.wait()
        self.actions.close_scan()
        self.actions.check_for_unchecked_statuses()

    @scan_hook
    def on_exception(self, exception: Exception):
        """
        Handle exceptions that occur during the scan.
        This is a good place to implement any cleanup logic that needs to be executed in case of an exception,
        such as returning the devices to a safe state or moving the motors back to their starting position.
        """
        self._reset_device_settings()

    ####
    # Utility functions
    ####

    def _reset_device_settings(self):
        """Reset the motor settings that were changed for the scan."""
        self.motor_fly.velocity.set(self.motor_fly_initial_velocity).wait()
        self.actions.rpc("ddg1", "set_shutter_to_open_delay", self.shutter_to_open_delay)

    def _calculate_step(self, start: float, stop: float, interval: int) -> float:
        """Calculate the step size based on the start, stop and interval."""
        if interval < 2:
            raise ValueError("Interval must be at least 2 to define a valid scan range.")
        return (stop - start) / (interval - 1)

    def _compute_scan_params(self):

        self.high_velocity = self.motor_fly.velocity.get()
        self.high_acc_time = self.motor_fly.acceleration.get()
        self.base_velocity = self.motor_fly.base_velocity.get()
        self.motor_fly_initial_velocity = self.motor_fly.velocity.get()
        self.shutter_to_open_delay = self.actions.rpc("ddg1", "get_shutter_to_open_delay")

        self.target_velocity = self.motor_fly_step / self.exp_time
        self.acc_time = (
            (self.target_velocity - self.base_velocity)
            / (self.high_velocity - self.base_velocity)
            * self.high_acc_time
        )
        self.premove_distance = (
            0.5 * (self.target_velocity + self.base_velocity) * self.acc_time
            + self.add_pre_move_time * self.target_velocity
        )

        if self.target_velocity > self.high_velocity or self.target_velocity < self.base_velocity:
            raise ScanAbortion(
                f"Requested velocity of {self.target_velocity} exceeds {self.high_velocity}"
            )
