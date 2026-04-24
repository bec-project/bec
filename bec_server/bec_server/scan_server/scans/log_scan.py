"""
Logarithmic line scan implementation for one or more motors.

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

from typing import Annotated

import numpy as np

from bec_lib.device import DeviceBase
from bec_lib.scan_args import ScanArgument, Units
from bec_server.scan_server.scans import position_generators
from bec_server.scan_server.scans.scan_modifier import scan_hook
from bec_server.scan_server.scans.scans_v4 import ScanBase, ScanType, bundle_args


class LogScan(ScanBase):
    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = ScanType.SOFTWARE_TRIGGERED

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    # It must be a valid Python identifier, that is, it can only contain letters, numbers, and underscores, and must not start with a number.
    scan_name = "_v4_log_scan"

    # arg_input and arg_bundle_size are only relevant for scans that accept an arbitrary number of motor / position arguments (e.g. line scans, grid scans).
    # For scans with a fixed set of parameters (e.g. Fermat spiral), these can be simply removed.
    arg_input = {
        "device": DeviceBase,
        "start": Annotated[
            float, ScanArgument(display_name="Start Position", reference_units="device")
        ],
        "stop": Annotated[
            float, ScanArgument(display_name="Stop Position", reference_units="device")
        ],
    }
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    gui_config = {
        "Movement Parameters": ["steps", "relative"],
        "Acquisition Parameters": [
            "exp_time",
            "frames_per_trigger",
            "settling_time",
            "settling_time_after_trigger",
            "readout_time",
            "burst_at_each_point",
        ],
    }

    def __init__(
        self,
        *args,
        steps: Annotated[int, ScanArgument(display_name="Steps", ge=1)],
        relative: bool,
        exp_time: Annotated[
            float, ScanArgument(display_name="Exposure Time", units=Units.s, ge=0)
        ] = 0,
        frames_per_trigger: Annotated[
            int, ScanArgument(display_name="Frames per Trigger", ge=1)
        ] = 1,
        settling_time: Annotated[
            float, ScanArgument(display_name="Settling Time", units=Units.s, ge=0)
        ] = 0,
        settling_time_after_trigger: Annotated[
            float, ScanArgument(display_name="Settling Time After Trigger", units=Units.s, ge=0)
        ] = 0,
        readout_time: Annotated[
            float, ScanArgument(display_name="Readout Time", units=Units.s, ge=0)
        ] = 0,
        burst_at_each_point: Annotated[
            int, ScanArgument(display_name="Burst at Each Point", ge=1)
        ] = 1,
        **kwargs,
    ):
        """
        A scan for one or more motors with logarithmically spaced positions.

        Args:
            *args (Device, float, float): pairs of device / start / stop arguments
            steps (int): number of points along the trajectory.
            relative (bool): If True, the positions are interpreted relative to the
                current position.
            exp_time (float): exposure time in seconds. Default is 0.
            frames_per_trigger (int): number of frames acquired per trigger. Default is 1.
            settling_time (float): settling time in seconds. Default is 0.
            settling_time_after_trigger (float): settling time after trigger in seconds. Default is 0.
            readout_time (float): readout time in seconds. Default is 0.
            burst_at_each_point (int): number of exposures at each point. Default is 1.

        Returns:
            ScanReport

        Examples:
            >>> scans.log_scan(dev.motor1, 1, 100, steps=10, exp_time=0.1, relative=False)
        """
        super().__init__(**kwargs)
        self.motor_args = args
        self.motor_input_bundles = bundle_args(args, bundle_size=self.arg_bundle_size["bundle"])
        self.motors = list(self.motor_input_bundles.keys())
        self.steps = steps
        self.relative = relative
        self.exp_time = exp_time
        self.settling_time = settling_time
        self.burst_at_each_point = burst_at_each_point

        self.update_scan_info(
            exp_time=exp_time,
            frames_per_trigger=frames_per_trigger,
            settling_time=settling_time,
            settling_time_after_trigger=settling_time_after_trigger,
            readout_time=readout_time,
            relative=relative,
            burst_at_each_point=burst_at_each_point,
            scan_report_devices=self.motors,
        )
        self.actions.set_device_readout_priority(self.motors, priority="monitored")

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the logarithmically spaced scan trajectory before the scan starts.
        This generates the point list, resolves relative coordinates if requested,
        checks device limits, initializes progress reporting, and starts baseline readout.
        """
        self.positions = position_generators.log_scan_positions(
            list(self.motor_input_bundles.values()), steps=self.steps
        )

        if self.relative:
            self.start_positions = self.components.get_start_positions(self.motors)
            self.positions += self.start_positions

        self.components.check_limits(self.motors, self.positions)

        self.update_scan_info(
            positions=self.positions,
            num_points=len(self.positions),
            num_monitored_readouts=len(self.positions) * self.burst_at_each_point,
        )

        self.actions.add_scan_report_instruction_scan_progress(
            points=self.scan_info.num_monitored_readouts, show_table=False
        )

        self._premove_motor_status = self.actions.set(self.motors, self.positions[0], wait=False)

        self._baseline_readout_status = self.actions.read_baseline_devices(wait=False)

    @scan_hook
    def open_scan(self):
        """Open the scan."""
        self.actions.open_scan()

    @scan_hook
    def stage(self):
        """
        Stage all devices participating in the scan.
        """
        self.actions.stage_all_devices()

    @scan_hook
    def pre_scan(self):
        """
        Execute pre-scan device logic before the point-by-point trajectory begins.
        """
        self._premove_motor_status.wait()
        self.actions.pre_scan_all_devices()

    @scan_hook
    def scan_core(self):
        """
        Execute the logarithmic step scan over the prepared trajectory.
        """
        self.components.step_scan(
            self.motors,
            self.positions,
            at_each_point=self.at_each_point,
            last_positions=self.positions[0],
        )

    @scan_hook
    def at_each_point(
        self,
        motors: list[str | DeviceBase],
        positions: np.ndarray,
        last_positions: np.ndarray | None,
    ):
        """
        Execute the acquisition logic for a single point on the logarithmic trajectory.

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances being moved.
            positions (np.ndarray): Current positions of the motors, shape (len(motors),).
            last_positions (np.ndarray | None): Previous positions of the motors, shape
                (len(motors),) or None if this is the first point.
        """
        self.components.step_scan_at_each_point(motors, positions, last_positions=last_positions)

    @scan_hook
    def post_scan(self):
        """
        Complete device activity after the point-by-point trajectory finishes.
        If the scan was configured as relative, the motors are returned to their starting positions.
        """
        status = self.actions.complete_all_devices(wait=False)

        if self.relative:
            # Move the motors back to their starting position
            self.components.move_and_wait(self.motors, self.start_positions)
        status.wait()

    @scan_hook
    def unstage(self):
        """Unstage all devices after the scan completes."""
        self.actions.unstage_all_devices()

    @scan_hook
    def close_scan(self):
        """
        Close the scan after any pending baseline readout has completed.
        """
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
        if self.relative:
            # Move the motors back to their starting position
            self.components.move_and_wait(self.motors, self.start_positions)
