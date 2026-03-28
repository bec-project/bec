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

from typing import Annotated

import numpy as np

from bec_lib.device import DeviceBase
from bec_lib.logger import bec_logger
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
    scan_type = ScanType.SOFTWARE_TRIGGERED

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    scan_name = "grid_scan"

    # arg_input and arg_bundle_size are only relevant for scans that accept an arbitrary number of motor / position arguments (e.g. line scans, grid scans).
    # For scans with a fixed set of parameters (e.g. Fermat spiral), these can be simply removed.
    arg_input = {
        "device": ScanArgType.DEVICE,
        "start": Annotated[float, "device"],
        "stop": Annotated[float, "device"],
        "steps": ScanArgType.INT,
    }
    arg_bundle_size = {"bundle": len(arg_input), "min": 2, "max": None}
    required_kwargs = ["relative"]

    gui_config = {
        "Scan Parameters": [
            "exp_time",
            "settling_time",
            "burst_at_each_point",
            "relative",
            "snaked",
        ]
    }

    def __init__(
        self,
        *args,
        exp_time: Annotated[float, Units.s] = 0,
        settling_time: Annotated[float, Units.s] = 0,
        relative: bool = False,
        snaked: bool = True,
        burst_at_each_point: int = 1,
        **kwargs,
    ):
        """
        Scan two or more motors in a grid.

        Args:
            *args (Device, float, float, int): pairs of device / start / stop / steps arguments
            exp_time (Annotated[float, Units.s]): exposure time in seconds. Default is 0.
            settling_time (Annotated[float, Units.s]): settling time in seconds. Default is 0.
            relative (bool): if True, the motors will be moved relative to their current position. Default is False.
            burst_at_each_point (int): number of exposures at each point. Default is 1.
            snaked (bool): if True, the scan will be snaked. Default is True.

        Returns:
            ScanReport

        Examples:
            >>> scans.grid_scan(dev.motor1, -5, 5, 10, dev.motor2, -5, 5, 10, exp_time=0.1, relative=True)

        """
        super().__init__(**kwargs)
        self.motor_args = args
        self.motor_input_bundles = bundle_args(args, bundle_size=self.arg_bundle_size["bundle"])
        self.motors = list(self.motor_input_bundles.keys())
        self.exp_time = exp_time
        self.settling_time = settling_time
        self.relative = relative
        self.snaked = snaked
        self.burst_at_each_point = burst_at_each_point

        # Update the default scan info with provided parameters.
        self.update_scan_info(
            exp_time=exp_time,
            settling_time=settling_time,
            relative=relative,
            snaked=snaked,
            burst_at_each_point=burst_at_each_point,
        )

        # We elevate the readout priority of the scan motors to "monitored" to ensure
        # that their positions are included in every readout of the step scan.
        self.actions.set_device_readout_priority(self.motors, priority="monitored")

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """
        self.positions = position_generators.nd_grid_positions(
            self.motor_input_bundles.values(), snaked=self.snaked
        )

        if self.relative:
            self.start_positions = self.components.get_start_positions(self.motors)
            self.positions += self.start_positions

        self.components.check_limits(self.motors, self.positions)

        self.actions.add_scan_report_instruction_scan_progress(
            points=len(self.positions), show_table=False
        )

        self.update_scan_info(positions=self.positions, num_points=len(self.positions))

        self._baseline_readout_status = self.actions.read_baseline_devices(wait=False)

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
        self.actions.pre_scan()

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
        """
        self.components.step_scan(self.motors, self.positions, at_each_point=self.at_each_point)

    @scan_hook
    def at_each_point(
        self,
        motors: list[str | DeviceBase],
        positions: np.ndarray,
        last_positions: np.ndarray | None,
    ):
        """
        Logic to be executed at each point during the scan. This is called by the step_scan method at each point.

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances being moved.
            positions (np.ndarray): Current positions of the motors, shape (len(motors),).
            last_positions (np.ndarray | None): Previous positions of the motors, shape (len(motors),) or None if this is the first point.
        """
        self.components.step_scan_at_each_point(motors, positions, last_positions=last_positions)

    @scan_hook
    def post_scan(self):
        """
        Post-scan steps to be executed after the main scan logic.
        """
        status = self.actions.complete_all_devices(wait=False)

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
