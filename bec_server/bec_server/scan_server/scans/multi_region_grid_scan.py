"""
Multi-region grid scan implementation for two-motor scans with disjoint regions.

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
from bec_server.scan_server.scans.scans_v4 import ScanBase, ScanType


class MultiRegionGridScan(ScanBase):
    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = ScanType.SOFTWARE_TRIGGERED

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    scan_name = "_v4_multi_region_grid_scan"

    required_kwargs = ["regions", "relative"]

    gui_config = {
        "Motors": ["motor1", "motor2"],
        "Movement Parameters": ["regions", "relative", "snaked"],
        "Acquisition Parameters": ["exp_time", "settling_time", "burst_at_each_point"],
    }

    def __init__(
        self,
        motor1: DeviceBase,
        motor2: DeviceBase,
        *,
        regions: list[tuple[tuple[float, float, int], tuple[float, float, int]]],
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
        relative: bool = False,
        snaked: bool = True,
        burst_at_each_point: Annotated[
            int, ScanArgument(display_name="Burst at Each Point", ge=1)
        ] = 1,
        **kwargs,
    ):
        """
        Scan two motors on multiple independent rectangular sub-grids.

        For a single region,
        ``scans.multi_region_grid_scan(motor1, motor2, regions=[((start1, stop1, steps1), (start2, stop2, steps2))], ...)``
        is equivalent to the standard scan
        ``scans.grid_scan(motor1, start1, stop1, steps1, motor2, start2, stop2, steps2, ...)``.

        Args:
            motor1 (DeviceBase): first motor
            motor2 (DeviceBase): second motor
            regions (list[tuple[tuple[float, float, int], tuple[float, float, int]]]):
                sequence of paired region definitions. Each entry contains one
                ``(start, stop, steps)`` tuple for ``motor1`` and one for ``motor2``.
            exp_time (float): exposure time in seconds. Default is 0.
            frames_per_trigger (int): number of frames acquired per trigger. Default is 1.
            settling_time (float): settling time in seconds. Default is 0.
            settling_time_after_trigger (float): settling time after trigger in seconds. Default is 0.
            readout_time (float): readout time in seconds. Default is 0.
            relative (bool): if True, the generated positions are interpreted relative to the
                current motor positions. Default is False.
            snaked (bool): if True, the second axis is traversed in alternating directions
                within each sub-grid. Default is True.
            burst_at_each_point (int): number of exposures at each point. Default is 1.

        Returns:
            ScanReport

        Examples:
            >>> scans.multi_region_grid_scan(dev.motor1, dev.motor2, regions=[((-5, -1, 5), (-4, 0, 5)), ((1, 5, 3), (-4, 0, 5))], exp_time=0.1, relative=True)
        """
        super().__init__(**kwargs)
        self.motors = [motor1, motor2]
        self.regions = regions
        self.exp_time = exp_time
        self.settling_time = settling_time
        self.relative = relative
        self.snaked = snaked
        self.burst_at_each_point = burst_at_each_point

        self.update_scan_info(
            exp_time=exp_time,
            frames_per_trigger=frames_per_trigger,
            settling_time=settling_time,
            settling_time_after_trigger=settling_time_after_trigger,
            readout_time=readout_time,
            relative=relative,
            snaked=snaked,
            burst_at_each_point=burst_at_each_point,
            regions=regions,
        )
        self.actions.set_device_readout_priority(self.motors, priority="monitored")

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """
        self.positions = position_generators.multi_region_grid_positions(
            self.regions, snaked=self.snaked
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
        self._premove_motor_status.wait()
        self.actions.pre_scan()

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
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
        if self.relative:
            # Move the motors back to their starting position
            self.components.move_and_wait(self.motors, self.start_positions)
