"""
Continuous line scan implementation for one motor with software-managed readout.

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
from bec_server.scan_server.errors import LimitError, ScanAbortion
from bec_server.scan_server.scans.scan_base import ScanBase, ScanType
from bec_server.scan_server.scans.scan_modifier import scan_hook


class ContLineScan(ScanBase):
    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = ScanType.SOFTWARE_TRIGGERED

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    # It must be a valid Python identifier, that is, it can only contain letters, numbers, and underscores, and must not start with a number.
    scan_name = "_v4_cont_line_scan"
    required_kwargs = ["steps", "relative"]
    gui_config = {
        "Device": ["device", "start", "stop"],
        "Movement Parameters": ["steps", "relative", "offset", "atol"],
        "Acquisition Parameters": ["exp_time", "readout_time", "frames_per_trigger"],
    }

    def __init__(
        self,
        device: DeviceBase,
        start: Annotated[
            float, ScanArgument(display_name="Start Position", reference_units="device")
        ],
        stop: Annotated[
            float, ScanArgument(display_name="Stop Position", reference_units="device")
        ],
        steps: Annotated[int, ScanArgument(display_name="Number of Steps", ge=1)],
        offset: Annotated[
            float | None, ScanArgument(display_name="Offset", reference_units="device")
        ] = None,
        atol: Annotated[
            float | None, ScanArgument(display_name="Tolerance", reference_units="device")
        ] = None,
        exp_time: Annotated[
            float, ScanArgument(display_name="Exposure Time", units=Units.s, ge=0)
        ] = 0,
        readout_time: Annotated[
            float, ScanArgument(display_name="Readout Time", units=Units.s, ge=0)
        ] = 0,
        frames_per_trigger: Annotated[
            int, ScanArgument(display_name="Frames per Trigger", ge=1)
        ] = 1,
        relative: bool = False,
        **kwargs,
    ):
        """
        A continuous line scan. Use this scan if you want to move a motor continuously
        from start to stop position while acquiring data at predefined positions.

        Args:
            device (DeviceBase): motor to move continuously
            start (float): start position
            stop (float): stop position
            offset (float | None): optional trigger offset from the nominal positions.
            atol (float | None): optional tolerance used for position matching.
            exp_time (Annotated[float, Units.s]): exposure time in seconds. Default is 0.
            steps (int): number of acquisition points. Default is 10.
            relative (bool): if True, interpret start and stop relative to the current motor position.

        Returns:
            ScanReport

        Examples:
            >>> scans.cont_line_scan(dev.motor1, -5, 5, steps=20, exp_time=0.05, relative=True)
        """
        super().__init__(**kwargs)
        self.device = device
        self.motors = [device]
        self.start = start
        self.stop = stop
        self.offset = offset
        self.atol = atol
        self.exp_time = exp_time
        self.steps = steps
        self.relative = relative
        self.readout_time = readout_time
        self.frames_per_trigger = frames_per_trigger
        self.motor_acceleration = None
        self.motor_velocity = None
        self.dist_step = None
        self.time_per_step = None
        self._point_index = 0

        self.update_scan_info(
            exp_time=exp_time,
            relative=relative,
            readout_time=readout_time,
            frames_per_trigger=frames_per_trigger,
            scan_report_devices=self.motors,
        )
        self.actions.set_device_readout_priority(self.motors, priority="monitored")

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """
        self._get_motor_attributes()
        self.positions = np.linspace(self.start, self.stop, self.steps, dtype=float)[:, np.newaxis]
        if self.relative:
            self.start_positions = self.components.get_start_positions(self.motors)
            self.positions += self.start_positions
        self.dist_step = self.positions[1][0] - self.positions[0][0]
        self._calculate_offset()
        self._calculate_atol()
        self.time_per_step = self.dist_step / self.motor_velocity
        if self.time_per_step < self.exp_time:
            raise ScanAbortion(
                f"Motor {self.device} is moving too fast. Time per step: {self.time_per_step:.03f} < Exp_time: {self.exp_time:.03f}. Consider reducing speed {self.motor_velocity} or reducing exp_time {self.exp_time}"
            )
        self._check_continuous_limits()

        self.update_scan_info(
            positions=self.positions,
            num_points=len(self.positions),
            num_monitored_readouts=len(self.positions),
        )

        self.actions.add_scan_report_instruction_scan_progress(
            points=self.scan_info.num_monitored_readouts, show_table=False
        )

        self._baseline_readout_status = self.actions.read_baseline_devices(wait=False)

        # Pre-move the motor to the start position
        self._premove_motor_status = self.actions.set(
            self.device, self.positions[0][0] - self.offset, wait=False
        )

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
        self.actions.pre_scan_all_devices()

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
        """
        self.actions.set(self.device, self.positions[0][0] - self.offset, wait=True)
        status = self.actions.set(self.device, self.positions[-1][0], wait=False)

        while self._point_index < len(self.positions):
            cont_motor_positions = self.device.read(cached=True)
            if not cont_motor_positions:
                continue
            cont_motor_position = cont_motor_positions[self.device.full_name].get("value")
            target_position = self.positions[self._point_index][0]
            if np.isclose(cont_motor_position, target_position, atol=self.atol):
                self.at_each_point()
                self._point_index += 1
                continue
            if cont_motor_position > target_position:
                raise ScanAbortion(
                    f"Skipped point {self._point_index + 1}: Consider reducing speed {self.motor_velocity}, increasing the atol {self.atol}, or increasing the offset {self.offset}"
                )
        status.wait()

    @scan_hook
    def at_each_point(self):
        """
        Logic to be executed at each acquisition point during the scan.
        This hook allows concrete continuous-line variants to extend or override the
        per-point behavior without reimplementing the full scan_core method.
        """
        self.components.trigger_and_read()

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

    #######################################################
    ######### Helper methods for the scan logic ###########
    #######################################################

    def _get_motor_attributes(self):
        if not hasattr(self.device, "velocity"):
            raise ScanAbortion(f"Motor {self.device} does not have a velocity attribute.")
        if not hasattr(self.device, "acceleration"):
            raise ScanAbortion(f"Motor {self.device} does not have an acceleration attribute.")
        self.motor_velocity = self.device.velocity.get()
        self.motor_acceleration = self.device.acceleration.get()

    def _calculate_offset(self):
        if self.offset is not None:
            return
        self.offset = 0.5 * self.motor_acceleration * self.motor_velocity

    def _calculate_atol(self):
        update_freq = 10
        tolerance = 0.1
        precision = 10 ** (-self.device.precision)
        if self.atol is not None:
            return
        self.atol = tolerance * self.motor_velocity * self.exp_time
        self.atol = max(self.atol, 2 * precision)
        if self.atol / update_freq > self.motor_velocity:
            raise ScanAbortion(
                f"Motor {self.device} is moving too fast with the calculated tolerance. Consider reducing speed {self.motor_velocity} or increasing the atol {self.atol}"
            )
        self.atol = max(self.atol, 2 * 1 / update_freq * self.motor_velocity)

    def _check_continuous_limits(self):
        low_limit, high_limit = self.device.limits
        if low_limit >= high_limit:
            return
        for ii, pos in enumerate(self.positions):
            pos_axis = pos[0] - self.offset if ii == 0 else pos[0]
            if not low_limit <= pos_axis <= high_limit:
                raise LimitError(
                    f"Target position including offset {pos_axis} (offset: {self.offset}) for motor {self.device} is outside of range: [{low_limit}, {high_limit}]",
                    device=self.device.name,
                )
