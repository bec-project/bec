"""
Acquire scan implementation for taking one or more exposures without moving motors.

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

import numpy as np

from bec_lib.scan_args import DefaultArgType
from bec_server.scan_server.scans.scan_base import ScanBase, ScanType
from bec_server.scan_server.scans.scan_modifier import scan_hook


class Acquire(ScanBase):
    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = ScanType.SOFTWARE_TRIGGERED

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    # It must be a valid Python identifier, that is, it can only contain letters, numbers, and underscores, and must not start with a number.
    scan_name = "_v4_acquire"

    gui_config = {
        "Scan Parameters": [
            "exp_time",
            "frames_per_trigger",
            "settling_time",
            "settling_time_after_trigger",
            "readout_time",
            "burst_at_each_point",
        ]
    }

    def __init__(
        self,
        exp_time: DefaultArgType.ExposureTime = 0,
        frames_per_trigger: DefaultArgType.FramesPerTrigger = 1,
        settling_time: DefaultArgType.SettlingTime = 0,
        settling_time_after_trigger: DefaultArgType.SettlingTimeAfterTrigger = 0,
        readout_time: DefaultArgType.ReadoutTime = 0,
        burst_at_each_point: DefaultArgType.BurstAtEachPoint = 1,
        **kwargs,
    ):
        """
        A simple acquisition at the current position.

        Args:
            exp_time (float): exposure time in seconds. Default is 0.
            frames_per_trigger (int): number of frames acquired per trigger. Default is 1.
            settling_time (float): settling time before the trigger in seconds. Default is 0.
            settling_time_after_trigger (float): settling time after the trigger in seconds. Default is 0.
            readout_time (float): readout time after the trigger in seconds. Default is 0.
            burst_at_each_point (int): number of acquisitions. Default is 1.

        Returns:
            ScanReport

        Examples:
            >>> scans.acquire(exp_time=0.1)

        """
        super().__init__(**kwargs)
        self.motors = []
        self.exp_time = exp_time
        self.frames_per_trigger = frames_per_trigger
        self.settling_time = settling_time
        self.settling_time_after_trigger = settling_time_after_trigger
        self.readout_time = readout_time
        self.burst_at_each_point = burst_at_each_point

        # Update the default scan info with provided parameters.
        self.update_scan_info(
            exp_time=exp_time,
            frames_per_trigger=frames_per_trigger,
            settling_time=settling_time,
            settling_time_after_trigger=settling_time_after_trigger,
            readout_time=readout_time,
            burst_at_each_point=burst_at_each_point,
        )

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """
        self.update_scan_info(
            positions=np.array([]), num_points=1, num_monitored_readouts=self.burst_at_each_point
        )

        self.actions.add_scan_report_instruction_scan_progress(
            points=self.scan_info.num_monitored_readouts, show_table=False
        )

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
        self.actions.pre_scan_all_devices()

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
        """
        for _ in range(self.burst_at_each_point):
            self.at_each_point()

    @scan_hook
    def at_each_point(self):
        """
        Logic to be executed at each acquisition point during the scan.
        This hook allows concrete acquire-like scans to extend or override the
        per-point behavior without reimplementing the full scan_core method.
        """
        self.components.trigger_and_read()

    @scan_hook
    def post_scan(self):
        """
        Post-scan steps to be executed after the main scan logic.
        """
        self.actions.complete_all_devices()

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
