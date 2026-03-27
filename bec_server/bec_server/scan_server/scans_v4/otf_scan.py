"""
Module for handling scans for v4 of BEC. In contrast to previous implementations, the scan logic does not rely on generators
executed on the worker but instead uses the RedisConnector to send commands directly to the devices.
"""

from __future__ import annotations

from bec_lib.logger import bec_logger
from bec_server.scan_server.scans_v4 import ScanBase, ScanType, scan_hook

logger = bec_logger.logger


class OTFScan(ScanBase):

    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = ScanType.HARDWARE_TRIGGERED
    scan_name = "otf_scan"
    gui_config = {"Scan Parameters": ["e1", "e2", "time"]}

    def __init__(
        self, e1: float, e2: float, time: float, mono: str = "mono", otf: str = "otf", **kwargs
    ):
        """
        OTF scan

        Args:
            e1 (float): first energy parameter.
            e2 (float): second energy parameter.
            time (float): time parameter.

        Returns:
            ScanReport

        Examples:
            >>> scans.otf_scan(e1=700, e2=740, time=4)

        """
        super().__init__(**kwargs)
        self.e1 = e1
        self.e2 = e2
        self.time = time
        self.mono = self.dev[mono]
        self.otf = self.dev[otf]
        self._baseline_readout_status = None

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        If you modify the readout priority of any devices, make sure to trigger the baseline
        readout after that to ensure that the new readout groups are properly applied.
        """
        self.actions.set_device_readout_priority([self.mono], priority="monitored")

        self.actions.add_scan_report_instruction_device_progress(device=self.otf)

        self._baseline_readout_status = self.actions.read_baseline_devices(wait=False)

    @scan_hook
    def open_scan(self):
        """
        Open the scan.
        This step must call self.actions.open_scan() to ensure that a new scan is
        opened. Make sure to prepare the scan metadata before, either in
        prepare_scan() or in open_scan() itself.
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
        logic is executed. For example, this is a good place to initialize time-critical
        devices, e.g. devices that have a short timeout.
        The pre-scan logic is typically implemented on the device itself.
        """
        self.actions.pre_scan()

        # Move to the first energy
        self.mono.set(self.e1).wait()

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
        """
        self.otf.kickoff(parameter={"e1": self.e1, "e2": self.e2, "time": self.time}).wait()
        status = self.otf.complete()
        while not status.done:
            self.at_each_point()

    @scan_hook
    def at_each_point(self):
        """
        Logic to be executed at each point during the scan.
        """
        self.actions.read_monitored_devices()

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
