"""
Updated Move Scan.

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

from bec_lib.logger import bec_logger
from bec_server.scan_server.legacy_scans import ScanArgType
from bec_server.scan_server.scans import ScanBase, bundle_args, scan_hook

logger = bec_logger.logger


class UpdatedMoveScan(ScanBase):

    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = None

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    scan_name = "_v4_umv"

    # arg_input and arg_bundle_size are only relevant for scans that accept an arbitrary number of motor / position arguments (e.g. line scans, grid scans).
    # For scans with a fixed set of parameters (e.g. Fermat spiral), these can be simply removed.
    arg_input = {"device": ScanArgType.DEVICE, "target": ScanArgType.FLOAT}
    arg_bundle_size = {"bundle": len(arg_input), "min": 1, "max": None}
    required_kwargs = ["relative"]

    # We set is_scan to False to separate this class from the other scans in the user interface
    is_scan = False

    def __init__(self, *args, relative: bool = False, **kwargs):
        """
        Simple move command that moves one or more motors to the specified positions.
        The umv command is the blocking version of the mv command.
        It waits for the motors to reach their target positions before returning control to the user.


        Args:
            *args (Device, float): pairs of device / target position arguments
            relative (bool): if True, the motors will be moved relative to their current position. Default is False.

        Returns:
            ScanReport

        Examples:
            >>> scans.umv(dev.motor1, -5, dev.motor2, 5, relative=True)

        """
        super().__init__(**kwargs)
        self.motor_args = args
        self.motor_args_bundles = bundle_args(args, self.arg_bundle_size["bundle"])
        self.motors = list(self.motor_args_bundles.keys())
        self.relative = relative

        # Update the default scan info with provided parameters.
        self.update_scan_info(relative=relative)

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """

    @scan_hook
    def open_scan(self):
        """
        Open the scan.
        This step must call self.actions.open_scan() to ensure that a new scan is
        opened. Make sure to prepare the scan metadata before, either in
        prepare_scan() or in open_scan() itself and call self.update_scan_info(...)
        to update the scan metadata if needed.
        """

    @scan_hook
    def stage(self):
        """
        Stage the devices for the upcoming scan. The stage logic is typically
        implemented on the device itself (i.e. by the device's stage method).
        However, if there are any additional steps that need to be executed before
        staging the devices, they can be implemented here.
        """

    @scan_hook
    def pre_scan(self):
        """
        Pre-scan steps to be executed before the main scan logic.
        This is typically the last chance to prepare the devices before the core scan
        logic is executed. For example, this is a good place to initialize time-criticial
        devices, e.g. devices that have a short timeout.
        The pre-scan logic is typically implemented on the device itself.
        """

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
        """
        current_positions = self.components.get_start_positions(self.motors)
        target_positions = list(self.motor_args_bundles.values())
        target_positions = [pos[0] for pos in target_positions]
        if self.relative:
            target_positions += current_positions

        self.actions.add_scan_report_instruction_readback(
            devices=self.motors,
            start=current_positions,
            stop=target_positions,
            request_id=self.scan_info.metadata["RID"],
        )

        self.components.move_and_wait(self.motors, target_positions)

    @scan_hook
    def at_each_point(self):
        """
        Logic to be executed at each point during the scan. This is called by the step_scan method at each point.

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances being moved.
            positions (np.ndarray): Current positions of the motors, shape (len(motors),).
            last_positions (np.ndarray | None): Previous positions of the motors, shape (len(motors),) or None if this is the first point.
        """

    @scan_hook
    def post_scan(self):
        """
        Post-scan steps to be executed after the main scan logic.
        """

    @scan_hook
    def unstage(self):
        """Unstage the scan by executing post-scan steps."""

    @scan_hook
    def close_scan(self):
        """Close the scan."""

    @scan_hook
    def on_exception(self, exception: Exception):
        """
        Handle exceptions that occur during the scan.
        This is a good place to implement any cleanup logic that needs to be executed in case of an exception,
        such as returning the devices to a safe state or moving the motors back to their starting position.
        """
