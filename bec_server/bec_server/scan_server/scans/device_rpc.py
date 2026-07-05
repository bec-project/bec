"""
Device RPC implementation.

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

from bec_lib.device import DeviceBase
from bec_lib.logger import bec_logger
from bec_lib.scan_args import ScanArgument
from bec_server.scan_server.scans.scan_base import ScanBase
from bec_server.scan_server.scans.scan_modifier import scan_hook

logger = bec_logger.logger


class DeviceRpc(ScanBase):
    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = None
    is_scan = False

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    # It must be a valid Python identifier, that is, it can only contain letters, numbers, and underscores, and must not start with a number.
    scan_name = "_v4_device_rpc"

    def __init__(
        self,
        device: Annotated[DeviceBase, ScanArgument(display_name="Device", description="Device.")],
        func: Annotated[
            str, ScanArgument(display_name="RPC function", description="RPC function.")
        ],
        func_args: Annotated[
            list,
            ScanArgument(
                display_name="RPC function arguments", description="RPC function arguments."
            ),
        ],
        func_kwargs: Annotated[
            dict,
            ScanArgument(
                display_name="RPC function keyword arguments",
                description="RPC function keyword arguments.",
            ),
        ],
        rpc_id: Annotated[str, ScanArgument(display_name="RPC ID", description="RPC ID.")],
        **kwargs,
    ):
        """
        Scan implementation.

        Args:
            device (DeviceBase): Device.
            func (str): RPC function.
            func_args (list): RPC function arguments.
            func_kwargs (dict): RPC function keyword arguments.

        Returns:
            ScanReport
        """
        super().__init__(**kwargs)
        self.device = device
        self.rpc_id = rpc_id
        self.func = func
        self.func_args = func_args
        self.func_kwargs = func_kwargs

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
        status = self.actions.rpc_call_no_wait(
            self.device, self.func, self.rpc_id, *self.func_args, **self.func_kwargs
        )
        # We don't wait for the RPC to resolve
        self.actions._status_registry.pop(status._device_instr_id)

    @scan_hook
    def at_each_point(self):
        """
        Logic to be executed at each acquisition point during the scan.
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

    #######################################################
    ######### Helper methods for the scan logic ###########
    #######################################################

    # Implement scan-specific helper methods below.
