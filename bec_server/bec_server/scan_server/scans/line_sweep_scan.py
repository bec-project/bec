"""
Line sweep scan implementation for acquiring data while observing a moving device.

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

import threading
import time
from typing import Annotated

import numpy as np

from bec_lib.connector import MessageObject
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.scan_args import ScanArgument, Units
from bec_server.scan_server.scans.scan_modifier import scan_hook
from bec_server.scan_server.scans.scans_v4 import ScanBase, ScanType


class LineSweepScan(ScanBase):
    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = ScanType.SOFTWARE_TRIGGERED

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    scan_name = "_v4_line_sweep_scan"
    required_kwargs = ["relative"]
    gui_config = {
        "Device": ["device", "start", "stop"],
        "Scan Parameters": ["min_update", "relative"],
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
        min_update: Annotated[
            float, ScanArgument(display_name="Minimum Update", units=Units.s, ge=0)
        ] = 0,
        relative: bool = False,
        **kwargs,
    ):
        """
        Read out monitored devices while a single device moves continuously from
        start to stop.

        Args:
            device (DeviceBase): monitored device
            start (float): start position
            stop (float): stop position
            min_update (float): minimum delay between readout updates. Default is 0.
            relative (bool): if True, the start and stop positions are relative to the current position. Default is False.

        Returns:
            ScanReport

        Examples:
            >>> scans.line_sweep_scan(dev.motor1, -5, 5, min_update=0.1, relative=True)

        """
        super().__init__(**kwargs)
        device = self.dev[device] if isinstance(device, str) else device
        self.device = device
        self.motors = [device]
        self.start = start
        self.stop = stop
        self.min_update = min_update
        self.relative = relative
        self._readback_update_event = threading.Event()

        self.update_scan_info(relative=relative)
        self.actions.set_device_readout_priority(self.motors, priority="monitored")

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """
        self.positions = np.array([[self.start], [self.stop]], dtype=float)
        if self.relative:
            self.start_positions = self.components.get_start_positions(self.motors)
            self.positions += self.start_positions
        self.components.check_limits(self.motors, self.positions)
        self.actions.add_scan_report_instruction_scan_progress(points=0, show_table=False)

        self.update_scan_info(positions=self.positions, num_points=0, num_monitored_readouts=0)
        self._baseline_readout_status = self.actions.read_baseline_devices(wait=False)
        self._premove_motor_status = self.actions.set(self.motors, self.positions[0], wait=False)

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
        self._register_readback_updates()
        try:
            status = self.device.set(self.positions[1][0])

            while not status.done:
                if not self._readback_update_event.wait(timeout=0.05):
                    continue
                if not self._consume_received_update():
                    continue
                self.at_each_point()
                if self.min_update:
                    time.sleep(self.min_update)
        finally:
            self._unregister_readback_updates()

    @scan_hook
    def at_each_point(self):
        """
        Logic to be executed at each acquisition point during the scan.
        This hook allows concrete line-sweep variants to extend or override the
        per-point behavior without reimplementing the full scan_core method.
        """
        self.actions.read_monitored_devices()

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

    def _register_readback_updates(self):
        self._readback_update_event.clear()
        self.redis_connector.register(
            MessageEndpoints.device_readback(self.device.root.name),
            cb=self._device_readback_callback,
            parent=self,
        )

    def _unregister_readback_updates(self):
        self.redis_connector.unregister(
            MessageEndpoints.device_readback(self.device.root.name),
            cb=self._device_readback_callback,
        )

    @staticmethod
    def _device_readback_callback(msg: MessageObject, *, parent: "LineSweepScan", **_kwargs):
        parent._readback_update_event.set()

    def _consume_received_update(self) -> bool:
        if not self._readback_update_event.is_set():
            return False
        self._readback_update_event.clear()
        return True
