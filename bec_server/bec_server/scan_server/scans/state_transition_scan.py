"""
Updated move scan implementation for coordinated motor repositioning commands.

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

from typing import TYPE_CHECKING, Any, Tuple

from bec_lib.device import DeviceBase, Signal
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.scan_server.scans.scan_modifier import scan_hook
from bec_server.scan_server.scans.scans_v4 import ScanBase, bundle_args

if TYPE_CHECKING:
    from bec_lib.bl_states import AggregatedStateConfig, SubDeviceStateConfig
    from bec_lib.messages import AvailableBeamlineStatesMessage

logger = bec_logger.logger


class StateTransitionScan(ScanBase):

    # Scan Type: Hardware triggered or software triggered?
    # If the main trigger and readout logic is done within the at_each_point method in scan_core, choose SOFTWARE_TRIGGERED.
    # If the main trigger and readout logic is implemented on a device that is simply kicked off in this scan, choose HARDWARE_TRIGGERED.
    # This primarily serves as information for devices: The device may need to react differently if a software trigger is expected
    # for every point.
    scan_type = None

    # Scan name: This is the name of the scan, e.g. "line_scan". This is used for display purposes and to identify the scan type in user interfaces.
    # Choose a descriptive name that does not conflict with existing scan names.
    scan_name = "_v4_state_transition"

    # We set is_scan to False to separate this class from the other scans in the user interface
    is_scan = False

    def __init__(self, *args, state_name: str, target_label: str, **kwargs):
        """
        State transition scan that moves a motor in between two states.
        The main purpose of this scan is to be used in conjunction with state
        management in BEC, and transitioning the beamline in-between different aggregated states.
        """
        super().__init__(**kwargs)
        self.state_name = state_name
        self.target_label = target_label
        # Check if the state and the target label exists, if yes, fetch the configuration for the target state
        self.config_for_label = self._fetch_config_for_label(state_name, target_label)

        # We need to sort the devices and signals in the config, and identify which of them are motor setpoint/readback pairs
        # and which of them are just readouts and thereby can not be set within the transition.
        self._settable_signals_with_setpoint: list[Tuple[Signal, Any]] = []

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions (if not done already)
        or setting up the devices.
        """
        for device_name, signal_configs in self.config_for_label.devices.items():
            dev_obj = self.device_manager.devices.get(device_name, None)
            if dev_obj is None:
                raise ValueError(f"Device {device_name} not found in device manager.")
            if isinstance(dev_obj, Signal):
                if dev_obj._info["write_access"] is False:
                    logger.info(
                        f"Signal {device_name} is read-only, skipping during state transition."
                    )
                    continue  # This is a read-only signal, we can transition it

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
            target_positions = [
                target + current
                for target, current in zip(target_positions, current_positions, strict=False)
            ]

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

    #################
    ## Custom Methods
    #################

    def _fetch_config_for_label(self, state_name: str, target_label: str) -> SubDeviceStateConfig:
        available_states_msg: AvailableBeamlineStatesMessage = self.redis_connector.get(
            MessageEndpoints.available_beamline_states()
        )
        configs = [state for state in available_states_msg.states if state.name == state_name]
        if len(configs) == 0:
            raise ValueError(f"State {state_name} not found in available states.")
        elif len(configs) > 1:  # Should not be possible, but just in case
            raise ValueError(f"Multiple states with name {state_name} found in available states.")
        config: AggregatedStateConfig = configs[0]
        if config.state_type != "AggregatedState":
            raise ValueError(
                f"State {state_name} is not an aggregated state. Transitions are only supported for aggregated states."
            )
        available_labels = list(config.states.keys())
        if target_label not in available_labels:
            raise ValueError(
                f"Target label {target_label} not found in state {state_name}. Available labels: {available_labels}"
            )
        return config.states[target_label]
