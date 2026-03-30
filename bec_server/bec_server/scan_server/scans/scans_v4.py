"""
Module for handling scans for v4 of BEC. In contrast to previous implementations, the scan logic does not rely on generators
executed on the worker but instead uses the RedisConnector to send commands directly to the devices.
"""

from __future__ import annotations

import enum
import threading
import time
import uuid
from typing import Annotated, Callable, Literal

import numpy as np
import pint
from pydantic import BaseModel, ConfigDict, Field
from toolz import partition

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import DeviceBase
from bec_lib.devicemanager import DeviceManagerBase as DeviceManager
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.errors import LimitError
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.path_optimization import PathOptimizerMixin
from bec_server.scan_server.scan_stubs import ScanStubStatus
from bec_server.scan_server.scans.scan_modifier import scan_hook

logger = bec_logger.logger

Units = pint.UnitRegistry()


class ScanType(str, enum.Enum):
    HARDWARE_TRIGGERED = "hardware_triggered"
    SOFTWARE_TRIGGERED = "software_triggered"


def bundle_args(args: tuple, bundle_size: int) -> dict:
    """
    Bundle the given arguments into bundles of the given size.

    Args:
        args (tuple): arguments to bundle
        bundle_size (int): size of the bundles

    Returns:
        dict: bundled arguments

    """
    params = {}
    for cmds in partition(bundle_size, args):
        params[cmds[0]] = list(cmds[1:])
    return params


class ScanActions:
    """Class to handle the core actions for the scan logic."""

    def __init__(self, scan: ScanBase):
        self._scan = scan
        self._connector = scan.redis_connector
        self._device_manager = scan.device_manager
        self._instruction_handler = scan._instruction_handler
        self._status_registry = {}
        self._shutdown_event = scan._shutdown_event
        self._status_registry = {}
        self._shutdown_event = threading.Event()
        self._num_monitored_readouts = 0
        self._interruption_callback: Callable[[], None] | None = None
        self._update_queue_info_callback: Callable[[], None] | None = None
        self._scan_status_callback: (
            Callable[
                [
                    Literal["open", "paused", "closed", "aborted", "halted", "user_completed"],
                    Literal["user", "alarm"] | None,
                ],
                None,
            ]
            | None
        ) = None
        self._devices_with_required_response = set()
        self._readout_groups_read = False

    @property
    def readout_priority(self) -> dict:
        return self._scan.scan_info.readout_priority_modification

    def open_scan(self):
        """
        Open the scan.
        We fetch all relevant metadata from the scan object and emit a new scan status
        """
        if self._scan_status_callback is not None:
            self._scan_status_callback("open", None)

    def stage_all_devices(self, wait=True) -> ScanStubStatus:
        """
        Stage all devices for the scan. This will call the "stage" method
        on all devices. If you want to stage only specific devices, use the "stage" method.

        .. note ::
            We exclude devices that are on_request or continuous as they are not expected to be staged for a scan.

        Args:
            wait (bool, optional): if True, wait for the staging to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the staging process
        """
        status = self._create_status(is_container=True, name="stage_all_devices")

        # We separate the staging of async devices and regular devices to optimize the staging process.
        # Async devices are typically slower to stage and should be staged in parallel.
        async_devices = self._device_manager.devices.async_devices(
            readout_priority=self.readout_priority
        )
        excluded_devices = [device.name for device in async_devices]
        excluded_devices.extend(
            device.name
            for device in self._device_manager.devices.on_request_devices(
                readout_priority=self.readout_priority
            )
        )
        excluded_devices.extend(
            device.name
            for device in self._device_manager.devices.continuous_devices(
                readout_priority=self.readout_priority
            )
        )

        if async_devices:
            async_devices = sorted(async_devices, key=lambda x: x.name)

        for det in async_devices:
            sub_status = self.stage(det, status_name=f"stage_{det.name}", wait=False)
            status.add_status(sub_status)

        # Now we stage the remaining devices. This will be done sequentially, assuming that
        # they are typically no-op or fast operations.
        stage_device_names_without_async = [
            dev.root.name
            for dev in self._device_manager.devices.enabled_devices
            if dev.name not in excluded_devices
        ]

        if stage_device_names_without_async:
            sub_status = self.stage(
                stage_device_names_without_async, status_name="stage_sync_devices", wait=False
            )
            status.add_status(sub_status)
        if wait:
            status.wait()
        return status

    def stage(
        self,
        device: str | DeviceBase | list[str | DeviceBase],
        status_name: str | None = None,
        wait=True,
    ) -> ScanStubStatus:
        """
        Stage a device for the scan. If you want to stage all devices, use the `stage_all_devices` method.

        Args:
            device (str or DeviceBase or list[str or DeviceBase]): device(s) to stage
            status_name (str, optional): name for the status object. Defaults to None.
            wait (bool, optional): if True, wait for the staging to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the staging process
        """

        # We support str and DeviceBase inputs as well as lists of those.
        # We convert them to a list of device names for easier processing.
        if isinstance(device, list):
            device_names = []
            for dev in device:
                if isinstance(dev, DeviceBase):
                    device_names.append(dev.name)
                else:
                    device_names.append(dev)
        else:
            device_names = [device.name if isinstance(device, DeviceBase) else device]
        if len(device_names) == 1:
            device_names = device_names[0]
        status = self._create_status(name=status_name or f"stage_{device_names}")

        # If there are no devices to stage, we can immediately set the status to done and return.
        if len(device_names) == 0:
            status.set_done()
            return status

        instr = messages.DeviceInstructionMessage(
            device=device_names,
            action="stage",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def pre_scan(self, wait=True) -> ScanStubStatus:
        """
        Pre-scan steps to be executed before the main scan logic.
        This is typically the last chance to prepare the devices before the core scan
        logic is executed. For example, this is a good place to initialize time-critical
        devices, e.g. devices that have a short timeout.

        Args:
            wait (bool, optional): if True, wait for the pre-scan steps to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the pre-scan process
        """
        status = self._create_status(name="pre_scan")

        devices = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        if devices:
            devices = sorted(devices)

        instr = messages.DeviceInstructionMessage(
            device=devices,
            action="pre_scan",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def set(
        self,
        device: str | DeviceBase | list[str | DeviceBase],
        value: float | list[float],
        wait=True,
    ) -> ScanStubStatus:
        """
        Set one or multiple devices to specific values.

        Args:
            device (str or DeviceBase or list[str or DeviceBase]): device(s) to set
            value (float or list[float]): target value(s) for the device(s)
            wait (bool, optional): if True, wait for the set operation to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the set process
        """
        devices = device if isinstance(device, list) else [device]
        values = value.tolist() if isinstance(value, np.ndarray) else value
        values = values if isinstance(values, list) else [values]

        if len(devices) != len(values):
            raise ValueError("The number of devices and values must match.")

        status = self._create_status(is_container=True, name="set")
        for dev, val in zip(devices, values, strict=False):
            device_name = dev.name if isinstance(dev, DeviceBase) else dev
            sub_status = self._create_status(name=f"set_{device_name}")
            instr = messages.DeviceInstructionMessage(
                device=device_name,
                action="set",
                parameter={"value": val},
                metadata={"device_instr_id": sub_status._device_instr_id},
            )
            self._send(instr)
            status.add_status(sub_status)

        if wait:
            status.wait()
        return status

    def kickoff(
        self, device: str | DeviceBase, parameters: dict | None = None, wait=True
    ) -> ScanStubStatus:
        """
        Kickoff a device with the given parameters.

        Args:
            device (str or DeviceBase): device to kickoff
            parameters (dict, optional): parameters for the kickoff. Defaults to None.
            wait (bool, optional): if True, wait for the kickoff to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the kickoff process
        """
        device_name = device.name if isinstance(device, DeviceBase) else device
        status = self._create_status(name=f"kickoff_{device_name}")

        instr = messages.DeviceInstructionMessage(
            device=device_name,
            action="kickoff",
            parameter={"configure": parameters or {}},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def complete(self, device: str | DeviceBase, wait=True) -> ScanStubStatus:
        """
        Complete a device. This will call the "complete" method on the device.

        Args:
            device (str or DeviceBase): device to complete
            wait (bool, optional): if True, wait for the completion to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the completion process
        """
        device_name = device.name if isinstance(device, DeviceBase) else device
        status = self._create_status(name=f"complete_{device_name}")

        instr = messages.DeviceInstructionMessage(
            device=device_name,
            action="complete",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def complete_all_devices(self, wait=True) -> ScanStubStatus:
        """
        Complete all devices for the scan.

        Args:
            wait (bool, optional): if True, wait for the completion to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the completion process
        """
        status = self._create_status(name="complete_all_devices")
        device_names = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        instr = messages.DeviceInstructionMessage(
            device=device_names,
            action="complete",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def read_monitored_devices(self, wait=True) -> ScanStubStatus:
        """
        Read from the monitored devices.

        Args:
            wait (bool, optional): if True, wait for the read to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the read process
        """
        # We set a flag to indicate that we triggered the monitored devices.
        # This is used to raise a warning if the scan definition tries to modify the
        # readout groups after the monitored devices were read, which could lead to unexpected behavior.
        self._readout_groups_read = True

        status = self._create_status(name="read_monitored_devices")
        monitored_devices = [
            _dev.root.name
            for _dev in self._device_manager.devices.monitored_devices(
                readout_priority=self.readout_priority
            )
        ]
        if not monitored_devices:
            status.set_done()
            status.set_done_checked()
            return status
        monitored_devices = sorted(monitored_devices)
        instr = messages.DeviceInstructionMessage(
            device=monitored_devices,
            action="read",
            parameter={},
            metadata={
                "device_instr_id": status._device_instr_id,
                "point_id": self._num_monitored_readouts,
            },
        )
        self._send(instr)
        self._num_monitored_readouts += 1
        if wait:
            status.wait()
        return status

    def read_baseline_devices(self, wait=True) -> ScanStubStatus:
        """
        Read from the baseline devices.

        Args:
            wait (bool, optional): if True, wait for the read to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the read process
        """
        # We set a flag to indicate that we triggered the baseline devices
        # This is used to raise a warning if the scan definition tries to modify the
        # readout groups after the baseline devices were read, which could lead to unexpected behavior.
        self._readout_groups_read = True

        status = self._create_status(name="read_baseline_devices")
        baseline_devices = [
            _dev.root.name
            for _dev in self._device_manager.devices.baseline_devices(
                readout_priority=self.readout_priority
            )
        ]
        if not baseline_devices:
            status.set_done()
            status.set_done_checked()
            return status
        baseline_devices = sorted(baseline_devices)
        instr = messages.DeviceInstructionMessage(
            device=baseline_devices,
            action="read",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id, "readout_priority": "baseline"},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def trigger_all_devices(self, min_wait: float | None = None, wait=True) -> ScanStubStatus:
        """
        Trigger all devices for the scan. The list of devices to trigger is determined automatically
        based on their softwareTrigger configuration.
        This will call the "trigger" method on all devices that are configured to be triggered for the scan.

        Args:
            min_wait (float, optional): minimum time to wait before the trigger is executed. This can be used to ensure that the system has settled before the trigger is executed. Defaults to None.
            wait (bool, optional): if True, wait for the trigger to complete. Defaults to True.
        """
        status = self._create_status(name="trigger_all_devices")
        devices = [
            dev.root.name for dev in self._device_manager.devices.get_software_triggered_devices()
        ]
        if not devices:
            status.set_done()
            status.set_done_checked()
            return status

        devices = sorted(devices)
        instr = messages.DeviceInstructionMessage(
            device=devices,
            action="trigger",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if min_wait is not None:
            time.sleep(min_wait)
        if wait:
            status.wait()
        return status

    def unstage(self, device: str | DeviceBase, wait=True) -> ScanStubStatus:
        """
        Unstage a device for the scan.

        Args:
            device (str or DeviceBase): device to unstage
            wait (bool, optional): if True, wait for the unstaging to complete. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the unstaging process
        """
        device_name = device.name if isinstance(device, DeviceBase) else device
        status = self._create_status(name=f"unstage_{device_name}")

        instr = messages.DeviceInstructionMessage(
            device=device_name,
            action="unstage",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def unstage_all_devices(self, wait=True) -> ScanStubStatus:
        """
        Unstage all devices for the scan.
        This will call the "unstage" method on all devices. If you want to unstage only specific devices, use the "unstage" method.

        Args:
            wait (bool, optional): if True, wait for the unstaging to complete. Defaults to True.
        """

        status = self._create_status(name="unstage_all_devices")
        staged_devices = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        instr = messages.DeviceInstructionMessage(
            device=staged_devices,
            action="unstage",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def add_scan_report_instruction_readback(
        self,
        devices: list[str | DeviceBase],
        start: list[float],
        stop: list[float],
        request_id: str,
    ):
        """
        Add a readback report instruction to the instruction handler.
        Readback instructions allow clients to subscribe to the readback of the given devices
        and show a live update of their position during the scan as a progress bar.

        Args:
            devices (list[str | DeviceBase]): list of device names or DeviceBase instances to report
            start (list[float]): list of start positions for the devices
            stop (list[float]): list of stop positions for the devices
        """
        device_names = [dev.name if isinstance(dev, DeviceBase) else dev for dev in devices]
        scan_report_instruction = {
            "readback": {"RID": request_id, "devices": device_names, "start": start, "end": stop}
        }
        self.add_device_with_required_response(device_names)
        self._scan.scan_info.scan_report_instructions.append(scan_report_instruction)
        if self._update_queue_info_callback is not None:
            self._update_queue_info_callback()

    def add_scan_report_instruction_device_progress(self, device: str | DeviceBase):
        """
        Add a device progress report instruction to the instruction handler.
        Device progress instructions allow clients to subscribe to the progress signal of the given device
        and show a live update of the progress during the scan as a progress bar.

        Args:
            device (str | DeviceBase): name of the device or DeviceBase instance to report
        """
        if isinstance(device, DeviceBase):
            device_name = device.name
        else:
            device_name = device
        scan_report_instruction = {"device_progress": [device_name]}
        self._scan.scan_info.scan_report_instructions.append(scan_report_instruction)
        if self._update_queue_info_callback is not None:
            self._update_queue_info_callback()

    def add_scan_report_instruction_scan_progress(self, points: int, show_table: bool = True):
        """
        Add a scan progress report instruction to the instruction handler.
        Scan progress instructions inform clients to print a table-like report of the scan progress.
        If you don't know the number of points in advance, you can set points to 0. The progressbar will
        not be able to estimate the remaining time in this case, but it will still show the elapsed time and the number of points completed.

        Args:
            points (int): total number of points in the scan, used to calculate the progress percentage
            show_table (bool, optional): if True, show a progress table with estimated time remaining. Defaults to True.
        """
        scan_report_instruction = {"scan_progress": {"points": points, "show_table": show_table}}
        self._scan.scan_info.scan_report_instructions.append(scan_report_instruction)
        if self._update_queue_info_callback is not None:
            self._update_queue_info_callback()

    def set_device_readout_priority(
        self,
        devices: list[DeviceBase] | list[str],
        priority: Literal["baseline", "monitored", "on_request", "async"],
    ):
        """
        Set the readout priority for the given devices. This will determine when the devices are read out during the scan.
        The provided list of devices is a modification to the existing readout priority.

        Adding device A that is by default a baseline device to priority "monitored" will move it from the baseline
        readout to the monitored readout. All other devices will keep their default readout priority.
        This method is particularly useful for adding scan motors to the monitored readouts so that their positions
        are included in the scan report for each point.

        Args:
            devices (list[str | DeviceBase]): List of device names or DeviceBase instances to set the readout priority for.
            priority (str): Readout priority to set for the devices. Should be one of "baseline", "monitored", "on_request", or "async".
        """
        if self._readout_groups_read:
            msg = f"Warning: Modifying readout groups after they have been read can lead to unexpected behavior. Devices: {devices}, Priority: {priority}"
            error_info = messages.ErrorInfo(
                error_message=msg,
                compact_error_message=msg,
                exception_type="ReadoutGroupModificationWarning",
                device=None,
            )
            self._connector.raise_alarm(severity=Alarms.WARNING, info=error_info)

        if not isinstance(devices, list):
            devices = [devices]

        for device in devices:
            if isinstance(device, DeviceBase):
                device_name = device.name
            else:
                device_name = device
            self._scan.scan_info.readout_priority_modification[priority].append(device_name)

    def close_scan(self):
        """Close the scan."""
        self.check_for_unchecked_statuses()
        if self._scan_status_callback is not None:
            self._scan_status_callback("closed", None)

    def check_for_unchecked_statuses(self):
        """
        Check if there are any unchecked status objects left.
        Their done status was not checked nor were they waited for.
        While this is not an error, it is a warning that the scan
        might not have completed as expected.
        """

        unchecked_status_objects = self._get_remaining_status_objects(
            exclude_done=False, exclude_checked=True
        )
        if unchecked_status_objects:
            msg = f"Scan completed with unchecked status objects: {unchecked_status_objects}. Use .wait() or .done within the scan to check their status."
            error_info = messages.ErrorInfo(
                error_message=msg,
                compact_error_message=msg,
                exception_type="UncheckedStatusObjectsWarning",
                device=None,
            )
            self._connector.raise_alarm(severity=Alarms.WARNING, info=error_info)

        # Check if there are any remaining status objects that are not done.
        # This is not an error but we send a warning and wait for them to complete.
        remaining_status_objects = self._get_remaining_status_objects(
            exclude_done=True, exclude_checked=False
        )
        if remaining_status_objects:
            msg = f"Scan completed with remaining status objects: {remaining_status_objects}"
            error_info = messages.ErrorInfo(
                error_message=msg,
                compact_error_message=msg,
                exception_type="ScanCleanupWarning",
                device=None,
            )
            self._connector.raise_alarm(severity=Alarms.WARNING, info=error_info)
            for obj in remaining_status_objects:
                obj.wait()

    def add_device_with_required_response(
        self, device: str | DeviceBase | list[DeviceBase] | list[str]
    ):
        """
        Add a device to the set of devices with required response.
        If a device is in this set, an additional "response" flag will be added to the metadata of the device instruction messages for this device.
        The device server will then include a "response" message in the instruction response for this device,
        which enabled clients to listen to the completion of the instruction more easily.

        If you are unsure whether a device needs to be added to this set, you probably don't need it.
        It is mostly relevant for the simple mv and umv scans.

        Args:
            device (str or DeviceBase or list[DeviceBase] or list[str]): device(s) to add to the set of devices with required response
        """
        if isinstance(device, list):
            for dev in device:
                device_name = dev.name if isinstance(dev, DeviceBase) else dev
                self._devices_with_required_response.add(device_name)
        else:
            device_name = device.name if isinstance(device, DeviceBase) else device
            self._devices_with_required_response.add(device_name)

    def _create_status(self, is_container=False, name: str | None = None) -> ScanStubStatus:
        """
        Helper method to create a status object and register it in the status registry.

        Args:
            is_container (bool, optional): if True, the status object is merely a container for other status objects and should not be waited on directly. Defaults to False.
            name (str, optional): name for the status object. Defaults to None.
        """
        status = ScanStubStatus(
            self._instruction_handler,
            shutdown_event=self._shutdown_event,
            registry=self._status_registry,
            is_container=is_container,
            name=name,
        )
        self._status_registry[status._device_instr_id] = status
        return status

    def _get_remaining_status_objects(self, exclude_done=True, exclude_checked=True):
        """
        Get the remaining status objects.

        Args:
            exclude_checked (bool, optional): Exclude checked status objects. Defaults to False.
            exclude_done (bool, optional): Exclude done status objects. Defaults to True.

        Returns:
            list: List of remaining status objects.
        """
        objs = list(self._status_registry.values())
        if exclude_checked:
            objs = [st for st in objs if not st._done_checked]
        if exclude_done:
            objs = [st for st in objs if not st.done]
        return objs

    def _send(self, msg: messages.DeviceInstructionMessage):
        """Send a message to the device server."""
        if self._interruption_callback is not None:
            self._interruption_callback()
        metadata = {}
        if self._scan.scan_info.scan_id is not None:
            metadata["scan_id"] = self._scan.scan_info.scan_id
        for key in ["RID", "queue_id"]:
            value = self._scan.scan_info.metadata.get(key)
            if value is not None:
                metadata[key] = value
        msg.metadata = {**metadata, **msg.metadata}
        instr_devices = msg.device if isinstance(msg.device, list) else [msg.device]
        if set(instr_devices) & self._devices_with_required_response:
            msg.metadata["response"] = True
        self._connector.send(MessageEndpoints.device_instructions(), msg)

    def rpc_call(self, device: str, func_name: str, *args, **kwargs):
        status = self._create_status(name=f"rpc_{device}_{func_name}")
        rpc_id = str(uuid.uuid4())
        parameter = {
            "device": device,
            "func": func_name,
            "rpc_id": rpc_id,
            "args": args,
            "kwargs": kwargs,
        }
        msg = messages.DeviceInstructionMessage(
            device=device,
            action="rpc",
            parameter=parameter,
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(msg)
        status.wait(resolve_on_known_type=True)
        if status._result_is_status:
            return status
        return status.result


class ScanComponents:
    """
    Class to handle the components for the scan logic.
    The components are reusable building blocks for the scan logic,
    such as step scans or grid scans. They use the ScanStubs to
    execute the scan logic.
    """

    def __init__(self, scan: ScanBase):
        self._scan = scan
        self._actions = scan.actions
        self._redis_connector = scan.redis_connector
        self._device_manager = scan.device_manager
        self._dev = self._device_manager.devices if self._device_manager else None
        self._path_optimizer = PathOptimizerMixin()

    def move_and_wait(
        self,
        motors: list[str | DeviceBase],
        positions: np.ndarray | list[float],
        last_positions: np.ndarray | None = None,
    ):
        """
        Move the given motors to the given positions and wait for the movement to complete.
        If last_positions is provided, only the motors with changed positions will be moved.

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances to move.
            positions (np.ndarray | list[float]): Array or list of positions to move to, shape (len(motors),).
            last_positions (np.ndarray, optional): Array of last positions, shape (len(motors),).
                If provided, only motors with changed positions will be moved. Defaults to None.
        """
        motors_to_move = []
        positions_to_move = []
        for motor_index, motor in enumerate(motors):
            if last_positions is not None:
                if np.isclose(positions[motor_index], last_positions[motor_index]):
                    continue
            motors_to_move.append(motor)
            positions_to_move.append(positions[motor_index])

        if motors_to_move:
            self._actions.set(motors_to_move, positions_to_move, wait=True)

    def trigger_and_read(self):
        """
        Trigger the devices and start the readout. This is typically used for step scans after the motors have been moved to the next position.

        The logic is as follows:
            1. Let the system settle before triggering
            2. Trigger the devices
            3. Let the system settle after the trigger
            4. Start the readout

        """
        # Let the system settle before triggering
        time.sleep(self._scan.scan_info.settling_time)
        trigger_time = self._scan.scan_info.exp_time * self._scan.scan_info.frames_per_trigger

        # Trigger the devices
        self._actions.trigger_all_devices(min_wait=trigger_time)

        # Let the system settle after the trigger
        time.sleep(self._scan.scan_info.settling_time_after_trigger)

        # Start the readout
        self._actions.read_monitored_devices()

    def step_scan(
        self,
        motors: list[str | DeviceBase],
        positions: np.ndarray,
        at_each_point: (
            Callable[[list[str | DeviceBase], np.ndarray, np.ndarray | None], None] | None
        ) = None,
        last_positions: np.ndarray | None = None,
    ):
        """
        Execute a step scan with the given positions. It is the core scan logic
        for most step scans.

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances to move.
            positions (np.ndarray): Array of positions to move to, shape (num_points, len(motors)).
            at_each_point (Callable[[list[str | DeviceBase], np.ndarray, np.ndarray | None], None], optional): Function to call at each point. Defaults to None.
            last_positions (np.ndarray, optional): Array of last positions, shape (num_points, len(motors)). If provided, only motors with changed positions will be moved. Defaults to None.
        """
        at_each_point = at_each_point or self.step_scan_at_each_point
        last_positions = None
        for pos in positions:
            for _ in range(self._scan.scan_info.burst_at_each_point):
                at_each_point(motors, pos, last_positions=last_positions)
                last_positions = pos.copy()

    def step_scan_at_each_point(
        self,
        motors: list[str | DeviceBase],
        pos: np.ndarray,
        last_positions: np.ndarray | None = None,
    ):
        """
        Execute a step scan at each point. This is the core logic that is executed at each point of the step scan.
        It is separated from the step_scan method to allow scan hooks to override the logic.

        The logic is as follows:
            1. Move the motors to the next position without waiting for each motor to complete
            2. Wait for each motor to complete
            3. Let the system settle before triggering
            4. Trigger the devices
            5. Let the system settle after the trigger
            6. Start the readout

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances to move.
            pos (np.ndarray): Array of positions to move to, shape (len(motors),).
            last_positions (np.ndarray, optional): Array of last positions, shape (len(motors),).
                If provided, only motors with changed positions will be moved. Defaults to None.
        """
        self.move_and_wait(motors, pos, last_positions=last_positions)
        self.trigger_and_read()

    def get_start_positions(self, motors: list[str | DeviceBase]) -> list[float]:
        """
        Get the current position of the given motors. This can be used to make the positions relative to the current position of the motors.

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances.

        Returns:
            list[float]: List of current positions of the motors.
        """
        start_positions = []
        for motor in motors:
            if isinstance(motor, str):
                obj = self._dev[motor]
            else:
                obj = motor
            val = obj.read()
            start_positions.append(val[obj.full_name].get("value"))
        return start_positions

    def optimize_trajectory(
        self,
        positions: np.ndarray,
        optimization_type: Literal["corridor", "shell", "nearest"] = "corridor",
        primary_axis: int = 1,
        preferred_directions: list[int] | None = None,
        corridor_size: int | None = None,
        num_iterations: int = 5,
    ) -> np.ndarray:
        """
        Optimize the trajectory of the scan by reordering the positions. This can help to minimize the movement time of the motors.
        The optimization can be done in different ways, depending on the optimization_type parameter:
            - "corridor": optimize the trajectory in a corridor-like way, where the scan moves back and forth along the primary axis. This is typically a good choice for grid scans. If preferred_directions are provided, the optimizer will try to optimize the trajectory in a way that minimizes the movement in the non-preferred direction.
            - "shell": optimize the trajectory in a shell-like way, where the scan moves in a spiral from the outside to the inside. This is typically a good choice for round scans.
            - "nearest": optimize the trajectory by always moving to the nearest next point. This is typically a good choice for random scans.

        Args:
            positions (np.ndarray): Array of positions to optimize, shape (num_points, num_motors).
            optimization_type (str, optional): Type of optimization to perform. Defaults to "corridor".
            primary_axis (int, optional): Primary axis for corridor optimization. Defaults to 1.
            preferred_directions (list[int] | None, optional): List of preferred directions for the non-primary axes. Each entry should be -1, 0, or 1, indicating the preferred direction of movement along that axis. The length of the list should be equal to the number of non-primary axes. Defaults to None, which means no preferred directions.
            corridor_size (int | None, optional): Size of the corridor for corridor optimization. Defaults to None, which means the default corridor size will be used.
        Returns:
            np.ndarray: Optimized array of positions, shape (num_points, num_motors).
        """

        if optimization_type == "corridor":
            if preferred_directions is None or len(preferred_directions) == 0:
                positions = self._path_optimizer.optimize_corridor(
                    positions, num_iterations=num_iterations, corridor_size=corridor_size
                )
            else:
                preferred_direction = (
                    preferred_directions[primary_axis]
                    if len(preferred_directions) > primary_axis
                    else None
                )
                positions = self._path_optimizer.optimize_corridor(
                    positions,
                    num_iterations=num_iterations,
                    sort_axis=primary_axis,
                    preferred_direction=preferred_direction,
                    corridor_size=corridor_size,
                )

        elif optimization_type == "shell":
            positions = self._path_optimizer.optimize_shell(
                positions, num_iterations=num_iterations
            )
        elif optimization_type == "nearest":
            positions = self._path_optimizer.optimize_nearest_neighbor(positions)
        else:
            raise ValueError(f"Invalid optimization type: {optimization_type}")
        return positions

    def check_limits(self, motors: list[str | DeviceBase], positions: np.ndarray):
        """
        Check if the given positions for the given motors are within the limits of the motors.
        If not, raise a LimitError.

        Args:
            motors (list[str | DeviceBase]): List of motor names or device instances.
            positions (np.ndarray): Array of positions to check, shape (num_points, len(motors)).

        Raises:
            LimitError: If any of the positions are out of limits for the corresponding motor.
        """
        for motor_index, motor in enumerate(motors):
            if isinstance(motor, str):
                low_limit, high_limit = self._dev[motor].limits
            else:
                low_limit, high_limit = motor.limits
            if low_limit >= high_limit:
                # if both limits are the same or low > high, no limits are set
                return
            for pos in positions[:, motor_index]:
                if not low_limit <= pos <= high_limit:
                    raise LimitError(
                        f"Target position {pos} for motor {motor} is out of limits ({low_limit}, {high_limit})",
                        device=motor,
                    )


class ScanInfo(BaseModel):

    # General scan information
    scan_name: Annotated[str, Field(description="Name of the scan type, e.g. 'grid_scan'")]
    scan_id: Annotated[str, Field(description="Unique identifier for the scan")]
    scan_type: Annotated[
        ScanType | None,
        Field(
            None, description="Type of the scan, e.g. 'software_triggered' or 'hardware_triggered'"
        ),
    ]
    scan_number: Annotated[int | None, Field(description="Scan number, if applicable")] = None
    dataset_number: Annotated[int | None, Field(description="Dataset number, if applicable")] = None

    # Scan parameters
    num_points: Annotated[int | None, Field(description="Number of points in the scan.")] = None
    positions: Annotated[
        np.ndarray | None,
        Field(description="Positions for the scan, shape (num_points, num_motors)"),
    ] = None
    exp_time: Annotated[float, Field(description="Exposure time for the scan", ge=0.0)] = 0.0
    frames_per_trigger: Annotated[int, Field(description="Number of frames per trigger", ge=1)] = 1
    settling_time: Annotated[
        float, Field(description="Settling time before the software trigger", ge=0.0)
    ] = 0.0
    settling_time_after_trigger: Annotated[
        float, Field(description="Settling time after the software trigger", ge=0.0)
    ] = 0.0
    readout_time: Annotated[float, Field(description="Readout time after the trigger", ge=0.0)] = (
        0.0
    )
    burst_at_each_point: Annotated[
        int, Field(description="Number of bursts at each point", ge=1)
    ] = 1
    relative: Annotated[
        bool, Field(description="Whether the positions are relative or absolute")
    ] = False
    return_to_start: Annotated[
        bool, Field(description="Whether to return to the starting position after the scan")
    ] = False

    request_inputs: Annotated[dict, Field(description="Request inputs")] = {}
    readout_priority_modification: Annotated[
        dict, Field(description="Readout priority modification")
    ] = {"baseline": [], "monitored": [], "on_request": [], "async": []}
    scan_report_instructions: Annotated[
        list[dict], Field(description="List of scan report instructions")
    ] = []
    scan_report_devices: Annotated[
        list[str], Field(description="List of devices to report during the scan")
    ] = []
    monitor_sync: Annotated[
        str | None, Field(description="Monitor synchronization mode for fly scans")
    ] = None
    additional_scan_parameters: Annotated[dict, Field(description="Additional scan parameters")] = (
        {}
    )
    user_metadata: Annotated[dict, Field(description="User-provided metadata for the scan")] = {}
    system_config: Annotated[dict, Field(description="System configuration for the scan")] = {}
    scan_queue: Annotated[str, Field(description="Name of the queue the scan belongs to")] = (
        "primary"
    )
    metadata: Annotated[dict, Field(description="Additional metadata for the scan")] = {}

    # progress tracking
    num_monitored_readouts: Annotated[
        int,
        Field(
            description="Number of performed readouts of monitored devices. For a step scan, this is equal to num_points * burst_at_each_point."
        ),
    ] = 0

    def __str__(self) -> str:
        data = self.model_dump(mode="python")
        positions = self.positions
        if isinstance(positions, np.ndarray):
            data["positions"] = np.array2string(positions, threshold=8, edgeitems=2, precision=4)
        return f"{self.__class__.__name__}({data})"

    __repr__ = __str__

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)


class ScanBase:
    scan_type = ScanType.SOFTWARE_TRIGGERED
    scan_name = "_v4_base_scan"
    required_kwargs = []
    arg_input = {}
    arg_bundle_size = {"bundle": len(arg_input), "min": None, "max": None}
    is_scan = True

    def __init__(
        self,
        scan_id: str,
        redis_connector: RedisConnector,
        device_manager: DeviceManager,
        instruction_handler: InstructionHandler,
        request_inputs: dict,
        system_config: dict,
        user_metadata: dict | None = None,
        metadata: dict | None = None,
        scan_queue: str | None = None,
    ):
        """Base class for all scans."""
        self.redis_connector = redis_connector
        self.device_manager = device_manager
        self._instruction_handler = instruction_handler
        self.dev = self.device_manager.devices
        self._shutdown_event = threading.Event()
        self.actions = ScanActions(scan=self)
        self.components = ScanComponents(scan=self)

        optional_kwargs = {}
        for kwarg in ["metadata", "user_metadata", "scan_queue"]:
            data = locals()[kwarg]
            if data is not None:
                optional_kwargs[kwarg] = data
        self.scan_info = ScanInfo(
            scan_name=self.scan_name, scan_id=scan_id, scan_type=self.scan_type, **optional_kwargs
        )
        self.scan_info.request_inputs = request_inputs
        self.scan_info.system_config = system_config
        self._baseline_readout_status = None
        self.positions = np.array([])
        self.start_positions = []

    def update_scan_info(
        self,
        num_points: int | None = None,
        positions: np.ndarray | None = None,
        exp_time: float | None = None,
        frames_per_trigger: int | None = None,
        settling_time: float | None = None,
        settling_time_after_trigger: float | None = None,
        burst_at_each_point: int | None = None,
        relative: bool | None = None,
        return_to_start: bool | None = None,
        **kwargs,
    ):
        """
        Update the scan info with the given keyword arguments.
        If the scan info model has an attribute with the same name as the keyword argument,
        it will be updated. Otherwise, the keyword argument will be added to the additional_scan_parameters dictionary.
        This allows for flexible scan info management, where standard parameters can be defined as attributes of the
        ScanInfo model, and any additional parameters can be stored in the additional_scan_parameters dictionary.

        Args:
            num_points (int, optional): Number of points in the scan. Defaults to None.
            positions (np.ndarray, optional): Positions for the scan, shape (num_points, num_motors). Defaults to None.
            exp_time (float, optional): Exposure time for the scan. Defaults to None.
            frames_per_trigger (int, optional): Number of frames per trigger. Defaults to None.
            settling_time (float, optional): Settling time before the software trigger. Defaults to None.
            settling_time_after_trigger (float, optional): Settling time after the software trigger. Defaults to None.
            burst_at_each_point (int, optional): Number of bursts at each point. Defaults to None.
            relative (bool, optional): Whether the positions are relative or absolute. Defaults to None.
            return_to_start (bool, optional): Whether to return to the starting position after the scan. Defaults to None.
            **kwargs: Keyword arguments to update the scan info with.
        """
        for attr_name, value in [
            ("num_points", num_points),
            ("positions", positions),
            ("exp_time", exp_time),
            ("frames_per_trigger", frames_per_trigger),
            ("settling_time", settling_time),
            ("settling_time_after_trigger", settling_time_after_trigger),
            ("burst_at_each_point", burst_at_each_point),
            ("relative", relative),
            ("return_to_start", return_to_start),
        ]:
            if value is not None:
                setattr(self.scan_info, attr_name, value)
        for key, value in kwargs.items():
            if hasattr(self.scan_info, key):
                setattr(self.scan_info, key, value)
            else:
                self.scan_info.additional_scan_parameters[key] = value

    @scan_hook
    def prepare_scan(self):
        """
        Prepare the scan. This can include any steps that need to be executed
        before the scan is opened, such as preparing the positions or
        setting up the devices.
        """
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

    @scan_hook
    def scan_core(self):
        """
        Core scan logic to be executed during the scan.
        This is where the main scan logic should be implemented.
        """
        self.components.step_scan(self.motors, self.positions)

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

    @scan_hook
    def on_exception(self, exception: Exception):
        """
        Handle exceptions that occur during the scan.
        This is a good place to implement any cleanup logic that needs to be executed in case of an exception,
        such as returning the devices to a safe state or moving the motors back to their starting position.
        """
