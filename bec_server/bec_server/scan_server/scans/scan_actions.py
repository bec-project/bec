from __future__ import annotations

import os
import time
import uuid
from string import Template
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeAlias

import numpy as np

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import DeviceBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import compile_file_components
from bec_lib.logger import bec_logger
from bec_server.scan_server.scan_stubs import ScanStubStatus

if TYPE_CHECKING:
    from bec_server.scan_server.scans.scans_v4 import ScanBase, ScanInfo

logger = bec_logger.logger

ReadoutPriorityMap: TypeAlias = dict[
    Literal["monitored", "baseline", "async", "continuous", "on_request"], list[str]
]


class ScanActions:
    """Class to handle the core actions for the scan logic."""

    def __init__(self, scan: ScanBase):
        self._scan = scan
        self._connector = scan.redis_connector
        self._device_manager = scan.device_manager
        self._instruction_handler = scan._instruction_handler
        self._status_registry = {}
        self._shutdown_event = scan._shutdown_event
        self._num_monitored_readouts = 0
        self._interruption_callback: Callable[[], None] | None = None
        self._update_queue_info_callback: Callable[[], None] | None = None
        self._devices_with_required_response = set()
        self._readout_groups_read = False
        self._metadata_suffix = ""

    @property
    def readout_priority(self) -> dict:
        return self._scan.scan_info.readout_priority_modification

    def open_scan(self):
        """
        Open the scan.
        We fetch all relevant metadata from the scan object and emit a new scan status.
        """
        self._send_scan_status("open")

    def stage_all_devices(
        self, wait=True, exclude: str | DeviceBase | list[str | DeviceBase] | None = None
    ) -> ScanStubStatus:
        """
        Stage all devices for the scan. This will call the "stage" method
        on all devices.

        If you want to stage only specific devices, use the "stage" method.

        .. note ::
            We exclude devices that are on_request or continuous as they are not expected to be staged for a scan.

        Args:
            wait (bool, optional): if True, wait for the staging to complete. Defaults to True.
            exclude (str | DeviceBase | list[str | DeviceBase] | None, optional):
                device(s) to exclude from staging. Defaults to None.

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
        excluded_device_names = set(excluded_devices)
        user_excluded_device_names = set()
        if exclude is not None:
            user_excluded_device_names = set(self._normalize_device_names(exclude))
            excluded_device_names.update(user_excluded_device_names)

        if async_devices:
            async_devices = sorted(async_devices, key=lambda x: x.name)
            async_devices = [
                device for device in async_devices if device.name not in user_excluded_device_names
            ]

        for det in async_devices:
            sub_status = self.stage(det, status_name=f"stage_{det.name}", wait=False)
            status.add_status(sub_status)

        # Now we stage the remaining devices. This will be done sequentially, assuming that
        # they are typically no-op or fast operations.
        stage_device_names_without_async = [
            dev.root.name
            for dev in self._device_manager.devices.enabled_devices
            if dev.name not in excluded_device_names
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
        Stage a device for the scan. This will call the "stage" method
        on the specified device(s).

        If you want to stage all devices, use the `stage_all_devices` method.

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

    def pre_scan(
        self,
        device: str | DeviceBase | list[str | DeviceBase],
        status_name: str | None = None,
        wait=True,
    ) -> ScanStubStatus:
        """
        Run the pre-scan step for one or multiple devices.

        If you want to run pre-scan on all enabled devices, use the
        `pre_scan_all_devices` method.

        Args:
            device (str | DeviceBase | list[str | DeviceBase]): device(s) to run pre-scan for.
            status_name (str, optional): name for the status object. Defaults to None.
            wait (bool, optional): if True, wait for completion. Defaults to True.

        Returns:
            ScanStubStatus: status object to track the pre-scan process.
        """
        device_names = self._normalize_device_names(device)
        if len(device_names) == 1:
            device_names = device_names[0]
        status = self._create_status(name=status_name or f"pre_scan_{device_names}")

        if len(device_names) == 0:
            status.set_done()
            return status

        instr = messages.DeviceInstructionMessage(
            device=device_names,
            action="pre_scan",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if wait:
            status.wait()
        return status

    def pre_scan_all_devices(
        self, wait=True, exclude: str | DeviceBase | list[str | DeviceBase] | None = None
    ) -> ScanStubStatus:
        """
        Pre-scan steps to be executed before the main scan logic. This will call
        the "pre_scan" method all devices that implement it.

        This is typically the last chance to prepare the devices before the core scan
        logic is executed. For example, this is a good place to initialize time-critical
        devices, e.g. devices that have a short timeout.

        Args:
            wait (bool, optional): if True, wait for the pre-scan steps to complete. Defaults to True.
            exclude (str | DeviceBase | list[str | DeviceBase] | None, optional):
                device(s) to exclude from pre-scan. Defaults to None.

        Returns:
            ScanStubStatus: status object to track the pre-scan process
        """
        status = self._create_status(name="pre_scan_all_devices")

        devices = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        if exclude is not None:
            excluded_device_names = set(self._normalize_device_names(exclude))
            devices = [
                device_name for device_name in devices if device_name not in excluded_device_names
            ]
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
        device: str | DeviceBase | list[str | DeviceBase] | list[str] | list[DeviceBase],
        value: float | list[float],
        wait=True,
    ) -> ScanStubStatus:
        """
        Set one or multiple devices to specific values. This will call the "set" method
        on the specified device(s) with the given value(s).

        Args:
            device (str or DeviceBase or list[str or DeviceBase] or list[str] or list[DeviceBase]): device(s) to set
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
        Kickoff a device with the given parameters. This will call the
        "kickoff" method on the specified device with the given parameters.

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

        To complete all devices, use the `complete_all_devices` method.

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

    def complete_all_devices(
        self, wait=True, exclude: str | DeviceBase | list[str | DeviceBase] | None = None
    ) -> ScanStubStatus:
        """
        Complete all devices for the scan. This will call the
        "complete" method on all devices that are enabled for the scan.

        If you want to complete only specific devices, use the `complete` method.

        Args:
            wait (bool, optional): if True, wait for the completion to complete. Defaults to True.
            exclude (str | DeviceBase | list[str | DeviceBase] | None, optional):
                device(s) to exclude from completion. Defaults to None.

        Returns:
            ScanStubStatus: status object to track the completion process
        """
        status = self._create_status(name="complete_all_devices")
        device_names = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        if exclude is not None:
            excluded_device_names = set(self._normalize_device_names(exclude))
            device_names = [
                device_name
                for device_name in device_names
                if device_name not in excluded_device_names
            ]
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
        Read from the monitored devices. This will call the "read" method on
        all devices that are currently configured with readout priority "monitored".

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

    def read_manually(
        self, devices: str | DeviceBase | list[str | DeviceBase], wait=True
    ) -> Any | ScanStubStatus:
        """
        Read the given devices and return the read data. This will call the
        "read" method on the specified device(s).

        This action performs a regular device-server read and asks the device server
        to include the read result in the instruction response. If ``wait`` is
        False, the status object is returned instead of the read data.

        .. note ::
            Reading manually is rarely the right choice; in almost all cases,
            :meth:`read_monitored_devices` is the preferred and optimized action because it lets
            the device server read and publish the monitored devices directly. Use ``read_manually``
            only when you need to intercept the read data for some reason before it is published and
            cannot implement the interception on the device.

        Args:
            devices (str | DeviceBase | list[str | DeviceBase]): device(s) to read.
            wait (bool, optional): if True, wait for the read and return the read data. Defaults to True.

        Returns:
            Any | ScanStubStatus: read data when ``wait`` is True, otherwise the status object.
        """
        device_names = self._normalize_device_names(devices)
        status = self._create_status(name=f"read_manually_{device_names}")
        if not device_names:
            status.set_done([])
            status.set_done_checked()
            return status.result if wait else status

        instr = messages.DeviceInstructionMessage(
            device=sorted(device_names),
            action="read",
            parameter={"return_result": True},
            metadata={"device_instr_id": status._device_instr_id},
        )
        self._send(instr)
        if not wait:
            return status
        status.wait()
        return status.result

    def publish_manual_read(
        self, readings: dict[str, dict] | list[dict], wait=True
    ) -> ScanStubStatus:
        """
        Publish externally provided data as the next monitored-device readout.

        The provided readings must comply with the scan's currently configured
        monitored devices. In almost all cases, :meth:`read_monitored_devices` is
        the preferred and optimized action because it lets the device server read
        and publish the monitored devices directly. ``publish_manual_read`` is
        rarely the right choice; use it only when the scan has already acquired
        equivalent monitored-device data manually and must attach that data to the
        next scan point.

        Args:
            readings (dict[str, dict] | list[dict]): readings for the currently
                monitored devices. Dict keys must match the monitored device names.
                A list may be provided either in monitored-device order or as
                single-key dictionaries keyed by device name.
            wait (bool, optional): retained for API consistency. Publishing is synchronous.
                Defaults to True.

        Returns:
            ScanStubStatus: status object to track the publish process.
        """
        self._readout_groups_read = True
        monitored_devices = self._get_monitored_device_names()
        normalized_readings = self._normalize_manual_readings(readings, monitored_devices)
        self._validate_manual_reading_signals(normalized_readings, monitored_devices)

        status = self._create_status(name="publish_manual_read")
        if not monitored_devices:
            status.set_done()
            status.set_done_checked()
            return status

        metadata = self._get_message_metadata()
        metadata["point_id"] = self._num_monitored_readouts
        if self._interruption_callback is not None:
            self._interruption_callback()
        pipe = self._connector.pipeline()
        for device, signals in zip(monitored_devices, normalized_readings, strict=False):
            msg = messages.DeviceMessage(signals=signals, metadata=metadata)
            self._connector.set_and_publish(MessageEndpoints.device_read(device), msg, pipe=pipe)
        pipe.execute()
        self._num_monitored_readouts += 1
        status.set_done()
        status.set_done_checked()
        return status

    def read_baseline_devices(self, wait=True) -> ScanStubStatus:
        """
        Read from the baseline devices. This will call the "read" method on all devices
        that are configured with readout priority "baseline".

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
        Unstage a device for the scan. This will call the "unstage" method on the specified device(s).

        If you want to unstage all devices, use the `unstage_all_devices` method.

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

    def unstage_all_devices(
        self, wait=True, exclude: str | DeviceBase | list[str | DeviceBase] | None = None
    ) -> ScanStubStatus:
        """
        Unstage all devices for the scan. This will call the "unstage" method on all devices.

        If you want to unstage only specific devices, use the "unstage" method.

        Args:
            wait (bool, optional): if True, wait for the unstaging to complete. Defaults to True.
            exclude (str | DeviceBase | list[str | DeviceBase] | None, optional):
                device(s) to exclude from unstaging. Defaults to None.
        """

        status = self._create_status(name="unstage_all_devices")
        staged_devices = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        if exclude is not None:
            excluded_device_names = set(self._normalize_device_names(exclude))
            staged_devices = [
                device_name
                for device_name in staged_devices
                if device_name not in excluded_device_names
            ]
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
        request_id: str | None = None,
    ):
        """
        Add a readback report instruction to the instruction handler.
        Readback instructions allow clients to subscribe to the readback of the given devices
        and show a live update of their position during the scan as a progress bar.

        Args:
            devices (list[str | DeviceBase]): list of device names or DeviceBase instances to report
            start (list[float]): list of start positions for the devices
            stop (list[float]): list of stop positions for the devices
            request_id (str, optional): request ID to associate the readback instruction with. If None, the scan's RID will be used. Defaults to None.
        """
        request_id = request_id or self._scan.scan_info.metadata["RID"]
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

    def add_scan_report_instruction_scan_progress(self, points: int = 0, show_table: bool = True):
        """
        Add a scan progress report instruction to the instruction handler.
        Scan progress instructions inform clients to print a table-like report of the scan progress.
        If you don't know the number of points in advance, you can set points to 0. The progressbar will
        not be able to estimate the remaining time in this case, but it will still show the elapsed time and the number of points completed.

        Args:
            points (int, optional): total number of points in the scan, used to calculate the progress percentage. Defaults to 0.
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
        # We set the number of monitored readouts to the actual number of monitored
        # readouts that were triggered during the scan. It will be broadcasted with
        # the next scan status.
        self._scan.scan_info.num_monitored_readouts = self._num_monitored_readouts

        self.check_for_unchecked_statuses()

        self._send_scan_status("closed")

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

    def rpc_call(self, device: str, func_name: str, *args, **kwargs) -> Any | ScanStubStatus:
        """
        Make an RPC call to a device. This will call the given function on the device with the given arguments.
        The device server will execute the function and return the result in the instruction response.
        This method is a low-level interface to call arbitrary functions on the device server and should be used with caution.

        Args:
            device (str): name of the device to call the function on
            func_name (str): name of the function to call on the device
            *args: positional arguments to pass to the function
            **kwargs: keyword arguments to pass to the function

        Example:
            >>> # Call the "acquire_image" method on the "detector1" device with an exposure time of 1 second.
            >>> # Similar to calling detector1.acquire_image(exposure_time=1.0) on the device server.
            >>> result = self.actions.rpc_call("detector1", "acquire_image", exposure_time=1.0)

            >>> # Call the "start_interferometer" method on the "controller" sub-device of the "rt" device with some parameters.
            >>> result = self.actions.rpc_call("rt.controller", "start_interferometer", param1=42, param2="foo")

        Returns:
            Any | ScanStubStatus: The result of the RPC call or a ScanStubStatus object if the result is a status object.

        """
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

    def send_client_info(self, message: str):
        """
        Emit a new client info message.
        Client info messages are meant to inform the user about the progress. They are shown in the GUI
        statusbar.

        Args:
            message (str): message to show in the statusbar
        """
        self._connector.send_client_info(
            message, rid=self._scan.scan_info.metadata.get("RID"), source="scan_server"
        )

    #########################################################################
    ############## Helper methods ###########################################
    #########################################################################

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
        metadata = self._get_message_metadata()
        msg.metadata = {**metadata, **msg.metadata}
        instr_devices = msg.device if isinstance(msg.device, list) else [msg.device]
        if set(instr_devices) & self._devices_with_required_response:
            msg.metadata["response"] = True
        self._connector.send(MessageEndpoints.device_instructions(), msg)

    def _get_message_metadata(self) -> dict:
        metadata = {}
        if self._scan.scan_info.scan_id is not None:
            metadata["scan_id"] = self._scan.scan_info.scan_id + self._metadata_suffix
        for key in ["RID", "queue_id"]:
            value = self._scan.scan_info.metadata.get(key)
            if value is not None:
                metadata[key] = value + self._metadata_suffix
        return metadata

    def _normalize_device_names(
        self, devices: str | DeviceBase | list[str | DeviceBase]
    ) -> list[str]:
        if not isinstance(devices, list):
            devices = [devices]
        return [dev.name if isinstance(dev, DeviceBase) else dev for dev in devices]

    def _get_monitored_device_names(self) -> list[str]:
        monitored_devices = [
            _dev.root.name
            for _dev in self._device_manager.devices.monitored_devices(
                readout_priority=self.readout_priority
            )
        ]
        return sorted(monitored_devices)

    @staticmethod
    def _normalize_manual_readings(
        readings: dict[str, dict] | list[dict], monitored_devices: list[str]
    ) -> list[dict]:
        if isinstance(readings, dict):
            reading_devices = sorted(readings)
            if reading_devices != monitored_devices:
                missing_devices = sorted(set(monitored_devices) - set(reading_devices))
                unexpected_devices = sorted(set(reading_devices) - set(monitored_devices))
                raise ValueError(
                    "Manual read devices must match the currently monitored devices. "
                    f"Missing devices: {missing_devices}. "
                    f"Unexpected devices: {unexpected_devices}."
                )
            return [readings[device] for device in monitored_devices]

        if not isinstance(readings, list):
            raise TypeError("Manual readings must be provided as a dict or list of dictionaries.")

        if len(readings) != len(monitored_devices):
            raise ValueError(
                "Manual read count must match the currently monitored devices. "
                f"Expected {len(monitored_devices)}, got {len(readings)}."
            )

        if all(isinstance(reading, dict) and len(reading) == 1 for reading in readings):
            keyed_readings = {}
            for reading in readings:
                device, data = next(iter(reading.items()))
                keyed_readings[device] = data
            return ScanActions._normalize_manual_readings(keyed_readings, monitored_devices)

        if not all(isinstance(reading, dict) for reading in readings):
            raise TypeError("Each manual reading must be a dictionary.")
        return readings

    def _validate_manual_reading_signals(
        self, readings: list[dict], monitored_devices: list[str]
    ) -> None:
        missing_signals = {}
        for device, reading in zip(monitored_devices, readings, strict=False):
            expected_signal_names = self._get_expected_read_signal_names(device)
            missing = sorted(set(expected_signal_names) - set(reading))
            if missing:
                missing_signals[device] = missing

        if missing_signals:
            raise ValueError(
                "Manual read data must include all signals from the currently monitored devices. "
                f"Missing signals: {missing_signals}."
            )

    def _get_expected_read_signal_names(self, device: str) -> list[str]:
        device_info = self._device_manager.devices[device]._info
        signals = device_info.get("signals", {})
        signal_names = [
            signal_info.get("obj_name", signal_name)
            for signal_name, signal_info in signals.items()
            if self._signal_is_read_signal(signal_info)
        ]
        if not signal_names:
            raise ValueError(
                f"Cannot validate manual read data for monitored device {device!r}: "
                "no read signals are configured in the device metadata."
            )
        return signal_names

    @staticmethod
    def _signal_is_read_signal(signal_info: dict) -> bool:
        kind = signal_info.get("kind_str", "").lower()
        if "config" in kind or "omitted" in kind:
            return False
        return True

    def _send_scan_status(
        self,
        status: Literal["open", "paused", "closed", "aborted", "halted", "user_completed"],
        reason: Literal["user", "alarm"] | None = None,
    ) -> None:
        """Publish the current scan status for the active direct scan."""
        scan = self._scan
        logger.info(f"New scan status: {scan.scan_info.scan_id} / {status} / {scan.scan_info}")
        msg = self._build_scan_status_message(status=status, reason=reason)

        expire = None if status in ["open", "paused"] else 1800
        pipe = self._connector.pipeline()
        self._connector.set(
            MessageEndpoints.public_scan_info(scan.scan_info.scan_id), msg, pipe=pipe, expire=expire
        )
        self._connector.set_and_publish(MessageEndpoints.scan_status(), msg, pipe=pipe)
        pipe.execute()

    def _build_scan_status_message(
        self,
        status: Literal["open", "paused", "closed", "aborted", "halted", "user_completed"],
        reason: Literal["user", "alarm"] | None = None,
    ) -> messages.ScanStatusMessage:
        """Build the scan status message for the active direct scan."""
        legacy_scan_parameters = self._get_legacy_scan_parameters(self._scan.scan_info)
        resolved_readout_priority = self._get_resolved_readout_priority()
        file_components = self._get_file_components(self._scan.scan_info)
        info = self._build_scan_status_info(
            legacy_scan_parameters=legacy_scan_parameters,
            resolved_readout_priority=resolved_readout_priority,
            file_components=file_components,
        )
        scan_info = self._scan.scan_info
        scan_type = scan_info.scan_type
        return messages.ScanStatusMessage(
            scan_id=scan_info.scan_id,
            status=status,
            reason=reason,
            scan_name=scan_info.scan_name,
            scan_number=scan_info.scan_number,
            session_id=scan_info.metadata.get("session_id"),
            dataset_number=scan_info.dataset_number,
            num_points=scan_info.num_points,
            scan_type=scan_type if scan_type in {"step", "fly"} else None,
            scan_report_devices=scan_info.scan_report_devices,
            user_metadata=scan_info.user_metadata,
            readout_priority=resolved_readout_priority,
            scan_parameters=legacy_scan_parameters,
            request_inputs=scan_info.request_inputs,
            num_monitored_readouts=scan_info.num_monitored_readouts,
            info=info,
        )

    def _build_scan_status_info(
        self,
        legacy_scan_parameters: dict,
        resolved_readout_priority: ReadoutPriorityMap,
        file_components: tuple[str, str] | None,
    ) -> dict:
        """Build the compatibility-augmented info payload for scan status messages."""
        base_info = self._scan.scan_info.model_dump(mode="python")
        if base_info.get("positions") is not None:
            base_info["positions"] = base_info["positions"].tolist()
        compatibility_fields = {
            "scan_parameters": legacy_scan_parameters,
            "readout_priority": resolved_readout_priority,
            "file_components": file_components,
        }
        return {**base_info, **compatibility_fields}

    def _get_legacy_scan_parameters(self, scan_info: ScanInfo) -> dict:
        scan_parameters = {
            "exp_time": scan_info.exp_time,
            "frames_per_trigger": scan_info.frames_per_trigger,
            "settling_time": scan_info.settling_time,
            "readout_time": scan_info.readout_time,
            "relative": scan_info.relative,
        }
        scan_parameters.update(scan_info.additional_scan_parameters or {})
        if scan_info.system_config is not None:
            scan_parameters["system_config"] = scan_info.system_config
        return {key: value for key, value in scan_parameters.items() if value is not None}

    def _get_resolved_readout_priority(self) -> ReadoutPriorityMap:
        readout_priority = self._scan.scan_info.readout_priority_modification
        return {
            "monitored": [
                dev.full_name
                for dev in self._device_manager.devices.monitored_devices(
                    readout_priority=readout_priority
                )
            ],
            "baseline": [
                dev.full_name
                for dev in self._device_manager.devices.baseline_devices(
                    readout_priority=readout_priority
                )
            ],
            "async": [
                dev.full_name
                for dev in self._device_manager.devices.async_devices(
                    readout_priority=readout_priority
                )
            ],
            "continuous": [
                dev.full_name
                for dev in self._device_manager.devices.continuous_devices(
                    readout_priority=readout_priority
                )
            ],
            "on_request": [
                dev.full_name
                for dev in self._device_manager.devices.on_request_devices(
                    readout_priority=readout_priority
                )
            ],
        }

    def _get_file_components(self, scan_info: ScanInfo) -> tuple[str, str] | None:
        scan_number = scan_info.scan_number
        system_config = scan_info.system_config or {}
        if scan_number is None or "file_directory" not in system_config:
            return None
        return compile_file_components(
            base_path=self._get_file_base_path(),
            scan_nr=scan_number,
            file_directory=system_config["file_directory"],
            user_suffix=system_config.get("file_suffix"),
        )

    def _get_file_base_path(self) -> str:
        current_account_msg = self._connector.get_last(MessageEndpoints.account(), "data")
        if current_account_msg:
            current_account = current_account_msg.value
            if not isinstance(current_account, str):
                logger.warning(
                    f"Account name is not a string: {current_account}. Ignoring specified value."
                )
                current_account = None
            else:
                if "/" in current_account:
                    raise ValueError(
                        f"Account name cannot contain a slash (/): {current_account}. "
                    )
                check_value = current_account.replace("_", "").replace("-", "")
                if not check_value.isalnum() or not check_value.isascii():
                    raise ValueError(
                        f"Account name can only contain alphanumeric characters: {current_account}. "
                    )
        else:
            current_account = None

        file_base_path = self._device_manager.parent._service_config.config["file_writer"][
            "base_path"
        ]
        if "$" not in file_base_path:
            if current_account:
                return os.path.abspath(os.path.join(file_base_path, current_account))
            return os.path.abspath(file_base_path)

        file_base_path = Template(file_base_path)
        try:
            return os.path.abspath(file_base_path.substitute(account=current_account or ""))
        except KeyError as exc:
            raise ValueError(
                f"Invalid template variable: {exc} in the file base path. Please check your service config."
            ) from exc
