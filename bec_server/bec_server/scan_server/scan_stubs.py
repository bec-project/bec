"""
Scan stubs are commands that can be used to control devices during a scan. They typically yield device messages that are
consumed by the scan worker and potentially forwarded to the device server.
"""

from __future__ import annotations

import concurrent
import concurrent.futures
import threading
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generator, Literal

import numpy as np
from typeguard import typechecked

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .errors import DeviceInstructionError, DeviceMessageError

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.devicemanager import DeviceManagerBase
    from bec_lib.redis_connector import RedisConnector
    from bec_server.scan_server.instruction_handler import InstructionHandler


logger = bec_logger.logger


class ScanStubStatus:
    """
    Status object that can be used to wait for the completion of a device instruction.
    """

    def __init__(
        self,
        instruction_handler: InstructionHandler,
        device_instr_id: str = None,
        done: bool = False,
        shutdown_event: threading.Event = None,
        registry: dict = None,
        is_container: bool = False,
        name: str | None = None,
    ) -> None:
        """
        Initialize the status object.

        Args:
            instruction_handler (InstructionHandler): Instruction handler.
            device_instr_id (str): Device instruction ID.
            done (bool, optional): Flag that indicates if the status object is done. Defaults to False.
            shutdown_event (threading.Event, optional): Shutdown event. Defaults to None.
            registry (dict, optional): Registry for status objects. Defaults to None.
            is_container (bool, optional): Flag that indicates if the status object is a container. Defaults to False.
            name (str, optional): Name of the status object. Defaults to None.

        """
        self._name = name
        self._instruction_handler = instruction_handler
        self._device_instr_id = (
            device_instr_id if device_instr_id is not None else str(uuid.uuid4())
        )
        self._shutdown_event = shutdown_event if shutdown_event is not None else threading.Event()
        self._registry = registry if registry is not None else {}
        self._sub_status_objects: list[ScanStubStatus] = []
        self._done = done
        self._done_checked = False
        self.value = None
        self.message = None
        self._future = concurrent.futures.Future()
        self._is_container = is_container
        if is_container:
            self.set_done()
        else:
            self._instruction_handler.register_callback(self._device_instr_id, self._update_future)

    @property
    def done(self) -> bool:
        """
        Get the done flag.

        Returns:
            bool: Done flag
        """
        self.set_done_checked()
        sub_status_done = self._get_sub_status_done()
        return self._done and sub_status_done

    @done.setter
    def done(self, value: bool):
        self._done = value

    def set_done_checked(self):
        """
        Manually set the done checked flag to avoid creating warnings for unchecked status objects.
        """
        self._done_checked = True
        for st in self._sub_status_objects:
            st._done_checked = True

    def add_status(self, status: ScanStubStatus):
        """
        Add a status object to the current status object.
        This can be used to wait for the completion of multiple status objects.

        Args:
            status (ScanStubStatus): Status object

        """

        self._sub_status_objects.append(status)

    def _update_future(self, message: messages.DeviceInstructionResponse = None):
        self.message = message
        if message.status == "completed":
            self.set_done(message.result)
        elif message.status == "error":
            self.set_failed(message.error_info)
        else:
            self.set_running()

    def set_done(self, result=None):
        """
        Set the status object to done.

        Args:
            result (any, optional): Result of the operation. Defaults to None.
        """
        self.done = True
        self._future.set_result(result)

    def set_failed(self, error_info: messages.ErrorInfo | None = None):
        """
        Set the status object to failed.

        Args:
            error_info (messages.ErrorInfo, optional): Error information. Defaults to None.
        """
        self.done = True
        exc = DeviceInstructionError(error_info.compact_error_message if error_info else None)
        if error_info:
            exc.set_info(error_info)
        self._future.set_exception(exc)

    def set_running(self):
        """
        Set the status object to running.
        """
        self.done = False
        self._future.set_running_or_notify_cancel()

    @property
    def result(self) -> Any:
        """
        Get the result of the operation.
        If the status object is a container of multiple status objects, the result will be a list of the results of the sub status objects.
        If the status object is not done, the result will be None.

        Returns:
            Any: Result of the operation, or list of results of the sub status objects.
        """

        if not self._done or not self._get_sub_status_done():
            return None

        if self._sub_status_objects:
            out = []
            for st in self._sub_status_objects:
                out.append(st._future.result())
            if not self._is_container:
                out.append(self._future.result())
            return out
        return self._future.result()

    def _get_sub_status_done(self) -> bool:
        return (
            all(st._done for st in self._sub_status_objects) if self._sub_status_objects else True
        )

    @staticmethod
    def _raise_if_failed(obj: concurrent.futures.Future) -> None:
        if obj.exception() is not None:
            raise obj.exception()

    def wait(
        self, min_wait: float = None, timeout: float = np.inf, logger_wait=5
    ) -> ScanStubStatus:
        """
        Wait for the completion of the status object.

        Args:
            min_wait (float, optional): Minimum wait time in seconds. Defaults to None.
            timeout (float, optional): Timeout in seconds. Defaults to None.
            logger_wait (int, optional): Time in seconds before logging the remaining status objects. Defaults to 5.

        Raises:
            TimeoutError: Raised if the timeout is reached.
            DeviceInstructionError: Raised if the instruction failed.

        Returns:
            ScanStubStatus: Status object
        """
        self._registry.pop(self._device_instr_id, None)
        for st in self._sub_status_objects:
            self._registry.pop(st._device_instr_id, None)

        if min_wait is not None:
            time.sleep(min_wait)

        if self._done and self._get_sub_status_done():
            self._done_checked = True
            self._raise_if_failed(self._future)
            for st in self._sub_status_objects:
                st._done_checked = True
                self._raise_if_failed(st._future)
            return self

        # pylint: disable=protected-access
        futures = [st._future for st in self._sub_status_objects]
        futures.append(self._future)

        increment = 0.5
        wait_time = 0

        while not all(e.done() for e in futures):
            done, _ = concurrent.futures.wait(
                futures, timeout=increment, return_when=concurrent.futures.FIRST_EXCEPTION
            )
            for future in done:
                self._raise_if_failed(future)
            wait_time += increment
            if wait_time >= timeout:
                raise TimeoutError("The wait operation timed out.")
            if self._shutdown_event.is_set():
                break
            if wait_time > logger_wait:
                objs = []
                objs.extend([str(st) for st in self._sub_status_objects if not st.done])
                objs.append(str(self))
                logger.info(f"Waiting for the completion of the following status objects: {objs}")

        return self

    def __repr__(self):
        name = f"{self._name}, " if self._name else ""
        if self.message:
            instr = self.message.instruction.action
            devices = self.message.instruction.device
            return f"ScanStubStatus({name}{self._device_instr_id}, action={instr}, devices={devices}, done={self._done})"
        return f"ScanStubStatus({name}{self._device_instr_id}, done={self._done})"


class ScanStubs:
    """
    Scan stubs are commands that can be used to control devices during a scan. They typically yield device messages that are
    consumed by the scan worker and potentially forwarded to the device server.
    """

    def __init__(
        self,
        device_manager: DeviceManagerBase,
        instruction_handler: InstructionHandler,
        connector: RedisConnector,
        device_msg_callback: Callable = None,
        shutdown_event: threading.Event = None,
    ) -> None:
        self._device_manager = device_manager
        self._instruction_handler = instruction_handler
        self.connector = connector
        self.device_msg_metadata = (
            device_msg_callback if device_msg_callback is not None else lambda: {}
        )
        self.shutdown_event = shutdown_event
        self._readout_priority = {}
        self._status_registry = {}

    def _create_status(self, is_container=False, name: str | None = None) -> ScanStubStatus:
        status = ScanStubStatus(
            self._instruction_handler,
            shutdown_event=self.shutdown_event,
            registry=self._status_registry,
            is_container=is_container,
            name=name,
        )
        self._status_registry[status._device_instr_id] = status
        return status

    def get_remaining_status_objects(
        self, exclude_checked=False, exclude_done=True
    ) -> list[ScanStubStatus]:
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

    @staticmethod
    def _exclude_nones(input_dict: dict):
        for key in list(input_dict.keys()):
            if input_dict[key] is None:
                input_dict.pop(key)

    def _device_msg(self, **kwargs) -> messages.DeviceInstructionMessage:
        """"""
        msg = messages.DeviceInstructionMessage(**kwargs)
        msg.metadata = {**self.device_msg_metadata(), **msg.metadata}
        return msg

    def send_rpc_and_wait(self, device: str, func_name: str, *args, **kwargs) -> any:
        """
        Perform an RPC (remote procedure call) on a device and wait for its return value. If
        the return value is an ophyd status object, the method will wait for the completion of the
        status object before returning.
        This method can be used to call any function on a device, irrespective of the
        function's USER ACCESS settings. The function will be called with the provided arguments
        and return the return value of the function.

        .. important ::
            Please note that to avoid shadowing the keyword arguments of the device's function,
            `send_rpc_and_wait` does not accept a "wait" keyword argument. The function will always
            wait for the completion of the RPC. If you want to perform a non-blocking RPC, use
            :func:`send_rpc` instead.

        .. attention ::
            Sending an RPC to a device often creates a strong coupling between the scan and the device.
            While this can be useful in some cases, it is recommended to implement the desired device-specific
            functionality on the device itself and simply rely on the generic scan stubs to control the device.
            This helps to keep the scan logic clean and the device-specific logic on the device. Moreover, once
            the device-specific logic is implemented on the device, it can be used in any scan without the need
            re-implement the logic in the scan.

        .. note ::
            Running
                >>> return_val = yield from send_rpc_and_wait("samx", "controller.my_custom_function")

            is equivalent to running
                >>> status = yield from send_rpc("samx", "controller.my_custom_function")
                >>> status.wait()
                >>> return_val = status.result

        Args:
            device (str): Name of the device
            func_name (str): Function name. The function name will be appended to the device.
            args (tuple): Arguments to pass on to the RPC function
            kwargs (dict): Keyword arguments to pass on to the RPC function

        Raises:
            ScanAbortion: Raised if the RPC's success is False

        Returns:
            any: Return value of the executed rpc function

        Examples:
            >>> yield from send_rpc_and_wait("samx", "controller.my_custom_function")
            >>> out = yield from send_rpc_and_wait("samx", "controller.func_with_args", arg1=1, arg2=2)
        """

        status = yield from self.send_rpc(device, func_name, *args, **kwargs)
        status.wait()
        return self._get_result_from_status(status)

    def _get_result_from_status(self, status: ScanStubStatus) -> Any:
        """
        Get the result from a status object.
        Little wrapper to simplify the testability of the status object.
        It should not be used directly in a scan.

        Args:
            status (ScanStubStatus): Status object

        Returns:
            Any: Result of the status object
        """
        return status.result

    def open_scan(
        self,
        *,
        scan_motors: list,
        readout_priority: dict,
        num_pos: int,
        scan_name: str,
        scan_type: Literal["step", "fly"],
        positions=None,
        metadata=None,
    ) -> Generator[messages.DeviceInstructionMessage, None, None]:
        """Open a new scan. This is typically not used directly but called by the underlying base class.

        Args:
            scan_motors (list): List of scan motors.
            readout_priority (dict): Modification of the readout priority.
            num_pos (int): Number of positions within the scope of this scan.
            positions (list): List of positions for this scan.
            scan_name (str): Scan name.
            scan_type (str): Scan type (e.g. 'step' or 'fly')

        Returns:
            Generator[messages.DeviceInstructionMessage, None, None]: Generator that yields a device message.

        """
        self._readout_priority = readout_priority
        yield self._device_msg(
            device=None,
            action="open_scan",
            parameter={
                "scan_motors": scan_motors,
                "readout_priority": readout_priority,
                "num_points": num_pos,
                "positions": positions,
                "scan_name": scan_name,
                "scan_type": scan_type,
            },
            metadata=metadata if metadata is not None else {},
        )

    def kickoff(
        self, *, device: str, parameter: dict = None, metadata=None, wait: bool = True
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """Kickoff a fly scan device.

        On the device server, `kickoff` will call the `kickoff` method of the device.

        Args:
            device (str): Device name of flyer.
            parameter (dict, optional): Additional parameters that should be forwarded to the device. Defaults to {}.
            metadata (dict, optional): Metadata that should be forwarded to the device. Defaults to {}.
            wait (bool, optional): If True, the kickoff command will wait for the completion of the kickoff operation before returning. Defaults to True.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.
        """
        status = self._create_status(name="kickoff")
        parameter = parameter if parameter is not None else {}
        parameter = {"configure": parameter}
        metadata = metadata if metadata is not None else {}
        metadata["device_instr_id"] = status._device_instr_id
        yield self._device_msg(
            device=device, action="kickoff", parameter=parameter, metadata=metadata
        )
        if wait:
            status.wait()
        return status

    def complete(
        self, *, device: str = None, metadata=None, wait: bool = True
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Run the complete command on a device. "Complete" typically resolves once the device has finished its operation,
        e.g. the process initiated by a kickoff command has finished.

        On the device server, `complete` will call the `complete` method of the device.

        Args:
            device (str): Device name of flyer.
            metadata (dict, optional): Metadata that should be forwarded to the device. Defaults to {}.
            wait (bool, optional): If True, the complete command will wait for the completion of the complete operation before returning. Defaults to True.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.
        """
        status = self._create_status(name="complete")
        if device is None:
            device = [dev.root.name for dev in self._device_manager.devices.enabled_devices]

        if not isinstance(device, list):
            device = [device]

        device = sorted(device)

        metadata = metadata if metadata is not None else {}
        metadata["device_instr_id"] = status._device_instr_id
        yield self._device_msg(device=device, action="complete", parameter={}, metadata=metadata)
        if wait:
            status.wait()
        return status

    def get_device_progress(self, device: str, RID: str) -> float | None:
        """Get reported device progress

        Args:
            device (str): Name of the device
            RID (str): request ID

        Returns:
            float: reported progress value

        """
        msg = self.connector.get(MessageEndpoints.device_progress(device))
        if not msg:
            return None
        matching_RID = msg.metadata.get("RID") == RID
        if not matching_RID:
            return None
        if not isinstance(msg, messages.ProgressMessage):
            raise DeviceMessageError(
                f"Expected to receive a Progressmessage for device {device} but instead received {msg}."
            )
        return msg.content["value"]

    def close_scan(self) -> Generator[None, None, None]:
        """
        Close the scan.

        Returns:
            Generator[None, None, None]: Generator that yields a device message.

        see also: :func:`open_scan`
        """

        yield self._device_msg(device=None, action="close_scan", parameter={}, metadata={})

    def stage(self) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Stage all devices.

        On the device server, `stage` will call the `stage` method on all enabled devices. Devices
        that are marked as async will receive a separate stage command, while all other devices will
        receive a single stage command. As stage can be a time-consuming operation on async devices,
        this procedure allows for a more efficient staging process.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator
                that yields a device message and returns a status object.

        see also: :func:`unstage`
        """
        status = self._create_status(name="stage")

        async_devices = self._device_manager.devices.async_devices(
            readout_priority=self._readout_priority
        )
        excluded_devices = [device.name for device in async_devices]
        excluded_devices.extend(
            device.name
            for device in self._device_manager.devices.on_request_devices(
                readout_priority=self._readout_priority
            )
        )
        excluded_devices.extend(
            device.name
            for device in self._device_manager.devices.continuous_devices(
                readout_priority=self._readout_priority
            )
        )
        stage_device_names_without_async = [
            dev.root.name
            for dev in self._device_manager.devices.enabled_devices
            if dev.name not in excluded_devices
        ]

        if async_devices:
            async_devices = sorted(async_devices, key=lambda x: x.name)
        for det in async_devices:
            sub_status = self._create_status(name=f"stage_{det.name}")
            instr = messages.DeviceInstructionMessage(
                device=det.name,
                action="stage",
                parameter={},
                metadata={"device_instr_id": sub_status._device_instr_id},
            )
            yield instr
            status.add_status(sub_status)

        if stage_device_names_without_async:
            stage_device_names_without_async = sorted(stage_device_names_without_async)
            instr = messages.DeviceInstructionMessage(
                device=stage_device_names_without_async,
                action="stage",
                parameter={},
                metadata={"device_instr_id": status._device_instr_id},
            )
            yield instr
        else:
            status.set_done()
        status.wait()
        return status

    def unstage(self) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Unstage all devices.

        On the device server, `unstage` will call the `unstage` method of all enabled devices.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.

        see also: :func:`stage`
        """
        status = self._create_status(name="unstage")
        staged_devices = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        if staged_devices:
            staged_devices = sorted(staged_devices)
        instr = messages.DeviceInstructionMessage(
            device=staged_devices,
            action="unstage",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        yield instr
        status.wait()
        return status

    def pre_scan(
        self, wait=True
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Run the pre-scan actions on all devices. Typically, pre-scan actions are called directly before the scan core starts and
        are used to perform time-critical actions.
        The event will be sent to all devices that have a pre_scan method implemented.

        On the device server, `pre_scan` will call the `pre_scan` method of the device, if implemented.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.
        """
        status = self._create_status(name="pre_scan")
        devices = [dev.root.name for dev in self._device_manager.devices.enabled_devices]
        if devices:
            devices = sorted(devices)
        yield self._device_msg(
            device=devices,
            action="pre_scan",
            parameter={},
            metadata={"device_instr_id": status._device_instr_id},
        )
        if wait:
            status.wait()
        return status

    def baseline_reading(
        self,
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Run the baseline readings. This will readout all devices that are marked with the readout_priority "baseline".

        On the device server, `baseline_reading` will call the `read` method of the device.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.

        """
        status = self._create_status(name="baseline_reading")
        baseline_devices = [
            dev.root.name
            for dev in self._device_manager.devices.baseline_devices(
                readout_priority=self._readout_priority
            )
        ]
        if not baseline_devices:
            status.set_done()
            status.set_done_checked()
            return status
        baseline_devices = sorted(baseline_devices)
        yield self._device_msg(
            device=baseline_devices,
            action="read",
            parameter={},
            metadata={"readout_priority": "baseline", "device_instr_id": status._device_instr_id},
        )
        return status

    @typechecked
    def read(
        self,
        *,
        device: list[str] | str | None = None,
        point_id: int | None = None,
        group: Literal["scan_motor", "monitored", None] = None,
        wait: bool = True,
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Perform a reading on a device or device group.

        On the device server, `read` will call the `read` method of the device.

        Args:
            device (list[str], str, optional): Device name. Can be a list of devices or a single device. Defaults to None.
            point_id (int, optional): point_id to assign this reading to point within the scan. If None, the read will simply update
                the cache without assigning the read to a specific point. Defaults to None.
            group (Literal["scan_motor", "monitored", None], optional): Device group. Can be used instead of device. Defaults to None.
            wait (bool, optional): If True, the read command will wait for the completion of the read operation before returning. Defaults to True.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.

        Example:
            >>> yield from self.stubs.read(group="monitored", point_id=self.point_id)
            >>> yield from self.stubs.read(device="samx", point_id=self.point_id)

        """
        name = f"read_{device}" if device is not None else f"read_{group}"
        status = self._create_status(name=name)
        self._check_device_and_groups(device, group)
        parameter = {"group": group}
        metadata = {"point_id": point_id, "device_instr_id": status._device_instr_id}
        self._exclude_nones(parameter)
        self._exclude_nones(metadata)
        if device is None:
            device = [
                dev.root.name
                for dev in self._device_manager.devices.monitored_devices(
                    readout_priority=self._readout_priority
                )
            ]
        if not device:
            status.set_done()
            status.set_done_checked()
            return status
        if not isinstance(device, list):
            device = [device]
        device = sorted(device)
        yield self._device_msg(device=device, action="read", parameter=parameter, metadata=metadata)
        if wait:
            status.wait()
        return status

    def publish_data_as_read(
        self, *, device: str, data: dict, point_id: int
    ) -> Generator[messages.DeviceInstructionMessage, None, None]:
        """
        Publish the given data as a read event and assign it to the given point_id.
        This method can be used to customize the assignment of data to a specific point within a scan.

        Args:
            device (str): Device name.
            data (dict): Data that should be published.
            point_id (int): point_id that should be attached to this data.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, None]: Generator that yields a device message.
        """
        metadata = {"point_id": point_id}
        yield self._device_msg(
            device=device,
            action="publish_data_as_read",
            parameter={"data": data},
            metadata=metadata,
        )

    def trigger(
        self, *, min_wait=0, wait: bool = True
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Trigger all devices that are software triggered.

        On the device server, `trigger` will call the `trigger` method of the device.

        Args:
            min_wait (float, optional): Minimum wait time in seconds. Can be used to wait for at least the exposure time. Defaults to 0.
            wait (bool, optional): If True, the trigger command will wait for the completion of the trigger operation before returning. Defaults to True.

        Returns:
            Generator[None, None, None]: Generator that yields a device message.

        Examples:
            >>> # Trigger all devices and wait for the completion
            >>> yield from self.stubs.trigger()

            >>> # Trigger all devices and wait for the completion with a minimum wait time of 0.1 seconds
            >>> yield from self.stubs.trigger(min_wait=0.1)

            >>> # Trigger all devices and do not wait for the completion immediately
            >>> status = yield from self.stubs.trigger(wait=False)
            >>> # ... do something else
            >>> status.wait()
        """
        status = self._create_status(name="trigger")
        metadata = {"device_instr_id": status._device_instr_id}
        devices = [
            dev.root.name for dev in self._device_manager.devices.get_software_triggered_devices()
        ]
        if not devices:
            yield None
            status.set_done()
            status.set_done_checked()
            return status

        devices = sorted(devices)
        yield self._device_msg(device=devices, action="trigger", parameter={}, metadata=metadata)
        if min_wait:
            time.sleep(min_wait)
        if wait:
            status.wait()
        return status

    def set(
        self,
        *,
        device: str | list[str],
        value: float | list[float],
        metadata=None,
        wait: bool = True,
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Set one or multiple devices to specific values. The number of devices and values must match.
        On the device server, `set` will call the `set` method of the device.
        When setting multiple devices, the set operation will be performed in parallel and the status object will wait for all devices to complete.

        Args:
            device (str, list[str]): Device name or list of device names.
            value (float, list[float]): Value or list of values that should be set.
            metadata (dict, optional): Metadata that should be attached to this event. Defaults to None.
            wait (bool, optional): If True, the set command will wait for the completion of the set operation before returning. Defaults to True.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.

        Examples:
            >>> # Set a single device
            >>> yield from self.stubs.set(device="samx", value=1)

            >>> # Set multiple devices
            >>> yield from self.stubs.set(device=["samx", "samy"], value=[1, 2])

            >>> # Set a single device and wait for the completion
            >>> status = yield from self.stubs.set(device="samx", value=1, wait=False)
            >>> # ... do something else
            >>> status.wait()

        """
        metadata = metadata if metadata is not None else {}

        if not isinstance(device, list):
            device = [device]

        if isinstance(value, np.ndarray):
            value = value.tolist()

        if not isinstance(value, list):
            value = [value]

        if len(device) != len(value):
            raise DeviceMessageError("The number of devices and values must match.")

        status = self._create_status(is_container=True, name="set")
        for dev, val in zip(device, value):
            sub_status = self._create_status(name=f"set_{dev}")
            # pylint: disable=protected-access
            metadata["device_instr_id"] = sub_status._device_instr_id
            yield self._device_msg(
                device=dev, action="set", parameter={"value": val}, metadata=metadata
            )
            status.add_status(sub_status)

        if wait:
            status.wait()
        return status

    def open_scan_def(self) -> Generator[messages.DeviceInstructionMessage, None, None]:
        """
        Open a new scan definition

        Returns:
            Generator[messages.DeviceInstructionMessage, None, None]: Generator that yields a device message.
        """
        yield self._device_msg(device=None, action="open_scan_def", parameter={})

    def close_scan_def(self) -> Generator[messages.DeviceInstructionMessage, None, None]:
        """
        Close a scan definition

        Returns:
            Generator[messages.DeviceInstructionMessage, None, None]: Generator that yields a device message.
        """
        yield self._device_msg(device=None, action="close_scan_def", parameter={})

    def close_scan_group(self) -> Generator[messages.DeviceInstructionMessage, None, None]:
        """
        Close a scan group

        Returns:
            Generator[messages.DeviceInstructionMessage, None, None]: Generator that yields a device message.
        """
        yield self._device_msg(device=None, action="close_scan_group", parameter={})

    def send_rpc(
        self, device: str, func_name: str, *args, metadata=None, rpc_id=None, **kwargs
    ) -> Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]:
        """
        Perfrom an RPC (remote procedure call) on a device.
        For blocking calls, use :func:`send_rpc_and_wait`.
        Any additional arguments and keyword arguments will be passed on to the RPC function.

        Args:
            device (str): Device name.
            func_name (str): Function name.
            args (tuple): Arguments to pass on to the RPC function.
            metadata (dict, optional): Metadata that should be forwarded to the device. Defaults to None.
            rpc_id (str, optional): RPC ID. Defaults to None.
            kwargs (dict): Keyword arguments to pass on to the RPC function.

        Returns:
            Generator[messages.DeviceInstructionMessage, None, ScanStubStatus]: Generator that yields a device message and returns a status object.

        Examples:
            >>> # Call a function on a device without waiting for the completion
            >>> yield from self.send_rpc("samx", "controller.my_custom_function", 1, 2, arg1="test")

            >>> # Call a function on a device and wait for the completion
            >>> status = yield from self.send_rpc("samx", "controller.my_custom_function", 1, 2, arg1="test")
            >>> status.wait()
            >>> # Get the result of the RPC call
            >>> print(status.result)

        """
        rpc_id = str(uuid.uuid4()) if rpc_id is None else rpc_id
        parameter = {
            "device": device,
            "func": func_name,
            "rpc_id": rpc_id,
            "args": args,
            "kwargs": kwargs,
        }
        status = self._create_status(name=f"rpc_{func_name}")

        metadata = metadata if metadata is not None else {}

        # pylint: disable=protected-access
        metadata["device_instr_id"] = status._device_instr_id

        yield self._device_msg(device=device, action="rpc", parameter=parameter, metadata=metadata)

        return status

    def scan_report_instruction(
        self, instructions: dict
    ) -> Generator[messages.DeviceInstructionMessage, None, None]:
        """Scan report instructions

        Args:
            instructions (dict): Dict containing the scan report instructions

        Examples:
            >>> # Report on a motor readback
            >>> instructions = {
            ...     "readback": {
            ...         "RID": "1234",
            ...         "devices": ["fsamroy"],
            ...         "start": [0],
            ...         "end": [90],
            ...     }
            ... }
            >>> yield from self.stubs.scan_report_instruction(instructions)

            >>> # Report device progress
            >>> instructions = {
            ...     "device_progress": ["mcs"]
            ... }
            >>> yield from self.stubs.scan_report_instruction(instructions)

            >>> # Scan progress
            >>> instructions = {
            ...     "scan_progress": {
            ...         "points": 10,
            ...         "show_table": True,
            ...     }
            ... }
            >>> yield from self.stubs.scan_report_instruction(instructions)


        Returns:
            Generator[messages.DeviceInstructionMessage, None, None]: Generator that yields a device message.
        """
        yield self._device_msg(
            device=None, action="scan_report_instruction", parameter=instructions
        )

    def _check_device_and_groups(self, device, group) -> None:
        if device and group:
            raise DeviceMessageError("Device and device group was specified. Pick one.")
        if device is None and group is None:
            raise DeviceMessageError("Either devices or device groups have to be specified.")
