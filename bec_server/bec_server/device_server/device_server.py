from __future__ import annotations

import enum
import inspect
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import ophyd
from ophyd import Kind, OphydObject, Staged, StatusBase
from ophyd.utils import errors as ophyd_errors

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_service import BECService
from bec_lib.device import OnFailure
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import BECStatus
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.devices.devicemanager import DeviceManagerDS
from bec_server.device_server.rpc_mixin import RPCMixin

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger

register_stop = threading.Event()


class DisabledDeviceError(Exception):
    """Exception raised when a disabled device is accessed"""


class InvalidDeviceError(Exception):
    """Exception raised when an invalid device is accessed"""


class ResponseState(str, enum.Enum):
    """Enum for the state of the response message"""

    COMPLETED = "completed"
    ERROR = "error"
    RUNNING = "running"


class RequestHandler:
    """
    Class to handle requests for device instructions. This class is responsible for storing device instructions and
    status objects and sending the appropriate responses back to redis.
    """

    def __init__(self, parent: DeviceServer):
        self.parent = parent
        self.connector = parent.connector
        self._storage = {}
        self._lock = threading.Lock()

    def add_request(
        self,
        instr: messages.DeviceInstructionMessage,
        num_status_objects: int,
        done: bool = False,
        success: bool | None = None,
    ):
        """
        Add a new device instruction to the storage and the expected number of status objects.
        If the instruction is done, the success status must be set.

        Args:
            instr(messages.DeviceInstructionMessage): The instruction to add to the storage.
            num_status_objects(int): The number of status objects that are expected to be generated by the instruction.
            done(bool): Whether the instruction is done. Defaults to False.
            success(bool): Whether the instruction was successful. Defaults to None.
        """
        instr_id = instr.metadata["device_instr_id"]
        self._storage[instr_id] = {
            "instr": instr,
            "status_objects": [],
            "num_status_objects": num_status_objects,
            "done": done,
        }
        if done and success is None:
            raise ValueError("If the instruction is done, the success status must be set.")
        self.send_device_instruction_response(instr_id, success, done)

    def get_request(self, instr_id: str) -> dict | None:
        """
        Get a request from the storage.

        Args:
            instr_id(str): The ID of the instruction.
        """
        return self._storage.get(instr_id)

    def patch_num_status_objects(
        self, instr: messages.DeviceInstructionMessage, num_status_objects: int
    ) -> None:
        """
        Patch the number of status objects for a request.

        Args:
            instr(messages.DeviceInstructionMessage): The instruction to patch.
            num_status_objects(int): The new number of status objects.
        """
        instr_id = instr.metadata["device_instr_id"]
        request_info = self.get_request(instr_id)
        if request_info is None:
            return
        request_info["num_status_objects"] = num_status_objects

        self._update_instruction(instr_id)

    def remove_request(self, instr_id: str):
        """
        Remove a request from the storage.

        Args:
            instr_id(str): The ID of the instruction.
        """
        self._storage.pop(instr_id, None)

    def clear(self):
        """
        Clear the storage and remove all requests.
        """
        self._storage.clear()

    def add_status_object(self, instr_id: str, status_obj: ophyd.StatusBase):
        """
        Add a status object to the storage.

        Args:
            instr_id(str): The ID of the instruction.
            status_obj(ophyd.StatusBase): The status object to add.
        """
        self._storage[instr_id]["status_objects"].append(status_obj)
        status_obj.add_callback(self.on_status_object_update)
        self._update_instruction(instr_id)

    def set_finished(self, instr_id: str, success: bool = None, error_message=None, result=None):
        """
        Set the request to finished.

        Args:
            instr_id(str): The ID of the instruction.
            success(bool): Whether the instruction was successful. Defaults to None.
            error_message(str): The error message if the instruction failed. Defaults to None.
        """
        with self._lock:
            request_info = self.get_request(instr_id)
            if request_info is None:
                return
            if success is None:
                if request_info["num_status_objects"] > 0:
                    success = all(
                        status_obj.success
                        for status_obj in self._storage[instr_id]["status_objects"]
                    )
                else:
                    success = True
            self.send_device_instruction_response(
                instr_id, success, done=True, error_message=error_message, result=result
            )
            self.remove_request(instr_id)

    def on_status_object_update(self, status_obj: ophyd.StatusBase):
        """
        Callback for status updates from devices during device instructions.

        Args:
            status_obj(ophyd.StatusBase): The status object that was updated.
        """
        self.parent.status_callback(status_obj)
        instr_id = status_obj.instruction.metadata["device_instr_id"]
        self._update_instruction(instr_id)

    def _update_instruction(self, instr_id: str) -> None:
        """
        Update the instruction in the storage.

        Args:
            instr_id(str): The ID of the instruction.
        """
        request_info = self.get_request(instr_id)
        if request_info is None:
            return

        if len(request_info["status_objects"]) != request_info["num_status_objects"]:
            return

        if all(status_obj.done for status_obj in self._storage[instr_id]["status_objects"]):
            exceptions = [
                status_obj.exception() for status_obj in self._storage[instr_id]["status_objects"]
            ]
            if any(exceptions):
                error = next(val for val in exceptions if val)
                self.set_finished(instr_id, success=False, error_message=str(error))
            else:
                self.set_finished(instr_id, success=True)

    def send_device_instruction_response(
        self, instr_id: str, success: bool, done: bool, error_message=None, result=None
    ):
        """
        Send a request status message.

        Args:
            instr_id(str): The ID of the instruction.
            success(bool): Whether the instruction was successful.
            done(bool): Whether the instruction is done.
            error_message(str): The error message if the instruction failed. Defaults to None.
            result(Any): The result of the instruction. Defaults to None.
        """
        metadata = self._storage[instr_id]["instr"].metadata

        if success:
            status = ResponseState.COMPLETED
        else:
            status = ResponseState.ERROR if done else ResponseState.RUNNING

        error_message = error_message if error_message else "An error occurred."
        response_msg = messages.DeviceInstructionResponse(
            device=self._storage[instr_id]["instr"].content["device"],
            status=status.value,
            error_message=error_message if status == ResponseState.ERROR else None,
            instruction_id=instr_id,
            instruction=self._storage[instr_id]["instr"],
            result=result,
            metadata=metadata,
        )
        self.connector.send(MessageEndpoints.device_instructions_response(), response_msg)


class DeviceServer(RPCMixin, BECService):
    """DeviceServer using ophyd as a service
    This class is intended to provide a thin wrapper around ophyd and the devicemanager. It acts as the entry point for other services
    """

    def __init__(self, config, connector_cls: type[RedisConnector]) -> None:
        super().__init__(config, connector_cls, unique_service=True)
        self._tasks = []
        self.device_manager = None
        self.connector.register(MessageEndpoints.stop_all_devices(), cb=self.on_stop_all_devices)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._start_device_manager()
        self.requests_handler = RequestHandler(self)

    def _start_device_manager(self):
        self.device_manager = DeviceManagerDS(self, status_cb=self.update_status)
        self.device_manager.initialize(self.bootstrap_server)

    def start(self) -> None:
        """start the device server"""
        if register_stop.is_set():
            register_stop.clear()

        self.connector.register(
            MessageEndpoints.device_instructions(),
            event=register_stop,
            cb=self.instructions_callback,
            parent=self,
        )

        self.status = BECStatus.RUNNING

    def update_status(self, status: BECStatus):
        """update the status of the device server"""
        self.status = status

    def stop(self) -> None:
        """stop the device server"""
        register_stop.set()
        self.status = BECStatus.IDLE

    def shutdown(self) -> None:
        """shutdown the device server"""
        super().shutdown()
        self.stop()
        if self.device_manager:
            self.device_manager.shutdown()

    def _update_device_metadata(self, instr) -> None:
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]
        for dev in devices:
            device_root = dev.split(".")[0]
            self.device_manager.devices.get(device_root).metadata = instr.metadata

    def on_stop_all_devices(self, msg, **_kwargs) -> None:
        """callback for receiving scan modifications / interceptions"""
        mvalue = msg.value
        if mvalue is None:
            logger.warning("Failed to parse scan queue modification message.")
            return
        logger.info("Received request to stop all devices.")
        self.stop_devices()

    def stop_devices(self) -> None:
        """stop all enabled devices"""
        logger.info("Stopping devices after receiving 'abort' request.")
        self.status = BECStatus.BUSY
        for dev in self.device_manager.devices.enabled_devices:
            if dev.read_only:
                # don't stop devices that we haven't set
                continue
            if hasattr(dev.obj, "stop"):
                try:
                    dev.obj.stop()
                except Exception as exc:  # pylint: disable=broad-except
                    content = traceback.format_exc()
                    logger.error(content)
                    self.connector.raise_alarm(
                        severity=Alarms.WARNING,
                        source={"device": dev.obj.name, "method": "stop"},
                        msg=content,
                        alarm_type=exc.__class__.__name__,
                        metadata=self._get_metadata_for_alarm(None),
                    )
        self.status = BECStatus.RUNNING

    def _assert_device_is_enabled(self, instructions: messages.DeviceInstructionMessage) -> None:
        devices = instructions.content["device"]

        if isinstance(devices, str):
            devices = [devices]

        for dev in devices:
            dev = dev.split(".")[0]
            if not self.device_manager.devices[dev].enabled:
                raise DisabledDeviceError(f"Cannot access disabled device {dev}.")

    def _assert_device_is_valid(self, instructions: messages.DeviceInstructionMessage) -> None:
        devices = instructions.content["device"]
        if not devices:
            raise InvalidDeviceError("At least one device must be specified.")
        if isinstance(devices, str):
            devices = [devices]
        for dev in devices:
            dev = dev.split(".")[0]
            if dev not in self.device_manager.devices:
                raise InvalidDeviceError(f"There is no device with the name {dev}.")

    def _get_metadata_for_alarm(
        self, instruction: messages.DeviceInstructionMessage | None = None
    ) -> dict:
        """
        Get the metadata for the current scan. This is used to add the scan ID and scan number to alarms.
        Returns:
            dict: Metadata dictionary with scan ID and scan number.
        """
        metadata = {}
        if instruction is not None:
            metadata.update(instruction.metadata)

        if not self.device_manager:
            return metadata

        if not self.device_manager.scan_info:
            return metadata

        msg = self.device_manager.scan_info.msg

        if not msg:
            return metadata

        scan_id_instruction = metadata.get("scan_id")
        if msg.scan_id == scan_id_instruction and msg.scan_number is not None:
            metadata["scan_number"] = msg.scan_number
        return metadata

    def handle_device_instructions(self, msg: messages.DeviceInstructionMessage) -> None:
        """Parse a device instruction message and handle the requested action. Action
        types are set, read, rpc, kickoff or trigger.

        Args:
            msg (str): A DeviceInstructionMessage string containing the action and its parameters

        """
        action = None
        try:
            instructions = msg
            if not instructions.content["device"]:
                return
            action = instructions.content["action"]
            self._assert_device_is_valid(instructions)
            if action != "rpc":
                # rpc has its own error handling
                self._assert_device_is_enabled(instructions)
            self._update_device_metadata(instructions)

            if action == "set":
                self._set_device(instructions)
            elif action == "read":
                self._read_device(instructions)
            elif action == "rpc":
                self.run_rpc(instructions)
            elif action == "kickoff":
                self._kickoff_device(instructions)
            elif action == "complete":
                self._complete_device(instructions)
            elif action == "trigger":
                self._trigger_device(instructions)
            elif action == "stage":
                self._stage_device(instructions)
            elif action == "unstage":
                self._unstage_device(instructions)
            elif action == "pre_scan":
                self._pre_scan(instructions)
            else:
                logger.warning(f"Received unknown device instruction: {instructions}")
        except ophyd_errors.LimitError as limit_error:
            content = traceback.format_exc()
            self.requests_handler.set_finished(
                instructions.metadata["device_instr_id"], success=False, error_message=content
            )

            logger.error(content)
            self.connector.raise_alarm(
                severity=Alarms.MAJOR,
                source=instructions.content,
                msg=content,
                alarm_type=limit_error.__class__.__name__,
                metadata=self._get_metadata_for_alarm(msg),
            )
        except Exception as exc:  # pylint: disable=broad-except
            content = traceback.format_exc()
            self.requests_handler.set_finished(
                instructions.metadata["device_instr_id"], success=False, error_message=content
            )
            if action == "rpc":
                self._send_rpc_exception(exc, instructions)
            else:
                logger.error(content)
                self.connector.raise_alarm(
                    severity=Alarms.MAJOR,
                    source=instructions.content,
                    msg=content,
                    alarm_type=exc.__class__.__name__,
                    metadata=self._get_metadata_for_alarm(msg),
                )

    @staticmethod
    def instructions_callback(msg, *, parent, **_kwargs) -> None:
        """callback for handling device instructions"""
        parent.executor.submit(parent.handle_device_instructions, msg.value)

    def _trigger_device(self, instr: messages.DeviceInstructionMessage) -> None:
        logger.debug(f"Trigger device: {instr}")
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]
        self.requests_handler.add_request(instr, num_status_objects=len(devices))
        for dev in devices:
            obj = self.device_manager.devices.get(dev)
            obj.metadata = instr.metadata
            status = obj.obj.trigger()

            status.__dict__["instruction"] = instr
            status.__dict__["obj"] = obj.obj
            self.requests_handler.add_status_object(instr.metadata["device_instr_id"], status)

    def _kickoff_device(self, instr: messages.DeviceInstructionMessage) -> None:
        logger.debug(f"Kickoff device: {instr}")

        obj = self.device_manager.devices.get(instr.content["device"]).obj
        kickoff_args = inspect.getfullargspec(obj.kickoff).args
        kickoff_parameter = instr.content["parameter"].get("configure", {})
        if len(kickoff_args) > 1:
            obj.kickoff(metadata=instr.metadata, **kickoff_parameter)
            self.requests_handler.add_request(instr, num_status_objects=0, done=True, success=True)
            return

        self.requests_handler.add_request(instr, num_status_objects=1)
        obj.configure(kickoff_parameter)
        status = obj.kickoff()

        status.__dict__["instruction"] = instr
        status.__dict__["obj"] = obj
        self.requests_handler.add_status_object(instr.metadata["device_instr_id"], status)

    def _complete_device(self, instr: messages.DeviceInstructionMessage) -> None:
        if instr.content["device"] is None:
            devices = [dev.name for dev in self.device_manager.devices.enabled_devices]
        else:
            devices = instr.content["device"]
            if not isinstance(devices, list):
                devices = [devices]

        self.requests_handler.add_request(instr, num_status_objects=len(devices))
        num_status_objects = 0
        for dev in devices:
            obj = self.device_manager.devices.get(dev).obj
            if not hasattr(obj, "complete"):
                continue
            num_status_objects += 1
            logger.debug(f"Completing device: {dev}")
            status = obj.complete()
            if status is None:
                raise InvalidDeviceError(
                    f"The complete method of device {dev} does not return a StatusBase object."
                )

            status.__dict__["instruction"] = instr
            status.__dict__["obj"] = obj
            self.requests_handler.add_status_object(instr.metadata["device_instr_id"], status)

        self.requests_handler.patch_num_status_objects(instr, num_status_objects)

    def _set_device(self, instr: messages.DeviceInstructionMessage) -> None:
        self.requests_handler.add_request(instr, num_status_objects=1)
        device_name = instr.content["device"]
        child_access = None
        if "." in device_name:
            device_name, child_access = device_name.split(".", 1)
        device_obj = self.device_manager.devices.get(device_name)
        if device_obj.read_only:
            raise DisabledDeviceError(
                f"Setting the device {device_obj.name} is currently disabled."
            )
        logger.debug(f"Setting device: {instr}")
        val = instr.content["parameter"]["value"]
        sub_id = None
        if child_access:
            obj = rgetattr(device_obj.obj, child_access)
            if "readback" in obj.event_types or "value" in obj.event_types:
                # pylint: disable=protected-access
                sub_id = obj.subscribe(self.device_manager._obj_callback_readback, run=True)
        else:
            obj = device_obj.obj

        status = obj.set(val)
        status.__dict__["instruction"] = instr
        status.__dict__["sub_id"] = sub_id
        self.requests_handler.add_status_object(instr.metadata["device_instr_id"], status)

    def _pre_scan(self, instr: messages.DeviceInstructionMessage) -> None:
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]

        self.requests_handler.add_request(instr, num_status_objects=len(devices))
        num_status_objects = 0
        for dev in devices:
            status = None
            obj = self.device_manager.devices[dev].obj
            if hasattr(obj, "pre_scan"):
                status = obj.pre_scan()
            if status is None:
                continue
            if not isinstance(status, StatusBase):
                raise ValueError(
                    f"The pre_scan method of {dev} does not return a StatusBase object."
                )

            num_status_objects += 1
            status.__dict__["instruction"] = instr
            status.__dict__["obj"] = obj
            self.requests_handler.add_status_object(instr.metadata["device_instr_id"], status)

        self.requests_handler.patch_num_status_objects(instr, num_status_objects)

    def status_callback(self, status):
        pipe = self.connector.pipeline()
        if hasattr(status, "device"):
            obj = status.device
        elif hasattr(status, "obj"):
            obj = status.obj
        else:
            obj = status.__dict__["obj"]

        # if we've started a subscription, we need to unsubscribe now
        # this is typically the case for operations on nested devices.
        # For normal devices, we don't need to unsubscribe, as the
        # subscription is handled by the device manager
        if getattr(status, "sub_id", None):
            obj.unsubscribe(status.sub_id)

        device_name = (
            ".".join([obj.root.name, obj.dotted_name]) if obj.dotted_name else obj.root.name
        )
        metadata = {"action": status.instruction.content["action"]}
        metadata.update(status.instruction.metadata)

        content = status.instruction.content
        is_config_set = content["action"] == "set"
        is_rpc_set = content["action"] == "rpc" and (".set" in content["parameter"]["func"])

        if is_config_set or is_rpc_set:
            if obj.kind == Kind.config:
                self._update_read_configuration(obj, status.instruction.metadata, pipe)
            elif obj.kind in [Kind.normal, Kind.hinted]:
                self._read_device(status.instruction)

        if status.instruction.metadata.get("response"):
            # if the user requested a response on a single status object, we need to send it
            # to the device_req_status_container
            dev_msg = messages.DeviceReqStatusMessage(
                device=device_name, success=status.success, metadata=metadata
            )
            logger.debug(f"req status for device {device_name}: {status.success}")
            self.connector.set(
                MessageEndpoints.device_req_status(device_name), dev_msg, pipe, expire=18000
            )
            self.connector.lpush(
                MessageEndpoints.device_req_status_container(status.instruction.metadata["RID"]),
                dev_msg,
                pipe,
                expire=18000,
            )
        pipe.execute()

    def _update_read_configuration(self, obj: OphydObject, metadata: dict, pipe) -> None:
        dev_config_msg = messages.DeviceMessage(
            signals=obj.root.read_configuration(), metadata=metadata
        )
        self.connector.set_and_publish(
            MessageEndpoints.device_read_configuration(obj.root.name), dev_config_msg, pipe
        )

    def _read_device(self, instr: messages.DeviceInstructionMessage, new_status=True) -> None:
        # check performance -- we might have to change it to a background thread
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]

        if not new_status:
            return self._read_and_update_devices(devices, instr.metadata)

        self.requests_handler.add_request(instr, num_status_objects=0)
        self._read_and_update_devices(devices, instr.metadata)
        self.requests_handler.set_finished(instr.metadata["device_instr_id"], success=True)

    def _read_and_update_devices(self, devices: list[str], metadata: dict) -> list:
        start = time.time()
        pipe = self.connector.pipeline()
        signal_container = []
        for dev in devices:
            device_root = dev.split(".")[0]
            self.device_manager.devices.get(device_root).metadata = metadata
            obj = self.device_manager.devices.get(device_root).obj
            try:
                signals = obj.read()
                signal_container.append(signals)
            # pylint: disable=broad-except
            except Exception as exc:
                signals = self._retry_obj_method(dev, obj, "read", exc)

            self.connector.set_and_publish(
                MessageEndpoints.device_read(device_root),
                messages.DeviceMessage(signals=signals, metadata=metadata),
                pipe,
            )
            self.connector.set_and_publish(
                MessageEndpoints.device_readback(device_root),
                messages.DeviceMessage(signals=signals, metadata=metadata),
                pipe,
            )
        pipe.execute()
        logger.debug(
            f"Elapsed time for reading and updating status info: {(time.time()-start)*1000} ms"
        )
        return signal_container

    def _read_config_and_update_devices(self, devices: list[str], metadata: dict) -> list:
        start = time.time()
        pipe = self.connector.pipeline()
        signal_container = []
        for dev in devices:
            self.device_manager.devices.get(dev).metadata = metadata
            obj = self.device_manager.devices.get(dev).obj
            try:
                signals = obj.read_configuration()
                signal_container.append(signals)
            # pylint: disable=broad-except
            except Exception as exc:
                signals = self._retry_obj_method(dev, obj, "read_configuration", exc)
            self.connector.set_and_publish(
                MessageEndpoints.device_read_configuration(dev),
                messages.DeviceMessage(signals=signals, metadata=metadata),
                pipe,
            )
        pipe.execute()
        logger.debug(
            f"Elapsed time for reading and updating status info: {(time.time()-start)*1000} ms"
        )
        return signal_container

    def _retry_obj_method(self, device: str, obj: OphydObject, method: str, exc: Exception) -> dict:
        self.device_manager.connector.raise_alarm(
            severity=Alarms.WARNING,
            alarm_type="Warning",
            source={"device": device, "method": method},
            msg=f"Failed to run {method} on device {device}.",
            metadata=self._get_metadata_for_alarm(),
        )
        device_root = device.split(".")[0]
        ds_dev = self.device_manager.devices.get(device_root)

        if ds_dev.on_failure == OnFailure.RETRY:
            # try to read it again, may have been only a glitch
            signals = getattr(obj, method)()
        elif ds_dev.on_failure == OnFailure.RAISE:
            raise exc
        elif ds_dev.on_failure == OnFailure.BUFFER:
            # if possible, fall back to past readings
            logger.warning(
                f"Failed to run {method} on device {device_root}. Trying to load an old value."
            )
            if method == "read":
                old_msg = self.connector.get(MessageEndpoints.device_read(device_root))
            elif method == "read_configuration":
                old_msg = self.connector.get(
                    MessageEndpoints.device_read_configuration(device_root)
                )
            else:
                raise ValueError(f"Unknown method {method}.")
            if not old_msg:
                raise exc
            signals = old_msg.content["signals"]
        else:
            raise ValueError(f"Unknown on_failure value {ds_dev.on_failure}.")
        return signals

    def _stage_device(
        self, instr: messages.DeviceInstructionMessage, timeout_on_unstage: int = 10
    ) -> None:
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]

        self.requests_handler.add_request(instr, num_status_objects=len(devices))

        num_status_objects = 0
        for dev in devices:
            status = None
            obj = self.device_manager.devices[dev].obj
            if hasattr(obj, "_staged"):
                # pylint: disable=protected-access
                if obj._staged == Staged.yes:
                    logger.info(f"Device {obj.name} was already staged and will be first unstaged.")
                    status = self.device_manager.devices[dev].obj.unstage()
                    if isinstance(status, StatusBase):
                        for ii in range(3):
                            try:
                                status.wait(timeout=timeout_on_unstage)
                                status = None  # Set status None and break the loop since unstage is successful
                                break
                            except ophyd_errors.WaitTimeoutError:
                                logger.warning(
                                    f"Unstaging device {dev} still running, {timeout_on_unstage*(ii+1)} seconds passed."
                                )
                        if status is not None:
                            raise ValueError(
                                f"Unstaging device {dev} failed to finish in 30 seconds"
                            )
                status = self.device_manager.devices[dev].obj.stage()
                if status is None or isinstance(status, list):
                    continue
                if not isinstance(status, StatusBase):
                    raise ValueError(
                        f"The stage method of {dev} does not return a StatusBase object."
                    )
                num_status_objects += 1
                status.__dict__["instruction"] = instr
                status.__dict__["obj"] = obj
                status.__dict__["status"] = 1
                status.add_callback(self._device_staged_callback)
                self.requests_handler.add_status_object(instr.metadata["device_instr_id"], status)

        self.requests_handler.patch_num_status_objects(instr, num_status_objects)

    def _device_staged_callback(self, status: StatusBase) -> None:
        """Set the device status to staged"""
        obj = status.__dict__["obj"]
        dev_name = obj.name
        instr = status.__dict__["instruction"]
        state = status.__dict__["status"]
        self.connector.set(
            MessageEndpoints.device_staged(dev_name),
            messages.DeviceStatusMessage(device=dev_name, status=state, metadata=instr.metadata),
        )
        if state == 1:  # Device was/is staged
            obj = self.device_manager.devices[dev_name].obj
            # pylint: disable=protected-access
            if hasattr(obj, "_staged") and obj._staged != Staged.yes:
                raise ValueError(f"Failed to stage device {dev_name}.")

    def _unstage_device(self, instr: messages.DeviceInstructionMessage) -> None:
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]

        self.requests_handler.add_request(instr, num_status_objects=len(devices))
        num_status_objects = 0
        for dev in devices:
            status = None
            obj = self.device_manager.devices[dev].obj
            if hasattr(obj, "_staged"):
                # pylint: disable=protected-access
                if obj._staged == Staged.yes:
                    status = self.device_manager.devices[dev].obj.unstage()
                else:
                    logger.debug(f"Device {obj.name} was already unstaged.")
            if status is None or isinstance(status, list):
                continue
            if not isinstance(status, StatusBase):
                raise ValueError(
                    f"The unstage method of {dev} does not return a StatusBase object."
                )
            num_status_objects += 1
            status.__dict__["instruction"] = instr
            status.__dict__["obj"] = obj
            status.__dict__["status"] = 0
            status.add_callback(self._device_staged_callback)
            self.requests_handler.add_status_object(instr.metadata["device_instr_id"], status)

        self.requests_handler.patch_num_status_objects(instr, num_status_objects)
