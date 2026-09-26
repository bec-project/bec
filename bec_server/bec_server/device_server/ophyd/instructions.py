"""Ophyd execution routes for device instructions and completion effects."""

from __future__ import annotations

import inspect
import threading
import time
import traceback
from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING, Any

import ophyd
from ophyd import DeviceStatus, Kind, OphydObject, Staged, StatusBase
from ophyd.utils import errors as ophyd_errors

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device import OnFailure
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.errors import DisabledDeviceError, InvalidDeviceError
from bec_server.device_server.friendly_device_exceptions import reformat_known_device_exceptions
from bec_server.device_server.ophyd.status import OphydStatus

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector
    from bec_server.device_server.ophyd.device_manager import DeviceManagerDS
    from bec_server.device_server.request_handler import RequestHandler

logger = bec_logger.logger


class OphydInstructions:
    """Execute the device instruction API using native Ophyd objects."""

    def __init__(
        self,
        device_manager: DeviceManagerDS,
        connector: RedisConnector,
        requests_handler: RequestHandler,
        *,
        alarm_metadata: Callable[[messages.DeviceInstructionMessage | None], dict],
    ) -> None:
        self.device_manager = device_manager
        self.connector = connector
        self.requests_handler = requests_handler
        self._alarm_metadata = alarm_metadata
        self._completion_lock = threading.RLock()
        self._closed = False

    def close(self) -> None:
        """Wait for active finalizers and suppress transport work by late completions."""
        with self._completion_lock:
            self._closed = True

    def dispatch(self, instruction: messages.DeviceInstructionMessage) -> None:
        """Dispatch an exposed instruction through the Ophyd route.

        Args:
            instruction (messages.DeviceInstructionMessage): Instruction to execute.
        """
        routes = {
            "set": self._set_device,
            "read": self._read_device,
            "trigger": self._trigger_device,
            "kickoff": self._kickoff_device,
            "complete": self._complete_device,
            "stage": self._stage_device,
            "unstage": self._unstage_device,
            "pre_scan": self._pre_scan,
        }
        try:
            route = routes[instruction.action]
        except KeyError as exc:
            raise ValueError(f"Unknown device instruction {instruction.action}.") from exc
        route(instruction)

    def register_status(
        self,
        status: StatusBase,
        instruction: messages.DeviceInstructionMessage,
        obj: OphydObject,
        *,
        sub_id: int | None = None,
        staged_state: int | None = None,
    ) -> OphydStatus:
        """Adapt and register an Ophyd status for instruction or RPC completion.

        Args:
            status (StatusBase): Native operation status.
            instruction (messages.DeviceInstructionMessage): Owning instruction.
            obj (OphydObject): Device that produced the status.
            sub_id (int | None): Temporary subscription to release on completion.
            staged_state (int | None): Stage state to validate and publish.
        Returns:
            OphydStatus: Status settled after native cleanup and cache updates.
        """
        if not isinstance(status, StatusBase):
            if sub_id is not None:
                obj.unsubscribe(sub_id)
            raise InvalidDeviceError(f"The operation on {obj.name} did not return a status object.")
        adapted = OphydStatus(
            status,
            instruction,
            obj,
            on_complete=partial(self._finalize_status, sub_id=sub_id, staged_state=staged_state),
        )
        self.requests_handler.add_status_object(instruction, adapted)
        return adapted

    def _finalize_status(
        self, status: OphydStatus, *, sub_id: int | None = None, staged_state: int | None = None
    ) -> None:
        # Subscription cleanup remains necessary even after the service has shut down.
        if sub_id is not None:
            status.obj.unsubscribe(sub_id)
        with self._completion_lock:
            if not self._closed:
                self._finalize_device_status(status, staged_state)

    def _finalize_device_status(self, status: OphydStatus, staged_state: int | None) -> None:
        obj = status.obj
        instruction = status.instruction
        if staged_state is not None:
            # pylint: disable=protected-access
            if staged_state == 1 and hasattr(obj, "_staged") and obj._staged != Staged.yes:
                raise ValueError(f"Failed to stage device {obj.name}.")
            self.connector.set(
                MessageEndpoints.device_staged(obj.name),
                messages.DeviceStatusMessage(
                    device=obj.name, status=staged_state, metadata=instruction.metadata
                ),
            )
        pipe = self.connector.pipeline()
        content = instruction.content
        rpc_func = content["parameter"].get("func", "")
        is_set = content["action"] == "set" or (
            content["action"] == "rpc" and (rpc_func == "set" or rpc_func.endswith(".set"))
        )
        if is_set:
            if obj.kind == Kind.config:
                self._update_read_configuration(obj, instruction.metadata, pipe)
            elif obj.kind in [Kind.normal, Kind.hinted]:
                self.read_and_update_devices([obj.root.name], instruction.metadata)
            elif content["action"] == "rpc":
                # RPC set historically refreshes both caches for other Kind combinations.
                self.read_and_update_devices([obj.root.name], instruction.metadata)
                self.read_config_and_update_devices([obj.root.name], instruction.metadata)
        pipe.execute()

    def stop_devices(self, devices: list[str] | None = None) -> None:
        """
        Stop the specified devices or all devices if none are specified.

        Args:
            devices (list[str] | None): List of device names to stop. If None, all devices will be stopped.
        """
        if devices is None:
            logger.info("Stopping devices after receiving 'abort' request.")
            devices_to_stop = self.device_manager.devices.enabled_devices
        else:
            logger.info(f"Stopping devices {devices} after receiving 'abort' request.")
            devices_to_stop = [
                dev for dev in self.device_manager.devices.enabled_devices if dev.name in devices
            ]
        for dev in devices_to_stop:
            if dev.read_only:
                # don't stop devices that we haven't set
                continue
            if hasattr(dev.obj, "stop"):
                try:
                    dev.obj.stop()
                except Exception as exc:  # pylint: disable=broad-except
                    content = traceback.format_exc()
                    error_info = messages.ErrorInfo(
                        error_message=content,
                        compact_error_message=traceback.format_exc(limit=0),
                        exception_type=exc.__class__.__name__,
                        device=dev.obj.name,
                    )
                    self.connector.raise_alarm(
                        severity=Alarms.WARNING,
                        info=error_info,
                        metadata=self._alarm_metadata(None),
                    )

    @staticmethod
    def get_device_from_exception(exc: Exception) -> str | None:
        """Try to extract the device name from an exception message.

        Args:
            exc (Exception): The exception to extract the device name from.
        Returns:
            str | None: The device name if found, otherwise None.
        """
        if not hasattr(exc, "__traceback__"):
            return None
        tb = exc.__traceback__
        while tb:
            frame = tb.tb_frame
            local_vars = frame.f_locals
            if "self" in local_vars:
                obj = local_vars["self"]
                if isinstance(obj, ophyd.OphydObject):
                    return obj.dotted_name or obj.name
            tb = tb.tb_next
        return None

    def _trigger_device(self, instr: messages.DeviceInstructionMessage) -> None:
        logger.trace(f"Trigger device: {instr}")
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]
        devices = self.device_manager.get_device_order(devices)
        self.requests_handler.add_request(instr, num_status_objects=len(devices))
        for dev in devices:
            obj = self.device_manager.devices.get(dev)
            obj.metadata = instr.metadata
            obj = obj.obj
            status = obj.trigger()

            self.register_status(status, instr, obj)

    def _kickoff_device(self, instr: messages.DeviceInstructionMessage) -> None:
        logger.trace(f"Kickoff device: {instr}")

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

        self.register_status(status, instr, obj)

    def _complete_device(self, instr: messages.DeviceInstructionMessage) -> None:
        if instr.content["device"] is None:
            devices = [dev.name for dev in self.device_manager.devices.enabled_devices]
        else:
            devices = instr.content["device"]
            if not isinstance(devices, list):
                devices = [devices]

        devices = self.device_manager.get_device_order(devices)

        self.requests_handler.add_request(instr, num_status_objects=len(devices))
        num_status_objects = 0
        for dev in devices:
            obj = self.device_manager.devices.get(dev).obj
            if not hasattr(obj, "complete"):
                continue
            num_status_objects += 1
            logger.trace(f"Completing device: {dev}")
            status = obj.complete()
            if status is None:
                raise InvalidDeviceError(
                    f"The complete method of device {dev} does not return a StatusBase object."
                )

            self.register_status(status, instr, obj)

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
        logger.trace(f"Setting device: {instr}")
        val = instr.content["parameter"]["value"]
        sub_id = None
        if child_access:
            obj = rgetattr(device_obj.obj, child_access)
            if "readback" in obj.event_types or "value" in obj.event_types:
                # pylint: disable=protected-access
                sub_id = obj.subscribe(self.device_manager._obj_callback_readback, run=True)
        else:
            obj = device_obj.obj
        try:
            val = self.convert_value_if_needed(obj, val)
            status = obj.set(val)
        except Exception as exc:  # pylint: disable=broad-except
            exc = reformat_known_device_exceptions(
                exc, "set", f"Device: {device_name}, value: {val}."
            )
            status = DeviceStatus(device=obj)
            status.set_exception(exc)
        self.register_status(status, instr, obj, sub_id=sub_id)

    @staticmethod
    def convert_value_if_needed(obj: ophyd.OphydObject, val: Any) -> Any:
        """
        Convert the value to the appropriate type if needed. This is particularly
        needed for ophyd signals that implement enum strings but are given as floats.

        Args:
            obj (ophyd.OphydObject): The ophyd object to set.
            val (Any): The value to set.

        Returns:
            Any: The converted value.
        """
        if (
            hasattr(obj, "enum_strs")
            and obj.enum_strs is not None
            and len(obj.enum_strs) > 0
            and isinstance(val, float)
        ):
            if not val.is_integer():
                raise ValueError(
                    f"Cannot convert float {val} to enum index for {obj.name}. "
                    f"Value must be an integer to select one of the enum strings: {obj.enum_strs}."
                )
            val = int(val)
        return val

    def _pre_scan(self, instr: messages.DeviceInstructionMessage) -> None:
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]

        devices = self.device_manager.get_device_order(devices)

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
            self.register_status(status, instr, obj)

        self.requests_handler.patch_num_status_objects(instr, num_status_objects)

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

        devices = self.device_manager.get_device_order(devices)

        if not new_status:
            self.read_and_update_devices(devices, instr.metadata)
            return

        self.requests_handler.add_request(instr, num_status_objects=0)
        result = self.read_and_update_devices(devices, instr.metadata)
        response_result = result if instr.parameter.get("return_result", False) else None
        self.requests_handler.set_finished(
            instr.metadata["device_instr_id"], success=True, result=response_result
        )

    def read_and_update_devices(self, devices: list[str], metadata: dict) -> list:
        """Read devices and publish their readback caches without registering a request.

        Args:
            devices (list[str]): Device names in the requested read order.
            metadata (dict): Metadata attached to each published reading.
        Returns:
            list: Native readings for the requested devices.
        """
        start = time.time()
        pipe = self.connector.pipeline()
        signal_container = []
        devices = self.device_manager.get_device_order(devices)
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
        logger.trace(
            f"Elapsed time for reading and updating status info: {(time.time() - start) * 1000} ms"
        )
        return signal_container

    def read_config_and_update_devices(self, devices: list[str], metadata: dict) -> list:
        """Read and publish configuration caches without registering a request.

        Args:
            devices (list[str]): Device names to read.
            metadata (dict): Metadata attached to each published reading.
        Returns:
            list: Native configuration readings for the requested devices.
        """
        start = time.time()
        pipe = self.connector.pipeline()
        signal_container = []
        devices = self.device_manager.get_device_order(devices)
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
        logger.trace(
            f"Elapsed time for reading and updating status info: {(time.time() - start) * 1000} ms"
        )
        return signal_container

    def _retry_obj_method(self, device: str, obj: OphydObject, method: str, exc: Exception) -> dict:
        error_info = messages.ErrorInfo(
            error_message=f"Failed to run {method} on device {device}.\n{traceback.format_exc()}",
            compact_error_message=traceback.format_exc(limit=0),
            exception_type=exc.__class__.__name__,
            device=device,
        )
        self.device_manager.connector.raise_alarm(
            severity=Alarms.WARNING, info=error_info, metadata=self._alarm_metadata(None)
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

        devices = self.device_manager.get_device_order(devices)

        self.requests_handler.add_request(instr, num_status_objects=len(devices))

        num_status_objects = 0
        for dev in devices:
            status = None
            obj = self.device_manager.devices[dev].obj

            if not hasattr(obj, "_staged"):
                continue

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
                                f"Unstaging device {dev} still running, {timeout_on_unstage * (ii + 1)} seconds passed."
                            )
                    if status is not None:
                        raise ValueError(f"Unstaging device {dev} failed to finish in 30 seconds")
            status = self.device_manager.devices[dev].obj.stage()
            if status is None or isinstance(status, list):
                continue
            if not isinstance(status, StatusBase):
                raise ValueError(f"The stage method of {dev} does not return a StatusBase object.")
            num_status_objects += 1
            self.register_status(status, instr, obj, staged_state=1)

        self.requests_handler.patch_num_status_objects(instr, num_status_objects)

    def _unstage_device(self, instr: messages.DeviceInstructionMessage) -> None:
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]

        devices = self.device_manager.get_device_order(devices)

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
            self.register_status(status, instr, obj, staged_state=0)

        self.requests_handler.patch_num_status_objects(instr, num_status_objects)
