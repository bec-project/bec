"""Hardware-independent BEC device service and instruction dispatch."""

from __future__ import annotations

import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Callable

from bec_lib import messages
from bec_lib.bec_errors import ExceptionWithErrorInfo
from bec_lib.bec_service import BECService
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import BECStatus
from bec_server.device_server.device_layer import DeviceLayer, create_default_device_layer
from bec_server.device_server.errors import DisabledDeviceError, InvalidDeviceError
from bec_server.device_server.request_handler import RequestHandler, ResponseState

if TYPE_CHECKING:
    from bec_lib.redis_connector import MessageObject, RedisConnector

__all__ = [
    "DeviceServer",
    "DisabledDeviceError",
    "InvalidDeviceError",
    "RequestHandler",
    "ResponseState",
]

logger = bec_logger.logger
register_stop = threading.Event()


class DeviceServer(BECService):
    """Route BEC device requests through an injectable hardware layer."""

    def __init__(
        self,
        config,
        connector_cls: type[RedisConnector],
        *,
        layer_factory: Callable[[DeviceServer], DeviceLayer] = create_default_device_layer,
    ) -> None:
        super().__init__(config, connector_cls, unique_service=True)
        self._tasks = []
        self._closing = False
        self._shutdown_complete = False
        self._shutdown_lock = threading.Lock()
        self._instruction_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.requests_handler = RequestHandler(self.connector)
        self.device_layer = layer_factory(self)
        self.device_manager = self.device_layer.device_manager
        self.rpc_handler = self.device_layer.rpc
        self._start_device_manager()
        self.connector.register(MessageEndpoints.stop_devices(), cb=self.on_stop_devices)

    def _start_device_manager(self) -> None:
        self.device_layer.initialize(self.bootstrap_server)

    def shutdown(self, per_thread_timeout_s: float | None = None) -> None:
        """Quiesce operations and backend callbacks before closing BEC transport.

        Args:
            per_thread_timeout_s (float | None): Timeout passed to service transport shutdown.
        """
        with self._shutdown_lock:
            if self._shutdown_complete:
                return
            with self._instruction_lock:
                self._closing = True
            register_stop.set()
            # Transport errors must not prevent executor/backend cleanup. Unregistering is
            # best-effort because dispatched callbacks also check the admission guard.
            for topic, callback in (
                (MessageEndpoints.device_instructions(), self.instructions_callback),
                (MessageEndpoints.stop_devices(), self.on_stop_devices),
            ):
                try:
                    self.connector.unregister(topic, cb=callback)
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        "Failed to unregister device service callback during shutdown."
                    )
            try:
                self.executor.shutdown(wait=True, cancel_futures=True)
            finally:
                self.requests_handler.shutdown()
                try:
                    self.device_layer.shutdown()
                finally:
                    self._service_info_event.set()
                    self._metrics_emitter_event.set()
                    super().shutdown(per_thread_timeout_s=per_thread_timeout_s)

            self._shutdown_complete = True

    def stop_devices(self, devices: list[str] | None = None) -> None:
        """Stop selected devices through the hardware layer.

        Args:
            devices (list[str] | None): Device names, or None for all enabled devices.
        """
        self.status = BECStatus.BUSY
        try:
            self.device_layer.instructions.stop_devices(devices)
        finally:
            self.status = BECStatus.RUNNING

    def get_device_from_exception(self, exc: Exception) -> str | None:
        """Obtain device identity from backend-specific exception context.

        Args:
            exc (Exception): Exception raised during a device operation.
        Returns:
            str | None: Device name, when available.
        """
        return self.device_layer.get_device_from_exception(exc)

    def instructions_callback(self, msg, **_kwargs) -> None:
        """Schedule instructions while the service accepts work."""
        with self._instruction_lock:
            if not self._closing:
                self.executor.submit(self.handle_device_instructions, msg.value)

    def start(self) -> None:
        """start the device server"""
        if register_stop.is_set():
            register_stop.clear()

        self.connector.register(
            MessageEndpoints.device_instructions(),
            event=register_stop,
            cb=self.instructions_callback,
        )

        self.status = BECStatus.RUNNING

    def update_status(self, status: BECStatus):
        """update the status of the device server"""
        self.status = status

    def stop(self) -> None:
        """stop the device server"""
        register_stop.set()
        self.status = BECStatus.IDLE

    def _update_device_metadata(self, instr) -> None:
        devices = instr.content["device"]
        if not isinstance(devices, list):
            devices = [devices]
        for dev in devices:
            device_root = dev.split(".")[0]
            self.device_manager.devices.get(device_root).metadata = instr.metadata

    def on_stop_devices(self, msg: MessageObject, **_kwargs) -> None:
        """Accept stop requests until shutdown begins.

        Args:
            msg (MessageObject): Stop request received from Redis.
        """
        with self._instruction_lock:
            if not self._closing:
                self._handle_stop_devices(msg)

    def _handle_stop_devices(self, msg: MessageObject) -> None:
        """
        Callback for receiving device stop requests.
        Handles stop-all (`None`), stop-none (`[]`), and stopping specific devices (device-name list).

        Args:
            msg: MessageObject containing the stop request.
        """
        mvalue: messages.VariableMessage = msg.value
        if mvalue is None:
            logger.warning("Failed to parse scan queue modification message.")
            return
        if mvalue.metadata.get("stop_id"):
            if isinstance(mvalue.metadata["stop_id"], str):
                self.requests_handler.add_stopped_request(mvalue.metadata["stop_id"])
            elif isinstance(mvalue.metadata["stop_id"], list):
                # We don't allow None, so we remove it if present
                for stop_id in mvalue.metadata["stop_id"]:
                    if stop_id is not None:
                        self.requests_handler.add_stopped_request(stop_id)
        if mvalue.value is None:
            self.stop_devices()
            logger.info("Received request to stop all devices.")
            return
        if mvalue.value == []:
            logger.info("Received request to stop no devices.")
            return
        logger.info(f"Received request to stop devices: {mvalue.value}")
        self.stop_devices(mvalue.value)

    def assert_device_is_enabled(self, instructions: messages.DeviceInstructionMessage) -> None:
        """
        Assert that the device(s) in the instructions are enabled.

        Args:
            instructions (messages.DeviceInstructionMessage): The device instruction message.

        Raises:
            DisabledDeviceError: If any of the devices are disabled.
        """
        devices = instructions.content["device"]

        if isinstance(devices, str):
            devices = [devices]

        for dev in devices:
            dev = dev.split(".")[0]
            if not self.device_manager.devices[dev].enabled:
                raise DisabledDeviceError(f"Cannot access disabled device {dev}.")

    def assert_device_is_valid(self, instructions: messages.DeviceInstructionMessage) -> None:
        """
        Assert that the device(s) in the instructions are valid.

        Args:
            instructions (messages.DeviceInstructionMessage): The device instruction message.

        Raises:
            InvalidDeviceError: If any of the devices are invalid.
        """
        devices = instructions.content["device"]
        if not devices:
            raise InvalidDeviceError("At least one device must be specified.")
        if isinstance(devices, str):
            devices = [devices]
        for dev in devices:
            dev = dev.split(".")[0]
            if dev not in self.device_manager.devices:
                raise InvalidDeviceError(f"There is no device with the name {dev}.")

    def get_metadata_for_alarm(
        self, instruction: messages.DeviceInstructionMessage | None = None
    ) -> dict:
        """
        Get instruction and scan metadata for alarms.

        Args:
            instruction (messages.DeviceInstructionMessage | None): Instruction being handled.
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

    def _ensure_request_registered(
        self, instruction: messages.DeviceInstructionMessage | None
    ) -> None:
        """Register a placeholder request so early failures can still resolve."""
        if instruction is None:
            return
        instr_id = instruction.metadata.get("device_instr_id")
        if instr_id is None:
            return
        if not self.requests_handler.has_request(instr_id):
            self.requests_handler.add_request(instruction, num_status_objects=0)

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
            self.assert_device_is_valid(instructions)
            if action != "rpc":
                # rpc has its own error handling
                self.assert_device_is_enabled(instructions)
            self._update_device_metadata(instructions)

            if action == "rpc":
                self.device_layer.rpc.run_rpc(instructions)
            else:
                self.device_layer.instructions.dispatch(instructions)
        except Exception as exc:  # pylint: disable=broad-except
            content = traceback.format_exc()
            if isinstance(exc, ExceptionWithErrorInfo):
                error_info = exc.error_info
                if not error_info.device:
                    error_info.device = self.get_device_from_exception(exc)
            else:
                compact_msg = traceback.format_exc(limit=0)
                error_info = messages.ErrorInfo(
                    error_message=content,
                    compact_error_message=compact_msg,
                    exception_type=exc.__class__.__name__,
                    device=self.get_device_from_exception(exc),
                )
            self._ensure_request_registered(instructions)
            if action == "rpc":
                self.rpc_handler.send_rpc_exception(exc, instructions)
            else:
                logger.error(content)
            self.requests_handler.set_finished(
                instructions.metadata["device_instr_id"], success=False, error_info=error_info
            )
