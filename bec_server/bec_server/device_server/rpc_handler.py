from __future__ import annotations

import traceback
from contextlib import redirect_stdout
from io import StringIO
from typing import TYPE_CHECKING, Any, cast

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

logger = bec_logger.logger


if TYPE_CHECKING:
    from bec_server.device_server.device_server import DeviceServer
    from bec_server.device_server.instruction_execution import InstructionExecution


class RPCHandler:
    """
    RPC Handler for the Device Server.
    """

    def __init__(self, device_server: DeviceServer):
        self.device_server = device_server
        self.device_manager = device_server.device_manager
        self.connector = device_server.connector
        self.requests_handler = device_server.requests_handler

    def run_rpc(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Run RPC call and send result to client. RPC calls also capture stdout and
        stderr and send it to the client.

        Args:
            instr: DeviceInstructionMessage

        """
        result = StringIO()
        with redirect_stdout(result):
            try:
                self.requests_handler.add_request(instr, num_status_objects=1)
                instr_params = instr.parameter
                device = cast(str, instr.device)
                self.device_server.assert_device_is_enabled(instr)
                res = self.process_rpc_instruction(instr)
                # send result to client
                self.send_rpc_result_to_client(instr, device, instr_params, res, result)
                logger.trace(res)
            except Exception as exc:  # pylint: disable=broad-except
                # send error to client
                self.send_rpc_exception(exc, instr)

    def process_rpc_instruction(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Dispatch an RPC to the provider that owns the registered root.

        Args:
            instr (messages.DeviceInstructionMessage): Request being processed.

        Returns:
            Any: Client-ready result returned by the provider manager.
        """
        device_name = cast(str, instr.device)
        device = self.device_manager.devices[device_name.split(".", 1)[0]]
        return self.device_manager.device_provider_manager.run_rpc(device, instr, self)

    def send_rpc_result_to_client(
        self,
        instr: messages.DeviceInstructionMessage,
        device: str,
        instr_params: dict,
        res: Any,
        result: StringIO,
    ):
        """
        Send RPC result to client.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
            device(str): Device name
            instr_params(dict): RPC instruction parameters
            res(Any): RPC result
            result(StringIO): Captured stdout and stderr
        """
        diid = instr.metadata.get("device_instr_id", "")
        request = self.requests_handler.get_request(diid)
        # if the request was already resolved by a status object,
        # we won't find it in the requests_handler
        if request and not request.get("status_objects"):
            self.requests_handler.set_finished(diid, success=True, result=res)
        self.connector.set(
            MessageEndpoints.device_rpc(instr_params.get("rpc_id")),
            messages.DeviceRPCMessage(
                device=device, return_val=res, out=result.getvalue(), success=True
            ),
            expire=1800,
        )

    def send_rpc_exception(self, exc: Exception, instr: messages.DeviceInstructionMessage):
        """
        Send RPC exception to client.

        Args:
            exc(Exception): Exception raised during RPC
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        error_traceback = traceback.format_exc()
        error_info = messages.ErrorInfo(
            error_message=error_traceback,
            compact_error_message=traceback.format_exc(limit=0),
            exception_type=exc.__class__.__name__,
            device=self.device_server.get_device_from_exception(exc),
        )
        logger.info(f"Received exception: {error_info}, {exc}")
        instr_params = instr.parameter
        self.connector.set(
            MessageEndpoints.device_rpc(instr_params.get("rpc_id")),
            messages.DeviceRPCMessage(
                device=instr.content["device"], return_val=None, out=error_info, success=False
            ),
            expire=1800,
        )
        diid = instr.metadata.get("device_instr_id", "")
        request = self.requests_handler.get_request(diid)
        # A callback on an already-finished status may fail before completing the request.
        if request and all(status.done for status in request["status_objects"]):
            self.requests_handler.set_finished(diid, success=False, error_info=error_info)

    def read_rpc(
        self, instr: messages.DeviceInstructionMessage, *, configuration: bool = False
    ) -> Any:
        """
        Acquire and publish the section requested by the provider.

        Args:
            instr (messages.DeviceInstructionMessage): Request being processed.
            configuration (bool): Select configuration readings instead of device readback.

        Returns:
            Any: Device readings returned by the device server.
        """
        read = (
            self.device_server._read_config_and_update_devices
            if configuration
            else self.device_server._read_and_update_devices
        )
        result = read([cast(str, instr.device)], instr.metadata)
        return result[0] if isinstance(result, list) and len(result) == 1 else result

    def register_rpc_execution(
        self, instr: messages.DeviceInstructionMessage, execution: InstructionExecution
    ) -> None:
        """
        Bind an execution to the request and register its completion callbacks.

        Args:
            instr (messages.DeviceInstructionMessage): Request being processed.
            execution (InstructionExecution): Provider execution that tracks the native completion.
        """
        execution.bind(instr)
        self.requests_handler.add_status_object(instr.metadata["device_instr_id"], execution)

    def warn_unserializable_rpc(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Raise an alarm for a result the client cannot receive.

        Args:
            instr (messages.DeviceInstructionMessage): Request being processed.
        """
        message = f"Return value of rpc call {instr.parameter} is not serializable."
        error_info = messages.ErrorInfo(
            error_message=message,
            compact_error_message=message,
            exception_type="TypeError",
            device=instr.device,
        )
        self.connector.raise_alarm(severity=Alarms.WARNING, info=error_info)
