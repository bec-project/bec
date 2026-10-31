from __future__ import annotations

import traceback
from contextlib import redirect_stdout
from dataclasses import replace
from io import StringIO
from typing import TYPE_CHECKING, Any, cast

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.device_backend import DeviceHandler, ReadCategory, RPCResult
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.device_server.devices import is_serializable

logger = bec_logger.logger


if TYPE_CHECKING:
    from bec_server.device_server.device_server import DeviceServer


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
                request = self.requests_handler.get_request(instr.metadata["device_instr_id"])
                if request and not request["status_objects"]:
                    self.device_manager.devices[self._get_device_root(instr)].handler.flush_events(
                        timeout=10
                    )
                # send result to client
                self.send_rpc_result_to_client(instr, device, instr_params, res, result)
                logger.trace(res)
            except Exception as exc:  # pylint: disable=broad-except
                # send error to client
                self.send_rpc_exception(exc, instr)

    def process_rpc_instruction(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Process RPC instruction and return result.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            Any: Result of RPC instruction
        """

        instr_params = instr.parameter

        if self._instr_with_operation(instr, "read"):
            # handle ophyd read operations. This will update the cache as well
            return self._handle_rpc_read(instr)

        if self._instr_with_operation(instr, "read_configuration"):
            # handle ophyd read_configuration operations. This will update the cache as well
            return self._rpc_read_configuration_and_return(instr)

        if instr_params.get("kwargs", {}).get("_set_property"):
            self._assert_rpc_write_allowed(instr)
            return self._handle_rpc_property_set(instr)

        if self._instr_with_operation(instr, "set"):
            self._assert_rpc_write_allowed(instr)
            return self._handle_set(instr)

        if self._instr_with_operation(instr, "put"):
            self._assert_rpc_write_allowed(instr)
            return self._handle_misc_rpc(instr)

        return self._handle_misc_rpc(instr)

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
            device=self.device_server.device_provider.error_device(exc),
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

    ##################################################
    ###### Handlers for specific RPC operations ######
    ##################################################

    def _handle_rpc_read(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Handle an ophyd read operation via RPC.
        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        _, obj, _ = self._get_rpc_components(instr)
        if obj.read_category == ReadCategory.CONFIGURATION:
            return self._rpc_read_configuration_and_return(instr)
        if obj.read_category == ReadCategory.OMITTED:
            return obj.read()
        return self._rpc_read_and_return(instr)

    def _handle_rpc_property_set(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Handle setting a property via RPC.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        handler = self.device_manager.devices[self._get_device_root(instr)].handler
        handler.set_property(instr.parameter.get("func", ""), instr.parameter.get("args")[0])

    def _handle_set(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Handle an ophyd set operation via RPC.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        return self._handle_misc_rpc(instr)

    def _handle_misc_rpc(self, instr: messages.DeviceInstructionMessage) -> Any:
        """Run an explicit provider RPC and preserve BEC response/cache semantics."""
        _, root_handler, invocation = self._get_rpc_components(instr)
        result = self._execute_rpc_call(invocation, instr)
        target = self.device_manager.devices[self._get_device_root(instr)].handler.resolve(
            result.target_path
        )
        if result.completion is not None:
            # Register callback context before observing an already-complete result.
            context = self.device_server._add_status_object_info(result.completion, instr, target)
            is_set = instr.parameter.get("func") == "set" or instr.parameter.get(
                "func", ""
            ).endswith(".set")
            context.refresh = result.refresh and is_set and not result.completion.done
            if result.refresh and not context.refresh:
                self._update_cache(target, instr)
            self.requests_handler.add_status_object(instr.metadata["device_instr_id"], context)
        elif result.refresh:
            self._update_cache(target, instr)
        return (
            result.status.as_rpc(instr.metadata.get("RID"))
            if result.status is not None
            else result.value
        )

    ##################################################
    ###### Helper functions for RPC processing ######
    ##################################################

    def _execute_rpc_call(
        self, rpc_var: tuple[DeviceHandler, str], instr: messages.DeviceInstructionMessage
    ) -> RPCResult:
        """Execute a typed provider invocation; HAL serialization stays in the provider."""
        handler, path = rpc_var
        result = handler.rpc(
            path,
            tuple(instr.parameter.get("args", ())),
            instr.parameter.get("kwargs", {}),
            rid=instr.metadata.get("RID"),
        )
        if result.warning is not None:
            text = f"Return value of rpc call {instr.parameter} is not serializable."
            warning = messages.ErrorInfo(
                error_message=text,
                compact_error_message=text,
                exception_type="TypeError",
                device=instr.device,
            )
            self.connector.raise_alarm(severity=Alarms.WARNING, info=warning)
            result = replace(result, warning=warning)
        return result

    def _assert_rpc_write_allowed(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Reject write-style RPCs for read-only devices.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        # Imported lazily to avoid a circular import during module initialization.
        from bec_server.device_server.device_server import DisabledDeviceError

        device_root = self._get_device_root(instr)
        device_obj = self.device_manager.devices[device_root]
        if device_obj.read_only:
            raise DisabledDeviceError(
                f"Setting the device {device_obj.name} is currently disabled."
            )

    def _get_rpc_components(
        self, instr: messages.DeviceInstructionMessage
    ) -> tuple[str, DeviceHandler, tuple[DeviceHandler, str]]:
        """Resolve the requested handler/path without exposing a native bound method."""
        root = self._get_device_root(instr)
        handler = self.device_manager.devices[root].handler
        path = instr.parameter.get("func", "")
        if path.endswith((".set", ".put", ".read")):
            target = handler.resolve(path.rsplit(".", 1)[0])
        else:
            target = handler
        return root, target, (handler, path)

    def _get_device_root(self, instr: messages.DeviceInstructionMessage) -> str:
        """
        Get the device root from the instruction.
        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        Returns:
            str: Device root
        """
        return instr.device.split(".")[0]

    def _rpc_read_and_return(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Handle RPC read operation and return the result.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            Any: Result of RPC instruction
        """
        res = self.device_server._read_and_update_devices([instr.content["device"]], instr.metadata)
        if isinstance(res, list) and len(res) == 1:
            res = res[0]
        return res

    def _rpc_read_configuration_and_return(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Handle RPC read_configuration operation and return the result.
        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        Returns:
            Any: Result of RPC instruction
        """
        res = self.device_server._read_config_and_update_devices(
            [instr.content["device"]], instr.metadata
        )
        if isinstance(res, list) and len(res) == 1:
            res = res[0]
        return res

    def _instr_with_operation(
        self, instr: messages.DeviceInstructionMessage, operation: str
    ) -> bool:
        """
        Check if the instruction has the specified operation, either
        as exact match or as suffix.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
            operation(str): Operation to check

        Returns:
            bool: True if the instruction has the specified operation, False otherwise
        """
        instr_params = instr.parameter
        func_name = instr_params.get("func", "")
        return func_name == operation or func_name.endswith(f".{operation}")

    def _update_cache(self, obj: DeviceHandler, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Update the redis cache.

        Args:
            obj(ophyd.OphydObject): Ophyd object
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            Any: Result of RPC instruction
        """
        if obj.read_category == ReadCategory.CONFIGURATION:
            return self._rpc_read_configuration_and_return(instr)
        if obj.read_category == ReadCategory.NORMAL:
            return self._rpc_read_and_return(instr)
        self._rpc_read_and_return(instr)
        return self._rpc_read_configuration_and_return(instr)
