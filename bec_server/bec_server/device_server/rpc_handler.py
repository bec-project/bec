from __future__ import annotations

import traceback
from contextlib import redirect_stdout
from io import StringIO
from typing import TYPE_CHECKING, Any, cast

import ophyd

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils.rpc_utils import rgetattr
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
            return self._handle_rpc_property_set(instr)

        if self._instr_with_operation(instr, "set"):
            return self._handle_set(instr)

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
            device=self.device_server.get_device_from_exception(exc),
        )
        logger.info(f"Received exception: {error_info}, {exc}")
        instr_params = instr.parameter
        self.connector.set(
            MessageEndpoints.device_rpc(instr_params.get("rpc_id")),
            messages.DeviceRPCMessage(
                device=instr.content["device"], return_val=None, out=error_info, success=False
            ),
        )
        diid = instr.metadata.get("device_instr_id", "")
        request = self.requests_handler.get_request(diid)
        if request and not request.get("status_objects"):

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

        if not hasattr(obj, "kind"):
            return self._rpc_read_and_return(instr)

        if obj.kind not in [ophyd.Kind.omitted, ophyd.Kind.config]:
            return self._rpc_read_and_return(instr)
        if obj.kind == ophyd.Kind.config:
            return self._rpc_read_configuration_and_return(instr)
        if obj.kind == ophyd.Kind.omitted:
            return obj.read()

        return None

    def _handle_rpc_property_set(self, instr: messages.DeviceInstructionMessage) -> None:
        """
        Handle setting a property via RPC.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        instr_params = instr.parameter
        device_root = self._get_device_root(instr)
        sub_access = instr_params.get("func", "").split(".")
        property_name = sub_access[-1]
        if len(sub_access) > 1:
            sub_access = sub_access[0:-1]
        else:
            sub_access = []
        obj = self.device_manager.devices[device_root].obj
        if sub_access:
            obj = rgetattr(obj, ".".join(sub_access))
        setattr(obj, property_name, instr_params.get("args")[0])

    def _handle_set(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Handle an ophyd set operation via RPC.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        _, obj, rpc_var = self._get_rpc_components(instr)
        res = self._execute_rpc_call(rpc_var, instr)

        if not isinstance(res, ophyd.StatusBase) or res.done:
            self._update_cache(obj, instr)
        else:
            # for set operations that return a Status object, we update the cache
            # when the status object is done
            def _update_cache_on_set_done(status_obj, rpc_mixin_self=self, obj=obj, instr=instr):
                rpc_mixin_self._update_cache(obj, instr)

            res.add_callback(_update_cache_on_set_done)

        if isinstance(res, ophyd.StatusBase):
            self._register_status_object(instr, res, obj)

        return self._serialize_rpc_response(instr, res)

    def _handle_misc_rpc(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Handle miscellaneous RPC operations and update the cache if necessary.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            Any: Result of RPC instruction
        """
        instr_params = instr.content.get("parameter")

        _, obj, rpc_var = self._get_rpc_components(instr)
        res = self._execute_rpc_call(rpc_var, instr)

        # update the cache for value-updating functions
        if instr_params.get("func") in ["put", "get"] or instr_params.get("func").endswith(
            (".put", ".get")
        ):
            self._update_cache(obj, instr)

        if isinstance(res, ophyd.StatusBase):
            self._register_status_object(instr, res, obj)

        return self._serialize_rpc_response(instr, res)

    ##################################################
    ###### Helper functions for RPC processing ######
    ##################################################

    def _execute_rpc_call(self, rpc_var: Any, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Get result from RPC call. This is the core function that executes the RPC call.

        Args:
            rpc_var(Any): RPC variable or callable
            instr_params(dict): RPC instruction parameters

        Returns:
            Any: Result of RPC call
        """
        instr_params = instr.parameter
        if callable(rpc_var):
            args = tuple(instr_params.get("args", ()))
            kwargs = instr_params.get("kwargs", {})
            res = rpc_var(*args, **kwargs)
        else:
            res = rpc_var
        if not is_serializable(res):
            if isinstance(res, ophyd.StatusBase):
                return res
            if isinstance(res, list) and instr_params.get("func") in ["stage", "unstage"]:
                # pylint: disable=protected-access
                return [obj._staged for obj in res]
            res = None
            msg = f"Return value of rpc call {instr_params} is not serializable."
            error_info = messages.ErrorInfo(
                error_message=msg,
                compact_error_message=msg,
                exception_type="TypeError",
                device=instr.device,
            )
            self.connector.raise_alarm(severity=Alarms.WARNING, info=error_info)
        return res

    def _get_rpc_components(
        self, instr: messages.DeviceInstructionMessage
    ) -> tuple[str, ophyd.Device | ophyd.Signal, Any]:
        """
        Get RPC components: device root, RPC object, and RPC method.

        As an example of the returned values:
        For instruction:
            device: "motor1"
            parameter: {"func": "move", "args": [5], "kwargs": {}}
        The returned values would be:
            device_root: "motor1"
            rpc_obj: <ophyd.Device motor1>
            rpc_method: <bound method Device.move of <ophyd.Device motor1>>

        For instruction:
            device: "motor1.position"
            parameter: {"func": "set", "args": [10], "kwargs": {}}
        The returned values would be:
            device_root: "motor1"
            rpc_obj: <ophyd.Signal motor1.position>
            rpc_method: <bound method Signal.set of <ophyd.Signal motor1.position>>

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            tuple[str, ophyd.Device | ophyd.Signal, Any]: Device root, RPC object, and RPC method
        """
        instr_params = instr.parameter
        device_root = self._get_device_root(instr)
        obj = self.device_manager.devices[device_root].obj
        rpc_method = rgetattr(obj, instr_params.get("func"))
        if instr_params.get("func", "").endswith((".set", ".put", ".read")):
            rpc_obj = rgetattr(
                self.device_manager.devices[device_root].obj,
                instr_params.get("func").rsplit(".", 1)[0],
            )
        else:
            rpc_obj = obj
        return device_root, rpc_obj, rpc_method

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

    def _register_status_object(
        self,
        instr: messages.DeviceInstructionMessage,
        res: ophyd.StatusBase,
        obj: ophyd.OphydObject,
    ) -> None:
        """
        Register status object with the requests handler if applicable.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
            res(ophyd.StatusBase): Result of RPC instruction
        """
        if not isinstance(res, ophyd.StatusBase):
            return
        res.__dict__["instruction"] = instr
        res.__dict__["obj_ref"] = obj
        self.requests_handler.add_status_object(instr.metadata["device_instr_id"], res)

    def _serialize_rpc_response(self, instr: messages.DeviceInstructionMessage, res: Any) -> Any:
        """
        Serialize RPC response based on its type.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
            res(ophyd.StatusBase): StatusBase object returned from RPC

        Returns:
            Any: Serialized RPC response
        """
        if isinstance(res, ophyd.StatusBase):
            return {
                "type": "status",
                "RID": instr.metadata.get("RID"),
                "success": res.success,
                "timeout": res.timeout,
                "done": res.done,
                "settle_time": res.settle_time,
            }

        if isinstance(res, tuple) and hasattr(res, "_asdict") and hasattr(res, "_fields"):
            # convert namedtuple to dict
            return {
                "type": "namedtuple",
                "RID": instr.metadata.get("RID"),
                "fields": res._fields,
                "values": res._asdict(),
            }

        if isinstance(res, list) and res and isinstance(res[0], ophyd.Staged):
            return [str(stage) for stage in res]

        # no-op for other types
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

    def _update_cache(
        self, obj: ophyd.OphydObject, instr: messages.DeviceInstructionMessage
    ) -> Any:
        """
        Update the redis cache.

        Args:
            obj(ophyd.OphydObject): Ophyd object
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            Any: Result of RPC instruction
        """
        if obj.kind == ophyd.Kind.config:
            return self._rpc_read_configuration_and_return(instr)
        if obj.kind in [ophyd.Kind.normal, ophyd.Kind.hinted]:
            return self._rpc_read_and_return(instr)

        # handle other other weird combinations of ophyd Kind
        self._rpc_read_and_return(instr)
        return self._rpc_read_configuration_and_return(instr)
