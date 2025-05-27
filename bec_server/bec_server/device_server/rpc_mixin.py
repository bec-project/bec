import traceback
from contextlib import redirect_stdout
from io import StringIO
from typing import Any

import ophyd

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.devices import is_serializable

logger = bec_logger.logger


class RPCMixin:
    """Mixin for handling RPC calls"""

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
                instr_params = instr.content.get("parameter")
                device = instr.content["device"]
                self._assert_device_is_enabled(instr)
                res = self.process_rpc_instruction(instr)
                # send result to client
                self._send_rpc_result_to_client(instr, device, instr_params, res, result)
                logger.trace(res)
            except Exception as exc:  # pylint: disable=broad-except
                # send error to client
                self._send_rpc_exception(exc, instr)

    def _get_result_from_rpc(self, rpc_var: Any, instr_params: dict) -> Any:
        if callable(rpc_var):
            args = tuple(instr_params.get("args", ()))
            kwargs = instr_params.get("kwargs", {})
            if args and kwargs:
                res = rpc_var(*args, **kwargs)
            elif args:
                res = rpc_var(*args)
            elif kwargs:
                res = rpc_var(**kwargs)
            else:
                res = rpc_var()
        else:
            res = rpc_var
        if not is_serializable(res):
            if isinstance(res, ophyd.StatusBase):
                return res
            if isinstance(res, list) and instr_params.get("func") in ["stage", "unstage"]:
                # pylint: disable=protected-access
                return [obj._staged for obj in res]
            res = None
            self.connector.raise_alarm(
                severity=Alarms.WARNING,
                alarm_type="TypeError",
                source=instr_params,
                msg=f"Return value of rpc call {instr_params} is not serializable.",
                metadata={},
            )
        return res

    def _send_rpc_result_to_client(
        self,
        instr: messages.DeviceInstructionMessage,
        device: str,
        instr_params: dict,
        res: Any,
        result: StringIO,
    ):
        diid = instr.metadata.get("device_instr_id")
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

    def _rpc_read_and_return(self, instr: messages.DeviceInstructionMessage) -> Any:
        res = self._read_and_update_devices([instr.content["device"]], instr.metadata)
        if isinstance(res, list) and len(res) == 1:
            res = res[0]
        return res

    def _rpc_read_configuration_and_return(self, instr: messages.DeviceInstructionMessage) -> Any:
        res = self._read_config_and_update_devices([instr.content["device"]], instr.metadata)
        if isinstance(res, list) and len(res) == 1:
            res = res[0]
        return res

    def _handle_rpc_read(self, instr: messages.DeviceInstructionMessage) -> Any:
        instr_params = instr.content.get("parameter")
        device_root = instr.content["device"].split(".")[0]
        if instr_params.get("func") == "read":
            obj = self.device_manager.devices[device_root].obj
        else:
            obj = rgetattr(
                self.device_manager.devices[device_root].obj,
                instr_params.get("func").split(".read")[0],
            )
        if hasattr(obj, "kind"):
            if obj.kind not in [ophyd.Kind.omitted, ophyd.Kind.config]:
                return self._rpc_read_and_return(instr)
            if obj.kind == ophyd.Kind.config:
                return self._rpc_read_configuration_and_return(instr)
            if obj.kind == ophyd.Kind.omitted:
                return obj.read()
        return self._rpc_read_and_return(instr)

    def _handle_rpc_property_set(self, instr: messages.DeviceInstructionMessage) -> None:
        instr_params = instr.content.get("parameter")
        device_root = instr.content["device"].split(".")[0]
        sub_access = instr_params.get("func").split(".")
        property_name = sub_access[-1]
        if len(sub_access) > 1:
            sub_access = sub_access[0:-1]
        else:
            sub_access = []
        obj = self.device_manager.devices[device_root].obj
        if sub_access:
            obj = rgetattr(obj, ".".join(sub_access))
        setattr(obj, property_name, instr_params.get("args")[0])

    def _handle_misc_rpc(self, instr: messages.DeviceInstructionMessage) -> Any:
        instr_params = instr.content.get("parameter")
        device_root = instr.content["device"].split(".")[0]
        obj = self.device_manager.devices[device_root].obj
        rpc_var = rgetattr(obj, instr_params.get("func"))
        res = self._get_result_from_rpc(rpc_var, instr_params)

        # update the cache for value-updating functions
        if instr_params.get("func") in ["put", "get"]:
            obj = self.device_manager.devices[device_root].obj
            self._update_cache(obj, instr)
        elif instr_params.get("func").endswith(".put"):
            obj = rgetattr(
                self.device_manager.devices[device_root].obj,
                instr_params.get("func").split(".put")[0],
            )
            self._update_cache(obj, instr)
        elif instr_params.get("func").endswith(".get"):
            obj = rgetattr(
                self.device_manager.devices[device_root].obj,
                instr_params.get("func").split(".get")[0],
            )
            self._update_cache(obj, instr)

        if isinstance(res, ophyd.StatusBase):
            res.__dict__["instruction"] = instr
            res.__dict__["obj"] = obj
            self.requests_handler.add_status_object(instr.metadata["device_instr_id"], res)
            res = {
                "type": "status",
                "RID": instr.metadata.get("RID"),
                "success": res.success,
                "timeout": res.timeout,
                "done": res.done,
                "settle_time": res.settle_time,
            }
        elif isinstance(res, tuple) and hasattr(res, "_asdict") and hasattr(res, "_fields"):
            # convert namedtuple to dict
            res = {
                "type": "namedtuple",
                "RID": instr.metadata.get("RID"),
                "fields": res._fields,
                "values": res._asdict(),
            }
        elif isinstance(res, list) and res and isinstance(res[0], ophyd.Staged):
            res = [str(stage) for stage in res]
        return res

    def process_rpc_instruction(self, instr: messages.DeviceInstructionMessage) -> Any:
        """
        Process RPC instruction and return result.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            Any: Result of RPC instruction
        """

        instr_params = instr.content.get("parameter")

        if instr_params.get("func") == "read" or instr_params.get("func").endswith(".read"):
            # handle ophyd read. This is a special case because we also want to update the
            # buffered value in redis
            return self._handle_rpc_read(instr)

        if instr_params.get("func") == "read_configuration" or instr_params.get("func").endswith(
            ".read_configuration"
        ):
            return self._rpc_read_configuration_and_return(instr)

        if instr_params.get("kwargs", {}).get("_set_property"):
            return self._handle_rpc_property_set(instr)

        return self._handle_misc_rpc(instr)

    def _update_cache(self, obj, instr):
        if obj.kind == ophyd.Kind.config:
            return self._rpc_read_configuration_and_return(instr)
        if obj.kind in [ophyd.Kind.normal, ophyd.Kind.hinted]:
            return self._rpc_read_and_return(instr)

        # handle other other weird combinations of ophyd Kind
        self._rpc_read_and_return(instr)
        return self._rpc_read_configuration_and_return(instr)

    def _send_rpc_exception(self, exc: Exception, instr: messages.DeviceInstructionMessage):
        error_traceback = traceback.format_exc()
        exc_formatted = {
            "error": exc.__class__.__name__,
            "msg": exc.args,
            "traceback": error_traceback,
        }
        logger.info(f"Received exception: {exc_formatted}, {exc}")
        instr_params = instr.content.get("parameter")
        self.connector.set(
            MessageEndpoints.device_rpc(instr_params.get("rpc_id")),
            messages.DeviceRPCMessage(
                device=instr.content["device"], return_val=None, out=exc_formatted, success=False
            ),
        )
        diid = instr.metadata.get("device_instr_id")
        request = self.requests_handler.get_request(diid)
        if request and not request.get("status_objects"):
            self.requests_handler.set_finished(diid, success=False, error_message=error_traceback)
