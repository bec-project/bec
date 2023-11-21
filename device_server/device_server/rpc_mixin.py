import traceback
from contextlib import redirect_stdout
from io import StringIO
from typing import Any

import ophyd
from bec_lib import Alarms, MessageEndpoints, bec_logger, messages

from device_server.devices import is_serializable, rgetattr

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
                instr_params = instr.content.get("parameter")
                device = instr.content["device"]
                self._assert_device_is_enabled(instr)
                res = self._process_rpc_instruction(instr)
                # send result to client
                self._send_rpc_result_to_client(device, instr_params, res, result)
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
                content=f"Return value of rpc call {instr_params} is not serializable.",
                metadata={},
            )
        return res

    def _send_rpc_result_to_client(
        self,
        device: str,
        instr_params: dict,
        res: Any,
        result: StringIO,
    ):
        self.producer.set(
            MessageEndpoints.device_rpc(instr_params.get("rpc_id")),
            messages.DeviceRPCMessage(
                device=device,
                return_val=res,
                out=result.getvalue(),
                success=True,
            ).dumps(),
            expire=1800,
        )

    def _process_rpc_instruction(self, instr: messages.DeviceInstructionMessage) -> Any:
        # handle ophyd read. This is a special case because we also want to update the
        # buffered value in redis
        instr_params = instr.content.get("parameter")
        if instr_params.get("func") == "read" or instr_params.get("func").endswith(".read"):
            res = self._read_and_update_devices([instr.content["device"]], instr.metadata)
            if isinstance(res, list) and len(res) == 1:
                res = res[0]
            return res

        # handle other ophyd methods
        rpc_var = rgetattr(
            self.device_manager.devices[instr.content["device"]].obj,
            instr_params.get("func"),
        )
        res = self._get_result_from_rpc(rpc_var, instr_params)
        if isinstance(res, ophyd.StatusBase):
            res.__dict__["instruction"] = instr
            res.add_callback(self._status_callback)
            res = {
                "type": "status",
                "RID": instr.metadata.get("RID"),
                "success": res.success,
                "timeout": res.timeout,
                "done": res.done,
                "settle_time": res.settle_time,
            }
        elif isinstance(res, list) and isinstance(res[0], ophyd.Staged):
            res = [str(stage) for stage in res]
        return res

    def _send_rpc_exception(self, exc: Exception, instr: messages.DeviceInstructionMessage):
        exc_formatted = {
            "error": exc.__class__.__name__,
            "msg": exc.args,
            "traceback": traceback.format_exc(),
        }
        logger.info(f"Received exception: {exc_formatted}, {exc}")
        instr_params = instr.content.get("parameter")
        self.producer.set(
            MessageEndpoints.device_rpc(instr_params.get("rpc_id")),
            messages.DeviceRPCMessage(
                device=instr.content["device"],
                return_val=None,
                out=exc_formatted,
                success=False,
            ).dumps(),
        )