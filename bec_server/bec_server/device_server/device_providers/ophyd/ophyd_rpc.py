"""Native classic Ophyd RPC execution and wire value adaptation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import ophyd

from bec_lib import messages
from bec_lib.utils.rpc_utils import rgetattr
from bec_server.device_server.device_providers.device_errors import DisabledDeviceError
from bec_server.device_server.devices import is_serializable

if TYPE_CHECKING:
    from bec_server.device_server.device_providers.ophyd.ophyd_device_provider import (
        OphydDeviceProvider,
    )
    from bec_server.device_server.devices.ds_device import DSDevice
    from bec_server.device_server.rpc_handler import RPCHandler


class OphydRPC:
    """Execute native RPCs for one Ophyd provider."""

    def __init__(self, provider: OphydDeviceProvider):
        """
        Create the RPC dispatcher for one provider.

        Args:
            provider (OphydDeviceProvider): Provider that normalizes set values and adapts native
                statuses.
        """
        self.provider = provider

    def run(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        """
        Execute a classic Ophyd RPC.

        Args:
            device (DSDevice): Registered wrapper for the device that owns the RPC.
            instr (messages.DeviceInstructionMessage): RPC request and its metadata.
            handler (RPCHandler): BEC handler for read publication, completion tracking, and alarms.

        Returns:
            Any: Client-ready value returned by the native call.
        """

        instr_params = instr.parameter

        if self._instr_with_operation(instr, "read"):
            # handle ophyd read operations. This will update the cache as well
            return self._handle_rpc_read(device, instr, handler)

        if self._instr_with_operation(instr, "read_configuration"):
            # handle ophyd read_configuration operations. This will update the cache as well
            return self._rpc_read_configuration_and_return(instr, handler)

        if instr_params.get("kwargs", {}).get("_set_property"):
            self._assert_rpc_write_allowed(device)
            return self._handle_rpc_property_set(device, instr)

        if self._instr_with_operation(instr, "set"):
            self._assert_rpc_write_allowed(device)
            return self._handle_set(device, instr, handler)

        if self._instr_with_operation(instr, "put"):
            self._assert_rpc_write_allowed(device)
            return self._handle_misc_rpc(device, instr, handler)

        return self._handle_misc_rpc(device, instr, handler)

    ##################################################
    ###### Handlers for specific RPC operations ######
    ##################################################

    def _handle_rpc_read(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        """
        Handle an ophyd read operation via RPC.
        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        obj, _ = self._get_rpc_components(device, instr)

        if not hasattr(obj, "kind"):
            return self._rpc_read_and_return(instr, handler)

        if obj.kind not in [ophyd.Kind.omitted, ophyd.Kind.config]:
            return self._rpc_read_and_return(instr, handler)
        if obj.kind == ophyd.Kind.config:
            return self._rpc_read_configuration_and_return(instr, handler)
        if obj.kind == ophyd.Kind.omitted:
            return obj.read()

        return None

    def _handle_rpc_property_set(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage
    ) -> None:
        """
        Handle setting a property via RPC.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        instr_params = instr.parameter
        sub_access = instr_params.get("func", "").split(".")
        property_name = sub_access[-1]
        if len(sub_access) > 1:
            sub_access = sub_access[0:-1]
        else:
            sub_access = []
        obj = device.obj
        if sub_access:
            obj = rgetattr(obj, ".".join(sub_access))
        setattr(obj, property_name, instr_params.get("args")[0])

    def _handle_set(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        """
        Handle an ophyd set operation via RPC.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction
        """
        obj, rpc_var = self._get_rpc_components(device, instr)

        if isinstance(obj, ophyd.Signal) and getattr(rpc_var, "__name__", None) in ["set", "put"]:
            val = instr.parameter.get("args")[0]
            val = self.provider.convert_value_if_needed(obj, val)
            instr.parameter["args"][0] = val
        res = self._execute_rpc_call(rpc_var, instr)

        if not isinstance(res, ophyd.StatusBase) or res.done:
            self._update_cache(obj, instr, handler)
        else:
            # for set operations that return a Status object, we update the cache
            # when the status object is done
            def _update_cache_on_set_done(_status):
                self._update_cache(obj, instr, handler)

            res.add_callback(_update_cache_on_set_done)

        if isinstance(res, ophyd.StatusBase):
            handler.register_rpc_execution(instr, self.provider.adapt_rpc_status(obj, res))

        return self._serialize_rpc_response(instr, res)

    def _handle_misc_rpc(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        """
        Handle miscellaneous RPC operations and update the cache if necessary.

        Args:
            instr(messages.DeviceInstructionMessage): RPC instruction

        Returns:
            Any: Result of RPC instruction
        """
        instr_params = instr.content.get("parameter")

        obj, rpc_var = self._get_rpc_components(device, instr)
        res = self._execute_rpc_call(rpc_var, instr)

        # update the cache for value-updating functions
        if instr_params.get("func") in ["put", "get"] or instr_params.get("func").endswith(
            (".put", ".get")
        ):
            self._update_cache(obj, instr, handler)

        if isinstance(res, ophyd.StatusBase):
            handler.register_rpc_execution(instr, self.provider.adapt_rpc_status(obj, res))

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
        if not callable(rpc_var):
            return rpc_var
        args = tuple(instr.parameter.get("args", ()))
        kwargs = instr.parameter.get("kwargs", {})
        return rpc_var(*args, **kwargs)

    def _assert_rpc_write_allowed(self, device: DSDevice) -> None:
        """
        Reject write-style RPCs for read-only devices.
        """
        if device.read_only:
            raise DisabledDeviceError(f"Setting the device {device.name} is currently disabled.")

    def _get_rpc_components(
        self, device: DSDevice, instr: messages.DeviceInstructionMessage
    ) -> tuple[ophyd.OphydObject, Any]:
        """Resolve the callable and the object whose kind and status drive publication."""
        func = instr.parameter.get("func")
        obj = device.obj
        rpc_method = rgetattr(obj, func)
        if func.endswith((".set", ".put", ".read")):
            obj = rgetattr(obj, func.rsplit(".", 1)[0])
        return obj, rpc_method

    def _rpc_read_and_return(
        self, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        return handler.read_rpc(instr)

    def _rpc_read_configuration_and_return(
        self, instr: messages.DeviceInstructionMessage, handler: RPCHandler
    ) -> Any:
        return handler.read_rpc(instr, configuration=True)

    def _serialize_rpc_response(self, instr: messages.DeviceInstructionMessage, res: Any) -> Any:
        """
        Convert native values to their legacy client representation.

        Args:
            instr (messages.DeviceInstructionMessage): RPC request used for response metadata and
                stage handling.
            res (Any): Value returned by the native operation.

        Returns:
            Any: Result with native statuses, stage values, or namedtuples adapted for the wire.
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

        if (
            isinstance(res, list)
            and instr.parameter.get("func") in ("stage", "unstage")
            and not is_serializable(res)
        ):
            # pylint: disable=protected-access
            res = [obj._staged for obj in res]

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
        self, obj: ophyd.OphydObject, instr: messages.DeviceInstructionMessage, handler: RPCHandler
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
            return self._rpc_read_configuration_and_return(instr, handler)
        if obj.kind in [ophyd.Kind.normal, ophyd.Kind.hinted]:
            return self._rpc_read_and_return(instr, handler)

        # handle other other weird combinations of ophyd Kind
        self._rpc_read_and_return(instr, handler)
        return self._rpc_read_configuration_and_return(instr, handler)
