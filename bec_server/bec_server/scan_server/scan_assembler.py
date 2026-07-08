from __future__ import annotations

import inspect
from functools import partial
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.logger import bec_logger
from bec_lib.scan_input_validator import ScanInputValidator
from bec_lib.signature_serializer import serialize_dtype

from .scans.legacy_scans import RequestBase, ScanArgType, ScanBase, unpack_scan_args
from .scans.scan_argument_modifier import (
    apply_scan_argument_defaults,
    get_scan_modifier,
    scan_signature_with_modifiers,
)
from .scans.scan_base import ScanBase as ScanBaseV4

logger = bec_logger.logger

if TYPE_CHECKING:
    from .scan_server import ScanServer


class ScanAssembler:
    """
    ScanAssembler receives scan messages and translates the scan message into device instructions.
    """

    def __init__(self, *, parent: ScanServer):
        self.parent = parent
        self.device_manager = self.parent.device_manager
        self.connector = self.parent.connector
        self.scan_manager = self.parent.scan_manager
        self.input_validator = ScanInputValidator(device_manager=self.device_manager)

    def is_scan_message(self, msg: messages.ScanQueueMessage) -> bool:
        """Check if the scan queue message would construct a new scan.

        Args:
            msg (messages.ScanQueueMessage): message to be checked

        Returns:
            bool: True if the message is a scan message, False otherwise
        """
        scan = msg.content.get("scan_type")
        scan_cls = self.scan_manager.scan_dict[scan]
        return issubclass(scan_cls, ScanBase)

    def is_direct_scan_message(self, msg: messages.ScanQueueMessage) -> bool:
        """Check if the scan queue message would construct a new direct scan.

        Args:
            msg (messages.ScanQueueMessage): message to be checked
        Returns:
            bool: True if the message is a direct scan message, False otherwise
        """
        scan = msg.content.get("scan_type")
        scan_cls = self.scan_manager.scan_dict[scan]
        return issubclass(scan_cls, ScanBaseV4)

    def assemble_device_instructions(
        self, msg: messages.ScanQueueMessage, scan_id: str
    ) -> RequestBase:
        """Assemble the device instructions for a given ScanQueueMessage.
        This will be achieved by calling the specified class (must be a derived class of RequestBase)

        Args:
            msg (messages.ScanQueueMessage): scan queue message for which the instruction should be assembled
            scan_id (str): scan id of the scan

        Raises:
            ScanAbortion: Raised if the scan initialization fails.

        Returns:
            RequestBase: Scan instance of the initialized scan class
        """
        scan = msg.content.get("scan_type")
        scan_cls = self.scan_manager.scan_dict[scan]

        logger.info(f"Preparing instructions of request of type {scan} / {scan_cls.__name__}")
        args = unpack_scan_args(msg.content.get("parameter", {}).get("args", []))
        kwargs = msg.content.get("parameter", {}).get("kwargs", {})

        request_inputs = self._assemble_request_inputs(scan_cls, args, kwargs)

        scan_instance = scan_cls(
            *args,
            device_manager=self.device_manager,
            parameter=msg.content.get("parameter"),
            metadata=msg.metadata,
            instruction_handler=self.parent.queue_manager.instruction_handler,
            scan_id=scan_id,
            request_inputs=request_inputs,
            **kwargs,
        )
        return scan_instance

    def assemble_direct_scan(
        self, msg: messages.ScanQueueMessage, scan_id: str | None
    ) -> ScanBaseV4:
        """Assemble the device instructions for a given ScanQueueMessage.
        This will be achieved by calling the specified class (must be a derived class of ScanBaseV4)

        Args:
            msg (messages.ScanQueueMessage): scan queue message for which the instruction should be assembled
            scan_id (str | None): scan id of the scan

        Raises:
            ScanAbortion: Raised if the scan initialization fails.

        Returns:
            ScanBaseV4: Scan instance of the initialized scan class
        """
        scan = msg.content.get("scan_type")
        scan_cls = self.scan_manager.scan_dict[scan]

        logger.info(f"Preparing instructions of direct scan of type {scan} / {scan_cls.__name__}")
        args = unpack_scan_args(msg.content.get("parameter", {}).get("args", []))
        kwargs = msg.content.get("parameter", {}).get("kwargs", {})
        scan_info = self._get_scan_info(scan, scan_cls)

        resolved_args, resolved_kwargs = self.input_validator.validate(
            scan, scan_info, args, kwargs
        )
        request_inputs = self._assemble_request_inputs(scan_cls, args, kwargs)
        resolved_kwargs = apply_scan_argument_defaults(
            scan_cls, scan_info["signature"], resolved_args, resolved_kwargs
        )

        with self.device_manager._rpc_method(partial(self._raise_on_rpc_call, scan_cls)):
            scan_instance = scan_cls(
                *resolved_args,
                device_manager=self.device_manager,
                redis_connector=self.connector,
                scan_modifier=get_scan_modifier(),
                metadata=msg.metadata,
                instruction_handler=self.parent.queue_manager.instruction_handler,
                scan_id=scan_id,
                request_inputs=request_inputs,
                **resolved_kwargs,
            )
            return scan_instance

    def _raise_on_rpc_call(self, scan_cls, device: str, func_call: str, *args, **kwargs):
        # This function is used to raise an error if a runtime RPC call is made during scan initialization as it
        # can lead to unpredictable behavior
        raise RuntimeError(
            f"The scan {scan_cls.__name__} attempted to make a runtime RPC call to `{func_call}` on device `{device}`. "
            f"Please ensure that all runtime RPC calls are made within the scan's scan hooks and not during the scan's initialization."
            f"If you want to set up a device before the scan starts, move the RPC call to the `prepare_scan` hook of the scan class."
        )

    def _assemble_request_inputs(self, scan_cls, args, kwargs) -> dict:
        request_inputs = {}
        if scan_cls.arg_bundle_size["bundle"] > 0:
            request_inputs["arg_bundle"] = args
            request_inputs["inputs"] = {}
            request_inputs["kwargs"] = kwargs
            return request_inputs

        signature = inspect.signature(scan_cls)
        request_inputs["arg_bundle"] = []
        request_inputs["inputs"] = {}
        request_inputs["kwargs"] = {}
        bound = signature.bind_partial(*args, **kwargs)
        var_keyword_name = next(
            (
                name
                for name, parameter in signature.parameters.items()
                if parameter.kind == inspect.Parameter.VAR_KEYWORD
            ),
            None,
        )

        for name, parameter in signature.parameters.items():
            if name not in bound.arguments:
                continue

            value = bound.arguments[name]
            if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
                request_inputs["inputs"][name] = list(value)
            elif parameter.kind == inspect.Parameter.VAR_KEYWORD:
                request_inputs["kwargs"].update(value)
            elif parameter.default == inspect.Parameter.empty:
                request_inputs["inputs"][name] = value
            else:
                request_inputs["kwargs"][name] = value

        for key, val in kwargs.items():
            if key not in signature.parameters and key not in (
                bound.arguments.get(var_keyword_name) or {}
            ):
                request_inputs["kwargs"][key] = val
        return request_inputs

    def _get_scan_info(self, scan_name: str, scan_cls) -> dict:
        available_scans = getattr(self.scan_manager, "available_scans", {})
        if scan_name in available_scans:
            return available_scans[scan_name]

        return {
            "arg_input": self._serialize_arg_input(getattr(scan_cls, "arg_input", {}) or {}),
            "required_kwargs": getattr(scan_cls, "required_kwargs", []),
            "arg_bundle_size": getattr(
                scan_cls, "arg_bundle_size", {"bundle": 0, "min": None, "max": None}
            ),
            "doc": scan_cls.__doc__ or scan_cls.__init__.__doc__,
            "signature": scan_signature_with_modifiers(scan_cls),
        }

    def _serialize_arg_input(self, arg_input: dict) -> dict[str, str | dict | list]:
        converted = {}
        for key, value in arg_input.items():
            if value == ScanArgType.DEVICE:
                value = DeviceBase
            elif value == ScanArgType.FLOAT:
                value = float
            elif value == ScanArgType.INT:
                value = int
            elif value == ScanArgType.BOOL:
                value = bool
            elif value == ScanArgType.STR:
                value = str
            elif value == ScanArgType.LIST:
                value = list
            elif value == ScanArgType.DICT:
                value = dict
            elif inspect.isclass(value) and issubclass(value, DeviceBase):
                value = DeviceBase
            converted[key] = serialize_dtype(value)
        return converted
