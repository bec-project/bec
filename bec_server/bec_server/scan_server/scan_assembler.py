from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.logger import bec_logger
from bec_lib.scan_input_validator import ScanInputValidator
from bec_lib.signature_serializer import serialize_dtype, signature_to_dict

from .scans.legacy_scans import RequestBase, ScanArgType, ScanBase, unpack_scan_args
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

    def assemble_direct_scan(self, msg: messages.ScanQueueMessage, scan_id: str) -> ScanBaseV4:
        """Assemble the device instructions for a given ScanQueueMessage.
        This will be achieved by calling the specified class (must be a derived class of ScanBaseV4)

        Args:
            msg (messages.ScanQueueMessage): scan queue message for which the instruction should be assembled
            scan_id (str): scan id of the scan

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

        request_inputs = self._assemble_request_inputs(scan_cls, args, kwargs)
        resolved_args, resolved_kwargs = self.input_validator.validate(
            scan, scan_info, args, kwargs
        )

        scan_instance = scan_cls(
            *resolved_args,
            device_manager=self.device_manager,
            redis_connector=self.connector,
            metadata=msg.metadata,
            instruction_handler=self.parent.queue_manager.instruction_handler,
            scan_id=scan_id,
            request_inputs=request_inputs,
            **resolved_kwargs,
        )
        return scan_instance

    def _assemble_request_inputs(self, scan_cls, args, kwargs) -> dict:

        cls_input_args = [
            name
            for name, val in inspect.signature(scan_cls).parameters.items()
            if val.default == inspect.Parameter.empty and name != "kwargs"
        ]
        request_inputs = {}
        if scan_cls.arg_bundle_size["bundle"] > 0:
            request_inputs["arg_bundle"] = args
            request_inputs["inputs"] = {}
            request_inputs["kwargs"] = kwargs
        else:
            request_inputs["arg_bundle"] = []
            request_inputs["inputs"] = {}
            request_inputs["kwargs"] = {}

            if "args" in cls_input_args:
                split_index = cls_input_args.index("args")
                defined_cls_args = cls_input_args[:split_index]
                defined_args = args[:split_index]

                for ii, key in enumerate(defined_args):
                    input_name = defined_cls_args[ii]
                    request_inputs["inputs"][input_name] = key

                request_inputs["inputs"]["args"] = args[split_index:]
            else:
                for ii, key in enumerate(args):
                    request_inputs["inputs"][cls_input_args[ii]] = key

            for key in kwargs:
                if key in cls_input_args:
                    request_inputs["inputs"][key] = kwargs[key]

            for key, val in kwargs.items():
                if key not in cls_input_args:
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
            "signature": signature_to_dict(scan_cls.__init__),
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
