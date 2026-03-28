from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.device import DeviceBase
from bec_lib.logger import bec_logger

from .scan_gui_models import GUIInput
from .scans.legacy_scans import RequestBase, ScanArgType, ScanBase, unpack_scan_args
from .scans.scans_v4 import ScanBase as ScanBaseV4

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

        request_inputs = self._assemble_request_inputs(scan_cls, args, kwargs)
        resolved_args, resolved_kwargs = self._resolve_direct_scan_inputs(scan_cls, args, kwargs)

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

    def _resolve_direct_scan_inputs(self, scan_cls, args, kwargs) -> tuple[list, dict]:
        """Resolve v4 scan device arguments from names to device objects."""
        arg_input = getattr(scan_cls, "arg_input", {}) or {}
        if not arg_input:
            return args, kwargs

        resolved_args = list(args)
        resolved_kwargs = kwargs.copy()
        arg_names = list(arg_input.keys())

        if scan_cls.arg_bundle_size["bundle"] > 0:
            bundle_size = scan_cls.arg_bundle_size["bundle"]
            for bundle_start in range(0, len(resolved_args), bundle_size):
                for offset, arg_name in enumerate(arg_names):
                    arg_index = bundle_start + offset
                    if arg_index >= len(resolved_args):
                        break
                    if self._is_device_arg(arg_input.get(arg_name)):
                        resolved_args[arg_index] = self._resolve_device(resolved_args[arg_index])
            for key, value in resolved_kwargs.items():
                if self._is_device_arg(arg_input.get(key)):
                    resolved_kwargs[key] = self._resolve_device(value)
            return resolved_args, resolved_kwargs

        for arg_index, arg_name in enumerate(arg_names):
            if arg_index >= len(resolved_args):
                break
            if self._is_device_arg(arg_input.get(arg_name)):
                resolved_args[arg_index] = self._resolve_device(resolved_args[arg_index])

        for key, value in resolved_kwargs.items():
            if self._is_device_arg(arg_input.get(key)):
                resolved_kwargs[key] = self._resolve_device(value)

        return resolved_args, resolved_kwargs

    def _is_device_arg(self, arg_type) -> bool:
        converted = GUIInput.convert_to_legacy_scan_arg_type(arg_type)
        if converted == ScanArgType.DEVICE:
            return True
        return inspect.isclass(converted) and issubclass(converted, DeviceBase)

    def _resolve_device(self, value):
        if isinstance(value, DeviceBase):
            return value
        return self.device_manager.devices[value]
