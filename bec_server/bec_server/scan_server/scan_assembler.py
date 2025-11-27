from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.logger import bec_logger

from .scans import RequestBase, ScanBase, unpack_scan_args

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
        cls_name = self.scan_manager.available_scans[scan]["class"]
        scan_cls = self.scan_manager.scan_dict[cls_name]
        return issubclass(scan_cls, ScanBase)

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
        cls_name = self.scan_manager.available_scans[scan]["class"]
        scan_cls = self.scan_manager.scan_dict[cls_name]

        logger.info(f"Preparing instructions of request of type {scan} / {scan_cls.__name__}")
        args = unpack_scan_args(msg.content.get("parameter", {}).get("args", []))
        kwargs = msg.content.get("parameter", {}).get("kwargs", {})

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
