from __future__ import annotations

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_service import BECService
from bec_lib.connector import ConnectorBase
from bec_lib.devicemanager import DeviceManagerBase as DeviceManager
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.service_config import ServiceConfig

from .scan_assembler import ScanAssembler
from .scan_guard import ScanGuard
from .scan_manager import ScanManager
from .scan_queue import QueueManager

logger = bec_logger.logger


class ScanServer(BECService):
    device_manager = None
    queue_manager = None
    scan_guard = None
    scan_server = None
    scan_assembler = None
    scan_manager = None

    def __init__(self, config: ServiceConfig, connector_cls: ConnectorBase):
        super().__init__(config, connector_cls, unique_service=True)
        self._start_scan_manager()
        self._start_queue_manager()
        self._start_device_manager()
        self._start_scan_guard()
        self._start_scan_assembler()
        self._start_alarm_handler()
        self._reset_scan_number()
        self.status = messages.BECStatus.RUNNING

    def _start_device_manager(self):
        self.wait_for_service("DeviceServer")
        self.device_manager = DeviceManager(self)
        self.device_manager.initialize([self.bootstrap_server])

    def _start_scan_manager(self):
        self.scan_manager = ScanManager(parent=self)

    def _start_queue_manager(self):
        self.queue_manager = QueueManager(parent=self)

    def _start_scan_assembler(self):
        self.scan_assembler = ScanAssembler(parent=self)

    def _start_scan_guard(self):
        self.scan_guard = ScanGuard(parent=self)

    def _start_alarm_handler(self):
        self.connector.register(MessageEndpoints.alarm(), cb=self._alarm_callback, parent=self)

    def _reset_scan_number(self):
        if self.connector.get(MessageEndpoints.scan_number()) is None:
            self.scan_number = 1
        if self.connector.get(MessageEndpoints.dataset_number()) is None:
            self.dataset_number = 1

    @staticmethod
    def _alarm_callback(msg, parent: ScanServer, **_kwargs):
        msg = msg.value
        queue = msg.metadata.get("queue", "primary")
        if Alarms(msg.content["severity"]) == Alarms.MAJOR:
            logger.info(f"Received alarm: {msg}")
            parent.queue_manager.set_abort(queue=queue)

    @property
    def scan_number(self) -> int:
        """get the current scan number"""
        msg = self.connector.get(MessageEndpoints.scan_number())
        if msg is None:
            logger.warning("Failed to retrieve scan number from redis.")
            return -1
        if not isinstance(msg, messages.VariableMessage):
            # This is a temporary fix for the scan number being stored as a string
            # in the redis database. This will be removed in the future.
            num = int(msg)
            self.scan_number = num
            return num
        return int(msg.value)

    @scan_number.setter
    def scan_number(self, val: int):
        """set the current scan number"""
        msg = messages.VariableMessage(value=val)
        self.connector.set(MessageEndpoints.scan_number(), msg)

    @property
    def dataset_number(self) -> int:
        """get the current dataset number"""
        msg = self.connector.get(MessageEndpoints.dataset_number())
        if msg is None:
            logger.warning("Failed to retrieve dataset number from redis.")
            return -1
        if not isinstance(msg, messages.VariableMessage):
            # This is a temporary fix for the dataset number being stored as a string
            # in the redis database. This will be removed in the future.
            num = int(msg)
            self.dataset_number = num
            return num
        return int(msg.value)

    @dataset_number.setter
    def dataset_number(self, val: int):
        """set the current dataset number"""
        msg = messages.VariableMessage(value=val)
        self.connector.set(MessageEndpoints.dataset_number(), msg)

    def shutdown(self) -> None:
        """shutdown the scan server"""
        self.device_manager.shutdown()
        self.queue_manager.shutdown()
