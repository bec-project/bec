from bec_lib.messages import BECStatus
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.tests.utils import DMMock
from bec_server.scan_server.scan_server import ScanServer
from bec_server.scan_server.scan_worker import InstructionQueueStatus

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class WorkerMock:
    def __init__(self) -> None:
        self.scan_id = None
        self.scan_motors = []
        self.current_scan_id = None
        self.current_scan_info = None
        self.status = InstructionQueueStatus.IDLE
        self.current_instruction_queue_item = None


class ProcManagerMock:
    def shutdown(self):
        pass


class ScanServerMock(ScanServer):
    def __init__(self, device_manager: DMMock) -> None:
        self.device_manager = device_manager
        super().__init__(
            ServiceConfig(redis={"host": "dummy", "port": 6379}), connector_cls=ConnectorMock
        )
        self.scan_worker = WorkerMock()
        self.proc_manager = ProcManagerMock()

    def _start_metrics_emitter(self):
        pass

    def _start_update_service_info(self):
        pass

    def _start_device_manager(self):
        pass

    def _start_procedure_manager(self, *args, **kwargs):
        pass

    def wait_for_service(self, name, status=BECStatus.RUNNING):
        pass

    @property
    def scan_number(self) -> int:
        """get the current scan number"""
        return 2

    @scan_number.setter
    def scan_number(self, val: int):
        pass

    @property
    def dataset_number(self) -> int:
        """get the current dataset number"""
        return 3

    @dataset_number.setter
    def dataset_number(self, val: int):
        pass
