from typing import Any

from bec_lib.messages import BECStatus
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.tests.utils import DMMock
from bec_server.scan_server.scan_server import ScanServer
from bec_server.scan_server.scans.scan_base import ScanBase

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


class NoopScan(ScanBase):
    __doc__ = None

    def prepare_scan(self) -> None:
        pass

    def open_scan(self) -> None:
        pass

    def stage(self) -> None:
        pass

    def pre_scan(self) -> None:
        pass

    def scan_core(self) -> None:
        pass

    def at_each_point(self, *args: Any, **kwargs: Any) -> None:
        pass

    def post_scan(self) -> None:
        pass

    def unstage(self) -> None:
        pass

    def close_scan(self) -> None:
        pass

    def on_exception(self, exception: Exception) -> None:
        pass


class ProcManagerMock:
    def shutdown(self) -> None:
        pass


class ScanServerMock(ScanServer):
    def __init__(self, device_manager: DMMock) -> None:
        self.device_manager = device_manager
        super().__init__(
            ServiceConfig(redis={"host": "dummy", "port": 6379}), connector_cls=ConnectorMock
        )
        self.proc_manager = ProcManagerMock()

    def _start_actor_managers(self) -> None:
        self.actor_manager = ProcManagerMock()
        self.builtin_actor_manager = ProcManagerMock()

    def _start_metrics_emitter(self) -> None:
        pass

    def _start_update_service_info(self) -> None:
        pass

    def _start_device_manager(self) -> None:
        pass

    def _start_procedure_manager(self, *args: Any, **kwargs: Any) -> None:
        pass

    def wait_for_service(self, name: str, status: BECStatus = BECStatus.RUNNING) -> None:
        pass

    @property
    def scan_number(self) -> int:
        """get the current scan number"""
        return 2

    @scan_number.setter
    def scan_number(self, val: int) -> None:
        pass

    @property
    def dataset_number(self) -> int:
        """get the current dataset number"""
        return 3

    @dataset_number.setter
    def dataset_number(self, val: int) -> None:
        pass
