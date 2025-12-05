from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.bec_service import BECService
from bec_lib.service_config import ServiceConfig
from bec_server.scihub.atlas import AtlasConnector
from bec_server.scihub.scilog import SciLogConnector

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


class SciHub(BECService):
    def __init__(self, config: ServiceConfig, connector_cls: type[RedisConnector]) -> None:
        super().__init__(config, connector_cls, unique_service=True)
        self.config = config
        self.atlas_connector = None
        self.scilog_connector = None
        self.service_handler = None
        self._start_atlas_connector()
        self._start_scilog_connector()
        self.status = messages.BECStatus.RUNNING

    def _start_atlas_connector(self):
        self.wait_for_service("DeviceServer")
        self.atlas_connector = AtlasConnector(self, self.connector)
        self.atlas_connector.start()

    def _start_scilog_connector(self):
        self.scilog_connector = SciLogConnector(self, self.connector)

    def shutdown(self):
        super().shutdown()
        if self.atlas_connector:
            self.atlas_connector.shutdown()
        if self.scilog_connector:
            self.scilog_connector.shutdown()
