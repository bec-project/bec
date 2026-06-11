from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.bec_service import BECService
from bec_lib.messaging_hooks import MessagingManager
from bec_lib.service_config import ServiceConfig
from bec_server.scihub.atlas import AtlasConnector
from bec_server.scihub.service_handler.service_handler import ServiceHandler

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


class SciHub(BECService):
    def __init__(self, config: ServiceConfig, connector_cls: type[RedisConnector]) -> None:
        super().__init__(config, connector_cls, unique_service=True)
        self.config = config
        self.atlas_connector = None
        self.service_handler = None
        self.messaging_manager = None
        self._start_atlas_connector()
        self._start_service_handler()
        self._start_messaging_manager()
        self.status = messages.BECStatus.RUNNING

    def _start_atlas_connector(self):
        self.wait_for_service("DeviceServer")
        self.atlas_connector = AtlasConnector(self, self.connector)
        self.atlas_connector.start()

    def _start_service_handler(self):
        self.service_handler = ServiceHandler(self.connector)
        self.service_handler.start()

    def _start_messaging_manager(self):
        self.messaging_manager = MessagingManager(self.connector)

    def shutdown(self):
        """
        Shutdown the SciHub service with all its components.
        """
        if self.messaging_manager:
            self.messaging_manager.shutdown()
        if self.atlas_connector:
            self.atlas_connector.shutdown()
        super().shutdown()
