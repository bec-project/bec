from bec_lib.client import BECClient
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig

from .dap_service_manager import DAPServiceManager

logger = bec_logger.logger
bec_logger.level = bec_logger.LOGLEVEL.INFO


class DAPServer(BECClient):
    """Data processing server class."""

    def __init__(
        self,
        config: ServiceConfig,
        connector_cls: type[RedisConnector],
        provided_services: list,
        forced=True,
    ) -> None:
        super().__init__(config=config, connector_cls=connector_cls, forced=forced)
        self.config = config
        self.connector_cls = connector_cls
        self._provided_services = (
            provided_services if isinstance(provided_services, list) else [provided_services]
        )
        self._dap_service_manager = DAPServiceManager(self._provided_services)

    @property
    def _service_id(self):
        return f"{'_'.join([service.__name__ for service in self._provided_services])}"

    def start(self):
        if not self._provided_services:
            raise ValueError("No services provided")
        super().start()
        self._start_dap_service()

    def _start_dap_service(self):
        self._dap_service_manager.start(self)

    def shutdown(self):
        self._dap_service_manager.shutdown()
        super().shutdown()
