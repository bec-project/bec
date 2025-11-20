"""
Supervisor service for managing BEC service restart requests.

This service listens for ServiceRequestMessage messages and executes
the requested actions (e.g., restarting services) by invoking the
appropriate service handler commands.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.bec_service import BECService
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.service_config import ServiceConfig

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.redis_connector import MessageObject, RedisConnector

logger = bec_logger.logger


class SupervisorService(BECService):
    """
    Supervisor service for handling service management requests.

    This service listens for ServiceRequestMessage messages and executes
    the requested actions (e.g., restarting services) by invoking the
    launch utility.

    Args:
        config: ServiceConfig instance for service configuration
        connector_cls: RedisConnector class for message communication
    """

    def __init__(self, config: ServiceConfig, connector_cls: type[RedisConnector]) -> None:
        super().__init__(config, connector_cls, unique_service=True)
        self.config = config
        self.command = f"{sys.executable} -m bec_server.bec_server_utils.launch"
        self._register_handlers()
        self.status = messages.BECStatus.RUNNING

    def _register_handlers(self):
        """Register the service request message handler."""
        self.connector.register(
            MessageEndpoints.service_request(), cb=self._handle_service_request, parent=self
        )

    @staticmethod
    def _handle_service_request(msg: MessageObject, parent: SupervisorService) -> None:
        """
        Handle incoming service request messages.

        Args:
            msg: Message object containing the ServiceRequestMessage
            parent: Parent SupervisorService instance
        """
        message = msg.value
        if not isinstance(message, messages.ServiceRequestMessage):
            return
        if message.action == "restart":
            parent._on_restart(service_name=message.service_name)

    def _on_restart(self, service_name: str | None = None):
        """
        Restart BEC services.

        Launches a subprocess to restart either all services or a specific service.
        The subprocess runs independently and the method returns immediately.
        When restarting all services, the supervisor service itself is skipped.

        Args:
            service_name: Name of the specific service to restart (e.g., "scan_server",
                         "device_server"). If None, all services will be restarted.
        """
        if service_name:
            logger.info(f"Restarting service '{service_name}' through supervisor")
            # Note: We use --interface tmux to ensure that we skip systemctl and subprocess interfaces,
            # which does not support restarting individual services.
            command = f"{self.command} restart --service {service_name} --interface tmux"
        else:
            logger.info("Restarting all services through supervisor")
            # Note: Here, we do not need to specify the interface. If we are in tmux,
            # we skip the supervisor service. For systemctl, the supervisor service does
            # not need to be kept alive during restart as systemctl is sufficiently isolated.
            command = f"{self.command} restart --skip-service supervisor"

        # pylint: disable=subprocess-popen-preexec-fn
        logger.info(f"Executing command: {command}")
        subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            preexec_fn=os.setsid,
        )

    def shutdown(self):
        """Shutdown the supervisor service."""
        self.connector.unregister(
            MessageEndpoints.service_request(), cb=self._handle_service_request
        )
        super().shutdown()
