from __future__ import annotations

import os
import subprocess
import sys
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.redis_connector import MessageObject, RedisConnector

logger = bec_logger.logger


class ServiceHandler:
    """
    Handler for service management requests received through Redis messages.

    This handler listens for ServiceRequestMessage messages and executes
    the requested actions (e.g., restarting services).

    Args:
        connector: RedisConnector instance for message communication
    """

    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self.command = f"{sys.executable} -m bec_server.bec_server_utils.launch"

    def start(self):
        """Register the service request message handler."""
        self.connector.register(
            MessageEndpoints.service_request(), cb=self.handle_service_request, parent=self
        )

    @staticmethod
    def handle_service_request(msg: MessageObject, parent: ServiceHandler) -> None:
        """
        Handle incoming service request messages.

        Args:
            msg: Message object containing the ServiceRequestMessage
            parent: Parent ServiceHandler instance
        """
        message = msg.value
        if not isinstance(message, messages.ServiceRequestMessage):
            return
        if message.action == "restart":
            parent.on_restart(service_name=message.service_name)

    def on_restart(self, service_name: str | None = None):
        """
        Restart BEC services.

        Launches a subprocess to restart either all services or a specific service.
        The subprocess runs independently and the method returns immediately.

        Args:
            service_name: Name of the specific service to restart (e.g., "scan_server",
                         "device_server"). If None, all services will be restarted.
        """
        if service_name:
            logger.info(f"Restarting service '{service_name}' through service handler")
            command = f"{self.command} restart --service {service_name}"
        else:
            logger.info("Restarting all services through service handler")
            command = f"{self.command} restart"

        # pylint: disable=subprocess-popen-preexec-fn
        subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            preexec_fn=os.setsid,
        )
