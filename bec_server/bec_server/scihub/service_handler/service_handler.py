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
    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self.command = f"{sys.executable} -m bec_server.bec_server_utils.launch"

    def start(self):
        self.connector.register(
            MessageEndpoints.service_request(), cb=self.handle_service_request, parent=self
        )

    @staticmethod
    def handle_service_request(msg: MessageObject, parent: ServiceHandler) -> None:
        message: messages.ServiceRequestMessage = msg.value
        if message.action == "restart":
            parent.on_restart()

    def on_restart(self):
        logger.info("Restarting services through service handler")
        subprocess.Popen(
            f"{self.command} restart",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            preexec_fn=os.setsid,
        )
