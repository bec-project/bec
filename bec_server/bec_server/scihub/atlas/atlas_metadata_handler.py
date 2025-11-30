"""
This module contains the AtlasMetadataHandler class, which is responsible for handling metadata sent to Atlas.
It subscribes to e.g. scan status messages and forwards them to Atlas. The ingestor on the Atlas side will then
process the data and store it in the database.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, cast

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector


class AtlasMetadataHandler:
    """
    The AtlasMetadataHandler class is responsible for handling metadata sent to Atlas.
    """

    def __init__(self, atlas_connector: AtlasConnector) -> None:
        self.atlas_connector = atlas_connector
        self._scan_status_register = None
        self._account = None
        self._start_account_subscription()
        self._start_deployment_info_subscription()
        self._start_scan_subscription()
        self._start_scan_history_subscription()
        self._start_messaging_subscription()

    def _start_account_subscription(self):
        self.atlas_connector.connector.register(
            MessageEndpoints.account(), cb=self._handle_account_info, parent=self, from_start=True
        )

    def _start_deployment_info_subscription(self):
        if not self.atlas_connector.redis_atlas:
            return
        if not self.atlas_connector.deployment_name:
            return
        self.atlas_connector.redis_atlas.register(
            MessageEndpoints.atlas_deployment_info(self.atlas_connector.deployment_name),
            cb=self._update_deployment_info,
            parent=self,
            from_start=True,
        )

    def _start_scan_subscription(self):
        self._scan_status_register = self.atlas_connector.connector.register(
            MessageEndpoints.scan_status(), cb=self._handle_scan_status, parent=self
        )

    def _start_scan_history_subscription(self):
        self._scan_history_register = self.atlas_connector.connector.register(
            MessageEndpoints.scan_history(), cb=self._handle_scan_history, parent=self
        )

    def _start_messaging_subscription(self):
        self.atlas_connector.connector.register(
            MessageEndpoints.message_service_queue(), cb=self._handle_messaging, parent=self
        )

    @staticmethod
    def _update_deployment_info(
        msg: dict[str, messages.DeploymentInfoMessage], *, parent: AtlasMetadataHandler, **_kwargs
    ) -> None:
        if not isinstance(msg, dict) or "data" not in msg:
            logger.error(f"Invalid deployment info message received: {msg}")
            return

        parent.update_deployment_info(msg["data"])

    def update_deployment_info(self, info: messages.DeploymentInfoMessage) -> None:
        """
        Update the deployment info in the Atlas connector

        Args:
            info (messages.DeploymentInfoMessage): Deployment information, including session and experiment info
        """
        # We store the deployment info in the local redis instance so that other services can access it if needed
        self.atlas_connector.connector.xadd(
            MessageEndpoints.deployment_info(), {"data": info}, max_size=1, approximate=False
        )
        self.update_messaging_services(info.messaging_services)
        self.update_local_account(info)

    def update_messaging_services(self, services: list[messages.MessagingServiceConfig]) -> None:
        """
        Update the messaging services in the Atlas connector
        """
        info = messages.AvailableResourceMessage(resource=services)
        self.atlas_connector.connector.xadd(
            MessageEndpoints.available_messaging_services(),
            {"data": info},
            max_size=1,
            approximate=False,
        )

    def update_local_account(self, info: messages.DeploymentInfoMessage) -> None:
        """
        Update the local account if it differs from the current one.
        Args:
            info (messages.DeploymentInfoMessage): Deployment information, including session and experiment info
        """
        session = info.active_session
        if session is None:
            return
        experiment = session.experiment
        if experiment is None:
            return
        account = experiment.pgroup
        if self._account != account:
            msg = messages.VariableMessage(value=account)
            self.atlas_connector.connector.xadd(
                MessageEndpoints.account(), {"data": msg}, max_size=1, approximate=False
            )
            logger.info(f"Updated local account to: {account}")
            self._account = account

    @staticmethod
    def _handle_account_info(msg, *, parent: AtlasMetadataHandler, **_kwargs) -> None:
        """
        Called if the account info is updated from the local redis instance.
        It forwards the account info to Atlas.
        """
        if not isinstance(msg, dict) or "data" not in msg:
            logger.error(f"Invalid account message received: {msg}")
            return
        msg = cast(messages.VariableMessage, msg["data"])
        parent._account = msg.value
        parent.send_atlas_update({"account": msg})
        logger.info(f"Updated account to: {parent._account}")

    @staticmethod
    def _handle_scan_status(msg, *, parent: AtlasMetadataHandler, **_kwargs) -> None:
        msg = msg.value
        try:
            parent.send_atlas_update({"scan_status": msg})
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update scan status: {content}")

    @staticmethod
    def _handle_scan_history(msg, *, parent: AtlasMetadataHandler, **_kwargs) -> None:
        msg = msg["data"]
        try:
            parent.send_atlas_update({"scan_history": msg})
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update scan history: {content}")

    @staticmethod
    def _handle_messaging(msg, *, parent: AtlasMetadataHandler, **_kwargs) -> None:
        try:
            parent.atlas_connector.ingest_message(msg)
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update messaging data: {content}")

    def send_atlas_update(self, msg: dict) -> None:
        """
        Update the scan status in Atlas
        """
        self.atlas_connector.ingest_data(msg)

    def shutdown(self):
        """
        Shutdown the metadata handler
        """
        if self._scan_status_register:
            self._scan_status_register.shutdown()
