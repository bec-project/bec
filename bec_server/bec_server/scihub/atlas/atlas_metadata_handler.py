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
    from bec_lib.redis_connector import MessageObject
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector


class AtlasMetadataHandler:
    """
    The AtlasMetadataHandler class is responsible for handling metadata sent to Atlas.
    """

    def __init__(self, atlas_connector: AtlasConnector) -> None:
        self.atlas_connector = atlas_connector
        self._scan_status_register = None
        self._account = None
        self._deployment_info: messages.DeploymentInfoMessage | None = None
        self._start_account_subscription()
        self._start_deployment_info_subscription()
        self._start_scan_subscription()
        self._start_scan_history_subscription()
        self._start_messaging_subscription()
        self._start_feedback_subscription()

        self._update_account_if_needed()

    def _start_account_subscription(self):
        init_data = self.atlas_connector.connector.get_last(MessageEndpoints.account())
        if init_data is not None:
            self._handle_account_info(init_data, emit_update=False)
        self.atlas_connector.connector.register(
            MessageEndpoints.account(), cb=self._handle_account_info
        )

    def _start_deployment_info_subscription(self):
        if not self.atlas_connector.redis_atlas:
            return
        if not self.atlas_connector.deployment_name:
            return
        self.atlas_connector.redis_atlas.register(
            MessageEndpoints.atlas_deployment_info(self.atlas_connector.deployment_name),
            cb=self._update_deployment_info,
            from_start=True,
        )

    def _start_scan_subscription(self):
        self._scan_status_register = self.atlas_connector.connector.register(
            MessageEndpoints.scan_status(), cb=self._handle_scan_status
        )

    def _start_scan_history_subscription(self):
        self._scan_history_register = self.atlas_connector.connector.register(
            MessageEndpoints.scan_history(), cb=self._handle_scan_history
        )

    def _start_messaging_subscription(self):
        self.atlas_connector.connector.register(
            MessageEndpoints.message_service_queue(), cb=self._handle_messaging
        )

    def _start_feedback_subscription(self):
        self.atlas_connector.connector.register(
            MessageEndpoints.user_feedback(), cb=self._handle_feedback
        )

    def _update_deployment_info(
        self, msg: dict[str, messages.DeploymentInfoMessage], **_kwargs
    ) -> None:
        if not isinstance(msg, dict) or "data" not in msg:
            logger.error(f"Invalid deployment info message received: {msg}")
            return

        self.update_deployment_info(msg["data"])

    def update_deployment_info(self, info: messages.DeploymentInfoMessage) -> None:
        """
        Update the deployment info in the Atlas connector

        Args:
            info (messages.DeploymentInfoMessage): Deployment information, including session and experiment info
        """
        # We store the deployment info in the local redis instance so that other services can access it if needed
        self._deployment_info = info
        self.atlas_connector.connector.xadd(
            MessageEndpoints.deployment_info(), {"data": info}, max_size=1, approximate=False
        )
        self.update_messaging_services(info)
        self.update_local_account(info)

    def update_messaging_services(self, info: messages.DeploymentInfoMessage) -> None:
        """
        Update the messaging services in the Atlas connector
        """
        service_info = messages.AvailableMessagingServicesMessage(
            config=info.messaging_config,
            deployment_services=info.messaging_services,
            session_services=info.active_session.messaging_services if info.active_session else [],
        )
        self.atlas_connector.connector.xadd(
            MessageEndpoints.available_messaging_services(),
            {"data": service_info},
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
            self._account = account
            msg = messages.VariableMessage(value=account)
            self.atlas_connector.connector.xadd(
                MessageEndpoints.account(), {"data": msg}, max_size=1, approximate=False
            )
            logger.info(f"Updated local account to: {account}")

    def _update_account_if_needed(self):
        """
        If the account is set locally but diverged from the account in Atlas,
        we need to send the account info to Atlas.
        """
        if self._account is None:
            return
        info = self._deployment_info
        if info is None:
            return
        if info.active_session is None:
            return
        if info.active_session.experiment is None:
            return
        if info.active_session.experiment.pgroup == self._account:
            return
        self.send_atlas_update({"account": self._account})

    def _handle_account_info(self, msg, emit_update=True, **_kwargs) -> None:
        """
        Called if the account info is updated from the local redis instance.
        It forwards the account info to Atlas if it differs from the current one and emit_update is True.

        Args:
            msg(dict): The message containing the account info
            parent(AtlasMetadataHandler): The instance of the AtlasMetadataHandler class
            emit_update(bool): Whether to emit an update to Atlas if the account info differs from the current one
        """
        if not isinstance(msg, dict) or "data" not in msg:
            logger.error(f"Invalid account message received: {msg}")
            return
        msg = cast(messages.VariableMessage, msg["data"])
        if msg.value == self._account:
            # Account is the same as the current one, no need to update
            return
        self._account = msg.value
        if emit_update:
            self.send_atlas_update({"account": msg})
        logger.info(f"Updated account to: {self._account}")

    def _handle_scan_status(self, msg, **_kwargs) -> None:
        msg = msg.value
        try:
            self.send_atlas_update({"scan_status": msg})
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update scan status: {content}")

    def _handle_scan_history(self, msg, **_kwargs) -> None:
        msg = msg["data"]
        try:
            self.send_atlas_update({"scan_history": msg})
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update scan history: {content}")

    def _handle_messaging(self, msg, **_kwargs) -> None:
        try:
            self.atlas_connector.ingest_message(msg)
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update messaging data: {content}")

    def send_atlas_update(self, msg: dict) -> None:
        """
        Update the scan status in Atlas
        """
        self.atlas_connector.ingest_data(msg)

    def _handle_feedback(self, msg_obj: MessageObject, **_kwargs) -> None:
        msg: messages.FeedbackMessage = msg_obj.value
        content = msg.model_dump()
        if self._deployment_info:
            if (
                self._deployment_info.active_session
                and self._deployment_info.active_session.experiment
            ):
                content["experiment_id"] = self._deployment_info.active_session.experiment.pgroup
                content["realm_id"] = self._deployment_info.active_session.experiment.realm_id
            content["deployment_id"] = self._deployment_info.deployment_id
        try:
            enriched_msg: messages.FeedbackMessage = messages.FeedbackMessage(**content)
            self.atlas_connector.ingest_data({"user_feedback": enriched_msg})
        # pylint: disable=broad-except
        except Exception:
            traceback_info = traceback.format_exc()
            logger.exception(f"Failed to update feedback: {traceback_info}")

    def shutdown(self):
        """
        Shutdown the metadata handler
        """
        if self._scan_status_register:
            self._scan_status_register.shutdown()
