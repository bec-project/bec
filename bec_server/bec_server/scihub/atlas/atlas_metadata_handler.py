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
        self._start_scan_subscription()
        self._start_scan_history_subscription()

    def _start_account_subscription(self):
        self.atlas_connector.connector.register(
            MessageEndpoints.account(), cb=self._handle_account_info, parent=self, from_start=True
        )
        self.atlas_connector.redis_atlas.register(
            MessageEndpoints.atlas_deployment_info(self.atlas_connector.deployment_name),
            cb=self._handle_atlas_account_update,
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

    @staticmethod
    def _handle_atlas_account_update(msg, *, parent, **_kwargs) -> None:
        if not isinstance(msg, dict) or "data" not in msg:
            logger.error(f"Invalid account message received from Atlas: {msg}")
            return
        msg = cast(messages.VariableMessage, msg["data"])
        parent._account = msg.value
        parent._update_local_account(msg.value)

    def _update_local_account(self, account: str) -> None:
        """
        Update the local account if it differs from the current one.
        """
        if self._account != account:
            msg = messages.VariableMessage(value=account)
            self.atlas_connector.connector.xadd(
                MessageEndpoints.account(), {"data": msg}, max_size=1
            )
            logger.info(f"Updated local account to: {account}")

    @staticmethod
    def _handle_account_info(msg, *, parent, **_kwargs) -> None:
        if not isinstance(msg, dict) or "data" not in msg:
            logger.error(f"Invalid account message received: {msg}")
            return
        msg = cast(messages.VariableMessage, msg["data"])
        parent._account = msg.value
        parent.send_atlas_update({"account": msg})
        logger.info(f"Updated account to: {parent._account}")

    @staticmethod
    def _handle_scan_status(msg, *, parent, **_kwargs) -> None:
        msg = msg.value
        try:
            parent.send_atlas_update({"scan_status": msg})
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update scan status: {content}")

    @staticmethod
    def _handle_scan_history(msg, *, parent, **_kwargs) -> None:
        msg = msg["data"]
        try:
            parent.send_atlas_update({"scan_history": msg})
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update scan history: {content}")

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
