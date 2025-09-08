"""
This module contains the AtlasMetadataHandler class, which is responsible for handling metadata sent to Atlas.
It subscribes to e.g. scan status messages and forwards them to Atlas. The ingestor on the Atlas side will then
process the data and store it in the database.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

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
        self._start_scan_subscription()

    def _start_scan_subscription(self):
        self._scan_status_register = self.atlas_connector.connector.register(
            MessageEndpoints.scan_status(), cb=self._handle_scan_status, parent=self
        )

    @staticmethod
    def _handle_scan_status(msg, *, parent, **_kwargs) -> None:
        msg = msg.value
        try:
            parent.update_scan_status({"scan_status": msg})
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            logger.exception(f"Failed to update scan status: {content}")

    def update_scan_status(self, msg: dict) -> None:
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
