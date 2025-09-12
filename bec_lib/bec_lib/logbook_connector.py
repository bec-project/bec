"""
This module provides a connector to the electronic logbook SciLog.
"""

from __future__ import annotations

import sys
import warnings
from typing import TYPE_CHECKING

from requests.exceptions import HTTPError

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

logger = bec_logger.logger

try:
    import scilog
except ImportError:
    logger.info("Unable to import `scilog` optional dependency")

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.redis_connector import RedisConnector


class LogbookConnector:
    def __init__(self, connector: RedisConnector) -> None:
        self.connector = connector
        self.connected = False
        self._scilog_module = None
        self._connect()
        self.logbook = None

    def _connect(self):
        if "scilog" not in sys.modules:
            return

        msg = self.connector.get(MessageEndpoints.logbook())
        if not msg:
            return
        scilog_creds = msg.credentials

        account_msg = self.connector.get_last(MessageEndpoints.account(), "data")
        if not account_msg:
            return
        account = account_msg.value
        account = account.replace("e", "p")

        self._scilog_module = scilog

        self.log = self._scilog_module.SciLog(
            scilog_creds["url"], options={"token": scilog_creds["token"]}
        )
        # FIXME the python sdk should not use the ownergroup
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                logbooks = self.log.get_logbooks(readACL={"inq": [account]})
            except HTTPError:
                self.connector.set(MessageEndpoints.logbook(), b"")
                return
        if len(logbooks) > 1:
            logger.warning("Found two logbooks. Taking the first one.")
        self.log.select_logbook(logbooks[0])

        # set aliases
        # pylint: disable=no-member, invalid-name
        self.LogbookMessage = self._scilog_module.LogbookMessage
        self.send_logbook_message = self.log.send_logbook_message
        self.send_message = self.log.send_message

        self.connected = True


# if __name__ == "__main__":
#     import datetime

#     logbook = LogbookConnector()
#     msg = LogbookMessage(logbook)
#     msg.add_text(
#         f"<p><mark class='pen-red'><strong>Beamline checks failed at {str(datetime.datetime.now())}.</strong></mark></p>"
#     ).add_tag("BEC")
#     logbook.send_logbook_message(msg)
