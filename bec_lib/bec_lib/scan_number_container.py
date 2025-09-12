from __future__ import annotations

from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


class ScanNumberContainer:
    """Container for scan number and response."""

    def __init__(self, connector: RedisConnector):
        self.connector = connector

    def _current_account(self) -> str:
        """Get the current account from the redis connector."""
        account = self.connector.get_last(MessageEndpoints.account(), "data")
        if account is None:
            return ""
        return account.value if isinstance(account.value, str) else ""

    @property
    def scan_number(self) -> int:
        """get the current scan number"""
        account = self._current_account()

        msg = self.connector.get(MessageEndpoints.scan_number())
        if msg is None:
            logger.warning("Failed to retrieve scan number from redis.")
            return -1
        if isinstance(msg.value, int):
            self.scan_number = msg.value
            return int(msg.value)
        return int(msg.value.get(account, 1))

    @scan_number.setter
    def scan_number(self, val: int):
        """set the current scan number"""
        account = self._current_account()
        old_message = self.connector.get(MessageEndpoints.scan_number())
        if old_message is None:
            values = {}
        elif isinstance(old_message.value, int):
            values = {account: old_message.value}
        else:
            values = old_message.value
        values[account] = val
        msg = messages.VariableMessage(value=values)
        return self.connector.set(MessageEndpoints.scan_number(), msg)

    @property
    def dataset_number(self) -> int:
        """get the current dataset number"""
        account = self._current_account()
        msg = self.connector.get(MessageEndpoints.dataset_number())
        if msg is None:
            logger.warning("Failed to retrieve dataset number from redis.")
            return -1
        if isinstance(msg.value, int):
            self.dataset_number = msg.value
            return int(msg.value)
        return int(msg.value.get(account, 1))

    @dataset_number.setter
    def dataset_number(self, val: int):
        """set the current dataset number"""
        account = self._current_account()

        old_message = self.connector.get(MessageEndpoints.dataset_number())
        if old_message is None:
            values = {}
        elif isinstance(old_message.value, int):
            values = {account: old_message.value}
        else:
            values = old_message.value
        values[account] = val
        msg = messages.VariableMessage(value=values)
        self.connector.set(MessageEndpoints.dataset_number(), msg)
