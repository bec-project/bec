"""
This module defines the interface for a connector
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_messages.messages import BECMessage

logger = bec_logger.logger


class ConsumerConnectorError(Exception):
    """
    ConsumerConnectorError is raised when there is an error with the connector
    """


class MessageObject:
    """
    MessageObject is a wrapper for a message and its topic
    """

    def __init__(self, topic: str, value: BECMessage | list[BECMessage]) -> None:
        self.topic = topic
        self._value = value

    @property
    def value(self) -> BECMessage | list[BECMessage]:
        """
        Get the message
        """
        return self._value

    def __eq__(self, ref_val: object) -> bool:
        if not isinstance(ref_val, MessageObject):
            return False
        return self._value == ref_val.value and self.topic == ref_val.topic

    def __str__(self):
        return f"MessageObject(topic={self.topic}, value={self._value})"


class StoreInterface(abc.ABC):
    """StoreBase defines the interface for storing data"""

    def __init__(self, store):
        pass

    @abc.abstractmethod
    def pipeline(self):
        """Create a pipeline for batch operations"""

    @abc.abstractmethod
    def execute_pipeline(self, pipeline):
        """Execute a pipeline"""

    @abc.abstractmethod
    def lpush(
        self,
        topic: str,
        msg: str | BECMessage,
        pipe: None = None,
        max_size: int | None = None,
        expire: int | None = None,
    ) -> None:
        """Push a message to the left of the list"""

    @abc.abstractmethod
    def lset(self, topic: str, index: int, msg: str, pipe=None) -> None:
        """Set a value in the list at the given index"""

    @abc.abstractmethod
    def rpush(self, topic: str, msg: str, pipe=None) -> int:
        """Push a message to the right of the list"""

    @abc.abstractmethod
    def lrange(self, topic: str, start: int, end: int, pipe=None):
        """Get a range of values from the list"""

    @abc.abstractmethod
    def set(self, topic: str, msg, pipe=None, expire: int | None = None) -> None:
        """Set a value"""

    @abc.abstractmethod
    def keys(self, pattern: str) -> list:
        """Get keys that match the pattern"""

    @abc.abstractmethod
    def delete(self, topic, pipe=None):
        """Delete a key"""

    @abc.abstractmethod
    def get(self, topic: str, pipe=None):
        """Get a value"""

    @abc.abstractmethod
    def xadd(self, topic: str, msg_dict: dict, max_size=None, pipe=None, expire: int | None = None):
        """Add a message to the stream"""

    @abc.abstractmethod
    def xread(
        self,
        topic: str,
        id: str | None = None,
        count: int | None = None,
        block: int | None = None,
        pipe=None,
        from_start=False,
    ) -> list:
        """Read from the stream"""

    @abc.abstractmethod
    def xrange(self, topic: str, min: str, max: str, count: int | None = None, pipe=None):
        """Read from the stream"""


class PubSubInterface(abc.ABC):
    """PubSubBase defines the interface for a pub/sub connector"""

    @abc.abstractmethod
    def raw_send(self, topic: str, msg: bytes) -> None:
        """Send a raw message without using the BECMessage class"""

    @abc.abstractmethod
    def send(self, topic: str, msg: BECMessage) -> None:
        """Send a message"""

    @abc.abstractmethod
    def register(self, topics=None, patterns=None, cb=None, start_thread=True, **kwargs):
        """Register a callback for a topic or pattern"""

    @abc.abstractmethod
    def unregister(self, topics=None, patterns=None, cb=None):
        """Unregister a callback for a topic or pattern"""

    @abc.abstractmethod
    def poll_messages(self, timeout: float | None = None) -> bool:
        """Poll for new messages, receive them and execute callbacks"""


class ConnectorBase(PubSubInterface, StoreInterface):
    """ConnectorBase defines the interface for a connector"""

    def raise_warning(self, msg):
        """Raise a warning"""
        raise NotImplementedError

    def send_client_info(self, msg):
        """send a msg to the client, will automatically be logged too."""
        raise NotImplementedError

    def set_and_publish(self, topic: str, msg, pipe=None, expire: int | None = None) -> None:
        """Set a value and publish it"""
        raise NotImplementedError

    def shutdown(self):
        """Shutdown the connector"""
