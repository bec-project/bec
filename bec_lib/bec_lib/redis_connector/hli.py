"""
This module provides a connector to a redis server. It is a wrapper around the
redis library providing a simple interface to send and receive messages from a
redis server.
"""

from __future__ import annotations

import collections
import copy
import inspect
import itertools
import queue
import socket
import sys
import threading
import time
import traceback
from collections.abc import MutableMapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from glob import fnmatch
from typing import TYPE_CHECKING, Any, Callable, DefaultDict, Generator, Literal, cast

import louie
import redis.client
import redis.exceptions
from redis.backoff import ExponentialBackoff
from redis.client import Pipeline, Redis
from redis.retry import Retry

from bec_lib.connector import MessageObject
from bec_lib.endpoints import EndpointInfo, MessageEndpoints, MessageOp
from bec_lib.logger import bec_logger
from bec_lib.messages import (
    AlarmMessage,
    BECMessage,
    ClientInfoMessage,
    DynamicMetricDict,
    DynamicMetricMessage,
    ErrorInfo,
    NotificationMessage,
)
from bec_lib.messaging_hooks import MessagingEvent
from bec_lib.messaging_services import NotificationMessageObject
from bec_lib.redis_connector.validation import (
    check_endpoint_type,
    error_log_with_context,
    validate_endpoint,
)
from bec_lib.serialization import MsgpackSerialization

from .buffered_publisher import RateLimitedPipelinePublisher
from .buffered_redis_connection import BufferedRedisConnector
from .constants import (
    IncompatibleMessageForEndpoint,
    IncompatibleRedisOperation,
    InvalidItemForOperation,
    PubSubMessage,
    _BecMsgT,
)
from .streams import (
    DirectReadStreamSubInfo,
    StreamMessage,
    StreamResponseList,
    StreamSubInfo,
    StreamSubs,
)


class RedisConnector:
    """Manages endpoint validation and provides high-level methods for communicating with Redis"""

    RETRY_ON_TIMEOUT: int = 20

    def __init__(
        self,
        bootstrap: list[str] | str,
        redis_cls: type[Redis] = Redis,
        name: str = "RedisConnector",
        **kwargs,
    ):
        self.buffered_connection = BufferedRedisConnector(bootstrap, redis_cls, name, **kwargs)

    def authenticate(self, *, username: str = "default", password: str | None = "null"):
        """
        Authenticate to the redis server.
        Please note that the arguments are keyword-only. This is to avoid confusion as the
        underlying redis library accepts the password as the first argument.

        Args:
            username (str, optional): username. Defaults to "default".
            password (str, optional): password. Defaults to "null".
        """
        self.buffered_connection.authenticate(username, password)

    #############################
    #     KEY-VALUE METHODS     #
    #############################

    @validate_endpoint("topic")
    def send(self, topic: str, msg: str | BECMessage, pipe: Pipeline | None = None) -> None:
        """
        Send a message to a topic

        Args:
            topic (str): topic
            msg (BECMessage): message
            pipe (Pipeline, optional): redis pipe. Defaults to None.
        """
        if isinstance(msg, BECMessage):
            msg = MsgpackSerialization.dumps(msg)
        self.buffered_connection.raw_send(topic, msg, pipe)  # type: ignore # using sync client

    ##########################
    #     STREAM METHODS     #
    ##########################

    def send_client_info(
        self,
        message: str,
        show_asap: bool = False,
        source: Literal[
            "bec_ipython_client",
            "scan_server",
            "device_server",
            "scan_bundler",
            "file_writer",
            "scihub",
            "dap",
            None,
        ] = None,
        severity: int = 0,
        expire: float = 60,
        scope: str | None = None,
        rid: str | None = None,
        metadata: dict | None = None,
    ):
        """
        Send a message to the client

        Args:
            msg (str): message
            show_asap (bool, optional): show asap. Defaults to False.
            source (Literal[str], optional): Any of the services: "bec_ipython_client", "scan_server", "device_server", "scan_bundler", "file_writer", "scihub", "dap". Defaults to None.
            severity (int, optional): severity. Defaults to 0.
            expire (float, optional): expire. Defaults to 60.
            rid (str, optional): request ID. Defaults to None.
            scope (str, optional): scope. Defaults to None.
            metadata (dict, optional): metadata. Defaults to None.
        """
        client_msg = ClientInfoMessage(
            message=message,
            source=source,
            severity=severity,
            show_asap=show_asap,
            expire=expire,
            scope=scope,
            RID=rid,
            metadata=metadata or {},
        )
        self.buffered_connection.xadd(
            MessageEndpoints.client_info(), msg_dict={"data": client_msg}, max_size=100
        )

    @validate_endpoint("topic")
    def xadd(
        self,
        topic: str,
        msg_dict: dict,
        max_size=None,
        pipe: Pipeline | None = None,
        expire: int | None = None,
        approximate=True,
    ):
        """
        add to stream

        Args:
            topic (str): redis topic
            msg_dict (dict | BECMessage): message to add
            max_size (int, optional): max size of stream. Defaults to None.
            pipe (Pipeline, optional): redis pipe. Defaults to None.
            expire (int, optional): expire time. Defaults to None.
            approximate (bool, optional): Set to False to enforce exact max size trimming. If True,
                redis may trim the stream approximately. Only used if max_size is set. Defaults to True.

        Examples:
            >>> redis.xadd("test", {"test": "test"})
            >>> redis.xadd("test", {"test": "test"}, max_size=10)
        """
        self.buffered_connection.xadd(topic, msg_dict, max_size, pipe, expire, approximate)

    ##########################
    #     PUBSUB METHODS     #
    ##########################

    def raise_alarm(self, severity: Alarms, info: ErrorInfo, metadata: dict | None = None):
        """
        Raise an alarm

        Args:
            severity (Alarms): alarm severity
            info (ErrorInfo): error information
            metadata (dict, optional): additional metadata. Defaults to None.

        Examples:
            >>> connector.raise_alarm(
                severity=Alarms.WARNING,
                info=ErrorInfo(
                    id=str(uuid.uuid4()),_stream_topic_subscriptions
                    error_message="ValueError",
                    compact_error_message="test alarm",
                    exception_type="ValueError",
                    device="samx",
                )
            )
        """
        alarm_msg = AlarmMessage(severity=severity, info=info, metadata=metadata or {})
        self.set_and_publish(MessageEndpoints.alarm(), alarm_msg)
        compact_message = info.compact_error_message or info.error_message or info.exception_type
        event_by_severity = {
            0: MessagingEvent.ALARM_WARNING,
            1: MessagingEvent.ALARM_MINOR,
            2: MessagingEvent.ALARM_MAJOR,
        }
        self.notify(event_by_severity[int(severity)], compact_message)
