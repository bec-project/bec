"""
This module provides a high level interface for interacting with the BEC Redis instance.
"""

from __future__ import annotations

import traceback

from redis.client import Pipeline, Redis

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import (
    AlarmMessage,
    ClientInfoMessage,
    DynamicMetricMessage,
    NotificationMessage,
)
from bec_lib.messaging_hooks import MessagingEvent
from bec_lib.messaging_services import NotificationMessageObject

from .buffered_redis_connector import BufferedRedisConnector

logger = bec_logger.logger


class RedisConnector:
    """
    Redis connector class. This class is a wrapper around the redis library providing
    a simple interface to send and receive messages from a redis server.
    """

    connector_cls = BufferedRedisConnector

    def __init__(
        self,
        bootstrap: list[str] | str,
        redis_cls: type[Redis] = Redis,
        name: str = "RedisConnector",
        **kwargs,
    ):
        """
        Initialize the connector

        Args:
            bootstrap (list): list of strings in the form "host:port"
            redis_cls (redis.client, optional): redis client class. Defaults to the standard client redis.Redis. Must not be an async client.
            name (str): Name to identify this instance
            **kwargs: additional keyword arguments to pass to the redis client.
        """
        self._buffered_connection = self.connector_cls(bootstrap, redis_cls, name, **kwargs)

    ##################################
    #    SETUP AND CONFIG METHODS    #
    ##################################

    def authenticate(self, *, username: str = "default", password: str | None = "null"):
        """
        Authenticate to the redis server.
        Please note that the arguments are keyword-only. This is to avoid confusion as the
        underlying redis library accepts the password as the first argument.

        Args:
            username (str, optional): username. Defaults to "default".
            password (str, optional): password. Defaults to "null".
        """
        return self._buffered_connection.authenticate(username=username, password=password)

    def set_retry_enabled(self, enabled: bool):
        """
        Enable or disable retry on timeout

        Args:
            enabled (bool): enable or disable retry
        """
        return self._buffered_connection.set_retry_enabled(enabled)

    def shutdown(self, per_thread_timeout_s: float | None = None):
        """
        Shutdown the connector
        """
        return self._buffered_connection.shutdown(per_thread_timeout_s)

    def register(
        self,
        topics=None,
        patterns=None,
        cb=None,
        start_thread=True,
        from_start=False,
        newest_only=False,
        **kwargs,
    ):
        """
        Register a callback for a topic or a pattern

        Args:
            topics (str, list, EndpointInfo, list[EndpointInfo], optional): topic or list of topics. Defaults to None. The topic should be a valid message endpoint in BEC and can be a string or an EndpointInfo object.
            patterns (str, list, EndpointInfo, list[EndpointInfo], optional): pattern or list of patterns. Defaults to None. In contrast to topics, patterns may contain "*" wildcards. The evaluated patterns should be a valid pub/sub message endpoint in BEC
            cb (callable, optional): callback. Defaults to None.
            start_thread (bool, optional): start the dispatcher thread. Defaults to True.
            from_start (bool, optional): for streams only: return data from start on first reading. Defaults to False.
            newest_only (bool, optional): for streams only: return newest data only. Defaults to False.
            **kwargs: additional keyword arguments to be transmitted to the callback

        Examples:
            >>> def my_callback(msg, **kwargs):
            ...     print(msg)
            ...
            >>> connector.register("test", my_callback)
            >>> connector.register(topics="test", cb=my_callback)
            >>> connector.register(patterns="test:*", cb=my_callback)
            >>> connector.register(patterns="test:*", cb=my_callback, start_thread=False)
            >>> connector.register(patterns="test:*", cb=my_callback, start_thread=False, my_arg="test")
        """
        return self._buffered_connection.register(
            topics=topics,
            patterns=patterns,
            cb=cb,
            start_thread=start_thread,
            from_start=from_start,
            newest_only=newest_only,
            **kwargs,
        )

    def unregister(self, topics=None, patterns=None, cb=None):
        return self._buffered_connection.unregister(topics, patterns, cb)

    def pipeline(self):
        return self._buffered_connection.pipeline()

    def execute_pipeline(self, pipeline):
        return self._buffered_connection.execute_pipeline(pipeline)

    ############################
    #    HIGH LEVEL METHODS    #
    ############################
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
        self._buffered_connection.set_and_publish(MessageEndpoints.alarm(), alarm_msg)
        compact_message = info.compact_error_message or info.error_message or info.exception_type
        event_by_severity = {
            0: MessagingEvent.ALARM_WARNING,
            1: MessagingEvent.ALARM_MINOR,
            2: MessagingEvent.ALARM_MAJOR,
        }
        self.notify(event_by_severity[int(severity)], compact_message)

    def notify(
        self,
        event: MessagingEvent | str,
        message: str | NotificationMessageObject,
        pipe: Pipeline | None = None,
    ) -> None:
        """
        Publish a notification event for downstream routing by SciHub.

        Args:
            event(MessagingEvent | str): The type of the event that triggered the notification.
            message(str | NotificationMessageObject): The notification content to be sent.
            pipe(Pipeline, optional): Optional pipeline to enqueue the publish operation into.
        """
        if isinstance(event, MessagingEvent):
            event = event.value
        if isinstance(message, str):
            message = NotificationMessageObject().add_text(message)
        outgoing = NotificationMessage(event=event, message=message._content)
        self._buffered_connection.send(MessageEndpoints.notification(event), outgoing, pipe=pipe)

    def send_client_info(
        self,
        message: str,
        show_asap: bool = False,
        source=None,
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
        self._buffered_connection.xadd(
            MessageEndpoints.client_info(), msg_dict={"data": client_msg}, max_size=100
        )

    def publish_metrics(self, group_name, metrics, separator="_"):
        msg = DynamicMetricMessage.from_dict(metrics, separator=separator)
        ep = MessageEndpoints.dynamic_metric(group_name)
        self._buffered_connection.set_and_publish(ep, msg)

    def get_last(self, topic, key=None, count=1):
        return self._buffered_connection.get_last(topic, key, count)

    def set_and_publish(self, topic, msg, pipe=None, expire=None):
        return self._buffered_connection.set_and_publish(topic, msg, pipe, expire)

    ##############################
    #    DIRECT REDIS METHODS    #
    ##############################

    def raw_send(self, topic: str, msg, pipe=None):
        return self._buffered_connection.raw_send(topic, msg, pipe)

    def send(self, topic, msg, pipe=None):
        return self._buffered_connection.send(topic, msg, pipe)

    def lpush(self, topic, msg, pipe=None, max_size=None, expire=None):
        return self._buffered_connection.lpush(topic, msg, pipe, max_size, expire)

    def lset(self, topic, index, msg, pipe=None):
        return self._buffered_connection.lset(topic, index, msg, pipe)

    def rpush(self, topic, msg, pipe=None, max_size=None, expire=None):
        return self._buffered_connection.rpush(topic, msg, pipe, max_size, expire)

    def lrange(self, topic, start, end, pipe=None):
        return self._buffered_connection.lrange(topic, start, end, pipe)

    def llen(self, topic, pipe=None):
        return self._buffered_connection.llen(topic, pipe)

    def lrem(self, topic, count, msg, pipe=None):
        return self._buffered_connection.lrem(topic, count, msg, pipe)

    def set(self, topic, msg, pipe=None, expire=None):
        return self._buffered_connection.set(topic, msg, pipe, expire)

    def keys(self, pattern):
        return self._buffered_connection.keys(pattern)

    def delete(self, topic, pipe=None):
        return self._buffered_connection.delete(topic, pipe)

    def get(self, topic, pipe=None):
        return self._buffered_connection.get(topic, pipe)

    def mget(self, topics, pipe=None):
        return self._buffered_connection.mget(topics, pipe)

    def xadd(self, topic, msg_dict, max_size=None, pipe=None, expire=None, approximate=True):
        return self._buffered_connection.xadd(
            topic, msg_dict, max_size=max_size, pipe=pipe, expire=expire, approximate=approximate
        )

    def xread(self, topic, id=None, count=None, block=None, from_start=False, user_id=None):
        return self._buffered_connection.xread(
            topic, id=id, count=count, block=block, from_start=from_start, user_id=user_id
        )

    def xrange(self, topic, min, max, count=None):
        return self._buffered_connection.xrange(topic, min, max, count)

    def remove_from_set(self, topic, msg, pipe=None):
        return self._buffered_connection.remove_from_set(topic, msg, pipe)

    def get_set_members(self, topic, pipe=None):
        return self._buffered_connection.get_set_members(topic, pipe)

    def blocking_list_pop_to_set_add(
        self, list_endpoint, set_endpoint, side="LEFT", timeout_s=None
    ):
        return self._buffered_connection.blocking_list_pop_to_set_add(
            list_endpoint, set_endpoint, side=side, timeout_s=timeout_s
        )

    def blocking_list_pop(self, endpoint, side="LEFT", timeout_s=None):
        return self._buffered_connection.blocking_list_pop(endpoint, side=side, timeout_s=timeout_s)

    #########################
    #    UTILITY METHODS    #
    #########################
    #
    def client_id(self):
        return self._buffered_connection.client_id()

    def unblock_client(self, id):
        return self._buffered_connection.unblock_client(id)

    def poll_messages(self, timeout=None):
        return self._buffered_connection.poll_messages(timeout)

    def any_stream_is_registered(self, topics, cb):
        return self._buffered_connection.any_stream_is_registered(topics, cb)

    def can_connect(self):
        return self._buffered_connection.can_connect()

    def redis_server_is_running(self):
        return self._buffered_connection.redis_server_is_running()

    def raw_xread(self, stream_keys: dict[str, str], block: int | None = None):
        return self._buffered_connection.raw_xread(stream_keys, block)

    def ping(self):
        return self._buffered_connection.ping()

    def acl_list(self):
        return self._buffered_connection.acl_list()

    def acl_getuser(self, username: str):
        return self._buffered_connection.acl_getuser(username)

    @property
    def host(self):
        return self._buffered_connection.host

    @property
    def port(self):
        return self._buffered_connection.port

    @property
    def connection_error_str(self):
        return self._buffered_connection.connection_error_str

    @property
    def username(self):
        return self._buffered_connection.username

    ############################
    #    DEPRECATED METHODS    #
    ############################

    @property
    def _redis_conn(self):
        logger.warning(
            f"Deprecated use of _redis_conn at:\n{' '.join(traceback.format_stack(limit=3))}\n Please migrate tests to access the buffered connector directly, and implement high level methods for use outside tests."
        )
        return self._buffered_connection._redis_conn

    def _convert_endpointinfo(self, endpoint, check_message_op=True):
        logger.warning(
            f"Deprecated use of _convert_endpointinfo at:\n{' '.join(traceback.format_stack(limit=3))}\n Please migrate tests to access the buffered connector directly, and implement high level methods for use outside tests."
        )
        return self._buffered_connection._convert_endpointinfo(endpoint, check_message_op)

    @property
    def _topics_cb(self):
        logger.warning(
            f"Deprecated use of _topics_cb at:\n{' '.join(traceback.format_stack(limit=3))}\n Please migrate tests to access the buffered connector directly, and implement high level methods for use outside tests."
        )
        return self._buffered_connection._topics_cb
