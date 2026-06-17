"""
This module provides a connector to a redis server. It is a wrapper around the
redis library providing a simple interface to send and receive messages from a
redis server.
"""

from __future__ import annotations

import traceback

from redis.client import Redis

from bec_lib.logger import bec_logger

from .buffered_redis_connector import BufferedRedisConnector

logger = bec_logger.logger


class RedisConnector:
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

    @property
    def _redis_conn(self):
        logger.warning(
            f"Deprecated use of _redis_conn at:\n{' '.join(traceback.format_stack(limit=3))}\n Please migrate tests to access the buffered connector directly, and implement high level methods for use outside tests."
        )
        return self._buffered_connection._redis_conn

    def _convert_endpointinfo(self, endpoint, check_message_op=True):
        logger.warning(
            f"Deprecated use of _redis_conn at:\n{' '.join(traceback.format_stack(limit=3))}\n Please migrate tests to access the buffered connector directly, and implement high level methods for use outside tests."
        )
        return self._buffered_connection._convert_endpointinfo(endpoint, check_message_op)

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

    @property
    def _topics_cb(self):
        return self._buffered_connection._topics_cb

    def authenticate(self, *, username: str = "default", password: str | None = "null"):
        return self._buffered_connection.authenticate(username=username, password=password)

    def set_retry_enabled(self, enabled: bool):
        return self._buffered_connection.set_retry_enabled(enabled)

    def shutdown(self, per_thread_timeout_s: float | None = None):
        return self._buffered_connection.shutdown(per_thread_timeout_s)

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
        return self._buffered_connection.send_client_info(
            message=message,
            show_asap=show_asap,
            source=source,
            severity=severity,
            expire=expire,
            scope=scope,
            rid=rid,
            metadata=metadata,
        )

    def raise_alarm(self, severity, info, metadata: dict | None = None):
        return self._buffered_connection.raise_alarm(severity, info, metadata)

    def notify(self, event, message, pipe=None):
        return self._buffered_connection.notify(event, message, pipe)

    def pipeline(self):
        return self._buffered_connection.pipeline()

    def execute_pipeline(self, pipeline):
        return self._buffered_connection.execute_pipeline(pipeline)

    def raw_send(self, topic: str, msg, pipe=None):
        return self._buffered_connection.raw_send(topic, msg, pipe)

    def send(self, topic, msg, pipe=None):
        return self._buffered_connection.send(topic, msg, pipe)

    def any_stream_is_registered(self, topics, cb):
        return self._buffered_connection.any_stream_is_registered(topics, cb)

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

    def _unregister_stream(self, topics, cb=None):
        return self._buffered_connection._unregister_stream(topics, cb)

    def poll_messages(self, timeout=None):
        return self._buffered_connection.poll_messages(timeout)

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

    def set_and_publish(self, topic, msg, pipe=None, expire=None):
        return self._buffered_connection.set_and_publish(topic, msg, pipe, expire)

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

    def get_last(self, topic, key=None, count=1):
        return self._buffered_connection.get_last(topic, key, count)

    def xread(self, topic, id=None, count=None, block=None, from_start=False, user_id=None):
        return self._buffered_connection.xread(
            topic, id=id, count=count, block=block, from_start=from_start, user_id=user_id
        )

    def xrange(self, topic, min, max, count=None):
        return self._buffered_connection.xrange(topic, min, max, count)

    def client_id(self):
        return self._buffered_connection.client_id()

    def unblock_client(self, id):
        return self._buffered_connection.unblock_client(id)

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

    def publish_metrics(self, group_name, metrics, separator="_"):
        return self._buffered_connection.publish_metrics(group_name, metrics, separator)

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
