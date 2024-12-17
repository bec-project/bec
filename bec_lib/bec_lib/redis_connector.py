"""
This module provides a connector to a redis server. It is a wrapper around the
redis library providing a simple interface to send and receive messages from a
redis server.
"""

from __future__ import annotations

import collections
import inspect
import itertools
import queue
import sys
import threading
import time
import traceback
import warnings
from collections.abc import MutableMapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import wraps
from glob import fnmatch
from typing import TYPE_CHECKING, Awaitable, Generator

import louie
import redis
import redis.client
import redis.exceptions

from bec_lib.connector import ConnectorBase, MessageObject
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import AlarmMessage, BECMessage, BundleMessage, ClientInfoMessage
from bec_lib.serialization import MsgpackSerialization

if TYPE_CHECKING:
    from typing import Union

    from bec_lib.alarm_handler import Alarms


def validate_endpoint(endpoint_arg):
    def decorator(func):
        argspec = inspect.getfullargspec(func)
        argument_index = argspec.args.index(endpoint_arg)

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                endpoint = args[argument_index]
                arg = list(args)
            except IndexError:
                endpoint = kwargs[endpoint_arg]
                arg = kwargs
            if isinstance(endpoint, str):
                warnings.warn(
                    "RedisConnector methods with a string topic are deprecated and should not be used anymore. Use RedisConnector methods with an EndpointInfo instead.",
                    DeprecationWarning,
                )
                return func(*args, **kwargs)
            if isinstance(endpoint, EndpointInfo):
                if func.__name__ not in endpoint.message_op:
                    raise ValueError(
                        f"Endpoint {endpoint} is not compatible with {func.__name__} method"
                    )
                for val in list(args) + list(kwargs.values()):
                    if isinstance(val, BECMessage) and not isinstance(val, endpoint.message_type):
                        if not isinstance(val, BundleMessage):
                            raise TypeError(
                                f"Message type {type(val)} is not compatible with endpoint {endpoint}. Expected {endpoint.message_type}"
                            )
                        for msg in val.messages:
                            if not isinstance(msg, endpoint.message_type):
                                raise TypeError(
                                    f"Message type {type(msg)} is not compatible with endpoint {endpoint}. Expected {endpoint.message_type}"
                                )

                    if isinstance(val, dict):
                        for sub_val in val.values():
                            if isinstance(sub_val, BECMessage) and not isinstance(
                                sub_val, endpoint.message_type
                            ):
                                raise TypeError(
                                    f"Message type {type(sub_val)} is not compatible with endpoint {endpoint}. Expected {endpoint.message_type}"
                                )
                    if isinstance(val, list):
                        for sub_val in val:
                            if isinstance(sub_val, BECMessage) and not isinstance(
                                sub_val, endpoint.message_type
                            ):
                                raise TypeError(
                                    f"Message type {type(sub_val)} is not compatible with endpoint {endpoint}. Expected {endpoint.message_type}"
                                )
                    if isinstance(val, tuple):
                        for sub_val in val:
                            if isinstance(sub_val, BECMessage) and not isinstance(
                                sub_val, endpoint.message_type
                            ):
                                raise TypeError(
                                    f"Message type {type(sub_val)} is not compatible with endpoint {endpoint}. Expected {endpoint.message_type}"
                                )

                if isinstance(arg, list):
                    arg[argument_index] = endpoint.endpoint
                    return func(*arg, **kwargs)

                arg[endpoint_arg] = endpoint.endpoint
                return func(*args, **arg)
            else:
                raise TypeError(f"Endpoint {endpoint} is not EndpointInfo")

        return wrapper

    return decorator


@dataclass
class GeneratorExecution:
    fut: Awaitable
    g: Generator


@dataclass
class StreamSubscriptionInfo:
    id: str
    topic: str
    newest_only: bool
    from_start: bool
    cb_ref: callable
    kwargs: dict

    def __eq__(self, other):
        if not isinstance(other, StreamSubscriptionInfo):
            return False
        return (
            self.topic == other.topic
            and self.cb_ref == other.cb_ref
            and self.from_start == other.from_start
        )


@dataclass
class DirectReadingStreamSubscriptionInfo(StreamSubscriptionInfo):
    thread = None
    stop_event = None


@dataclass
class StreamMessage:
    msg: dict
    callbacks: list


class RedisConnector(ConnectorBase):
    """
    Redis connector class. This class is a wrapper around the redis library providing
    a simple interface to send and receive messages from a redis server.
    """

    def __init__(self, bootstrap: list, redis_cls=None, **kwargs):
        """
        Initialize the connector

        Args:
            bootstrap (list): list of strings in the form "host:port"
            redis_cls (redis.client, optional): redis client class. Defaults to None.
        """
        super().__init__(bootstrap)
        self.host, self.port = (
            bootstrap[0].split(":") if isinstance(bootstrap, list) else bootstrap.split(":")
        )

        if redis_cls:
            self._redis_conn = redis_cls(host=self.host, port=self.port)
        else:
            self._redis_conn = redis.Redis(host=self.host, port=self.port, **kwargs)

        # main pubsub connection
        self._pubsub_conn = self._redis_conn.pubsub()
        self._pubsub_conn.ignore_subscribe_messages = True
        # keep track of topics and callbacks
        self._topics_cb = collections.defaultdict(list)
        self._topics_cb_lock = threading.Lock()
        self._stream_topics_subscription = collections.defaultdict(list)
        self._stream_topics_subscription_lock = threading.Lock()

        self._events_listener_thread = None
        self._stream_events_listener_thread = None
        self._events_dispatcher_thread = None
        self._messages_queue = queue.Queue()
        self._stop_events_listener_thread = threading.Event()
        self._stop_stream_events_listener_thread = threading.Event()
        self.stream_keys = {}

        self._generator_executor = ThreadPoolExecutor()

    def authenticate(self, password: str, username: str = "user"):
        """
        Authenticate to the redis server

        Args:
            password (str): password
            username (str, optional): username. Defaults to "default".
        """
        self._redis_conn.connection_pool.connection_kwargs["username"] = username
        self._redis_conn.connection_pool.connection_kwargs["password"] = password

    def shutdown(self):
        """
        Shutdown the connector
        """
        super().shutdown()

        self._generator_executor.shutdown()

        if self._events_listener_thread:
            self._stop_events_listener_thread.set()
            self._events_listener_thread.join()
            self._events_listener_thread = None
        if self._stream_events_listener_thread:
            self._stop_stream_events_listener_thread.set()
            self._stream_events_listener_thread.join()
            self._stream_events_listener_thread = None
        if self._events_dispatcher_thread:
            self._messages_queue.put(StopIteration)
            self._events_dispatcher_thread.join()
            self._events_dispatcher_thread = None

        # this will take care of shutting down direct listening threads
        self._unregister_stream(self._stream_topics_subscription)

        # release all connections
        self._pubsub_conn.close()
        self._redis_conn.close()

    def send_client_info(
        self,
        message: str,
        rid: str = None,
        source: str = None,
        severity: int = 0,
        show_asap: bool = False,
        scope: str = None,
        metadata: dict = None,
    ):
        """
        Send a message to the client

        Args:
            msg (str): message
        """
        client_msg = ClientInfoMessage(
            message=message,
            source=source,
            severity=severity,
            show_asap=show_asap,
            scope=scope,
            RID=rid,
            metadata=metadata,
        )
        self.xadd(MessageEndpoints.client_info(), msg_dict={"data": client_msg}, max_size=100)

    def raise_alarm(self, severity: Alarms, alarm_type: str, source: str, msg: str, metadata: dict):
        """
        Raise an alarm

        Args:
            severity (Alarms): alarm severity
            alarm_type (str): alarm type
            source (str): source
            msg (str): message
            metadata (dict): metadata
        """
        alarm_msg = AlarmMessage(
            severity=severity, alarm_type=alarm_type, source=source, msg=msg, metadata=metadata
        )
        self.set_and_publish(MessageEndpoints.alarm(), alarm_msg)

    def pipeline(self) -> redis.client.Pipeline:
        """Create a new pipeline"""
        return self._redis_conn.pipeline()

    def execute_pipeline(self, pipeline) -> list:
        """
        Execute a pipeline and return the results

        Args:
            pipeline (Pipeline): redis pipeline

        Returns:
            list: list of results
        """
        if not isinstance(pipeline, redis.client.Pipeline):
            raise TypeError(f"Expected a redis Pipeline, got {type(pipeline)}")
        ret = []
        results = pipeline.execute()
        for res in results:
            try:
                ret.append(MsgpackSerialization.loads(res))
            except RuntimeError:
                ret.append(res)
        return ret

    def raw_send(self, topic: str, msg: bytes, pipe=None):
        """
        Send a message to a topic. This is the raw version of send, it does not
        check the message type. Use this method if you want to send a message
        that is not a BECMessage.

        Args:
            topic (str): topic
            msg (bytes): message
            pipe (Pipeline, optional): redis pipe. Defaults to None.
        """
        client = pipe if pipe is not None else self._redis_conn
        client.publish(topic, msg)

    @validate_endpoint("topic")
    def send(self, topic: EndpointInfo, msg: BECMessage, pipe=None) -> None:
        """
        Send a message to a topic

        Args:
            topic (str): topic
            msg (BECMessage): message
            pipe (Pipeline, optional): redis pipe. Defaults to None.
        """
        if not isinstance(msg, BECMessage):
            raise TypeError(f"Message {msg} is not a BECMessage")
        self.raw_send(topic, MsgpackSerialization.dumps(msg), pipe)

    def _start_events_dispatcher_thread(self, start_thread):
        if start_thread and self._events_dispatcher_thread is None:
            # start dispatcher thread
            started_event = threading.Event()
            self._events_dispatcher_thread = threading.Thread(
                target=self._dispatch_events, args=(started_event,)
            )
            self._events_dispatcher_thread.start()
            started_event.wait()  # synchronization of thread start

    def _convert_endpointinfo(self, endpoint, check_message_op=True):
        if isinstance(endpoint, EndpointInfo):
            return [endpoint.endpoint], endpoint.message_op.name
        if isinstance(endpoint, str):
            return [endpoint], ""
        # Support list of endpoints or dict with endpoints as keys
        if isinstance(endpoint, (Sequence, MutableMapping)):
            endpoints_str = []
            ref_message_op = None
            for e in endpoint:
                e_str, message_op = self._convert_endpointinfo(e, check_message_op=check_message_op)
                if check_message_op:
                    if ref_message_op is None:
                        ref_message_op = message_op
                    else:
                        if message_op != ref_message_op:
                            raise ValueError(
                                f"All endpoints do not have the same type: {ref_message_op}"
                            )
                endpoints_str.append(e_str)
            return list(itertools.chain(*endpoints_str)), ref_message_op or ""
        raise ValueError(f"Invalid endpoint {endpoint}")

    def _normalize_patterns(self, patterns):
        patterns, _ = self._convert_endpointinfo(patterns)
        if isinstance(patterns, str):
            return [patterns]
        elif isinstance(patterns, list):
            if not all(isinstance(p, str) for p in patterns):
                raise ValueError("register: patterns must be a string or a list of strings")
        else:
            raise ValueError("register: patterns must be a string or a list of strings")
        return patterns

    def register(
        self,
        topics: str | list[str] | EndpointInfo | list[EndpointInfo] = None,
        patterns: str | list[str] = None,
        cb: callable = None,
        start_thread: bool = True,
        from_start: bool = False,
        newest_only: bool = False,
        **kwargs,
    ):
        """
        Register a callback for a topic or a pattern

        Args:
            topics (str, list, EndpointInfo, list[EndpointInfo], optional): topic or list of topics. Defaults to None. The topic should be a valid message endpoint in BEC and can be a string or an EndpointInfo object.
            patterns (str, list, optional): pattern or list of patterns. Defaults to None. In contrast to topics, patterns may contain "*" wildcards. The evaluated patterns should be a valid pub/sub message endpoint in BEC
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
        if cb is None:
            raise ValueError("Callback cb cannot be None")

        if topics is None and patterns is None:
            raise ValueError("topics and patterns cannot be both None")

        # make a weakref from the callable, using louie;
        # it can create safe refs for simple functions as well as methods
        cb_ref = louie.saferef.safe_ref(cb)
        item = (cb_ref, kwargs)

        if self._events_listener_thread is None:
            # create the thread that will get all messages for this connector;
            self._events_listener_thread = threading.Thread(target=self._get_messages_loop)
            self._events_listener_thread.start()

        if patterns is not None:
            patterns = self._normalize_patterns(patterns)

            self._pubsub_conn.psubscribe(patterns)
            with self._topics_cb_lock:
                for pattern in patterns:
                    if item not in self._topics_cb[pattern]:
                        self._topics_cb[pattern].append(item)
        else:
            topics, message_op = self._convert_endpointinfo(topics)
            if message_op == "STREAM":
                return self._register_stream(
                    topics=topics,
                    cb=cb,
                    from_start=from_start,
                    newest_only=newest_only,
                    start_thread=start_thread,
                    **kwargs,
                )

            self._pubsub_conn.subscribe(topics)
            with self._topics_cb_lock:
                for topic in topics:
                    if item not in self._topics_cb[topic]:
                        self._topics_cb[topic].append(item)
        self._start_events_dispatcher_thread(start_thread)

    def _add_direct_stream_listener(self, topic, cb_ref, **kwargs) -> int:
        """
        Add a direct listener for a topic. This is used when newest_only is True.

        Args:
            topic (str): topic
            cb (callable): weakref to callback
            kwargs (dict): additional keyword arguments to be transmitted to the callback

        Returns:
            int: stream id
        """
        info = DirectReadingStreamSubscriptionInfo(
            id="-", topic=topic, newest_only=True, from_start=False, cb_ref=cb_ref, kwargs=kwargs
        )
        if info in self._stream_topics_subscription[topic]:
            raise RuntimeError("Already registered stream topic with the same callback")

        info.stop_event = threading.Event()
        info.thread = threading.Thread(target=self._direct_stream_listener, args=(info,))
        with self._stream_topics_subscription_lock:
            self._stream_topics_subscription[topic].append(info)
        info.thread.start()

    def _direct_stream_listener(self, info: DirectReadingStreamSubscriptionInfo):
        stop_event = info.stop_event
        cb_ref = info.cb_ref
        kwargs = info.kwargs
        topic = info.topic
        while not stop_event.is_set():
            ret = self._redis_conn.xrevrange(topic, "+", info.id, count=1)
            if not ret:
                time.sleep(0.1)
                continue
            redis_id, msg_dict = ret[0]
            timestamp, _, ind = redis_id.partition(b"-")
            info.id = f"{timestamp.decode()}-{int(ind.decode())+1}"
            stream_msg = StreamMessage(
                {key.decode(): MsgpackSerialization.loads(val) for key, val in msg_dict.items()},
                ((cb_ref, kwargs),),
            )
            self._messages_queue.put(stream_msg)

    def _get_stream_topics_id(self) -> dict:
        stream_topics_id = {}
        from_start_stream_topics_id = {}
        with self._stream_topics_subscription_lock:
            for topic, subscription_info_list in self._stream_topics_subscription.items():
                for info in subscription_info_list:
                    if isinstance(info, DirectReadingStreamSubscriptionInfo):
                        continue
                    if info.from_start:
                        from_start_stream_topics_id[topic] = info.id
                    else:
                        stream_topics_id[topic] = info.id
        return from_start_stream_topics_id, stream_topics_id

    def _handle_stream_msg_list(self, msg_list, from_start=False):
        for topic, msgs in msg_list:
            subscription_info_list = self._stream_topics_subscription[topic.decode()]
            for index, record in msgs:
                callbacks = []
                for info in subscription_info_list:
                    info.id = index.decode()
                    if from_start and not info.from_start:
                        continue
                    callbacks.append((info.cb_ref, info.kwargs))
                if callbacks:
                    msg_dict = {
                        k.decode(): MsgpackSerialization.loads(msg) for k, msg in record.items()
                    }
                    msg = StreamMessage(msg_dict, callbacks)
                    self._messages_queue.put(msg)
            for info in subscription_info_list:
                info.from_start = False

    def _get_stream_messages_loop(self) -> None:
        """
        Get stream messages loop. This method is run in a separate thread and listens
        for messages from the redis server.
        """
        error = False

        while not self._stop_stream_events_listener_thread.is_set():
            try:
                from_start_stream_topics_id, stream_topics_id = self._get_stream_topics_id()
                if not any((stream_topics_id, from_start_stream_topics_id)):
                    time.sleep(0.1)
                    continue
                msg_list = []
                from_start_msg_list = []
                # first handle the 'from_start' streams ;
                # in the case of reading from start what is expected is to call the
                # callbacks for existing items, without waiting for a new element to be added
                # to the stream
                if from_start_stream_topics_id:
                    # read the streams contents from beginning, not blocking
                    from_start_msg_list = self._redis_conn.xread(from_start_stream_topics_id)
                if stream_topics_id:
                    msg_list = self._redis_conn.xread(stream_topics_id, block=200)
            except redis.exceptions.ConnectionError:
                if not error:
                    error = True
                    bec_logger.logger.error("Failed to connect to redis. Is the server running?")
                time.sleep(1)
            # pylint: disable=broad-except
            except Exception:
                sys.excepthook(*sys.exc_info())
            else:
                error = False
                with self._stream_topics_subscription_lock:
                    self._handle_stream_msg_list(from_start_msg_list, from_start=True)
                    self._handle_stream_msg_list(msg_list)
        return True

    def _register_stream(
        self,
        topics: list[str] = None,
        cb: callable = None,
        from_start: bool = False,
        newest_only: bool = False,
        start_thread: bool = True,
        **kwargs,
    ) -> None:
        """
        Register a callback for a stream topic or pattern

        Args:
            topic (str, optional): Topic. This should be a valid message endpoint string.
            cb (callable, optional): callback. Defaults to None.
            from_start (bool, optional): read from start. Defaults to False.
            newest_only (bool, optional): read newest only. Defaults to False.
            start_thread (bool, optional): start the dispatcher thread. Defaults to True.
            **kwargs: additional keyword arguments to be transmitted to the callback

        """
        if newest_only and from_start:
            raise ValueError("newest_only and from_start cannot be both True")

        # make a weakref from the callable, using louie;
        # it can create safe refs for simple functions as well as methods
        cb_ref = louie.saferef.safe_ref(cb)

        self._start_events_dispatcher_thread(start_thread)

        if newest_only:
            # if newest_only is True, we need to provide a separate callback for each topic,
            # directly calling the callback. This is because we need to have a backpressure
            # mechanism in place, and we cannot rely on the dispatcher thread to handle it.
            for topic in topics:
                self._add_direct_stream_listener(topic, cb_ref, **kwargs)
        else:
            with self._stream_topics_subscription_lock:
                for topic in topics:
                    try:
                        stream_info = self._redis_conn.xinfo_stream(topic)
                    except redis.exceptions.ResponseError:
                        # no such key
                        last_id = "0-0"
                    else:
                        last_id = stream_info["last-entry"][0].decode()
                    new_subscription = StreamSubscriptionInfo(
                        id="0-0" if from_start else last_id,
                        topic=topic,
                        newest_only=newest_only,
                        from_start=from_start,
                        cb_ref=cb_ref,
                        kwargs=kwargs,
                    )
                    subscriptions = self._stream_topics_subscription[topic]
                    if new_subscription in subscriptions:
                        # raise an error if attempted to register a stream with the same callback,
                        # whereas it has already been registered as a 'direct reading' stream with
                        # newest_only=True ; it is clearly an error case that would produce weird results
                        index = subscriptions.index(new_subscription)
                        if isinstance(subscriptions[index], DirectReadingStreamSubscriptionInfo):
                            raise RuntimeError(
                                "Already registered stream topic with the same callback with 'newest_only=True'"
                            )
                    else:
                        subscriptions.append(new_subscription)

            if self._stream_events_listener_thread is None:
                # create the thread that will get all messages for this connector
                self._stream_events_listener_thread = threading.Thread(
                    target=self._get_stream_messages_loop
                )
                self._stream_events_listener_thread.start()

    def _filter_topics_cb(self, topics: list, cb: Union[callable, None]):
        unsubscribe_list = []
        with self._topics_cb_lock:
            for topic in topics:
                topics_cb = self._topics_cb[topic]
                # remove callback from list
                self._topics_cb[topic] = list(
                    filter(lambda item: cb and item[0]() is not cb, topics_cb)
                )
                if not self._topics_cb[topic]:
                    # no callbacks left, unsubscribe
                    unsubscribe_list.append(topic)
            # clean the topics that have been unsubscribed
            for topic in unsubscribe_list:
                del self._topics_cb[topic]
        return unsubscribe_list

    def unregister(self, topics=None, patterns=None, cb=None):
        if self._events_listener_thread is None:
            return

        if patterns is not None:
            patterns = self._normalize_patterns(patterns)
            # see if registered streams can be unregistered
            for pattern in patterns:
                self._unregister_stream(
                    fnmatch.filter(self._stream_topics_subscription, pattern), cb
                )
            pubsub_unsubscribe_list = self._filter_topics_cb(patterns, cb)
            if pubsub_unsubscribe_list:
                self._pubsub_conn.punsubscribe(pubsub_unsubscribe_list)
        else:
            topics, _ = self._convert_endpointinfo(topics, check_message_op=False)
            if not self._unregister_stream(topics, cb):
                unsubscribe_list = self._filter_topics_cb(topics, cb)
                if unsubscribe_list:
                    self._pubsub_conn.unsubscribe(unsubscribe_list)

    def _unregister_stream(self, topics: list[str], cb: callable = None) -> bool:
        """
        Unregister a stream listener.

        Args:
            topics (list[str]): list of stream topics

        Returns:
            bool: True if the stream listener has been removed, False otherwise
        """
        unsubscribe_list = []
        with self._stream_topics_subscription_lock:
            for topic in topics:
                subscription_infos = self._stream_topics_subscription[topic]
                # remove from list if callback corresponds
                self._stream_topics_subscription[topic] = list(
                    filter(lambda sub_info: cb and sub_info.cb_ref() is not cb, subscription_infos)
                )
                if not self._stream_topics_subscription[topic]:
                    # no callbacks left, unsubscribe
                    unsubscribe_list += subscription_infos
            # clean the topics that have been unsubscribed
            for subscription_info in unsubscribe_list:
                if isinstance(subscription_info, DirectReadingStreamSubscriptionInfo):
                    subscription_info.stop_event.set()
                    subscription_info.thread.join()
                # it is possible to register the same stream multiple times with different
                # callbacks, in this case when unregistering with cb=None (unregister all)
                # the topic can be deleted multiple times, hence try...except in code below
                try:
                    del self._stream_topics_subscription[subscription_info.topic]
                except KeyError:
                    pass

        return len(unsubscribe_list) > 0

    def _get_messages_loop(self) -> None:
        """
        Get messages loop. This method is run in a separate thread and listens
        for messages from the redis server.

        Args:
            pubsub (redis.client.PubSub): pubsub object
        """
        error = False
        while not self._stop_events_listener_thread.is_set():
            try:
                msg = self._pubsub_conn.get_message(timeout=1)
            except redis.exceptions.ConnectionError:
                if not error:
                    error = True
                    bec_logger.logger.error("Failed to connect to redis. Is the server running?")
                time.sleep(1)
            # pylint: disable=broad-except
            except Exception:
                sys.excepthook(*sys.exc_info())
            else:
                error = False
                if msg is not None:
                    self._messages_queue.put(msg)

    def _execute_callback(self, cb, msg, kwargs):
        try:
            g = cb(msg, **kwargs)
        # pylint: disable=broad-except
        except Exception:
            sys.excepthook(*sys.exc_info())
        else:
            if inspect.isgenerator(g):
                # reschedule execution to delineate the generator
                self._messages_queue.put(g)

    def _handle_message(self, msg):
        if inspect.isgenerator(msg):
            g = msg
            fut = self._generator_executor.submit(next, g)
            self._messages_queue.put(GeneratorExecution(fut, g))
        elif isinstance(msg, StreamMessage):
            for cb_ref, kwargs in msg.callbacks:
                cb = cb_ref()
                if cb:
                    self._execute_callback(cb, msg.msg, kwargs)
        elif isinstance(msg, GeneratorExecution):
            fut, g = msg.fut, msg.g
            if fut.done():
                try:
                    res = fut.result()
                except StopIteration:
                    pass
                else:
                    fut = self._generator_executor.submit(g.send, res)
                    self._messages_queue.put(GeneratorExecution(fut, g))
            else:
                self._messages_queue.put(GeneratorExecution(fut, g))
        else:
            channel = msg["channel"].decode()
            with self._topics_cb_lock:
                if msg["pattern"] is not None:
                    callbacks = self._topics_cb[msg["pattern"].decode()]
                else:
                    callbacks = self._topics_cb[channel]
            msg = MessageObject(topic=channel, value=MsgpackSerialization.loads(msg["data"]))
            for cb_ref, kwargs in callbacks:
                cb = cb_ref()
                if cb:
                    self._execute_callback(cb, msg, kwargs)

    def poll_messages(self, timeout=None) -> None:
        """Poll messages from the messages queue

        If timeout is None, wait for at least one message. Processes until queue is empty,
        or until timeout is reached.

        Args:

          timeout (float): timeout in seconds
        """
        start_time = time.perf_counter()
        remaining_timeout = timeout
        while True:
            try:
                # wait for a message and return it before timeout expires
                msg = self._messages_queue.get(timeout=remaining_timeout, block=True)
            except queue.Empty as exc:
                if remaining_timeout < timeout:
                    # at least one message has been processed, so we do not raise
                    # the timeout error
                    return True
                raise TimeoutError(f"{self}: timeout waiting for messages") from exc
            else:
                if msg is StopIteration:
                    return False

                try:
                    self._handle_message(msg)
                # pylint: disable=broad-except
                except Exception:
                    content = traceback.format_exc()
                    bec_logger.logger.error(f"Error handling message {msg}:\n{content}")

                if timeout is None:
                    if self._messages_queue.empty():
                        # no message to process
                        return True
                else:
                    # calculate how much time remains and retry getting a message
                    remaining_timeout = timeout - (time.perf_counter() - start_time)
                    if remaining_timeout <= 0:
                        return True

    def _dispatch_events(self, started_event):
        started_event.set()
        while self.poll_messages():
            ...

    @validate_endpoint("topic")
    def lpush(
        self, topic: EndpointInfo, msg: str, pipe=None, max_size: int = None, expire: int = None
    ) -> None:
        """Time complexity: O(1) for each element added, so O(N) to
        add N elements when the command is called with multiple arguments.
        Insert all the specified values at the head of the list stored at key.
        If key does not exist, it is created as empty list before
        performing the push operations. When key holds a value that
        is not a list, an error is returned."""
        client = pipe if pipe is not None else self.pipeline()
        if isinstance(msg, BECMessage):
            msg = MsgpackSerialization.dumps(msg)
        client.lpush(topic, msg)
        if max_size:
            client.ltrim(topic, 0, max_size)
        if expire:
            client.expire(topic, expire)
        if not pipe:
            client.execute()

    @validate_endpoint("topic")
    def lset(self, topic: EndpointInfo, index: int, msg: str, pipe=None) -> None:
        client = pipe if pipe is not None else self._redis_conn
        if isinstance(msg, BECMessage):
            msg = MsgpackSerialization.dumps(msg)
        return client.lset(topic, index, msg)

    @validate_endpoint("topic")
    def rpush(self, topic: EndpointInfo, msg: str, pipe=None) -> int:
        """O(1) for each element added, so O(N) to add N elements when the
        command is called with multiple arguments. Insert all the specified
        values at the tail of the list stored at key. If key does not exist,
        it is created as empty list before performing the push operation. When
        key holds a value that is not a list, an error is returned."""
        client = pipe if pipe is not None else self._redis_conn
        if isinstance(msg, BECMessage):
            msg = MsgpackSerialization.dumps(msg)
        return client.rpush(topic, msg)

    @validate_endpoint("topic")
    def lrange(self, topic: EndpointInfo, start: int, end: int, pipe=None):
        """O(S+N) where S is the distance of start offset from HEAD for small
        lists, from nearest end (HEAD or TAIL) for large lists; and N is the
        number of elements in the specified range. Returns the specified elements
        of the list stored at key. The offsets start and stop are zero-based indexes,
        with 0 being the first element of the list (the head of the list), 1 being
        the next element and so on."""
        client = pipe if pipe is not None else self._redis_conn
        cmd_result = client.lrange(topic, start, end)
        if pipe:
            return cmd_result

        # in case of command executed in a pipe, use 'execute_pipeline' method
        ret = []
        for msg in cmd_result:
            try:
                ret.append(MsgpackSerialization.loads(msg))
            except RuntimeError:
                ret.append(msg)
        return ret

    @validate_endpoint("topic")
    def set_and_publish(self, topic: EndpointInfo, msg, pipe=None, expire: int = None) -> None:
        """piped combination of self.publish and self.set"""
        client = pipe if pipe is not None else self.pipeline()
        if isinstance(msg, BECMessage):
            msg = MsgpackSerialization.dumps(msg)
        client.set(topic, msg, ex=expire)
        self.raw_send(topic, msg, pipe=client)
        if not pipe:
            client.execute()

    @validate_endpoint("topic")
    def set(self, topic: EndpointInfo, msg, pipe=None, expire: int = None) -> None:
        """set redis value"""
        client = pipe if pipe is not None else self._redis_conn
        if isinstance(msg, BECMessage):
            msg = MsgpackSerialization.dumps(msg)
        client.set(topic, msg, ex=expire)

    @validate_endpoint("pattern")
    def keys(self, pattern: EndpointInfo) -> list:
        """returns all keys matching a pattern"""
        return self._redis_conn.keys(pattern)

    @validate_endpoint("topic")
    def delete(self, topic: EndpointInfo, pipe=None):
        """delete topic"""
        client = pipe if pipe is not None else self._redis_conn
        client.delete(topic)

    @validate_endpoint("topic")
    def get(self, topic: EndpointInfo, pipe=None):
        """retrieve entry, either via hgetall or get"""
        client = pipe if pipe is not None else self._redis_conn
        data = client.get(topic)
        if pipe:
            return data
        else:
            try:
                return MsgpackSerialization.loads(data)
            except RuntimeError:
                return data

    @validate_endpoint("topic")
    def xadd(
        self, topic: EndpointInfo, msg_dict: dict, max_size=None, pipe=None, expire: int = None
    ):
        """
        add to stream

        Args:
            topic (str): redis topic
            msg_dict (dict): message to add
            max_size (int, optional): max size of stream. Defaults to None.
            pipe (Pipeline, optional): redis pipe. Defaults to None.
            expire (int, optional): expire time. Defaults to None.

        Examples:
            >>> redis.xadd("test", {"test": "test"})
            >>> redis.xadd("test", {"test": "test"}, max_size=10)
        """
        if pipe:
            client = pipe
        elif expire:
            client = self.pipeline()
        else:
            client = self._redis_conn

        msg_dict = {key: MsgpackSerialization.dumps(val) for key, val in msg_dict.items()}

        if max_size:
            client.xadd(topic, msg_dict, maxlen=max_size)
        else:
            client.xadd(topic, msg_dict)
        if expire:
            client.expire(topic, expire)
        if not pipe and expire:
            client.execute()

    @validate_endpoint("topic")
    def get_last(self, topic: EndpointInfo, key=None, count=1):
        """
        Get last message from stream. Repeated calls will return
        the same message until a new message is added to the stream.

        Args:
            topic (str): redis topic
            key (str, optional): key to retrieve. Defaults to None. If None, the whole message is returned.
            count (int, optional): number of last elements to retrieve
        """
        if count <= 0:
            return None
        ret = []
        client = self._redis_conn
        try:
            res = client.xrevrange(topic, "+", "-", count=count)
            if not res:
                return None
            for _, msg_dict in reversed(res):
                ret.append(
                    {k.decode(): MsgpackSerialization.loads(msg) for k, msg in msg_dict.items()}
                    if key is None
                    else MsgpackSerialization.loads(msg_dict[key.encode()])
                )
        except TypeError:
            return None

        if count > 1:
            return ret
        else:
            return ret[0]

    @validate_endpoint("topic")
    def xread(
        self,
        topic: EndpointInfo,
        id: str = None,
        count: int = None,
        block: int = None,
        from_start=False,
    ) -> list:
        """
        read from stream

        Args:
            topic (str): redis topic
            id (str, optional): id to read from. Defaults to None.
            count (int, optional): number of messages to read. Defaults to None, which means all.
            block (int, optional): block for x milliseconds. Defaults to None.
            from_start (bool, optional): read from start. Defaults to False.

        Returns:
            [list]: list of messages

        Examples:
            >>> redis.xread("test", "0-0")
            >>> redis.xread("test", "0-0", count=1)

            # read one message at a time
            >>> key = 0
            >>> msg = redis.xread("test", key, count=1)
            >>> key = msg[0][1][0][0]
            >>> next_msg = redis.xread("test", key, count=1)
        """
        client = self._redis_conn
        if from_start:
            self.stream_keys[topic] = "0-0"
        if topic not in self.stream_keys:
            if id is None:
                try:
                    msg = client.xrevrange(topic, "+", "-", count=1)
                    if msg:
                        self.stream_keys[topic] = msg[0][0].decode()
                        out = {}
                        for key, val in msg[0][1].items():
                            out[key.decode()] = MsgpackSerialization.loads(val)
                        return [out]
                    self.stream_keys[topic] = "0-0"
                except redis.exceptions.ResponseError:
                    self.stream_keys[topic] = "0-0"
        if id is None:
            id = self.stream_keys[topic]

        msg = client.xread({topic: id}, count=count, block=block)
        return self._decode_stream_messages_xread(msg)

    def _decode_stream_messages_xread(self, msg):
        out = []
        for topic, msgs in msg:
            for index, record in msgs:
                out.append(
                    {k.decode(): MsgpackSerialization.loads(msg) for k, msg in record.items()}
                )
                self.stream_keys[topic.decode()] = index
        return out if out else None

    @validate_endpoint("topic")
    def xrange(self, topic: EndpointInfo, min: str, max: str, count: int = None):
        """
        read a range from stream

        Args:
            topic (str): redis topic
            min (str): min id. Use "-" to read from start
            max (str): max id. Use "+" to read to end
            count (int, optional): number of messages to read. Defaults to None.

        Returns:
            [list]: list of messages or None
        """
        client = self._redis_conn
        msgs = []
        for reading in client.xrange(topic, min, max, count=count):
            _, msg_dict = reading
            msgs.append(
                {k.decode(): MsgpackSerialization.loads(msg) for k, msg in msg_dict.items()}
            )
        return msgs if msgs else None

    def producer(self):
        """Return itself as a producer, to be compatible with old code"""
        warnings.warn(
            "RedisConnector.producer() is deprecated and should not be used anymore. A Connector is a producer now, just use the connector object.",
            FutureWarning,
        )
        return self

    def consumer(
        self,
        topics=None,
        patterns=None,
        group_id=None,
        event=None,
        cb=None,
        threaded=True,
        name=None,
        **kwargs,
    ):
        """Return a fake thread object to be compatible with old code

        In order to keep this fail-safe and simple it uses 'mock'...
        """
        from unittest.mock import (  # import is done here, to not pollute the file with something normally in tests
            Mock,
        )

        warnings.warn(
            "RedisConnector.consumer() is deprecated and should not be used anymore. Use RedisConnector.register() with 'topics', 'patterns', 'cb' or 'start_thread' instead. Additional keyword args are transmitted to the callback. For the caller, the main difference with RedisConnector.register() is that it does not return a new thread.",
            FutureWarning,
        )
        dummy_thread = Mock(spec=threading.Thread)
        dummy_thread.start.side_effet = lambda: self.register(
            topics, patterns, cb, threaded, **kwargs
        )
        return dummy_thread
