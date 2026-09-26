from typing import Any, ClassVar, Optional
from unittest import mock

import msgpack
import numpy as np
import pytest
from redis import Redis
from redis.exceptions import RedisError

import bec_lib.messages as bec_messages
from bec_lib import messages
from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.messages import BECMessage, BECStatus, BundleMessage, ClientInfoMessage
from bec_lib.redis_connector import IncompatibleRedisOperation
from bec_lib.redis_connector.constants import WrongArguments
from bec_lib.redis_connector.managed_redis_connection import ManagedRedisConnection
from bec_lib.redis_connector.streams import StreamSubInfo
from bec_lib.redis_connector.validation import validate_endpoint
from bec_lib.serialization import MsgpackSerialization

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=redefined-outer-name


class TestMessage(BECMessage):
    __test__: ClassVar[bool] = False  # just for pytest to ignore this class
    msg_type: ClassVar[str] = "test_message"
    msg: Optional[str] = None


# register at BEC messages module level, to be able to
# find it when using "loads()"
bec_messages.TestMessage = TestMessage


@pytest.fixture
def connector():
    _connector = ManagedRedisConnection("localhost:1", redis_cls=mock.MagicMock)
    try:
        yield _connector
    finally:
        _connector.shutdown()


@pytest.fixture
def legacy_object_array_payload():
    return msgpack.packb(
        {
            "__bec_codec__": {
                "encoder_name": "ndarray",
                "type_name": "ndarray",
                "data": {
                    b"nd": True,
                    b"kind": b"O",
                    b"type": [("", "|O")],
                    b"shape": [1],
                    b"data": np.array([1], dtype=object).dumps(),
                },
            }
        }
    )


@pytest.mark.parametrize("from_start", [True, False])
def test_stream_listener_skips_rejected_object_arrays(
    connector, legacy_object_array_payload, from_start
):
    topic = MessageEndpoints.processed_data("legacy").endpoint
    other_topic = MessageEndpoints.processed_data("valid").endpoint
    callback = mock.Mock()
    subscription = StreamSubInfo(lambda: callback, {})
    for stream_topic in (topic, other_topic):
        connector._stream_subs.add(from_start, "0-0", stream_topic, subscription)
    valid = messages.RawMessage(data="valid")
    response = [
        (
            topic.encode(),
            [
                (b"1-0", {b"data": legacy_object_array_payload}),
                (b"2-0", {b"data": MsgpackSerialization.dumps(valid)}),
                (b"3-0", {b"data": legacy_object_array_payload}),
            ],
        ),
        (other_topic.encode(), [(b"1-0", {b"data": MsgpackSerialization.dumps(valid)})]),
    ]
    connector._redis_conn.xread.return_value = [] if from_start else response
    connector._redis_conn.xrange.side_effect = lambda topic, *_: dict(response)[topic.encode()]

    with (
        mock.patch.object(
            connector._stop_stream_events_listener_thread, "is_set", side_effect=[False, True]
        ),
        mock.patch("bec_lib.redis_connector.managed_redis_connection.logger.error") as log_error,
    ):
        connector._get_stream_messages_loop()

    assert connector._stream_subs.topic_ids() == {topic: "3-0", other_topic: "1-0"}
    assert connector._stream_subs.from_start_subs == {}
    assert connector._message_callbacks_queue.qsize() == 2
    for _ in range(2):
        message = connector._message_callbacks_queue.get_nowait()
        assert message.msg == {"data": valid}
        assert list(message.callbacks) == [(subscription.cb_ref, {})]
    assert log_error.call_count == 2


def test_direct_stream_listener_skips_rejected_object_arrays(
    connector, legacy_object_array_payload
):
    topic = MessageEndpoints.processed_data("legacy").endpoint
    valid = messages.RawMessage(data="valid")
    connector._redis_conn.xrevrange.side_effect = [
        [(b"1-0", {b"data": legacy_object_array_payload})],
        [(b"2-0", {b"data": MsgpackSerialization.dumps(valid)})],
    ]
    stop_event = mock.Mock()
    stop_event.is_set.side_effect = [False, False, True]
    callback = mock.Mock()
    with mock.patch("bec_lib.redis_connector.managed_redis_connection.logger.error") as log_error:
        connector._direct_stream_listener(topic, stop_event, callback, {})

    assert connector._redis_conn.xrevrange.call_args_list == [
        mock.call(topic, "+", "-", count=1),
        mock.call(topic, "+", "1-1", count=1),
    ]
    assert connector._message_callbacks_queue.qsize() == 1
    message = connector._message_callbacks_queue.get_nowait()
    assert message.msg == {"data": valid}
    assert list(message.callbacks) == [(callback, {})]
    log_error.assert_called_once()


def test_redis_connector_send_client_info(connector: ManagedRedisConnection):
    with mock.patch.object(connector, "xadd", return_value=None):
        connector.send_client_info(message="msg", show_asap=True, source="scan_server")
        connector.xadd.assert_called_once_with(
            MessageEndpoints.client_info(),
            msg_dict={
                "data": ClientInfoMessage(message="msg", show_asap=True, source="scan_server")
            },
            max_size=100,
        )


@pytest.mark.parametrize(
    "topic , msg", [["topic1", TestMessage(msg="msg1")], ["topic2", TestMessage(msg="msg2")]]
)
def test_redis_connector_send(connector: ManagedRedisConnection, topic, msg):
    connector.send(topic, msg)
    connector._redis_conn.publish.assert_called_once_with(topic, MsgpackSerialization.dumps(msg))

    connector.send(topic, msg, pipe=connector.pipeline())
    connector._redis_conn.pipeline().publish.assert_called_once_with(
        topic, MsgpackSerialization.dumps(msg)
    )


@pytest.mark.parametrize(
    "topic, msgs, max_size, expire",
    [["topic1", "msgs", None, None], ["topic1", "msgs", 10, None], ["topic1", "msgs", None, 100]],
)
def test_redis_connector_lpush(connector: ManagedRedisConnection, topic, msgs, max_size, expire):
    pipe = None
    connector.lpush(topic, msgs, pipe, max_size, expire)

    connector._redis_conn.pipeline().lpush.assert_called_once_with(topic, msgs)

    if max_size:
        connector._redis_conn.pipeline().ltrim.assert_called_once_with(topic, 0, max_size - 1)
    if expire:
        connector._redis_conn.pipeline().expire.assert_called_once_with(topic, expire)
    if not pipe:
        connector._redis_conn.pipeline().execute.assert_called_once()


@pytest.mark.parametrize(
    "topic, msgs, max_size, expire",
    [
        ["topic1", TestMessage(msg="msgs"), None, None],
        ["topic1", TestMessage(msg="msgs"), 10, None],
        ["topic1", TestMessage(msg="msgs"), None, 100],
    ],
)
def test_redis_connector_lpush_BECMessage(
    connector: ManagedRedisConnection, topic, msgs, max_size, expire
):
    pipe = None
    connector.lpush(topic, msgs, pipe, max_size, expire)

    connector._redis_conn.pipeline().lpush.assert_called_once_with(
        topic, MsgpackSerialization.dumps(msgs)
    )

    if max_size:
        connector._redis_conn.pipeline().ltrim.assert_called_once_with(topic, 0, max_size - 1)
    if expire:
        connector._redis_conn.pipeline().expire.assert_called_once_with(topic, expire)
    if not pipe:
        connector._redis_conn.pipeline().execute.assert_called_once()


@pytest.mark.parametrize(
    "topic , index , msgs, use_pipe", [["topic1", 1, "msg1", True], ["topic2", 4, "msg2", False]]
)
def test_redis_connector_lset(connector: ManagedRedisConnection, topic, index, msgs, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.lset(topic, index, msgs, pipe)

    if pipe:
        connector._redis_conn.pipeline().lset.assert_called_once_with(topic, index, msgs)
        assert ret == connector._redis_conn.pipeline().lset()
    else:
        connector._redis_conn.lset.assert_called_once_with(topic, index, msgs)
        assert ret == connector._redis_conn.lset()


@pytest.mark.parametrize(
    "topic , index , msgs, use_pipe",
    [["topic1", 1, TestMessage(msg="msg1"), True], ["topic2", 4, TestMessage(msg="msg2"), False]],
)
def test_redis_connector_lset_BECMessage(
    connector: ManagedRedisConnection, topic, index, msgs, use_pipe
):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.lset(topic, index, msgs, pipe)

    if pipe:
        connector._redis_conn.pipeline().lset.assert_called_once_with(
            topic, index, MsgpackSerialization.dumps(msgs)
        )
        assert ret == pipe.lset()
    else:
        connector._redis_conn.lset.assert_called_once_with(
            topic, index, MsgpackSerialization.dumps(msgs)
        )
        assert ret == connector._redis_conn.lset()


@pytest.mark.parametrize(
    "topic, msgs, use_pipe, max_size, expire",
    [["topic1", "msg1", True, None, None], ["topic2", "msg2", False, 10, 100]],
)
def test_redis_connector_rpush(
    connector: ManagedRedisConnection, topic, msgs, use_pipe, max_size, expire
):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.rpush(topic, msgs, pipe, max_size=max_size, expire=expire)

    connector._redis_conn.pipeline().rpush.assert_called_once_with(topic, msgs)
    if max_size:
        connector._redis_conn.pipeline().ltrim.assert_called_once_with(topic, -max_size, -1)
    if expire:
        connector._redis_conn.pipeline().expire.assert_called_once_with(topic, expire)
    if pipe:
        connector._redis_conn.pipeline().execute.assert_not_called()
    else:
        connector._redis_conn.pipeline().execute.assert_called_once()
    assert ret is None


@pytest.mark.parametrize(
    "topic, msgs, use_pipe, max_size, expire",
    [
        ["topic1", TestMessage(msg="msg1"), True, None, None],
        ["topic2", TestMessage(msg="msg2"), False, 10, 100],
    ],
)
def test_redis_connector_rpush_BECMessage(
    connector: ManagedRedisConnection, topic, msgs, use_pipe, max_size, expire
):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.rpush(topic, msgs, pipe, max_size=max_size, expire=expire)

    connector._redis_conn.pipeline().rpush.assert_called_once_with(
        topic, MsgpackSerialization.dumps(msgs)
    )
    if max_size:
        connector._redis_conn.pipeline().ltrim.assert_called_once_with(topic, -max_size, -1)
    if expire:
        connector._redis_conn.pipeline().expire.assert_called_once_with(topic, expire)
    if pipe:
        connector._redis_conn.pipeline().execute.assert_not_called()
    else:
        connector._redis_conn.pipeline().execute.assert_called_once()
    assert ret is None


@pytest.mark.parametrize(
    "topic, start, end, use_pipe", [["topic1", 0, 4, True], ["topic2", 3, 7, False]]
)
def test_redis_connector_lrange(connector: ManagedRedisConnection, topic, start, end, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.lrange(topic, start, end, pipe)

    if pipe:
        connector._redis_conn.pipeline().lrange.assert_called_once_with(topic, start, end)
        assert ret == connector._redis_conn.pipeline().lrange()
    else:
        connector._redis_conn.lrange.assert_called_once_with(topic, start, end)
        assert ret == []


@pytest.mark.parametrize(
    "topic, msg, pipe, expire",
    [
        ["topic1", TestMessage(msg="msg1"), None, 400],
        ["topic2", TestMessage(msg="msg2"), None, None],
        ["topic3", "msg3", None, None],
    ],
)
def test_redis_connector_set_and_publish(
    connector: ManagedRedisConnection, topic, msg, pipe, expire
):
    if not isinstance(msg, BECMessage):
        msg_sent = msg
    else:
        msg_sent = MsgpackSerialization.dumps(msg)

    connector.set_and_publish(topic, msg, pipe, expire)

    connector._redis_conn.pipeline().publish.assert_called_once_with(topic, msg_sent)
    connector._redis_conn.pipeline().set.assert_called_once_with(topic, msg_sent, ex=expire)
    if not pipe:
        connector._redis_conn.pipeline().execute.assert_called_once()


@pytest.mark.parametrize("topic, msg, expire", [["topic1", "msg1", None], ["topic2", "msg2", 400]])
def test_redis_connector_set(connector: ManagedRedisConnection, topic, msg, expire):
    pipe = None

    connector.set(topic, msg, pipe, expire)

    if pipe:
        connector._redis_conn.pipeline().set.assert_called_once_with(topic, msg, ex=expire)
    else:
        connector._redis_conn.set.assert_called_once_with(topic, msg, ex=expire)


def test_redis_connector_pipeline(connector: ManagedRedisConnection):
    ret = connector.pipeline()
    connector._redis_conn.pipeline.assert_called_once()
    assert ret == connector._redis_conn.pipeline()


def use_pipe_fcn(connector: ManagedRedisConnection, use_pipe):
    if use_pipe:
        return connector.pipeline()
    return None


@pytest.mark.parametrize("topic,use_pipe", [["topic1", True], ["topic2", False]])
def test_redis_connector_delete(connector: ManagedRedisConnection, topic, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    connector.delete(topic, pipe)

    if pipe:
        connector.pipeline().delete.assert_called_once_with(topic)
    else:
        connector._redis_conn.delete.assert_called_once_with(topic)


@pytest.mark.parametrize("topic, use_pipe", [["topic1", True], ["topic2", False]])
def test_redis_connector_get(connector: ManagedRedisConnection, topic, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.get(topic, pipe)
    if pipe:
        connector.pipeline().get.assert_called_once_with(topic)
        assert ret == connector._redis_conn.pipeline().get()
    else:
        connector._redis_conn.get.assert_called_once_with(topic)
        assert ret == connector._redis_conn.get()


def test_redis_connector_xread(connector: ManagedRedisConnection):
    connector.xread("topic1", "id")
    connector._redis_conn.xread.assert_called_once_with({"topic1": "id"}, count=None, block=None)


@pytest.mark.parametrize(
    "block, expected_options", [(None, []), (0, [b"BLOCK", "0"]), (500, [b"BLOCK", "500"])]
)
def test_redis_connector_raw_xread_block(
    connector: ManagedRedisConnection, block, expected_options
):
    topic = MessageEndpoints.device_async_readback(scan_id="scan_id", device="monitor").endpoint
    with (
        mock.patch.object(connector, "_redis_conn", Redis()) as redis_client,
        mock.patch.object(redis_client, "execute_command", return_value=[]) as execute_command,
    ):
        assert connector.raw_xread({topic: "0-0"}, block=block) == []

    execute_command.assert_called_once_with(
        "XREAD", *expected_options, b"STREAMS", topic, "0-0", keys=(topic,)
    )


def test_redis_connector_xadd_with_maxlen(connector: ManagedRedisConnection):
    connector.xadd("topic1", {"key": "value"}, max_size=100)
    connector._redis_conn.xadd.assert_called_once_with(
        "topic1", {"key": MsgpackSerialization.dumps("value")}, maxlen=100, approximate=True
    )


def test_redis_connector_xadd_with_maxlen_and_approximate(connector: ManagedRedisConnection):
    connector.xadd("topic1", {"key": "value"}, max_size=100, approximate=False)
    connector._redis_conn.xadd.assert_called_once_with(
        "topic1", {"key": MsgpackSerialization.dumps("value")}, maxlen=100, approximate=False
    )


def test_redis_connector_xadd_with_expire(connector: ManagedRedisConnection):
    connector.xadd("topic1", {"key": "value"}, expire=100)
    connector._redis_conn.pipeline().xadd.assert_called_once_with(
        "topic1", {"key": MsgpackSerialization.dumps("value")}
    )
    connector._redis_conn.pipeline().expire.assert_called_once_with("topic1", 100)
    connector._redis_conn.pipeline().execute.assert_called_once()


def test_redis_connector_xread_from_end(connector: ManagedRedisConnection):
    connector.xread("topic1", from_start=False)
    connector._redis_conn.xrevrange.assert_called_once_with("topic1", "+", "-", count=1)


def test_redis_connector_xread_without_id(connector: ManagedRedisConnection):
    connector.xread("topic1", from_start=True)
    connector._redis_conn.xread.assert_called_once_with({"topic1": "0-0"}, count=None, block=None)
    connector._redis_conn.xread.reset_mock()

    connector.stream_keys["topic1"] = "id"
    connector.xread("topic1")
    connector._redis_conn.xread.assert_called_once_with({"topic1": "id"}, count=None, block=None)


def test_redis_xrange(connector: ManagedRedisConnection):
    connector.xrange("topic1", "start", "end")
    connector._redis_conn.xrange.assert_called_once_with("topic1", "start", "end", count=None)


def test_redis_xrange_topic_with_suffix(connector: ManagedRedisConnection):
    connector.xrange("topic1", "start", "end")
    connector._redis_conn.xrange.assert_called_once_with("topic1", "start", "end", count=None)


def test_mget(connector: ManagedRedisConnection):
    connector.mget(["topic1", "topic2"])
    connector._redis_conn.mget.assert_called_once_with(["topic1", "topic2"])


def test_validate_with_present_arg():

    endpoint = EndpointInfo("test", Any, ["method"])  # type: ignore

    @validate_endpoint("arg1")
    def method(_, arg1):
        assert isinstance(arg1, str)
        assert arg1 == "test"

    method(None, endpoint)


def test_validate_with_missing_arg():

    with pytest.raises(WrongArguments):

        @validate_endpoint("missing_arg")
        def method(_, arg1): ...


def test_validate_rejects_wrong_op():
    endpoint = EndpointInfo("test", Any, ["missing_ops"])  # type: ignore

    @validate_endpoint("arg1")
    def not_in_list(_, arg1): ...

    with pytest.raises(IncompatibleRedisOperation):
        not_in_list(None, endpoint)


def test_bundle_message_handled():
    endpoint = MessageEndpoints.scan_segment()
    messages = BundleMessage(
        messages=[
            endpoint.message_type(point_id=1, scan_id="", data={}),
            endpoint.message_type(point_id=1, scan_id="", data={}),
        ]
    )

    @validate_endpoint("endpoint")
    def send(_, endpoint, messages): ...  # pylint: ignore=unused-argument

    send(None, endpoint, messages)


def test_blocking_list_pop(connector: ManagedRedisConnection):
    msg = messages.StatusMessage(name="test", status=BECStatus.BUSY, info={})
    connector._redis_conn.blpop.return_value = [None, MsgpackSerialization.dumps(msg)]
    result = connector.blocking_list_pop("topic")
    connector._redis_conn.blpop.assert_called_once_with(["topic"], timeout=None)
    assert result == msg
    connector._redis_conn.brpop.assert_not_called()

    connector._redis_conn.brpop.return_value = None
    connector.blocking_list_pop("topic", side="RIGHT")
    connector._redis_conn.brpop.assert_called_once()


@pytest.mark.parametrize(
    ("test_dict"),
    (
        {"test_key": "test_val"},
        {"test_key": "test_val", "username": "test123", "password": "test123"},
    ),
)
def test_user_pw_restored_on_auth_fail(connector: ManagedRedisConnection, test_dict):
    connector._redis_conn.connection_pool.connection_kwargs = test_dict
    connector._redis_conn.auth.side_effect = RedisError
    with pytest.raises(RedisError):
        connector.authenticate(username="user", password="pass")
    assert connector._redis_conn.connection_pool.connection_kwargs == test_dict
