from typing import Any, ClassVar, Generator, Optional
from unittest import mock

import pytest

import bec_lib.messages as bec_messages
from bec_lib.alarm_handler import Alarms
from bec_lib.endpoints import EndpointInfo, EndpointType, MessageEndpoints, MessageOp
from bec_lib.messages import (
    AlarmMessage,
    BECMessage,
    BundleMessage,
    ClientInfoMessage,
    ProcedureExecutionMessage,
)
from bec_lib.redis_connector import (
    IncompatibleMessageForEndpoint,
    IncompatibleRedisOperation,
    RedisConnector,
    WrongArguments,
    validate_endpoint,
)
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
    _connector = RedisConnector("localhost:1", redis_cls=mock.MagicMock)
    try:
        yield _connector
    finally:
        _connector.shutdown()


def test_redis_connector_send_client_info(connector):
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
    "severity, alarm_type, source, msg, metadata",
    [
        [Alarms.MAJOR, "alarm", {"source": "test"}, "content1", {"metadata": "metadata1"}],
        [Alarms.MINOR, "alarm", {"source": "test"}, "content1", {"metadata": "metadata1"}],
        [Alarms.WARNING, "alarm", {"source": "test"}, "content1", {"metadata": "metadata1"}],
    ],
)
def test_redis_connector_raise_alarm(connector, severity, alarm_type, source, msg, metadata):
    with mock.patch.object(connector, "set_and_publish", return_value=None):
        connector.raise_alarm(severity, alarm_type, source, msg, metadata)

        connector.set_and_publish.assert_called_once_with(
            MessageEndpoints.alarm(),
            AlarmMessage(
                severity=severity, alarm_type=alarm_type, source=source, msg=msg, metadata=metadata
            ),
        )


@pytest.mark.parametrize(
    "topic , msg",
    [
        ["topic1", TestMessage(msg="msg1")],
        ["topic2", TestMessage(msg="msg2")],
        [
            MessageEndpoints.scan_segment(),
            bec_messages.ScanMessage(point_id=1, scan_id="scan_id", data={}),
        ],
    ],
)
def test_redis_connector_send(connector, topic, msg):
    connector.send(topic, msg)
    topic_str = topic if isinstance(topic, str) else topic.endpoint
    connector._redis_conn.publish.assert_called_once_with(
        topic_str, MsgpackSerialization.dumps(msg)
    )

    connector.send(topic, msg, pipe=connector.pipeline())
    connector._redis_conn.pipeline().publish.assert_called_once_with(
        topic_str, MsgpackSerialization.dumps(msg)
    )


@pytest.mark.parametrize(
    "topic, msgs, max_size, expire",
    [["topic1", "msgs", None, None], ["topic1", "msgs", 10, None], ["topic1", "msgs", None, 100]],
)
def test_redis_connector_lpush(connector, topic, msgs, max_size, expire):
    pipe = None
    connector.lpush(topic, msgs, pipe, max_size, expire)

    connector._redis_conn.pipeline().lpush.assert_called_once_with(topic, msgs)

    if max_size:
        connector._redis_conn.pipeline().ltrim.assert_called_once_with(topic, 0, max_size)
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
def test_redis_connector_lpush_BECMessage(connector, topic, msgs, max_size, expire):
    pipe = None
    connector.lpush(topic, msgs, pipe, max_size, expire)

    connector._redis_conn.pipeline().lpush.assert_called_once_with(
        topic, MsgpackSerialization.dumps(msgs)
    )

    if max_size:
        connector._redis_conn.pipeline().ltrim.assert_called_once_with(topic, 0, max_size)
    if expire:
        connector._redis_conn.pipeline().expire.assert_called_once_with(topic, expire)
    if not pipe:
        connector._redis_conn.pipeline().execute.assert_called_once()


@pytest.mark.parametrize(
    "topic , index , msgs, use_pipe", [["topic1", 1, "msg1", True], ["topic2", 4, "msg2", False]]
)
def test_redis_connector_lset(connector, topic, index, msgs, use_pipe):
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
def test_redis_connector_lset_BECMessage(connector, topic, index, msgs, use_pipe):
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
    "topic, msgs, use_pipe", [["topic1", "msg1", True], ["topic2", "msg2", False]]
)
def test_redis_connector_rpush(connector, topic, msgs, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.rpush(topic, msgs, pipe)

    if pipe:
        connector._redis_conn.pipeline().rpush.assert_called_once_with(topic, msgs)
        assert ret == connector._redis_conn.pipeline().rpush()
    else:
        connector._redis_conn.rpush.assert_called_once_with(topic, msgs)
        assert ret == connector._redis_conn.rpush()


@pytest.mark.parametrize(
    "topic, msgs, use_pipe",
    [["topic1", TestMessage(msg="msg1"), True], ["topic2", TestMessage(msg="msg2"), False]],
)
def test_redis_connector_rpush_BECMessage(connector, topic, msgs, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.rpush(topic, msgs, pipe)

    if pipe:
        connector._redis_conn.pipeline().rpush.assert_called_once_with(
            topic, MsgpackSerialization.dumps(msgs)
        )
        assert ret == connector._redis_conn.pipeline().rpush()
    else:
        connector._redis_conn.rpush.assert_called_once_with(topic, MsgpackSerialization.dumps(msgs))
        assert ret == connector._redis_conn.rpush()


@pytest.mark.parametrize(
    "topic, start, end, use_pipe", [["topic1", 0, 4, True], ["topic2", 3, 7, False]]
)
def test_redis_connector_lrange(connector, topic, start, end, use_pipe):
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
def test_redis_connector_set_and_publish(connector, topic, msg, pipe, expire):
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
def test_redis_connector_set(connector, topic, msg, expire):
    pipe = None

    connector.set(topic, msg, pipe, expire)

    if pipe:
        connector._redis_conn.pipeline().set.assert_called_once_with(topic, msg, ex=expire)
    else:
        connector._redis_conn.set.assert_called_once_with(topic, msg, ex=expire)


@pytest.mark.parametrize("pattern", ["samx", "samy", MessageEndpoints.device_read("sam*")])
def test_redis_connector_keys(connector, pattern):
    ret = connector.keys(pattern)
    endpoint = pattern if isinstance(pattern, str) else pattern.endpoint
    connector._redis_conn.keys.assert_called_once_with(endpoint)
    assert ret == connector._redis_conn.keys()


def test_redis_connector_pipeline(connector):
    ret = connector.pipeline()
    connector._redis_conn.pipeline.assert_called_once()
    assert ret == connector._redis_conn.pipeline()


def use_pipe_fcn(connector, use_pipe):
    if use_pipe:
        return connector.pipeline()
    return None


@pytest.mark.parametrize("topic,use_pipe", [["topic1", True], ["topic2", False]])
def test_redis_connector_delete(connector, topic, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    connector.delete(topic, pipe)

    if pipe:
        connector.pipeline().delete.assert_called_once_with(topic)
    else:
        connector._redis_conn.delete.assert_called_once_with(topic)


@pytest.mark.parametrize("topic, use_pipe", [["topic1", True], ["topic2", False]])
def test_redis_connector_get(connector, topic, use_pipe):
    pipe = use_pipe_fcn(connector, use_pipe)

    ret = connector.get(topic, pipe)
    if pipe:
        connector.pipeline().get.assert_called_once_with(topic)
        assert ret == connector._redis_conn.pipeline().get()
    else:
        connector._redis_conn.get.assert_called_once_with(topic)
        assert ret == connector._redis_conn.get()


def test_redis_connector_xread(connector):
    connector.xread("topic1", "id")
    connector._redis_conn.xread.assert_called_once_with({"topic1": "id"}, count=None, block=None)


def test_redis_connector_xadd_with_maxlen(connector):
    connector.xadd("topic1", {"key": "value"}, max_size=100)
    connector._redis_conn.xadd.assert_called_once_with(
        "topic1", {"key": MsgpackSerialization.dumps("value")}, maxlen=100
    )


def test_redis_connector_xadd_with_expire(connector):
    connector.xadd("topic1", {"key": "value"}, expire=100)
    connector._redis_conn.pipeline().xadd.assert_called_once_with(
        "topic1", {"key": MsgpackSerialization.dumps("value")}
    )
    connector._redis_conn.pipeline().expire.assert_called_once_with("topic1", 100)
    connector._redis_conn.pipeline().execute.assert_called_once()


def test_redis_connector_xread_from_end(connector):
    connector.xread("topic1", from_start=False)
    connector._redis_conn.xrevrange.assert_called_once_with("topic1", "+", "-", count=1)


def test_redis_connector_xread_without_id(connector):
    connector.xread("topic1", from_start=True)
    connector._redis_conn.xread.assert_called_once_with({"topic1": "0-0"}, count=None, block=None)
    connector._redis_conn.xread.reset_mock()

    connector.stream_keys["topic1"] = "id"
    connector.xread("topic1")
    connector._redis_conn.xread.assert_called_once_with({"topic1": "id"}, count=None, block=None)


def test_redis_xrange(connector):
    connector.xrange("topic1", "start", "end")
    connector._redis_conn.xrange.assert_called_once_with("topic1", "start", "end", count=None)


def test_redis_xrange_topic_with_suffix(connector):
    connector.xrange("topic1", "start", "end")
    connector._redis_conn.xrange.assert_called_once_with("topic1", "start", "end", count=None)


def test_send_raises_on_invalid_message_type(connector):
    correct_msg = bec_messages.DeviceMessage(
        signals={"samx": {"value": 1, "timestamp": 1}}, metadata={}
    )
    connector.set_and_publish(MessageEndpoints.device_read("samx"), correct_msg)
    with pytest.raises(TypeError) as excinfo:
        msg = bec_messages.ScanMessage(point_id=1, scan_id="scan_id", data={}, metadata={})
        connector.set_and_publish(MessageEndpoints.device_read("samx"), msg)
    assert "Message type <class 'bec_lib.messages.ScanMessage'> is not compatible " in str(
        excinfo.value
    )


def test_send_raises_on_invalid_topic(connector):
    with pytest.raises(IncompatibleRedisOperation):
        connector.send(MessageEndpoints.device_status("samx"), "msg")


def test_mget(connector):
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


@pytest.fixture
def test_set_connector(
    connected_connector,
) -> Generator[tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]], None, None]:

    test_set_endpoint = EndpointInfo(
        f"{EndpointType.INFO}/procedures/active_procedures",
        ProcedureExecutionMessage,
        MessageOp.SET,
    )
    test_set_messages = {
        ProcedureExecutionMessage(identifier="test1", queue="queue1"),  # type: ignore
        ProcedureExecutionMessage(identifier="test2", queue="queue2"),  # type: ignore
        ProcedureExecutionMessage(identifier="test3", queue="queue3"),  # type: ignore
        ProcedureExecutionMessage(identifier="test4", queue="queue4"),  # type: ignore
    }
    for msg in test_set_messages:
        connected_connector._redis_conn.sadd(
            test_set_endpoint.endpoint, MsgpackSerialization.dumps(msg)
        )
    yield connected_connector, test_set_endpoint, test_set_messages


def test_get_set_members(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]]
):
    connected_connector, test_set_endpoint, test_set_messages = test_set_connector
    result = connected_connector.get_set_members(test_set_endpoint)
    assert result == test_set_messages


def test_remove_from_set(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]]
):
    connected_connector, test_set_endpoint, test_set_messages = test_set_connector
    connected_connector.remove_from_set(test_set_endpoint, test_set_messages.pop())
    assert len(test_set_messages) == 3
    result = connected_connector.get_set_members(test_set_endpoint)
    assert result == test_set_messages


def test_list_pop_to_sadd_adds_to_set(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]]
):
    connected_connector, test_set_endpoint, test_set_messages = test_set_connector
    test_list_endpoint = EndpointInfo(
        f"{EndpointType.INTERNAL}/procedures/procedure_execution/queue5",
        ProcedureExecutionMessage,
        MessageOp.LIST,
    )
    test_message = ProcedureExecutionMessage(identifier="test5", queue="queue5")
    connected_connector.lpush(test_list_endpoint, test_message)
    connected_connector.blocking_list_pop_to_set_add(test_list_endpoint, test_set_endpoint)
    test_set_messages.add(test_message)
    result = connected_connector.get_set_members(test_set_endpoint)
    assert result == test_set_messages


def test_list_pop_to_sadd_rejects_wrong_messageop(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]]
):
    connected_connector, test_set_endpoint, _ = test_set_connector
    test_list_endpoint = MessageEndpoints.device_progress("samx")
    test_message = ProcedureExecutionMessage(identifier="test5", queue="queue5")
    connected_connector._redis_conn.lpush(
        test_list_endpoint.endpoint, MsgpackSerialization.dumps(test_message)
    )
    with pytest.raises(IncompatibleRedisOperation):
        connected_connector.blocking_list_pop_to_set_add(test_list_endpoint, test_set_endpoint)


def test_list_pop_to_sadd_rejects_wrong_message_for_set(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]]
):
    connected_connector, test_set_endpoint, _ = test_set_connector
    test_list_endpoint = EndpointInfo(
        f"{EndpointType.INTERNAL}/procedures/procedure_execution/queue5",
        ProcedureExecutionMessage,
        MessageOp.LIST,
    )
    test_message = bec_messages.ServiceMetricMessage(name="test service", metrics={})
    connected_connector._redis_conn.lpush(
        test_list_endpoint.endpoint, MsgpackSerialization.dumps(test_message)
    )
    with pytest.raises(IncompatibleMessageForEndpoint):
        connected_connector.blocking_list_pop_to_set_add(test_list_endpoint, test_set_endpoint)
