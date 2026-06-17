import time
from typing import Generator

import fakeredis
import pytest

from bec_lib import messages
from bec_lib.endpoints import EndpointInfo, EndpointType, MessageEndpoints, MessageOp
from bec_lib.messages import ProcedureExecutionMessage
from bec_lib.redis_connector import RedisConnector
from bec_lib.redis_connector.buffered_redis_connector import BufferedRedisConnector
from bec_lib.redis_connector.constants import (
    IncompatibleMessageForEndpoint,
    IncompatibleRedisOperation,
)
from bec_lib.serialization import MsgpackSerialization

from .test_buffered_redis_connector import TestMessage

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument


TestStreamEndpoint = EndpointInfo("test", TestMessage, MessageOp.STREAM)
TestStreamEndpoint2 = EndpointInfo("test2", TestMessage, MessageOp.STREAM)


def fake_redis_server(host, port, **kwargs):
    redis = fakeredis.FakeRedis()
    return redis


@pytest.fixture
def connected_connector():
    BufferedRedisConnector.RETRY_ON_TIMEOUT = 0
    connector = RedisConnector("localhost:1", redis_cls=fake_redis_server)
    connector._buffered_connection.flushall()
    try:
        yield connector
    finally:
        connector.shutdown()


def test_connector_publish_metrics(connected_connector):

    start = time.time()
    data = []
    ep = MessageEndpoints.dynamic_metric("test")

    def cb(msg):
        nonlocal data
        data.append(msg.value)

    connected_connector.register(ep, cb=cb, start_thread=False)
    connected_connector.publish_metrics(
        "test",
        {
            "m1": 5,
            "m2": 5.5,
            "m3": {"value": "test", "possible_values": ["test", "prod"]},
            "m4": True,
        },
    )
    connected_connector.poll_messages(timeout=1)

    stop = time.time()
    res = data[0]
    assert isinstance(res, messages.DynamicMetricMessage)
    assert start <= res.timestamp <= stop
    assert res.metrics["_m1"].value == 5
    assert res.metrics["_m2"].value == 5.5
    assert res.metrics["_m3"].value == "test"
    assert set(res.metrics["_m3"].possible_values) == set(["prod", "test"])
    assert res.metrics["_m4"].value is True


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
        ProcedureExecutionMessage(identifier="test1", queue="queue1", execution_id="1"),  # type: ignore
        ProcedureExecutionMessage(identifier="test2", queue="queue2", execution_id="2"),  # type: ignore
        ProcedureExecutionMessage(identifier="test3", queue="queue3", execution_id="3"),  # type: ignore
        ProcedureExecutionMessage(identifier="test4", queue="queue4", execution_id="4"),  # type: ignore
    }
    for msg in test_set_messages:
        connected_connector._redis_conn.sadd(
            test_set_endpoint.endpoint, MsgpackSerialization.dumps(msg)
        )
    yield connected_connector, test_set_endpoint, test_set_messages


def test_list_pop_to_sadd_adds_to_set(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]],
):
    connected_connector, test_set_endpoint, test_set_messages = test_set_connector
    test_list_endpoint = EndpointInfo(
        f"{EndpointType.INTERNAL}/procedures/procedure_execution/queue5",
        ProcedureExecutionMessage,
        MessageOp.LIST,
    )
    test_message = ProcedureExecutionMessage(
        identifier="test5", queue="queue5", execution_id="1234"
    )
    connected_connector.lpush(test_list_endpoint, test_message)
    connected_connector.blocking_list_pop_to_set_add(test_list_endpoint, test_set_endpoint)
    test_set_messages.add(test_message)
    result = connected_connector.get_set_members(test_set_endpoint)
    assert result == test_set_messages


def test_list_pop_to_sadd_rejects_wrong_messageop(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]],
):
    connected_connector, test_set_endpoint, _ = test_set_connector
    test_list_endpoint = MessageEndpoints.device_progress("samx")
    test_message = ProcedureExecutionMessage(
        identifier="test5", queue="queue5", execution_id="1234"
    )
    connected_connector._redis_conn.lpush(
        test_list_endpoint.endpoint, MsgpackSerialization.dumps(test_message)
    )
    with pytest.raises(IncompatibleRedisOperation):
        connected_connector.blocking_list_pop_to_set_add(test_list_endpoint, test_set_endpoint)


def test_list_pop_to_sadd_rejects_wrong_message_for_set(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]],
):
    connected_connector, test_set_endpoint, _ = test_set_connector
    test_list_endpoint = EndpointInfo(
        f"{EndpointType.INTERNAL}/procedures/procedure_execution/queue5",
        ProcedureExecutionMessage,
        MessageOp.LIST,
    )
    test_message = messages.ServiceMetricMessage(name="test service", metrics={})
    connected_connector._redis_conn.lpush(
        test_list_endpoint.endpoint, MsgpackSerialization.dumps(test_message)
    )
    with pytest.raises(IncompatibleMessageForEndpoint):
        connected_connector.blocking_list_pop_to_set_add(test_list_endpoint, test_set_endpoint)


def test_get_set_members(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]],
):
    connected_connector, test_set_endpoint, test_set_messages = test_set_connector
    result = connected_connector.get_set_members(test_set_endpoint)
    assert result == test_set_messages


def test_remove_from_set(
    test_set_connector: tuple[RedisConnector, EndpointInfo, set[ProcedureExecutionMessage]],
):
    connected_connector, test_set_endpoint, test_set_messages = test_set_connector
    connected_connector.remove_from_set(test_set_endpoint, test_set_messages.pop())
    assert len(test_set_messages) == 3
    result = connected_connector.get_set_members(test_set_endpoint)
    assert result == test_set_messages


@pytest.mark.parametrize("endpoint", ["test", MessageEndpoints.processed_data("test")])
def test_redis_connector_get_last(connected_connector: RedisConnector, endpoint):
    connector = connected_connector
    connector.xadd(endpoint, {"data": 1})
    connector.xadd(endpoint, {"data": 2})
    connector.xadd(endpoint, {"data": 3})
    assert connector.get_last(endpoint) == {"data": 3}
    assert connector.get_last(endpoint) == {"data": 3}
    assert connector.get_last("test2") is None
    with pytest.raises(TypeError):
        assert connector.get_last(5)
    assert list(connector.get_last(endpoint, "data", count=3)) == [1, 2, 3]
    assert list(connector.get_last(endpoint, count=4)) == [{"data": 1}, {"data": 2}, {"data": 3}]
    assert connector.get_last(endpoint, count=0) is None
    assert connector.get_last(endpoint, count=-1) is None
