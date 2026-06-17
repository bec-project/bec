import time

import fakeredis
import pytest

from bec_lib import messages
from bec_lib.endpoints import EndpointInfo, MessageEndpoints, MessageOp
from bec_lib.redis_connector import RedisConnector
from bec_lib.redis_connector.buffered_redis_connector import BufferedRedisConnector

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
