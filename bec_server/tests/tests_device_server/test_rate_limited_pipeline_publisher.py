import time

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.devices.rate_limited_pipeline_publisher import (
    RateLimitedPipelinePublisher,
)


@pytest.fixture
def publisher():
    connector = ConnectorMock("")
    publisher = RateLimitedPipelinePublisher(connector_getter=lambda: connector, rate_limit_s=0.1)
    try:
        yield publisher, connector
    finally:
        publisher.shutdown()


def test_rate_limited_publish_flushes_latest_same_topic(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 0.05
    connector.message_sent.clear()

    first = messages.ProgressMessage(value=1, max_value=10, done=False, metadata={"seq": 1})
    second = messages.ProgressMessage(value=2, max_value=10, done=False, metadata={"seq": 2})
    third = messages.ProgressMessage(value=2, max_value=10, done=False, metadata={"seq": 2})

    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: first)
    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: second)
    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: third)

    deadline = time.time() + 1
    progress_msgs = []
    while time.time() < deadline:
        progress_msgs = [
            msg
            for msg in connector.message_sent
            if msg["queue"] == MessageEndpoints.device_progress("samx").endpoint
        ]
        if len(progress_msgs) >= 2:
            break
        time.sleep(0.01)

    assert progress_msgs == [
        {"queue": MessageEndpoints.device_progress("samx").endpoint, "msg": first, "expire": None},
        {"queue": MessageEndpoints.device_progress("samx").endpoint, "msg": third, "expire": None},
    ]


def test_rate_limited_publish_is_per_topic(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 0.5
    connector.message_sent.clear()

    publisher.publish_set_and_publish(
        MessageEndpoints.device_progress("samx"),
        lambda: messages.ProgressMessage(value=1, max_value=2, done=False, metadata={}),
    )
    publisher.publish_set_and_publish(
        MessageEndpoints.device_progress("samy"),
        lambda: messages.ProgressMessage(value=2, max_value=2, done=False, metadata={}),
    )

    deadline = time.time() + 1
    progress_msgs = []
    while time.time() < deadline:
        progress_msgs = [
            msg
            for msg in connector.message_sent
            if msg["queue"]
            in (
                MessageEndpoints.device_progress("samx").endpoint,
                MessageEndpoints.device_progress("samy").endpoint,
            )
        ]
        if len(progress_msgs) >= 2:
            break
        time.sleep(0.01)

    assert progress_msgs == [
        {
            "queue": MessageEndpoints.device_progress("samx").endpoint,
            "msg": messages.ProgressMessage(value=1, max_value=2, done=False, metadata={}),
            "expire": None,
        },
        {
            "queue": MessageEndpoints.device_progress("samy").endpoint,
            "msg": messages.ProgressMessage(value=2, max_value=2, done=False, metadata={}),
            "expire": None,
        },
    ]


def test_rate_limited_publish_shutdown_flushes_last_pending(publisher):
    publisher, connector = publisher
    publisher.rate_limit_s = 10
    connector.message_sent.clear()

    first = messages.ProgressMessage(value=1, max_value=10, done=False, metadata={"seq": 1})
    last = messages.ProgressMessage(value=3, max_value=10, done=False, metadata={"seq": 3})

    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: first)

    deadline = time.time() + 1
    while time.time() < deadline:
        progress_msgs = [
            msg
            for msg in connector.message_sent
            if msg["queue"] == MessageEndpoints.device_progress("samx").endpoint
        ]
        if len(progress_msgs) >= 1:
            break
        time.sleep(0.01)

    publisher.publish_set_and_publish(MessageEndpoints.device_progress("samx"), lambda: last)
    publisher.shutdown()

    progress_msgs = [
        msg
        for msg in connector.message_sent
        if msg["queue"] == MessageEndpoints.device_progress("samx").endpoint
    ]
    assert progress_msgs == [
        {"queue": MessageEndpoints.device_progress("samx").endpoint, "msg": first, "expire": None},
        {"queue": MessageEndpoints.device_progress("samx").endpoint, "msg": last, "expire": None},
    ]
