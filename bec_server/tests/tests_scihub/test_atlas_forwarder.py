import time

import pytest

from bec_lib import messages
from bec_lib.serialization import json_ext as json


@pytest.fixture
def forwarder(atlas_connector):
    yield atlas_connector.atlas_forwarder


def test_atlas_forwarder_registers_state_and_request(forwarder):
    assert list(forwarder.atlas_connector.redis_atlas._topics_cb) == [
        "internal/deployment/test-deployment/*/state",
        "internal/deployment/test-deployment/request",
    ]


@pytest.mark.timeout(5)
def test_atlas_forwarder_redis_request(forwarder):
    updated = False

    def get_response(msg_object):
        nonlocal updated
        updated = True

    forwarder.atlas_connector.redis_atlas.register("test-topic", cb=get_response)
    forwarder.atlas_connector.redis_atlas.send(
        "internal/deployment/test-deployment/request",
        json.dumps({"action": "get", "key": "test-key", "response_endpoint": "test-topic"}),
    )
    while not updated:
        time.sleep(0.1)


def test_atlas_forwarder_update_deployment_state(forwarder):
    msg = messages.RawMessage(
        data={
            "test-socketio-sid": {
                "user": "john_doe",
                "subscriptions": [],
                "deployment": "test-deployment",
            }
        }
    )
    forwarder.update_deployment_state(msg)
    assert forwarder.active_subscriptions == set()

    subscriptions = [
        ["internal/devices/readback/samx", '{"endpoint":"device_readback","args":["samx"]}'],
        ["internal/devices/readback/samy", '{"endpoint":"device_readback","args":["samy"]}'],
        ["internal/queue/queue_status", '{"endpoint":"scan_queue_status","args":[]}'],
    ]

    msg = messages.RawMessage(
        data={
            "test-socketio-sid": {
                "user": "john_doe",
                "subscriptions": subscriptions,
                "deployment": "test-deployment",
            }
        }
    )
    forwarder.update_deployment_state(msg)
    assert forwarder.active_subscriptions == {sub[1] for sub in subscriptions}

    subscriptions = [
        ["internal/devices/readback/samy", '{"endpoint":"device_readback","args":["samy"]}'],
        ["internal/queue/queue_status", '{"endpoint":"scan_queue_status","args":[]}'],
    ]

    msg = messages.RawMessage(
        data={
            "test-socketio-sid": {
                "user": "john_doe",
                "subscriptions": subscriptions,
                "deployment": "test-deployment",
            }
        }
    )

    forwarder.update_deployment_state(msg)
    assert forwarder.active_subscriptions == {sub[1] for sub in subscriptions}
