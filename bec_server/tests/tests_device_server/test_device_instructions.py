import os
import threading
from unittest import mock

import pytest
import yaml

import bec_lib
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.service_config import ServiceConfig
from bec_server.device_server.device_server import DeviceServer


@pytest.fixture(scope="session")
def test_config_yaml_file_path():
    return os.path.join(os.path.dirname(bec_lib.__file__), "tests/test_config.yaml")


@pytest.fixture(scope="session")
def test_config_yaml(test_config_yaml_file_path):
    with open(test_config_yaml_file_path, "r") as config_yaml_file:
        return yaml.safe_load(config_yaml_file)


def set_redis_config(self, config):
    msg = messages.AvailableResourceMessage(resource=config)
    self.connector.set(MessageEndpoints.device_config(), msg)


def _convert_to_db_config(yaml_config: dict) -> None:
    for name, config in yaml_config.items():
        if "deviceConfig" in config and config["deviceConfig"] is None:
            config["deviceConfig"] = {}
        config["name"] = name


@pytest.fixture
def fakeredis_connector(connected_connector):
    return lambda x: connected_connector


@pytest.fixture
def device_server(connected_connector, test_config_yaml, fakeredis_connector):
    _convert_to_db_config(test_config_yaml)
    msg = messages.AvailableResourceMessage(resource=list(test_config_yaml.values()))
    connected_connector.set(MessageEndpoints.device_config(), msg)
    ds = DeviceServer(config=ServiceConfig(), connector_cls=fakeredis_connector)
    ds.start()
    yield ds
    ds.shutdown()


def test_device_server_init(device_server):
    assert device_server.status == messages.BECStatus.RUNNING


@pytest.mark.parametrize(
    "msg, response",
    [
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": "test"},
            ),
            messages.DeviceInstructionResponse(
                metadata={"device_instr_id": "test"},
                device="samx",
                status="completed",
                error_message=None,
                instruction=messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": 1},
                    metadata={"device_instr_id": "test"},
                ),
                instruction_id="test",
                result=None,
            ),
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": -5000},
                metadata={"device_instr_id": "test"},
            ),
            messages.DeviceInstructionResponse(
                metadata={"device_instr_id": "test"},
                device="samx",
                status="error",
                error_message="position=-5000 not within limits (-50, 50)",
                instruction=messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": -5000},
                    metadata={"device_instr_id": "test"},
                ),
                instruction_id="test",
                result=None,
            ),
        ),
    ],
)
def test_device_server_set(device_server, msg, response):

    wait_event = threading.Event()
    message = None

    def callback(msg):
        nonlocal message
        message = msg
        if msg.value.status in ["completed", "error"]:
            wait_event.set()

    device_server.connector.register(MessageEndpoints.device_instructions_response(), cb=callback)
    device_server.connector.send(MessageEndpoints.device_instructions(), msg)

    wait_event.wait(10)
    out = message.value
    orig_error_message = out.error_message
    response_error_message = response.error_message
    out.error_message = mock.ANY
    response.error_message = mock.ANY
    assert message.value == response
    if out.status == "error":
        assert response_error_message in orig_error_message
