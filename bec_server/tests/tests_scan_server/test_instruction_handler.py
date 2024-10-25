from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.instruction_handler import InstructionHandler


@pytest.fixture
def instruction_handler():
    handler = InstructionHandler(mock.MagicMock(spec=RedisConnector))
    yield handler


@pytest.fixture
def instruction_message():
    return messages.DeviceInstructionResponse(
        instruction_id="test_instruction_id",
        instruction=messages.DeviceInstructionMessage(
            device="test_device_id", action="set", parameter={"test_parameter": "test_value"}
        ),
        device="test_device_id",
        status="completed",
    )


@pytest.mark.parametrize(
    "msg, popped",
    [
        (
            messages.DeviceInstructionResponse(
                instruction_id="test_instruction_id",
                instruction=messages.DeviceInstructionMessage(
                    device="test_device_id",
                    action="set",
                    parameter={"test_parameter": "test_value"},
                ),
                device="test_device_id",
                status="running",
            ),
            False,
        ),
        (
            messages.DeviceInstructionResponse(
                instruction_id="test_instruction_id",
                instruction=messages.DeviceInstructionMessage(
                    device="test_device_id",
                    action="set",
                    parameter={"test_parameter": "test_value"},
                ),
                device="test_device_id",
                status="completed",
            ),
            True,
        ),
    ],
)
def test_add_instruction(instruction_handler, msg, popped):
    instruction_handler.add_instruction(msg)
    if popped:
        assert instruction_handler._instruction_storage.get(msg.instruction_id) is None
    else:
        assert instruction_handler._instruction_storage[msg.instruction_id] == msg


def test_add_instruction_with_callback(instruction_handler, instruction_message):
    msg = instruction_message
    callback = mock.MagicMock()
    instruction_handler.register_callback(msg.instruction_id, callback)
    instruction_handler.add_instruction(msg)
    callback.assert_called_once()


def test_add_instruction_with_multiple_callbacks(instruction_handler, instruction_message):
    msg = instruction_message
    callback1 = mock.MagicMock()
    callback2 = mock.MagicMock()
    instruction_handler.register_callback(msg.instruction_id, callback1)
    instruction_handler.register_callback(msg.instruction_id, callback2)
    instruction_handler.add_instruction(msg)
    callback1.assert_called_once()
    callback2.assert_called_once()
