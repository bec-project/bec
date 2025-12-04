import threading
import time
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.errors import DeviceInstructionError
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scan_stubs import ScanStubStatus


@pytest.fixture
def instruction_handler(connected_connector):
    handler = InstructionHandler(connector=connected_connector)
    yield handler


@pytest.fixture
def scan_stub_status(instruction_handler):
    return ScanStubStatus(instruction_handler)


def test_scan_stub_status_init(scan_stub_status):
    assert scan_stub_status.done is False


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionResponse(
            metadata={"device_instr_id": "test"},
            device="samx",
            status="completed",
            error_info=None,
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": "test"},
            ),
            instruction_id="test",
            result=None,
        ),
        messages.DeviceInstructionResponse(
            metadata={"device_instr_id": "test"},
            device="samx",
            status="completed",
            error_info=None,
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": "test"},
            ),
            instruction_id="test",
            result=None,
        ),
    ],
)
def test_scan_stub_status_update_future_completed(msg, scan_stub_status):
    with mock.patch.object(scan_stub_status, "set_done") as set_done:
        scan_stub_status._update_future(msg)
        set_done.assert_called_once_with(msg.result)


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionResponse(
            metadata={"device_instr_id": "test"},
            device="samx",
            status="error",
            error_info=None,
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": "test"},
            ),
            instruction_id="test",
            result=None,
        )
    ],
)
def test_scan_stub_status_update_future_error(msg, scan_stub_status):
    with mock.patch.object(scan_stub_status, "set_failed") as set_failed:
        scan_stub_status._update_future(msg)
        set_failed.assert_called_once_with(msg.error_info)


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionResponse(
            metadata={"device_instr_id": "test"},
            device="samx",
            status="running",
            error_info=None,
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": "test"},
            ),
            instruction_id="test",
            result=None,
        )
    ],
)
def test_scan_stub_status_update_future_running(msg, scan_stub_status):
    with mock.patch.object(scan_stub_status, "set_running") as set_running:
        scan_stub_status._update_future(msg)
        set_running.assert_called_once_with()


def test_scan_stub_status_set_done(scan_stub_status):
    scan_stub_status.set_done(10)
    assert scan_stub_status.done is True
    assert scan_stub_status.result == 10
    scan_stub_status.wait()


def test_scan_stub_status_set_failed(scan_stub_status):
    error_info = messages.ErrorInfo(
        error_message="Error occurred",
        compact_error_message="Error occurred",
        exception_type="ValueError",
        device="samx",
    )
    scan_stub_status.set_failed(error_info)
    assert scan_stub_status.done is True
    with pytest.raises(DeviceInstructionError) as exc_info:
        scan_stub_status.wait()
    assert str(exc_info.value) == "Error occurred"


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionResponse(
            metadata={"device_instr_id": "test"},
            device="samx",
            status="running",
            error_info=None,
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": "test"},
            ),
            instruction_id="test",
            result=None,
        )
    ],
)
def test_scan_stub_status_set_running(msg, scan_stub_status):
    scan_stub_status._update_future(msg)
    assert scan_stub_status.done is False

    out = repr(scan_stub_status)
    assert (
        out
        == f"ScanStubStatus({scan_stub_status._device_instr_id}, action=set, devices=samx, done=False)"
    )


def test_scan_stub_status_wait(scan_stub_status):

    def send_message(connector: RedisConnector, diid: str):
        time.sleep(2)
        msg = messages.DeviceInstructionResponse(
            metadata={"device_instr_id": diid},
            device="samx",
            status="completed",
            error_info=None,
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": diid},
            ),
            instruction_id=diid,
            result=None,
        )
        connector.send(topic=MessageEndpoints.device_instructions_response(), msg=msg)

    threading.Thread(
        target=send_message,
        args=(scan_stub_status._instruction_handler._connector, scan_stub_status._device_instr_id),
    ).start()
    scan_stub_status.wait()
    assert scan_stub_status.done is True


def test_scan_stub_status_wait_timeout(scan_stub_status):
    with pytest.raises(TimeoutError):
        scan_stub_status.wait(timeout=0.1)


def test_scan_stub_status_wait_error(scan_stub_status):

    def send_message(connector: RedisConnector, diid: str):
        time.sleep(2)
        msg = messages.DeviceInstructionResponse(
            metadata={"device_instr_id": diid},
            device="samx",
            status="error",
            error_info=messages.ErrorInfo(
                error_message="Error message",
                compact_error_message="Error message",
                exception_type="ValueError",
                device="samx",
            ),
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": diid},
            ),
            instruction_id=diid,
            result=None,
        )
        connector.send(topic=MessageEndpoints.device_instructions_response(), msg=msg)

    threading.Thread(
        target=send_message,
        args=(scan_stub_status._instruction_handler._connector, scan_stub_status._device_instr_id),
    ).start()
    with pytest.raises(DeviceInstructionError) as exc_info:
        scan_stub_status.wait()
    assert str(exc_info.value) == "Error message"


def test_scan_stub_status_wait_min_time(scan_stub_status):
    scan_stub_status.set_done()
    start = time.time()
    scan_stub_status.wait(min_wait=1)
    end = time.time()
    assert end - start >= 1


def test_scan_stub_status_wait_log_remaining(scan_stub_status):

    def send_message(connector: RedisConnector, diid: str):
        time.sleep(2)
        msg = messages.DeviceInstructionResponse(
            metadata={"device_instr_id": diid},
            device="samx",
            status="completed",
            error_info={
                "error_message": "Error message",
                "compact_error_message": "Error message",
                "exception_type": "ValueError",
                "device": "samx",
            },
            instruction=messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1},
                metadata={"device_instr_id": diid},
            ),
            instruction_id=diid,
            result=None,
        )
        connector.send(topic=MessageEndpoints.device_instructions_response(), msg=msg)

    threading.Thread(
        target=send_message,
        args=(scan_stub_status._instruction_handler._connector, scan_stub_status._device_instr_id),
    ).start()
    with mock.patch("bec_server.scan_server.scan_stubs.logger") as logger:
        scan_stub_status.wait(logger_wait=0.1)
        assert (
            f"Waiting for the completion of the following status objects: ['ScanStubStatus({scan_stub_status._device_instr_id}, done=False)']"
            in logger.info.call_args_list[0].args[0]
        )

        assert (
            f"Waiting for the completion of the following status objects: ['ScanStubStatus({scan_stub_status._device_instr_id}, action=set, devices=samx, done=True)']"
            in logger.info.call_args_list[-1].args[0]
        )


def test_stub_status_as_container(instruction_handler):
    container = ScanStubStatus(instruction_handler, is_container=True)
    sub_status = ScanStubStatus(instruction_handler)
    container.add_status(sub_status)
    assert container.done is False
    sub_status.set_done()
    container.wait()
    assert container.done is True


def test_stub_status_as_container_results(instruction_handler):
    container = ScanStubStatus(instruction_handler, is_container=True)
    for ii in range(10):
        sub_status = ScanStubStatus(instruction_handler)
        container.add_status(sub_status)
        sub_status.set_done(ii)
    container.wait()
    assert sub_status._done_checked is True
    assert container._done_checked is True
    assert container.done is True
    assert container.result == list(range(10))


def test_stub_status_result_does_not_block(scan_stub_status):
    status = scan_stub_status
    assert status.done is False
    assert status.result is None


def test_stub_status_repr(instruction_handler):
    status = ScanStubStatus(instruction_handler, name="test")
    assert repr(status) == f"ScanStubStatus(test, {status._device_instr_id}, done=False)"

    status = ScanStubStatus(instruction_handler)
    assert repr(status) == f"ScanStubStatus({status._device_instr_id}, done=False)"

    status = ScanStubStatus(instruction_handler)
    status.message = messages.DeviceInstructionResponse(
        metadata={"device_instr_id": "test_diid"},
        device="samx",
        status="completed",
        error_info={
            "error_message": "Error message",
            "compact_error_message": "Error message",
            "exception_type": "ValueError",
            "device": "samx",
        },
        instruction=messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={"value": 1},
            metadata={"device_instr_id": "test_diid"},
        ),
        instruction_id="test_diid",
        result=None,
    )
    assert (
        repr(status)
        == f"ScanStubStatus({status._device_instr_id}, action=set, devices=samx, done=False)"
    )

    status = ScanStubStatus(instruction_handler, name="test_name")
    status.message = messages.DeviceInstructionResponse(
        metadata={"device_instr_id": "test_diid"},
        device="samx",
        status="completed",
        error_info=None,
        instruction=messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={"value": 1},
            metadata={"device_instr_id": "test_diid"},
        ),
        instruction_id="test_diid",
        result=10,
    )
    assert (
        repr(status)
        == f"ScanStubStatus(test_name, {status._device_instr_id}, action=set, devices=samx, done=False)"
    )
