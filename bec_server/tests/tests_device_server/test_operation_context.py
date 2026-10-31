"""Service acknowledgement is separate from hardware completion."""

import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.device_backend import DeviceIdentity, Operation
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.device_server import RequestHandler
from bec_server.device_server.operation_context import OperationContext


@pytest.mark.parametrize("fail_service", [False, True])
def test_batch_is_terminal_only_after_all_service_callbacks(fail_service):
    queued = []
    connector = ConnectorMock("", store_data=False)
    parent = SimpleNamespace(
        connector=connector, status_callback=mock.Mock(), dispatch_status_response=mock.Mock()
    )
    requests = RequestHandler(parent)
    instruction = messages.DeviceInstructionMessage(
        device=["first", "second"],
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "batch"},
    )
    requests.add_request(instruction, 2)
    operations = [Operation[None](queued.append), Operation[None](queued.append)]
    contexts = [
        OperationContext(operation, instruction, SimpleNamespace(identity=DeviceIdentity(name)))
        for operation, name in zip(operations, ("first", "second"))
    ]
    for context in contexts:
        requests.add_status_object("batch", context)
    for operation in operations:
        operation.set_result(None)
    queued.pop(0)()
    assert requests.get_request("batch") is not None
    assert contexts[0].service_done.is_set()
    assert not contexts[1].processed
    if fail_service:
        parent.status_callback.side_effect = RuntimeError("cache publication failed")
    queued.pop(0)()
    assert requests.get_request("batch") is None
    terminal = [
        entry["msg"]
        for entry in connector.message_sent
        if getattr(entry["msg"], "status", None) in ("completed", "error")
    ]
    assert len(terminal) == 1
    assert terminal[0].status == ("error" if fail_service else "completed")
    assert contexts[1].service_done.is_set()
    if fail_service:
        assert "cache publication failed" in terminal[0].error_info.error_message


@pytest.mark.parametrize("second_already_done", [False, True])
def test_inline_service_failure_does_not_finish_batch_during_peer_registration(second_already_done):
    connector = ConnectorMock("", store_data=False)
    parent = SimpleNamespace(
        connector=connector,
        status_callback=mock.Mock(side_effect=[RuntimeError("file publication failed"), None]),
        dispatch_status_response=mock.Mock(),
    )
    requests = RequestHandler(parent)
    instruction = messages.DeviceInstructionMessage(
        device=["first", "second"],
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "batch"},
    )
    requests.add_request(instruction, 2)
    first = OperationContext(
        Operation.finished(None), instruction, SimpleNamespace(identity=DeviceIdentity("first"))
    )
    requests.add_status_object("batch", first)
    assert first.service_done.is_set()
    assert requests.get_request("batch") is not None
    assert first.service_error is not None
    second_operation = Operation[None]()
    if second_already_done:
        second_operation.set_result(None)
    second = OperationContext(
        second_operation, instruction, SimpleNamespace(identity=DeviceIdentity("second"))
    )
    requests.add_status_object("batch", second)
    if not second_already_done:
        assert requests.get_request("batch") is not None
        second_operation.set_result(None)
    assert requests.get_request("batch") is None
    terminal = [
        entry["msg"]
        for entry in connector.message_sent
        if getattr(entry["msg"], "status", None) in ("completed", "error")
    ]
    assert len(terminal) == 1
    assert terminal[0].status == "error"
    assert "file publication failed" in terminal[0].error_info.error_message
    assert "KeyError" not in terminal[0].error_info.error_message


def test_peer_cannot_finish_batch_before_error_publication_is_ready():
    queued = []
    entered, release = threading.Event(), threading.Event()
    connector = ConnectorMock("", store_data=False)

    def callback(context):
        if context.target.identity.root == "first":
            raise RuntimeError("publication failed")

    def publish_error(*args, **kwargs):
        entered.set()
        assert release.wait(2)

    parent = SimpleNamespace(
        connector=connector, status_callback=callback, dispatch_status_response=publish_error
    )
    requests = RequestHandler(parent)
    instruction = messages.DeviceInstructionMessage(
        device=["first", "second"],
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "batch"},
    )
    requests.add_request(instruction, 2)
    operations = [Operation[None](queued.append), Operation[None](queued.append)]
    contexts = [
        OperationContext(operation, instruction, SimpleNamespace(identity=DeviceIdentity(name)))
        for operation, name in zip(operations, ("first", "second"))
    ]
    for context in contexts:
        requests.add_status_object("batch", context)
    for operation in operations:
        operation.set_result(None)
    first = threading.Thread(target=queued.pop(0))
    first.start()
    try:
        assert entered.wait(2)
        queued.pop(0)()
        assert requests.get_request("batch") is not None
        assert not contexts[0].processed
    finally:
        release.set()
        first.join(2)
    assert requests.get_request("batch") is None
    terminal = [
        entry["msg"]
        for entry in connector.message_sent
        if getattr(entry["msg"], "status", None) in ("completed", "error")
    ]
    assert len(terminal) == 1
    assert terminal[0].error_info is contexts[0].service_error_info


def test_secondary_error_publication_failure_does_not_skip_request_aggregation():
    connector = ConnectorMock("", store_data=False)
    parent = SimpleNamespace(
        connector=connector,
        status_callback=mock.Mock(side_effect=RuntimeError("primary failure")),
        dispatch_status_response=mock.Mock(side_effect=ConnectionError("secondary failure")),
    )
    requests = RequestHandler(parent)
    instruction = messages.DeviceInstructionMessage(
        device="device", action="trigger", parameter={}, metadata={"device_instr_id": "request"}
    )
    requests.add_request(instruction, 1)
    context = OperationContext(
        Operation.finished(None), instruction, SimpleNamespace(identity=DeviceIdentity("device"))
    )
    requests.add_status_object("request", context)
    assert context.service_done.is_set()
    assert context.processed
    assert requests.get_request("request") is None
    terminal = [
        entry["msg"]
        for entry in connector.message_sent
        if getattr(entry["msg"], "status", None) == "error"
    ]
    assert len(terminal) == 1
    assert "primary failure" in terminal[0].error_info.error_message
