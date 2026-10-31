"""Service acknowledgement is separate from hardware completion."""

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
