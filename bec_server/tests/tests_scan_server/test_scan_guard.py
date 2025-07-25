from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject
from bec_server.scan_server.scan_guard import ScanGuard, ScanRejection, ScanStatus
from bec_server.scan_server.scan_queue import ScanQueueStatus
from bec_server.scan_server.tests.fixtures import scan_server_mock


@pytest.fixture
def scan_guard_mock(scan_server_mock):
    sg = ScanGuard(parent=scan_server_mock)
    sg.device_manager.connector = mock.MagicMock()
    yield sg


@pytest.mark.parametrize(
    "scan_queue_msg",
    [
        (
            messages.ScanQueueMessage(
                scan_type="fermat_scan",
                parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
                queue="primary",
            )
        ),
        (
            messages.ScanQueueMessage(
                scan_type="device_rpc",
                parameter={"device": "samy", "args": {}, "kwargs": {}},
                queue="primary",
            )
        ),
        (
            messages.ScanQueueMessage(
                scan_type="device_rpc",
                parameter={"device": ["samy"], "args": {}, "kwargs": {}},
                queue="primary",
            )
        ),
    ],
)
def test_check_motors_movable_enabled(scan_server_mock, scan_queue_msg):
    k = scan_server_mock

    sg = ScanGuard(parent=k)
    sg._check_motors_movable(scan_queue_msg)
    config_reply = messages.RequestResponseMessage(accepted=True, message="")
    with mock.patch.object(
        k.device_manager.config_helper, "wait_for_config_reply", return_value=config_reply
    ):
        with mock.patch.object(k.device_manager.config_helper, "wait_for_service_response"):
            k.device_manager.devices["samx"].enabled = True
            k.device_manager.devices["samy"].enabled = False
            with pytest.raises(ScanRejection) as scan_rejection:
                sg._check_motors_movable(scan_queue_msg)
            assert "Device samy is not enabled." in scan_rejection.value.args


@pytest.mark.parametrize("device,func,is_valid", [("samx", "read", True)])
def test_device_rpc_is_valid(scan_guard_mock, device, func, is_valid):
    sg = scan_guard_mock
    assert sg._device_rpc_is_valid(device, func) == is_valid


@pytest.mark.parametrize(
    "scan_queue_msg,valid",
    [
        (
            messages.ScanQueueMessage(
                scan_type="fermat_scan",
                parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
                queue="primary",
            ),
            True,
        ),
        (
            messages.ScanQueueMessage(
                scan_type="device_rpc",
                parameter={"device": "samy", "args": {}, "kwargs": {}},
                queue="primary",
            ),
            True,
        ),
        (
            messages.ScanQueueMessage(
                scan_type="device_rpc",
                parameter={"device": ["samy"], "args": {}, "kwargs": {}},
                queue="primary",
            ),
            True,
        ),
    ],
)
def test_valid_request(scan_server_mock, scan_queue_msg, valid):
    k = scan_server_mock

    sg = ScanGuard(parent=k)
    config_reply = messages.RequestResponseMessage(accepted=True, message="")
    with mock.patch.object(
        k.device_manager.config_helper, "wait_for_config_reply", return_value=config_reply
    ):
        with mock.patch.object(k.device_manager.config_helper, "wait_for_service_response"):
            with mock.patch.object(sg, "_check_valid_scan") as valid_scan:
                k.device_manager.devices["samx"].enabled = True
                k.device_manager.devices["samy"].enabled = True
                status = sg._is_valid_scan_request(scan_queue_msg)
                valid_scan.assert_called_once_with(scan_queue_msg)
                assert status.accepted == valid


def test_check_valid_scan_raises_for_unknown_scan(scan_guard_mock):
    sg = scan_guard_mock
    sg.connector = mock.MagicMock()
    sg.connector.get.return_value = messages.AvailableResourceMessage(
        resource={"fermat_scan": "fermat_scan"}
    )

    request = messages.ScanQueueMessage(
        scan_type="unknown_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )

    with pytest.raises(ScanRejection) as scan_rejection:
        sg._check_valid_scan(request)


def test_check_valid_scan_accepts_known_scan(scan_guard_mock):
    sg = scan_guard_mock
    sg.connector = mock.MagicMock()
    sg.connector.get.return_value = messages.AvailableResourceMessage(
        resource={"fermat_scan": "fermat_scan"}
    )

    request = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )

    sg._check_valid_scan(request)


def test_check_valid_scan_device_rpc(scan_guard_mock):
    sg = scan_guard_mock
    sg.connector = mock.MagicMock()
    sg.connector.get.return_value = messages.AvailableResourceMessage(
        resource={"device_rpc": "device_rpc"}
    )
    request = messages.ScanQueueMessage(
        scan_type="device_rpc",
        parameter={"device": "samy", "func": "read", "args": {}, "kwargs": {}},
        queue="primary",
    )
    with mock.patch.object(sg, "_device_rpc_is_valid") as rpc_valid:
        sg._check_valid_scan(request)
        rpc_valid.assert_called_once_with(device="samy", func="read")


def test_check_valid_scan_device_rpc_raises(scan_guard_mock):
    sg = scan_guard_mock
    sg.connector = mock.MagicMock()
    sg.connector.get.return_value = messages.AvailableResourceMessage(
        resource={"device_rpc": "device_rpc"}
    )
    request = messages.ScanQueueMessage(
        scan_type="device_rpc",
        parameter={"device": "samy", "func": "read", "args": {}, "kwargs": {}},
        queue="primary",
    )
    with pytest.raises(ScanRejection) as scan_rejection:
        with mock.patch.object(sg, "_device_rpc_is_valid") as rpc_valid:
            rpc_valid.return_value = False
            sg._check_valid_scan(request)
            rpc_valid.assert_called_once_with(device="samy", func="read")
        assert "Rejected rpc: " in scan_rejection.value.args


def test_handle_scan_modification_request(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueModificationMessage(
        scan_id="scan_id", action="abort", parameter={}, metadata={"RID": "RID"}
    )
    with mock.patch.object(sg.device_manager.connector, "send") as send:
        sg._handle_scan_modification_request(msg)
        send.assert_called_once_with(MessageEndpoints.scan_queue_modification(), msg)


def test_handle_scan_modification_request_restart(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueModificationMessage(
        scan_id="scan_id", action="restart", parameter={"RID": "RID"}, metadata={"RID": "new_RID"}
    )
    with mock.patch.object(sg, "_send_scan_request_response") as send_response:
        with mock.patch("bec_server.scan_server.scan_guard.ScanStatus") as scan_status:
            sg._handle_scan_modification_request(msg)
            send_response.assert_called_once_with(scan_status(), {"RID": "RID"})


def test_append_to_scan_queue(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    with mock.patch.object(sg.device_manager.connector, "send") as send:
        sg._append_to_scan_queue(msg)
        send.assert_called_once_with(MessageEndpoints.scan_queue_insert(), msg)


def test_scan_queue_request_callback(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    msg_obj = MessageObject(MessageEndpoints.scan_queue_request(), msg)
    with mock.patch.object(sg, "_handle_scan_request") as handle:
        sg._scan_queue_request_callback(msg_obj, sg)
        handle.assert_called_once_with(msg)


def test_scan_queue_modification_request_callback(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueModificationMessage(
        scan_id="scan_id", action="abort", parameter={}, metadata={"RID": "RID"}
    )
    msg_obj = MessageObject(MessageEndpoints.scan_queue_modification(), msg)
    with mock.patch.object(sg, "_handle_scan_modification_request") as handle:
        sg._scan_queue_modification_request_callback(msg_obj, sg)
        handle.assert_called_once_with(msg)


def test_send_scan_request_response(scan_guard_mock):
    sg = scan_guard_mock
    with mock.patch.object(sg.device_manager.connector, "send") as send:
        sg._send_scan_request_response(ScanStatus(), {"RID": "RID"})
        send.assert_called_once_with(
            MessageEndpoints.scan_queue_request_response(),
            messages.RequestResponseMessage(accepted=True, message="", metadata={"RID": "RID"}),
        )


def test_handle_scan_request(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    with mock.patch.object(sg, "_is_valid_scan_request") as valid:
        with mock.patch.object(sg, "_append_to_scan_queue") as append:
            valid.return_value = ScanStatus(accepted=True, message="")
            sg._handle_scan_request(msg)
            append.assert_called_once_with(msg)


@pytest.mark.parametrize(
    "msg",
    [
        messages.ScanQueueMessage(
            metadata={"RID": "ed2c85e4-d1ed-44d3-a1ed-ec99ea5991a2", "response": True},
            scan_type="device_rpc",
            parameter={
                "device": "samx",
                "rpc_id": "0e5c0ca1-e471-4e30-bb45-b94d4b713e2f",
                "func": "read",
                "args": [],
                "kwargs": {},
            },
            queue="primary",
        ),
        messages.ScanQueueMessage(
            metadata={"RID": "4be1449d-af68-457f-8e86-24fd9ddca803", "response": True},
            scan_type="device_rpc",
            parameter={
                "device": "hexapod",
                "rpc_id": "bc5fc2c3-540c-4881-b84e-89ba2c4ed3aa",
                "func": "x.read",
                "args": [],
                "kwargs": {},
            },
            queue="primary",
        ),
        messages.ScanQueueMessage(
            metadata={"RID": "ed2c85e4-d1ed-44d3-a1ed-ec99ea5991a2", "response": True},
            scan_type="device_rpc",
            parameter={
                "device": "samx",
                "rpc_id": "0e5c0ca1-e471-4e30-bb45-b94d4b713e2f",
                "func": "get",
                "args": [],
                "kwargs": {},
            },
            queue="primary",
        ),
        messages.ScanQueueMessage(
            metadata={"RID": "4be1449d-af68-457f-8e86-24fd9ddca803", "response": True},
            scan_type="device_rpc",
            parameter={
                "device": "hexapod",
                "rpc_id": "bc5fc2c3-540c-4881-b84e-89ba2c4ed3aa",
                "func": "x.get",
                "args": [],
                "kwargs": {},
            },
            queue="primary",
        ),
    ],
)
def test_handle_scan_request_bypassed_for_read(scan_guard_mock, msg):
    """
    Ensure that the .read and .get RPCs are bypassed in the scan guard.
    """
    sg = scan_guard_mock
    with mock.patch.object(sg.connector, "send") as send:
        with mock.patch.object(sg, "_is_valid_scan_request") as valid:
            with mock.patch.object(sg, "_append_to_scan_queue") as append:
                valid.return_value = ScanStatus(accepted=True, message="")
                sg._handle_scan_request(msg)
                append.assert_not_called()
                send.assert_called_once_with(MessageEndpoints.device_instructions(), mock.ANY)


def test_handle_scan_request_rejected(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    with mock.patch.object(sg, "_is_valid_scan_request") as valid:
        with mock.patch.object(sg, "_append_to_scan_queue") as append:
            valid.return_value = ScanStatus(accepted=False, message="")
            sg._handle_scan_request(msg)
            append.assert_not_called()


def test_is_valid_scan_request_returns_scan_status_on_error(scan_guard_mock):
    sg = scan_guard_mock
    msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    with mock.patch.object(sg, "_check_valid_scan") as valid:
        valid.side_effect = Exception("Test exception")
        status = sg._is_valid_scan_request(msg)
        assert status.accepted == False
        assert "Test exception" in status.message


def test_check_valid_request_raises_for_empty_request(scan_guard_mock):
    sg = scan_guard_mock
    with pytest.raises(ScanRejection) as scan_rejection:
        sg._check_valid_request(None)
    assert "Invalid request." in scan_rejection.value.args


@pytest.mark.parametrize(
    "msg, queue_paused, valid, response_message",
    [
        (
            messages.ScanQueueOrderMessage(
                queue="primary", scan_id="scan_id", action="move_bottom"
            ),
            True,
            True,
            "",
        ),
        (
            messages.ScanQueueOrderMessage(
                queue="primary", scan_id="scan_id", action="move_bottom"
            ),
            False,
            False,
            "Queue primary is not paused. Cannot move scans.",
        ),
        (
            messages.ScanQueueOrderMessage(queue="wrong", scan_id="scan_id", action="move_bottom"),
            False,
            False,
            "Invalid queue: wrong",
        ),
        (
            messages.ScanQueueOrderMessage(queue="primary", scan_id="scan_id", action="move_to"),
            True,
            False,
            "Missing target_position for move_to",
        ),
        (
            messages.ScanQueueOrderMessage(
                queue="primary", scan_id="wrong_scan_id", action="move_bottom"
            ),
            True,
            False,
            "Scan wrong_scan_id not found in queue primary",
        ),
    ],
)
def test_check_queue_order_callback(scan_guard_mock, msg, queue_paused, valid, response_message):
    sg = scan_guard_mock

    class MockQueue:
        def __init__(self):
            self.queue = [MockInstructionItem()]
            self.status = ScanQueueStatus.PAUSED if queue_paused else ScanQueueStatus.RUNNING

    class MockInstructionItem:
        def __init__(self):
            self.queue = MockRequestBlockQueue()

    class MockRequestBlockQueue:
        def __init__(self):
            self.scan_id = "scan_id"

    sg.parent.queue_manager.queues = {"primary": MockQueue()}
    msg_obj = MessageObject(MessageEndpoints.scan_queue_order_change_request(), msg)
    sg._scan_queue_order_callback(msg_obj, sg)
    success_call = mock.call(MessageEndpoints.scan_queue_order_change(), msg)
    if valid:
        assert success_call in sg.device_manager.connector.send.mock_calls
    else:
        assert (
            mock.call(
                MessageEndpoints.scan_queue_order_change_response(),
                messages.RequestResponseMessage(accepted=False, message=response_message),
            )
            in sg.device_manager.connector.send.mock_calls
        )
        assert success_call not in sg.device_manager.connector.send.calls
