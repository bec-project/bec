import threading
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.tests.utils import ConnectorMock
from bec_server.device_server.tests.utils import DMMock
from bec_server.scan_server.errors import DeviceMessageError
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scan_stubs import ScanStubs
from bec_server.scan_server.tests.fixtures import ScanStubStatusMock


@pytest.fixture
def stubs():
    connector = ConnectorMock("")
    device_manager = DMMock()
    device_manager.add_device("rtx")
    device_manager.add_device("samx")
    device_manager.add_device("samy")
    instruction_handler = InstructionHandler(connector)
    shutdown_event = threading.Event()
    yield ScanStubs(
        device_manager=device_manager,
        instruction_handler=instruction_handler,
        connector=connector,
        shutdown_event=shutdown_event,
    )
    shutdown_event.set()


@pytest.mark.parametrize(
    "device,parameter,metadata,reference_msg",
    [
        (
            "rtx",
            None,
            None,
            messages.DeviceInstructionMessage(
                device="rtx", action="kickoff", parameter={"configure": {}}, metadata={}
            ),
        ),
        (
            "rtx",
            {"num_pos": 5, "positions": [1, 2, 3, 4, 5], "exp_time": 2},
            None,
            messages.DeviceInstructionMessage(
                device="rtx",
                action="kickoff",
                parameter={
                    "configure": {"num_pos": 5, "positions": [1, 2, 3, 4, 5], "exp_time": 2}
                },
                metadata={},
            ),
        ),
    ],
)
def test_kickoff(stubs, device, parameter, metadata, reference_msg):
    msg = list(stubs.kickoff(device=device, parameter=parameter, metadata=metadata, wait=False))
    reference_msg.metadata["device_instr_id"] = msg[0].metadata["device_instr_id"]
    assert msg[0] == reference_msg


@pytest.mark.parametrize(
    "msg, ret_value, raised_error",
    [
        (messages.ProgressMessage(value=10, max_value=100, done=False), None, False),
        (
            messages.ProgressMessage(
                value=10, max_value=100, done=False, metadata={"RID": "wrong"}
            ),
            None,
            False,
        ),
        (
            messages.ProgressMessage(value=10, max_value=100, done=False, metadata={"RID": "rid"}),
            10,
            False,
        ),
        (
            messages.DeviceStatusMessage(device="samx", status=0, metadata={"RID": "rid"}),
            None,
            True,
        ),
    ],
)
def test_device_progress(stubs, msg, ret_value, raised_error):
    if raised_error:
        with pytest.raises(DeviceMessageError):
            with mock.patch.object(stubs.connector, "get", return_value=msg):
                assert stubs.get_device_progress(device="samx", RID="rid") == ret_value
        return
    with mock.patch.object(stubs.connector, "get", return_value=msg):
        assert stubs.get_device_progress(device="samx", RID="rid") == ret_value


def test_send_rpc_and_wait(stubs, ScanStubStatusMock):
    with mock.patch.object(stubs, "_get_result_from_status", return_value="msg") as get_rpc:
        original_rpc = stubs.send_rpc
        with mock.patch.object(stubs, "send_rpc") as mock_rpc:

            def mock_rpc_func(*args, **kwargs):
                yield from original_rpc(*args, **kwargs)
                return ScanStubStatusMock(lambda: iter([True]))

            mock_rpc.side_effect = mock_rpc_func

            instructions = list(stubs.send_rpc_and_wait("sim_profile", "readback_profile"))
            rpc_call_1 = instructions[0]
            instructions = list(stubs.send_rpc_and_wait("sim_profile", "readback_profile"))
            rpc_call_2 = instructions[0]
            assert rpc_call_1 != rpc_call_2
            assert rpc_call_1.metadata["device_instr_id"] != rpc_call_2.metadata["device_instr_id"]
