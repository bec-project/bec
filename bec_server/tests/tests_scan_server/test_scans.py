import collections
import inspect
from unittest import mock

import numpy as np
import pytest

from bec_lib import messages
from bec_server.device_server.tests.utils import DMMock
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scan_plugins.otf_scan import OTFScan
from bec_server.scan_server.scans import (
    Acquire,
    CloseInteractiveScan,
    ContLineFlyScan,
    ContLineScan,
    DeviceRPC,
    FermatSpiralScan,
    InteractiveReadMontiored,
    InteractiveTrigger,
    LineScan,
    ListScan,
    MonitorScan,
    Move,
    OpenInteractiveScan,
    RequestBase,
    RoundROIScan,
    RoundScan,
    RoundScanFlySim,
    Scan,
    ScanBase,
    TimeScan,
    UpdatedMove,
    get_2D_raster_pos,
    get_fermat_spiral_pos,
    get_round_roi_scan_positions,
    get_round_scan_positions,
    unpack_scan_args,
)

# the following imports are fixtures that are used in the tests
from bec_server.scan_server.tests.fixtures import *

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access


def test_unpack_scan_args_empty_dict():
    scan_args = {}
    expected_args = []
    assert unpack_scan_args(scan_args) == expected_args


def test_unpack_scan_args_non_dict_input():
    scan_args = [1, 2, 3]
    assert unpack_scan_args(scan_args) == scan_args


def test_unpack_scan_args_valid_input():
    scan_args = {"cmd1": [1, 2, 3], "cmd2": ["a", "b", "c"]}
    expected_args = ["cmd1", 1, 2, 3, "cmd2", "a", "b", "c"]
    assert unpack_scan_args(scan_args) == expected_args


@pytest.mark.parametrize(
    "mv_msg,reference_msg_list",
    [
        (
            messages.ScanQueueMessage(
                scan_type="mv",
                parameter={"args": {"samx": (1,), "samy": (2,)}, "kwargs": {}},
                queue="primary",
            ),
            [
                messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": 1},
                    metadata={"readout_priority": "monitored", "response": True},
                ),
                messages.DeviceInstructionMessage(
                    device="samy",
                    action="set",
                    parameter={"value": 2},
                    metadata={"readout_priority": "monitored", "response": True},
                ),
            ],
        ),
        (
            messages.ScanQueueMessage(
                scan_type="mv",
                parameter={"args": {"samx": (1,), "samy": (2,), "samz": (3,)}, "kwargs": {}},
                queue="primary",
            ),
            [
                messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": 1},
                    metadata={"readout_priority": "monitored", "response": True},
                ),
                messages.DeviceInstructionMessage(
                    device="samy",
                    action="set",
                    parameter={"value": 2},
                    metadata={"readout_priority": "monitored", "response": True},
                ),
                messages.DeviceInstructionMessage(
                    device="samz",
                    action="set",
                    parameter={"value": 3},
                    metadata={"readout_priority": "monitored", "response": True},
                ),
            ],
        ),
        (
            messages.ScanQueueMessage(
                scan_type="mv", parameter={"args": {"samx": (1,)}, "kwargs": {}}, queue="primary"
            ),
            [
                messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": 1},
                    metadata={"readout_priority": "monitored", "response": True},
                )
            ],
        ),
    ],
)
def test_scan_move(mv_msg, reference_msg_list, scan_assembler):

    def offset_mock():
        yield None

    s = scan_assembler(Move, parameter=mv_msg.content.get("parameter"))

    s._set_position_offset = offset_mock
    msg_list_reference = []
    for msg in list(s.run()):
        if msg is None:
            continue
        msg.metadata.pop("scan_id", None)
        msg.metadata.pop("device_instr_id", None)
        msg_list_reference.append(msg)

    assert msg_list_reference == reference_msg_list


@pytest.mark.parametrize(
    "mv_msg, reference_msg_list",
    [
        (
            messages.ScanQueueMessage(
                scan_type="umv",
                parameter={"args": {"samx": (1,), "samy": (2,)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "0bab7ee3-b384-4571-b...0fff984c05"},
            ),
            [
                messages.DeviceInstructionMessage(
                    device=None,
                    action="scan_report_instruction",
                    parameter={
                        "readback": {
                            "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                            "devices": ["samx", "samy"],
                            "start": [0, 0],
                            "end": np.array([1.0, 2.0]),
                        }
                    },
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
                messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": 1.0},
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
                messages.DeviceInstructionMessage(
                    device="samy",
                    action="set",
                    parameter={"value": 2.0},
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
            ],
        ),
        (
            messages.ScanQueueMessage(
                scan_type="umv",
                parameter={"args": {"samx": (1,), "samy": (2,), "samz": (3,)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "0bab7ee3-b384-4571-b...0fff984c05"},
            ),
            [
                messages.DeviceInstructionMessage(
                    device=None,
                    action="scan_report_instruction",
                    parameter={
                        "readback": {
                            "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                            "devices": ["samx", "samy", "samz"],
                            "start": [0, 0, 0],
                            "end": np.array([1.0, 2.0, 3.0]),
                        }
                    },
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
                messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": 1.0},
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
                messages.DeviceInstructionMessage(
                    device="samy",
                    action="set",
                    parameter={"value": 2.0},
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
                messages.DeviceInstructionMessage(
                    device="samz",
                    action="set",
                    parameter={"value": 3.0},
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
            ],
        ),
        (
            messages.ScanQueueMessage(
                scan_type="umv",
                parameter={"args": {"samx": (1,)}, "kwargs": {}},
                queue="primary",
                metadata={"RID": "0bab7ee3-b384-4571-b...0fff984c05"},
            ),
            [
                messages.DeviceInstructionMessage(
                    device=None,
                    action="scan_report_instruction",
                    parameter={
                        "readback": {
                            "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                            "devices": ["samx"],
                            "start": [0],
                            "end": np.array([1.0]),
                        }
                    },
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
                messages.DeviceInstructionMessage(
                    device="samx",
                    action="set",
                    parameter={"value": 1.0},
                    metadata={
                        "readout_priority": "monitored",
                        "RID": "0bab7ee3-b384-4571-b...0fff984c05",
                    },
                ),
            ],
        ),
    ],
)
def test_scan_updated_move(mv_msg, reference_msg_list, scan_assembler, ScanStubStatusMock):
    msg_list = []

    s = scan_assembler(
        UpdatedMove, parameter=mv_msg.content.get("parameter"), metadata=mv_msg.metadata
    )

    with mock.patch.object(s.stubs, "_get_result_from_status") as mock_get_from_rpc:
        # set reading to expected start values from scan_report_instruction
        mock_get_from_rpc.return_value = {
            dev: {"value": value}
            for dev, value in zip(
                reference_msg_list[0].content["parameter"]["readback"]["devices"],
                reference_msg_list[0].content["parameter"]["readback"]["start"],
            )
        }

        def mock_rpc_func(*args, **kwargs):
            yield None
            return ScanStubStatusMock(lambda: iter([True]))

        with mock.patch.object(s.stubs, "send_rpc") as mock_rpc:
            mock_rpc.side_effect = mock_rpc_func
            for step in s.run():
                if step:
                    step.metadata.pop("device_instr_id", None)
                    msg_list.append(step)

        assert msg_list == reference_msg_list


@pytest.mark.parametrize(
    "scan_msg,reference_scan_list",
    [
        (
            messages.ScanQueueMessage(
                scan_type="grid_scan",
                parameter={"args": {"samx": (-5, 5, 3)}, "kwargs": {}},
                queue="primary",
            ),
            [
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["samx"],
                    action="read",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=None,
                    action="open_scan",
                    parameter={
                        "scan_motors": ["samx"],
                        "readout_priority": {
                            "monitored": ["samx"],
                            "baseline": [],
                            "on_request": [],
                            "async": [],
                        },
                        "num_points": 3,
                        "positions": [[-5.0], [0.0], [5.0]],
                        "scan_name": "grid_scan",
                        "scan_type": "step",
                    },
                ),
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="stage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "baseline"},
                    device=["rtx", "samy", "samz"],
                    action="read",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="set",
                    parameter={"value": -5.0},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="pre_scan",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="set",
                    parameter={"value": -5.0},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 0},
                    device=["bpm4i", "eiger", "samx"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="set",
                    parameter={"value": 0.0},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 1},
                    device=["bpm4i", "eiger", "samx"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="set",
                    parameter={"value": 5.0},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 2},
                    device=["bpm4i", "eiger", "samx"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="complete",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="unstage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=None,
                    action="close_scan",
                    parameter={},
                ),
            ],
        )
    ],
)
def test_scan_scan(scan_msg, reference_scan_list, scan_assembler):
    device_manager = DMMock()
    device_manager.add_device("samx")
    device_manager.devices["samx"].readback.put(0)
    msg_list = []

    def offset_mock():
        yield None

    scan = scan_assembler(Scan, parameter=scan_msg.content.get("parameter"))

    scan._set_position_offset = offset_mock
    for step in scan.run():
        if step:
            step.metadata.pop("device_instr_id", None)
            msg_list.append(step)
    scan_uid = msg_list[0].metadata.get("scan_id")
    for ii, _ in enumerate(reference_scan_list):
        if reference_scan_list[ii].metadata.get("scan_id") is not None:
            reference_scan_list[ii].metadata["scan_id"] = scan_uid
    assert msg_list == reference_scan_list


@pytest.mark.parametrize(
    "scan_msg,reference_scan_list",
    [
        (
            messages.ScanQueueMessage(
                scan_type="fermat_scan",
                parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
                queue="primary",
            ),
            [
                (0, np.array([-1.1550884, -1.26090078])),
                (1, np.array([2.4090456, 0.21142208])),
                (2, np.array([-2.35049217, 1.80207841])),
                (3, np.array([0.59570227, -3.36772012])),
                (4, np.array([2.0522743, 3.22624707])),
                (5, np.array([-4.04502068, -1.08738572])),
                (6, np.array([4.01502502, -2.08525157])),
                (7, np.array([-1.6591442, 4.54313114])),
                (8, np.array([-1.95738438, -4.7418927])),
                (9, np.array([4.89775337, 2.29194501])),
            ],
        ),
        (
            messages.ScanQueueMessage(
                scan_type="fermat_scan",
                parameter={
                    "args": {"samx": (-5, 5), "samy": (-5, 5)},
                    "kwargs": {"step": 3, "spiral_type": 1},
                },
                queue="primary",
            ),
            [
                (0, np.array([1.1550884, 1.26090078])),
                (1, np.array([2.4090456, 0.21142208])),
                (2, np.array([2.35049217, -1.80207841])),
                (3, np.array([0.59570227, -3.36772012])),
                (4, np.array([-2.0522743, -3.22624707])),
                (5, np.array([-4.04502068, -1.08738572])),
                (6, np.array([-4.01502502, 2.08525157])),
                (7, np.array([-1.6591442, 4.54313114])),
                (8, np.array([1.95738438, 4.7418927])),
                (9, np.array([4.89775337, 2.29194501])),
            ],
        ),
    ],
)
def test_fermat_scan(scan_msg, reference_scan_list, scan_assembler):

    args = unpack_scan_args(scan_msg.content.get("parameter").get("args"))
    kwargs = scan_msg.content.get("parameter").get("kwargs")
    scan = scan_assembler(
        FermatSpiralScan, *args, parameter=scan_msg.content.get("parameter"), **kwargs
    )

    def offset_mock():
        yield None

    scan._set_position_offset = offset_mock
    next(scan.prepare_positions())
    # pylint: disable=protected-access
    pos = list(scan._get_position())
    assert pytest.approx(np.vstack(np.array(pos, dtype=object)[:, 1])) == np.vstack(
        np.array(reference_scan_list, dtype=object)[:, 1]
    )


@pytest.mark.parametrize(
    "scan_msg,reference_scan_list",
    [
        (
            messages.ScanQueueMessage(
                metadata={
                    "file_suffix": None,
                    "file_directory": None,
                    "user_metadata": {},
                    "RID": "a86acd69-ea4b-4b12-acbb-3f275fc5e8e3",
                },
                scan_type="cont_line_scan",
                parameter={
                    "args": ("samx", -1, 1),
                    "kwargs": {
                        "steps": 3,
                        "exp_time": 0.1,
                        "atol": 0.1,
                        "offset": 3,
                        "relative": False,
                        "system_config": {"file_suffix": None, "file_directory": None},
                    },
                },
                queue="primary",
            ),
            [
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["samx"],
                    action="read",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="rpc",
                    parameter={"device": "samx", "func": "velocity.get", "args": (), "kwargs": {}},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="rpc",
                    parameter={
                        "device": "samx",
                        "func": "acceleration.get",
                        "args": (),
                        "kwargs": {},
                    },
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="rpc",
                    parameter={"device": "samx", "func": "read", "args": (), "kwargs": {}},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=None,
                    action="open_scan",
                    parameter={
                        "scan_motors": ["samx"],
                        "readout_priority": {
                            "monitored": ["samx"],
                            "baseline": [],
                            "on_request": [],
                            "async": [],
                        },
                        "num_points": 3,
                        "positions": [[-1.0], [0.0], [1.0]],
                        "scan_name": "cont_line_scan",
                        "scan_type": "step",
                    },
                ),
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="stage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "baseline"},
                    device=["rtx", "samy", "samz"],
                    action="read",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="set",
                    parameter={"value": -1.0},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="pre_scan",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="set",
                    parameter={"value": -4.0},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device="samx",
                    action="set",
                    parameter={"value": 1.0},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 0},
                    device=["bpm4i", "eiger", "samx"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 1},
                    device=["bpm4i", "eiger", "samx"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 2},
                    device=["bpm4i", "eiger", "samx"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="complete",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="unstage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=None,
                    action="close_scan",
                    parameter={},
                ),
            ],
        )
    ],
)
def test_cont_line_scan(scan_msg, reference_scan_list, scan_assembler, device_manager_mock):
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]

    request = scan_assembler(ContLineScan, *args, parameter=scan_msg.content["parameter"], **kwargs)

    readback = collections.deque()
    readback.extend(
        [10, 1, {"samx": {"value": -1}}, {"samx": {"value": 0}}, {"samx": {"value": 1}}]
    )

    def mock_readback_return(*args, **kwargs):
        if len(readback) > 0:
            return readback.popleft()
        return None

    samx_read_val = collections.deque()
    samx_read_val.extend([{"samx": {"value": -1}}, {"samx": {"value": 0}}, {"samx": {"value": 1}}])

    def samx_read(*args, **kwargs):
        if len(samx_read_val) > 0:
            return samx_read_val.popleft()
        return None

    with (
        mock.patch.object(
            request.stubs, "_get_result_from_status", side_effect=mock_readback_return
        ),
        mock.patch.object(device_manager_mock.devices["samx"], "read", side_effect=samx_read),
    ):

        msg_list = list(request.run())

        scan_uid = msg_list[0].metadata.get("scan_id")
        diid_list = []
        for ii, msg in enumerate(msg_list):
            if msg is None:
                msg_list.pop(ii)
                continue
            msg.metadata.pop("RID", None)
            if msg.action == "rpc":
                msg.metadata.pop("rpc_id", None)
                msg.parameter.pop("rpc_id", None)
            if msg.metadata.get("device_instr_id"):
                diid_list.append(msg.metadata.pop("device_instr_id"))
            if msg.device and isinstance(msg.device, list):
                msg.device = sorted(msg.device)
        assert msg_list == reference_scan_list


def test_device_rpc(scan_assembler):
    parameter = {
        "device": "samx",
        "rpc_id": "baf7c4c0-4948-4046-8fc5-ad1e9d188c10",
        "func": "read",
        "args": [],
        "kwargs": {},
    }
    scan = scan_assembler(DeviceRPC, parameter=parameter)

    scan_instructions = list(scan.run())
    for ii, _ in enumerate(scan_instructions):
        scan_instructions[ii].metadata.pop("device_instr_id", None)
        scan_instructions[ii].parameter["rpc_id"] = parameter["rpc_id"]
    assert scan_instructions == [
        messages.DeviceInstructionMessage(
            device="samx",
            action="rpc",
            parameter=parameter,
            metadata={"readout_priority": "monitored"},
        )
    ]


@pytest.mark.parametrize(
    "scan_msg,reference_scan_list",
    [
        (
            messages.ScanQueueMessage(
                scan_type="acquire",
                parameter={"args": [], "kwargs": {"exp_time": 1.0}},
                queue="primary",
            ),
            [
                messages.DeviceInstructionMessage(
                    device=None,
                    action="open_scan",
                    parameter={
                        "scan_motors": [],
                        "readout_priority": {
                            "monitored": [],
                            "baseline": [],
                            "on_request": [],
                            "async": [],
                        },
                        "num_points": 1,
                        "positions": [],
                        "scan_name": "acquire",
                        "scan_type": "step",
                    },
                    metadata={"readout_priority": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="stage",
                    parameter={},
                    metadata={},
                ),
                messages.DeviceInstructionMessage(
                    device=["rtx", "samx", "samy", "samz"],
                    action="read",
                    parameter={},
                    metadata={"readout_priority": "baseline"},
                ),
                messages.DeviceInstructionMessage(
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="pre_scan",
                    parameter={},
                    metadata={"readout_priority": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                    metadata={"readout_priority": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    device=["bpm4i", "eiger"],
                    action="read",
                    parameter={"group": "monitored"},
                    metadata={"point_id": 0, "readout_priority": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="complete",
                    parameter={},
                    metadata={"readout_priority": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="unstage",
                    parameter={},
                    metadata={},
                ),
                messages.DeviceInstructionMessage(
                    device=None,
                    action="close_scan",
                    parameter={},
                    metadata={"readout_priority": "monitored"},
                ),
            ],
        )
    ],
)
def test_acquire(scan_msg, reference_scan_list, scan_assembler):

    scan = scan_assembler(Acquire, parameter=scan_msg.content.get("parameter"))

    scan_instructions = list(scan.run())
    scan_uid = scan_instructions[0].metadata.get("scan_id")
    for ii, _ in enumerate(reference_scan_list):
        if reference_scan_list[ii].metadata.get("scan_id") is not None:
            reference_scan_list[ii].metadata["scan_id"] = scan_uid
        scan_instructions[ii].metadata.pop("device_instr_id", None)
        if scan_instructions[ii].device and isinstance(scan_instructions[ii].device, list):
            scan_instructions[ii].device = sorted(scan_instructions[ii].device)
    assert scan_instructions == reference_scan_list


def test_pre_scan_macro():
    def pre_scan_macro(devices: dict, request: RequestBase):
        pass

    device_manager = DMMock()
    device_manager.add_device("samx")
    macros = inspect.getsource(pre_scan_macro).encode()
    scan_msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content.get("parameter", {}).get("args", []))
    kwargs = scan_msg.content.get("parameter", {}).get("kwargs", {})
    request = FermatSpiralScan(
        *args, device_manager=device_manager, parameter=scan_msg.content["parameter"], **kwargs
    )
    with mock.patch.object(
        request.device_manager.connector,
        "lrange",
        new_callable=mock.PropertyMock,
        return_value=[messages.VariableMessage(value=macros)],
    ) as macros_mock:
        with mock.patch.object(request, "_get_func_name_from_macro", return_value="pre_scan_macro"):
            with mock.patch("builtins.eval") as eval_mock:
                request.initialize()
                eval_mock.assert_called_once_with("pre_scan_macro")


# def test_scan_report_devices():
#     device_manager = DMMock()
#     device_manager.add_device("samx")
#     parameter = {
#         "args": {"samx": (-5, 5), "samy": (-5, 5)},
#         "kwargs": {"step": 3},
#     }
#     request = RequestBase(device_manager=device_manager, parameter=parameter)
#     assert request.scan_report_devices == ["samx", "samy"]
#     request.scan_report_devices = ["samx", "samz"]
#     assert request.scan_report_devices == ["samx", "samz"]


@pytest.mark.parametrize("in_args,reference_positions", [((5, 5, 1, 1), [[1, 0], [2, 0], [-2, 0]])])
def test_round_roi_scan_positions(in_args, reference_positions):
    positions = get_round_roi_scan_positions(*in_args)
    assert np.isclose(positions, reference_positions).all()


def test_round_roi_scan():
    device_manager = DMMock()
    device_manager.add_device("samx")
    device_manager.add_device("samy")
    scan_msg = messages.ScanQueueMessage(
        scan_type="round_roi_scan",
        parameter={
            "args": {"samx": (10,), "samy": (10,)},
            "kwargs": {"dr": 2, "nth": 4, "exp_time": 2, "relative": True},
        },
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]
    request = RoundROIScan(
        *args, device_manager=device_manager, parameter=scan_msg.content["parameter"], **kwargs
    )
    assert set(request.scan_report_devices) == set(["samx", "samy"])
    assert request.dr == 2
    assert request.nth == 4
    assert request.exp_time == 2
    assert request.relative is True


@pytest.mark.parametrize(
    "in_args,reference_positions", [((1, 5, 1, 1), [[0, -3], [0, -7], [0, 7]])]
)
def test_round_scan_positions(in_args, reference_positions):
    positions = get_round_scan_positions(*in_args)
    assert np.isclose(positions, reference_positions).all()


@pytest.mark.parametrize(
    "in_args,reference_positions,snaked",
    [
        (([list(range(2)), list(range(2))],), [[0, 1], [0, 0], [1, 0], [1, 1]], True),
        (
            ([list(range(2)), list(range(3))],),
            [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]],
            False,
        ),
    ],
)
def test_raster_scan_positions(in_args, reference_positions, snaked):
    positions = get_2D_raster_pos(*in_args, snaked=snaked)
    assert np.isclose(positions, reference_positions).all()


@pytest.mark.parametrize(
    "in_args, center, reference_positions",
    [
        (
            [-2, 2, -2, 2],
            False,
            [
                [-0.38502947, -0.42030026],
                [0.8030152, 0.07047403],
                [-0.78349739, 0.6006928],
                [0.19856742, -1.12257337],
                [0.68409143, 1.07541569],
                [-1.34834023, -0.36246191],
                [1.33834167, -0.69508386],
                [-0.55304807, 1.51437705],
                [-0.65246146, -1.5806309],
                [1.63258446, 0.76398167],
                [-1.80382449, 0.565789],
                [0.99004828, -1.70839234],
                [-1.74471832, -1.22660425],
                [-1.46933912, 1.74339971],
                [1.70582397, 1.71416585],
                [1.95717083, -1.63324289],
            ],
        ),
        (
            [-1, 1, -1, 1],
            1,
            [
                [0.0, 0.0],
                [-0.38502947, -0.42030026],
                [0.8030152, 0.07047403],
                [-0.78349739, 0.6006928],
            ],
        ),
    ],
)
def test_get_fermat_spiral_pos(in_args, center, reference_positions):
    positions = get_fermat_spiral_pos(*in_args, center=center)
    assert np.isclose(positions, reference_positions).all()


def test_get_func_name_from_macro():
    def pre_scan_macro(devices: dict, request: RequestBase):
        pass

    device_manager = DMMock()
    device_manager.add_device("samx")
    macros = [inspect.getsource(pre_scan_macro).encode()]
    scan_msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content.get("parameter", {}).get("args", []))
    kwargs = scan_msg.content.get("parameter", {}).get("kwargs", {})
    request = FermatSpiralScan(
        *args, device_manager=device_manager, parameter=scan_msg.content["parameter"], **kwargs
    )
    assert request._get_func_name_from_macro(macros[0].decode().strip()) == "pre_scan_macro"


def test_scan_report_devices():
    device_manager = DMMock()
    device_manager.add_device("samx")
    device_manager.add_device("samy")
    scan_msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content.get("parameter", {}).get("args", []))
    kwargs = scan_msg.content.get("parameter", {}).get("kwargs", {})

    request = FermatSpiralScan(
        *args, device_manager=device_manager, parameter=scan_msg.content["parameter"], **kwargs
    )
    assert set(request.scan_report_devices) == set(["samx", "samy"])

    request.scan_report_devices = ["samx", "samy", "samz"]
    assert request.scan_report_devices == ["samx", "samy", "samz"]


def test_request_base_check_limits():
    class RequestBaseMock(RequestBase):
        def run(self):
            pass

    device_manager = DMMock()
    device_manager.add_device("samx")
    device_manager.add_device("samy")
    scan_msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    request = RequestBaseMock(
        device_manager=device_manager, parameter=scan_msg.content["parameter"]
    )

    assert request.scan_motors == ["samx", "samy"]
    assert request.device_manager.devices["samy"]._config["deviceConfig"].get("limits", [0, 0]) == [
        -50,
        50,
    ]
    request.device_manager.devices["samy"]._config["deviceConfig"]["limits"] = [5, -5]
    assert request.device_manager.devices["samy"]._config["deviceConfig"].get("limits", [0, 0]) == [
        5,
        -5,
    ]

    request.positions = [[-100, 30]]

    for ii, dev in enumerate(request.scan_motors):
        low_limit, high_limit = (
            request.device_manager.devices[dev]._config["deviceConfig"].get("limits", [0, 0])
        )
        for pos in request.positions:
            pos_axis = pos[ii]
            if low_limit >= high_limit:
                continue
            if not low_limit <= pos_axis <= high_limit:
                with pytest.raises(Exception) as exc_info:
                    request._check_limits()
                assert (
                    exc_info.value.args[0]
                    == f"Target position {pos} for motor {dev} is outside of range: [{low_limit},"
                    f" {high_limit}]"
                )
            else:
                request._check_limits()

    assert request.positions == [[-100, 30]]


def test_request_baseupdate_scan_motors():
    class RequestBaseMock(RequestBase):
        def run(self):
            pass

    device_manager = DMMock()
    device_manager.add_device("samx")
    device_manager.add_device("samz")
    scan_msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={"args": {"samx": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    request = RequestBaseMock(
        device_manager=device_manager, parameter=scan_msg.content["parameter"]
    )

    assert request.scan_motors == ["samx"]
    request.caller_args = ""
    request.update_scan_motors()
    assert request.scan_motors == ["samx"]

    request.arg_bundle_size = {"bundle": 2, "min": None, "max": None}
    request.caller_args = {"samz": (-2, 2), "samy": (-1, 2)}
    request.update_scan_motors()
    assert request.scan_motors == ["samz", "samy"]

    request.caller_args = {"samx"}
    request.arg_bundle_size = {"bundle": 0, "min": None, "max": None}
    request.update_scan_motors()
    assert request.scan_motors == ["samz", "samy", "samx"]


def test_scan_base_init():
    device_manager = DMMock()
    device_manager.add_device("samx")

    class ScanBaseMock(ScanBase):
        scan_name = ""

        def _calculate_positions(self):
            pass

    scan_msg = messages.ScanQueueMessage(
        scan_type="",
        parameter={"args": {"samx": (-5, 5), "samy": (-5, 5)}, "kwargs": {"step": 3}},
        queue="primary",
    )
    with pytest.raises(ValueError) as exc_info:
        request = ScanBaseMock(
            device_manager=device_manager, parameter=scan_msg.content["parameter"]
        )
    assert exc_info.value.args[0] == "scan_name cannot be empty"


def test_scan_base_set_position_offset():
    device_manager = DMMock()
    device_manager.add_device("samx")
    device_manager.add_device("samy")

    scan_msg = messages.ScanQueueMessage(
        scan_type="fermat_scan",
        parameter={
            "args": {"samx": (-5, 5), "samy": (-5, 5)},
            "kwargs": {"step": 3, "relative": False},
        },
        queue="primary",
    )

    args = unpack_scan_args(scan_msg.content.get("parameter", {}).get("args", []))
    kwargs = scan_msg.content.get("parameter", {}).get("kwargs", {})
    request = FermatSpiralScan(
        *args, device_manager=device_manager, parameter=scan_msg.content["parameter"], **kwargs
    )

    assert request.positions == []
    request._set_position_offset()
    assert request.positions == []

    assert request.relative is False
    request._set_position_offset()

    assert request.start_pos == []


def test_round_scan_fly_simupdate_scan_motors():
    device_manager = DMMock()
    device_manager.add_device("flyer_sim")
    scan_msg = messages.ScanQueueMessage(
        scan_type="round_scan_fly",
        parameter={"args": {"flyer_sim": (0, 50, 5, 3)}, "kwargs": {"realtive": True}},
        queue="primary",
    )
    request = RoundScanFlySim(
        flyer="flyer_sim",
        inner_ring=0,
        outer_ring=50,
        number_of_rings=5,
        number_pos=3,
        device_manager=device_manager,
        parameter=scan_msg.content["parameter"],
    )

    request.update_scan_motors()
    assert request.scan_motors == []
    assert request.flyer == list(scan_msg.content["parameter"]["args"].keys())[0]


def test_round_scan_fly_sim_prepare_positions():
    device_manager = DMMock()
    device_manager.add_device("flyer_sim")
    scan_msg = messages.ScanQueueMessage(
        scan_type="round_scan_fly",
        parameter={"args": {"flyer_sim": (0, 50, 5, 3)}, "kwargs": {"realtive": True}},
        queue="primary",
    )
    request = RoundScanFlySim(
        flyer="flyer_sim",
        inner_ring=0,
        outer_ring=50,
        number_of_rings=5,
        number_pos=3,
        device_manager=device_manager,
        parameter=scan_msg.content["parameter"],
    )
    request._calculate_positions = mock.MagicMock()
    request._check_limits = mock.MagicMock()
    pos = [1, 2, 3, 4]
    request.positions = pos

    next(request.prepare_positions())

    request._calculate_positions.assert_called_once()
    assert request.num_pos == len(pos)
    request._check_limits.assert_called_once()


@pytest.mark.parametrize(
    "in_args,reference_positions", [((1, 5, 1, 1), [[0, -3], [0, -7], [0, 7]])]
)
def test_round_scan_fly_sim_calculate_positions(in_args, reference_positions):
    device_manager = DMMock()
    device_manager.add_device("flyer_sim")
    scan_msg = messages.ScanQueueMessage(
        scan_type="round_scan_fly",
        parameter={"args": {"flyer_sim": in_args}, "kwargs": {"realtive": True}},
        queue="primary",
    )
    request = RoundScanFlySim(
        flyer="flyer_sim",
        inner_ring=in_args[0],
        outer_ring=in_args[1],
        number_of_rings=in_args[2],
        number_pos=in_args[3],
        device_manager=device_manager,
        parameter=scan_msg.content["parameter"],
    )

    request._calculate_positions()
    assert np.isclose(request.positions, reference_positions).all()


@pytest.mark.parametrize(
    "in_args,reference_positions", [((1, 5, 1, 1), [[0, -3], [0, -7], [0, 7]])]
)
def test_round_scan_fly_sim_scan_core(in_args, reference_positions, scan_assembler):
    scan_msg = messages.ScanQueueMessage(
        scan_type="round_scan_fly",
        parameter={"args": {"samx": in_args}, "kwargs": {"realtive": True}},
        queue="primary",
    )
    request = scan_assembler(
        RoundScanFlySim,
        flyer="samx",
        inner_ring=in_args[0],
        outer_ring=in_args[1],
        number_of_rings=in_args[2],
        number_pos=in_args[3],
        parameter=scan_msg.content["parameter"],
    )

    request.positions = np.array(reference_positions)

    ret = next(request.scan_core())
    ret.metadata.pop("device_instr_id", None)
    assert ret == messages.DeviceInstructionMessage(
        device="samx",
        action="kickoff",
        parameter={"configure": {"num_pos": 0, "positions": reference_positions, "exp_time": 0}},
        metadata={"readout_priority": "monitored"},
    )


@pytest.mark.parametrize(
    "in_args,reference_positions",
    [
        (
            [[-3, 3], [-2, 2]],
            [
                [-3.0, -2.0],
                [-2.33333333, -1.55555556],
                [-1.66666667, -1.11111111],
                [-1.0, -0.66666667],
                [-0.33333333, -0.22222222],
                [0.33333333, 0.22222222],
                [1.0, 0.66666667],
                [1.66666667, 1.11111111],
                [2.33333333, 1.55555556],
                [3.0, 2.0],
            ],
        ),
        (
            [[-1, 1], [-1, 2]],
            [
                [-1.0, -1.0],
                [-0.77777778, -0.66666667],
                [-0.55555556, -0.33333333],
                [-0.33333333, 0.0],
                [-0.11111111, 0.33333333],
                [0.11111111, 0.66666667],
                [0.33333333, 1.0],
                [0.55555556, 1.33333333],
                [0.77777778, 1.66666667],
                [1.0, 2.0],
            ],
        ),
    ],
)
def test_line_scan_calculate_positions(in_args, reference_positions):
    device_manager = DMMock()
    scan_msg = messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={
            "args": {"samx": in_args[0], "samy": in_args[1]},
            "kwargs": {"relative": True, "steps": 10},
        },
        queue="primary",
    )
    request = LineScan(
        device_manager=device_manager,
        parameter=scan_msg.content["parameter"],
        **scan_msg.content["parameter"]["kwargs"],
    )

    request._calculate_positions()
    assert np.isclose(request.positions, reference_positions).all()


def test_list_scan_calculate_positions():
    device_manager = DMMock()
    scan_msg = messages.ScanQueueMessage(
        scan_type="list_scan",
        parameter={
            "args": {"samx": [[0, 1, 2, 3, 4]], "samy": [[0, 1, 2, 3, 4]]},
            "kwargs": {"realtive": True},
        },
        queue="primary",
    )

    request = ListScan(device_manager=device_manager, parameter=scan_msg.content["parameter"])
    request._calculate_positions()
    assert np.isclose(request.positions, [[0, 0], [1, 1], [2, 2], [3, 3], [4, 4]]).all()


def test_list_scan_raises_for_different_lengths():
    device_manager = DMMock()
    scan_msg = messages.ScanQueueMessage(
        scan_type="list_scan",
        parameter={
            "args": {"samx": [[0, 1, 2, 3, 4]], "samy": [[0, 1, 2, 3]]},
            "kwargs": {"realtive": True},
        },
        queue="primary",
    )
    with pytest.raises(ValueError):
        ListScan(device_manager=device_manager, parameter=scan_msg.content["parameter"])


@pytest.mark.parametrize(
    "scan_msg,reference_scan_list",
    [
        (
            messages.ScanQueueMessage(
                scan_type="time_scan",
                parameter={
                    "args": {},
                    "kwargs": {"points": 3, "interval": 1, "exp_time": 0.1, "relative": True},
                },
                queue="primary",
            ),
            [
                None,
                None,
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=None,
                    action="open_scan",
                    parameter={
                        "scan_motors": [],
                        "readout_priority": {
                            "monitored": [],
                            "baseline": [],
                            "on_request": [],
                            "async": [],
                        },
                        "num_points": 3,
                        "positions": [],
                        "scan_name": "time_scan",
                        "scan_type": "step",
                    },
                ),
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="stage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "baseline"},
                    device=["rtx", "samx", "samy", "samz"],
                    action="read",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="pre_scan",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 0},
                    device=["bpm4i", "eiger"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 1},
                    device=["bpm4i", "eiger"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["eiger"],
                    action="trigger",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "point_id": 2},
                    device=["bpm4i", "eiger"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="complete",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="unstage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored"},
                    device=None,
                    action="close_scan",
                    parameter={},
                ),
            ],
        )
    ],
)
def test_time_scan(scan_msg, reference_scan_list, scan_assembler):

    request = scan_assembler(
        TimeScan, parameter=scan_msg.content["parameter"], **scan_msg.content["parameter"]["kwargs"]
    )

    scan_instructions = list(request.run())
    for msg in scan_instructions:
        if msg:
            msg.metadata.pop("device_instr_id", None)
    assert scan_instructions == reference_scan_list


@pytest.mark.parametrize(
    "scan_msg,reference_scan_list",
    [
        (
            messages.ScanQueueMessage(
                scan_type="otf_scan",
                parameter={"args": {}, "kwargs": {"e1": 700, "e2": 740, "time": 4}},
                queue="primary",
                metadata={"RID": "1234"},
            ),
            [
                None,
                None,
                None,
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "RID": "1234"},
                    device=None,
                    action="open_scan",
                    parameter={
                        "scan_motors": [],
                        "readout_priority": {
                            "monitored": [],
                            "baseline": [],
                            "on_request": [],
                            "async": [],
                        },
                        "num_points": 0,
                        "positions": [],
                        "scan_name": "otf_scan",
                        "scan_type": "fly",
                    },
                ),
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="stage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "baseline", "RID": "1234"},
                    device=["rtx", "samx", "samy", "samz"],
                    action="read",
                    parameter={},
                ),
                None,
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "RID": "1234"},
                    device="mono",
                    action="set",
                    parameter={"value": 700},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "RID": "1234"},
                    device="otf",
                    action="kickoff",
                    parameter={"configure": {"e1": 700, "e2": 740, "time": 4}},
                ),
                "fake_complete",
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "RID": "1234"},
                    device=["bpm4i", "eiger"],
                    action="read",
                    parameter={"group": "monitored"},
                ),
                "fake_complete",
                messages.DeviceInstructionMessage(
                    metadata={},
                    device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                    action="unstage",
                    parameter={},
                ),
                messages.DeviceInstructionMessage(
                    metadata={"readout_priority": "monitored", "RID": "1234"},
                    device=None,
                    action="close_scan",
                    parameter={},
                ),
            ],
        )
    ],
)
def test_otf_scan(scan_msg, reference_scan_list, ScanStubStatusMock, scan_assembler):

    request = scan_assembler(
        OTFScan, parameter=scan_msg.content["parameter"], metadata=scan_msg.metadata
    )

    def fake_done():
        yield False
        yield True

    def fake_complete(*args, **kwargs):
        yield "fake_complete"
        return ScanStubStatusMock(done_func=fake_done)

    with mock.patch.object(request.stubs, "complete", side_effect=fake_complete):
        scan_instructions = list(request.run())
    for msg in scan_instructions:
        if msg and msg != "fake_complete":
            msg.metadata.pop("device_instr_id", None)
    assert scan_instructions == reference_scan_list


def test_monitor_scan():
    device_manager = DMMock()
    scan_msg = messages.ScanQueueMessage(
        scan_type="monitor_scan",
        parameter={"args": {"samx": [-5, 5]}, "kwargs": {"relative": True, "exp_time": 0.1}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]
    request = MonitorScan(
        *args,
        device_manager=device_manager,
        parameter=scan_msg.content["parameter"],
        **scan_msg.content["parameter"]["kwargs"],
    )
    request._calculate_positions()
    assert np.isclose(request.positions, [[-5], [5]]).all()


def test_monitor_scan_run(scan_assembler, ScanStubStatusMock):
    scan_msg = messages.ScanQueueMessage(
        scan_type="monitor_scan",
        parameter={"args": {"samx": [-5, 5]}, "kwargs": {"relative": True, "exp_time": 0.1}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]

    request = scan_assembler(MonitorScan, *args, parameter=scan_msg.content["parameter"], **kwargs)

    def fake_done():
        yield False
        yield False
        yield True

    def fake_set(*args, **kwargs):
        yield "fake_set"
        return ScanStubStatusMock(done_func=fake_done)

    with mock.patch.object(request, "_get_flyer_status") as flyer_status:
        with mock.patch.object(request, "_check_limits") as check_limits:
            with mock.patch.object(request, "_set_position_offset") as position_offset:
                with mock.patch.object(request.stubs, "set", side_effect=fake_set):
                    flyer_status.side_effect = [
                        None,
                        None,
                        messages.DeviceMessage(signals={"rb1": {"value": 1}}),
                    ]
                    ref_list = list(request.run())
                    for msg in ref_list:
                        if msg and msg != "fake_set":
                            msg.metadata.pop("device_instr_id", None)
                    assert ref_list == [
                        messages.DeviceInstructionMessage(
                            metadata={"readout_priority": "monitored"},
                            device=["samx"],
                            action="read",
                            parameter={},
                        ),
                        None,
                        messages.DeviceInstructionMessage(
                            metadata={"readout_priority": "monitored"},
                            device=None,
                            action="open_scan",
                            parameter={
                                "scan_motors": ["samx"],
                                "readout_priority": {
                                    "monitored": ["samx"],
                                    "baseline": [],
                                    "on_request": [],
                                    "async": [],
                                },
                                "num_points": 0,
                                "positions": [[-5.0], [5.0]],
                                "scan_name": "monitor_scan",
                                "scan_type": "fly",
                            },
                        ),
                        messages.DeviceInstructionMessage(
                            metadata={},
                            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                            action="stage",
                            parameter={},
                        ),
                        messages.DeviceInstructionMessage(
                            metadata={"readout_priority": "baseline"},
                            device=["rtx", "samy", "samz"],
                            action="read",
                            parameter={},
                        ),
                        "fake_set",
                        messages.DeviceInstructionMessage(
                            metadata={"readout_priority": "monitored"},
                            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                            action="pre_scan",
                            parameter={},
                        ),
                        "fake_set",
                        "fake_set",
                        messages.DeviceInstructionMessage(
                            metadata={"readout_priority": "monitored"},
                            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                            action="complete",
                            parameter={},
                        ),
                        messages.DeviceInstructionMessage(
                            metadata={},
                            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
                            action="unstage",
                            parameter={},
                        ),
                        messages.DeviceInstructionMessage(
                            metadata={"readout_priority": "monitored"},
                            device=None,
                            action="close_scan",
                            parameter={},
                        ),
                    ]


def test_OpenInteractiveScan(scan_assembler):
    scan_msg = messages.ScanQueueMessage(
        scan_type="open_interactive_scan",
        parameter={"args": {"samx": []}, "kwargs": {"relative": True, "exp_time": 0.1}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])

    request = scan_assembler(
        OpenInteractiveScan,
        *args,
        parameter=scan_msg.content["parameter"],
        **scan_msg.content["parameter"]["kwargs"],
    )

    ref_list = list(request.run())
    for msg in ref_list:
        msg.metadata.pop("device_instr_id", None)
    assert ref_list == [
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=None,
            action="open_scan_def",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=None,
            action="open_scan",
            parameter={
                "scan_motors": ["samx"],
                "readout_priority": {
                    "monitored": ["samx"],
                    "baseline": [],
                    "on_request": [],
                    "async": [],
                },
                "num_points": 0,
                "positions": [],
                "scan_name": "_open_interactive_scan",
                "scan_type": "step",
            },
        ),
        messages.DeviceInstructionMessage(
            metadata={},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="stage",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "baseline"},
            device=["rtx", "samy", "samz"],
            action="read",
            parameter={},
        ),
    ]


def test_InteractiveReadMontiored(scan_assembler):
    scan_msg = messages.ScanQueueMessage(
        scan_type="_interactive_scan_trigger",
        parameter={"args": {"samx": []}, "kwargs": {"relative": True, "exp_time": 0.1}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]
    request = scan_assembler(
        InteractiveReadMontiored, *args, parameter=scan_msg.content["parameter"], **kwargs
    )

    ref_list = list(request.run())
    ref_list[0].metadata.pop("device_instr_id", None)
    ref_list[0].device = sorted(ref_list[0].device)
    assert ref_list == [
        messages.DeviceInstructionMessage(
            device=["bpm4i", "eiger"],
            action="read",
            parameter={"group": "monitored"},
            metadata={"readout_priority": "monitored", "point_id": 0},
        )
    ]


def test_InteractiveTrigger(scan_assembler):
    scan_msg = messages.ScanQueueMessage(
        scan_type="_interactive_scan_trigger",
        parameter={"args": {"samx": []}, "kwargs": {"relative": True, "exp_time": 0.1}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]

    request = scan_assembler(
        InteractiveTrigger, *args, parameter=scan_msg.content["parameter"], **kwargs
    )

    ref_list = list(request.run())
    ref_list[0].metadata.pop("device_instr_id", None)
    ref_list[0].device = sorted(ref_list[0].device)
    assert ref_list == [
        messages.DeviceInstructionMessage(
            device=["eiger"],
            action="trigger",
            parameter={},
            metadata={"readout_priority": "monitored"},
        )
    ]


def test_CloseInteractiveScan(scan_assembler):
    scan_msg = messages.ScanQueueMessage(
        scan_type="close_interactive_scan",
        parameter={"args": {"samx": []}, "kwargs": {"relative": True, "exp_time": 0.1}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]
    request = scan_assembler(
        CloseInteractiveScan, *args, parameter=scan_msg.content["parameter"], **kwargs
    )

    request.start_pos = [0]
    ref_list = list(request.run())
    for ii, _ in enumerate(ref_list):
        ref_list[ii].metadata.pop("device_instr_id", None)
        if ref_list[ii].device and isinstance(ref_list[ii].device, list):
            ref_list[ii].device = sorted(ref_list[ii].device)

    assert ref_list == [
        messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={"value": 0},
            metadata={"readout_priority": "monitored"},
        ),
        messages.DeviceInstructionMessage(
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="complete",
            parameter={},
            metadata={"readout_priority": "monitored"},
        ),
        messages.DeviceInstructionMessage(
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="unstage",
            parameter={},
            metadata={},
        ),
        messages.DeviceInstructionMessage(
            device=None,
            action="close_scan",
            parameter={},
            metadata={"readout_priority": "monitored"},
        ),
        messages.DeviceInstructionMessage(
            device=None,
            action="close_scan_def",
            parameter={},
            metadata={"readout_priority": "monitored"},
        ),
    ]


def test_RoundScan(scan_assembler):
    scan_msg = messages.ScanQueueMessage(
        scan_type="round_scan",
        parameter={
            "args": {"samx": ["samy", 1, 2, 1, 3]},
            "kwargs": {"relative": True, "steps": 10},
        },
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])

    request = scan_assembler(
        RoundScan,
        *args,
        parameter=scan_msg.content["parameter"],
        **scan_msg.content["parameter"]["kwargs"],
    )

    with mock.patch.object(request, "_check_limits") as check_limits:
        with mock.patch.object(request, "_set_position_offset") as position_offset:
            ref = list(request.run())
            assert len(ref) == 47


def test_ContLineFlyScan(scan_assembler, ScanStubStatusMock):

    request = scan_assembler(ContLineFlyScan, motor="samx", start=0, stop=5, relative=False)

    def fake_done():
        yield False
        yield True

    def fake_set(*args, **kwargs):
        yield "fake_set"
        return ScanStubStatusMock(done_func=fake_done)

    with mock.patch.object(request.stubs, "set", side_effect=fake_set):
        with mock.patch.object(request.stubs, "_get_result_from_status") as get_result:
            get_result.return_value = {"samx": {"value": 0}}
            ref_list = list(request.run())

    ref_list[1].parameter["rpc_id"] = "rpc_id"
    ref_list[2].parameter["readback"]["RID"] = "ddaad496-6178-4f6a-8c2e-0c9d416e5d9c"
    for item in ref_list:
        if hasattr(item, "metadata") and "RID" in item.metadata:
            item.metadata["RID"] = "ddaad496-6178-4f6a-8c2e-0c9d416e5d9c"
        if hasattr(item, "metadata") and "device_instr_id" in item.metadata:
            item.metadata.pop("device_instr_id")

    assert ref_list == [
        None,
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device="samx",
            action="rpc",
            parameter={
                "device": "samx",
                "func": "read",
                "rpc_id": "rpc_id",
                "args": (),
                "kwargs": {},
            },
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=None,
            action="scan_report_instruction",
            parameter={
                "readback": {
                    "RID": "ddaad496-6178-4f6a-8c2e-0c9d416e5d9c",
                    "devices": ["samx"],
                    "start": [0],
                    "end": [5],
                }
            },
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=None,
            action="open_scan",
            parameter={
                "scan_motors": ["samx"],
                "readout_priority": {
                    "monitored": ["samx"],
                    "baseline": [],
                    "on_request": [],
                    "async": [],
                },
                "num_points": None,
                "positions": [[0.0], [5.0]],
                "scan_name": "cont_line_fly_scan",
                "scan_type": "fly",
            },
        ),
        messages.DeviceInstructionMessage(
            metadata={},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="stage",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "baseline"},
            device=["rtx", "samy", "samz"],
            action="read",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="pre_scan",
            parameter={},
        ),
        "fake_set",
        "fake_set",
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=["eiger"],
            action="trigger",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored", "point_id": 0},
            device=["bpm4i", "eiger", "samx"],
            action="read",
            parameter={"group": "monitored"},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="complete",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={},
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="unstage",
            parameter={},
        ),
        messages.DeviceInstructionMessage(
            metadata={"readout_priority": "monitored"},
            device=None,
            action="close_scan",
            parameter={},
        ),
    ]


def test_close_scan_implicitly(scan_assembler):
    """
    Test that the scan can be closed implicitly by calling the cleanup method.
    This test can be safely removed once the close_scan method is removed from the cleanup method of the scan classes.
    """

    class CloseInteractiveScanTest(CloseInteractiveScan):
        def run(self):
            yield from self.finalize()
            yield from self.unstage()
            yield from self.cleanup()
            yield from self.stubs.close_scan_def()

    scan_msg = messages.ScanQueueMessage(
        scan_type="close_interactive_scan",
        parameter={"args": {"samx": []}, "kwargs": {"relative": True, "exp_time": 0.1}},
        queue="primary",
    )
    args = unpack_scan_args(scan_msg.content["parameter"]["args"])
    kwargs = scan_msg.content["parameter"]["kwargs"]
    request = scan_assembler(
        CloseInteractiveScanTest, *args, parameter=scan_msg.content["parameter"], **kwargs
    )

    request.start_pos = [0]
    ref_list = list(request.run())
    for ii, _ in enumerate(ref_list):
        ref_list[ii].metadata.pop("device_instr_id", None)
        if ref_list[ii].device and isinstance(ref_list[ii].device, list):
            ref_list[ii].device = sorted(ref_list[ii].device)

    assert ref_list == [
        messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={"value": 0},
            metadata={"readout_priority": "monitored"},
        ),
        messages.DeviceInstructionMessage(
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="complete",
            parameter={},
            metadata={"readout_priority": "monitored"},
        ),
        messages.DeviceInstructionMessage(
            device=["bpm4i", "eiger", "rtx", "samx", "samy", "samz"],
            action="unstage",
            parameter={},
            metadata={},
        ),
        messages.DeviceInstructionMessage(
            device=None,
            action="close_scan",
            parameter={},
            metadata={"readout_priority": "monitored"},
        ),
        messages.DeviceInstructionMessage(
            device=None,
            action="close_scan_def",
            parameter={},
            metadata={"readout_priority": "monitored"},
        ),
    ]
