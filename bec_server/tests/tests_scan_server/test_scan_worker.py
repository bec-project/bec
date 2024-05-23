# pylint: skip-file
import uuid
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.utils import ConnectorMock
from bec_server.scan_server.errors import DeviceMessageError, ScanAbortion
from bec_server.scan_server.scan_assembler import ScanAssembler
from bec_server.scan_server.scan_queue import (
    InstructionQueueItem,
    InstructionQueueStatus,
    QueueManager,
    RequestBlock,
    RequestBlockQueue,
    ScanQueue,
)
from bec_server.scan_server.scan_worker import ScanWorker
from bec_server.scan_server.tests.fixtures import scan_server_mock


@pytest.fixture
def scan_worker_mock(scan_server_mock) -> ScanWorker:
    scan_server_mock.device_manager.connector = mock.MagicMock()
    scan_worker = ScanWorker(parent=scan_server_mock)
    yield scan_worker


class RequestBlockQueueMock(RequestBlockQueue):
    request_blocks = []
    _scan_id = []

    @property
    def scan_id(self):
        return self._scan_id

    def append(self, msg):
        pass


class InstructionQueueMock(InstructionQueueItem):
    def __init__(self, parent: ScanQueue, assembler: ScanAssembler, worker: ScanWorker) -> None:
        super().__init__(parent, assembler, worker)
        self.queue = RequestBlockQueueMock(self, assembler)
        # self.queue.active_rb = []
        self.idx = 1

    def append_scan_request(self, msg):
        self.scan_msgs.append(msg)
        self.queue.append(msg)

    def __next__(self):
        if (
            self.status
            in [
                InstructionQueueStatus.RUNNING,
                InstructionQueueStatus.DEFERRED_PAUSE,
                InstructionQueueStatus.PENDING,
            ]
            and self.idx < 5
        ):
            self.idx += 1
            return "instr_status"

        else:
            raise StopIteration

        # while self.status == InstructionQueueStatus.PAUSED:
        #     return "instr_paused"

        # return "instr"


@pytest.mark.parametrize(
    "instruction,devices",
    [
        (
            messages.DeviceInstructionMessage(
                device="samy",
                action="wait",
                parameter={"type": "move", "group": "scan_motor", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            ["samy"],
        ),
        (
            messages.DeviceInstructionMessage(
                device=["samx", "samy"],
                action="wait",
                parameter={"type": "move", "group": "scan_motor", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            ["samx", "samy"],
        ),
        (
            messages.DeviceInstructionMessage(
                device="",
                action="wait",
                parameter={"type": "move", "group": "scan_motor", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            ["samx", "samy"],
        ),
        (
            messages.DeviceInstructionMessage(
                device="",
                action="wait",
                parameter={"type": "move", "group": "primary", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            ["samx", "samy"],
        ),
        (
            messages.DeviceInstructionMessage(
                device="",
                action="wait",
                parameter={"type": "move", "group": "nogroup", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            ["samx", "samy"],
        ),
    ],
)
def test_get_devices_from_instruction(scan_worker_mock, instruction, devices):
    worker = scan_worker_mock
    worker.scan_motors = devices
    worker.readout_priority.update({"monitored": devices})

    returned_devices = worker._get_devices_from_instruction(instruction)

    if not instruction.content.get("device"):
        group = instruction.content["parameter"].get("group")
        if group == "primary":
            assert (
                set(
                    dev.name
                    for dev in worker.device_manager.devices.monitored_devices(worker.scan_motors)
                ).difference(set(dev.name for dev in returned_devices))
                == set()
            )
        elif group == "scan_motor":
            assert returned_devices == devices
        else:
            assert returned_devices == []
    else:
        assert returned_devices == [worker.device_manager.devices[dev] for dev in devices]


@pytest.mark.parametrize(
    "instructions",
    [
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 10, "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            )
        ),
        messages.DeviceInstructionMessage(
            device="samx",
            action="set",
            parameter={"value": 10, "wait_group": "scan_motor"},
            metadata={"readout_priority": "monitored", "DIID": None},
        ),
    ],
)
def test_add_wait_group(scan_worker_mock, instructions):
    worker = scan_worker_mock
    if instructions.metadata["DIID"]:
        worker._add_wait_group(instructions)
        assert worker._groups == {"scan_motor": {"samx": 3}}

        worker._groups["scan_motor"] = {"samy": 2}
        worker._add_wait_group(instructions)
        assert worker._groups == {"scan_motor": {"samy": 2, "samx": 3}}

    else:
        with pytest.raises(DeviceMessageError) as exc_info:
            worker._add_wait_group(instructions)
        assert exc_info.value.args[0] == "Device message metadata does not contain a DIID entry."


def test_add_wait_group_to_existing_wait_group(scan_worker_mock):
    instr1 = messages.DeviceInstructionMessage(
        device="samx",
        action="set",
        parameter={"value": 10, "wait_group": "scan_motor"},
        metadata={"readout_priority": "monitored", "DIID": 3},
    )
    instr2 = messages.DeviceInstructionMessage(
        device="samx",
        action="set",
        parameter={"value": 10, "wait_group": "scan_motor"},
        metadata={"readout_priority": "monitored", "DIID": 4},
    )
    worker = scan_worker_mock
    worker._add_wait_group(instr1)
    worker._add_wait_group(instr2)
    assert worker._groups == {"scan_motor": {"samx": 4}}


@pytest.mark.parametrize(
    "instructions,wait_type",
    [
        (
            messages.DeviceInstructionMessage(
                device="samy",
                action="wait",
                parameter={"type": "move", "group": "scan_motor", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            "move",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samy",
                action="wait",
                parameter={"type": "read", "group": "scan_motor", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            "read",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samy",
                action="wait",
                parameter={"type": "trigger", "group": "scan_motor", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            "trigger",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samy",
                action="wait",
                parameter={"type": None, "group": "scan_motor", "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 3},
            ),
            None,
        ),
    ],
)
def test_wait_for_devices(scan_worker_mock, instructions, wait_type):
    worker = scan_worker_mock

    with mock.patch.object(worker, "_wait_for_idle") as idle_mock:
        with mock.patch.object(worker, "_wait_for_read") as read_mock:
            with mock.patch.object(worker, "_wait_for_trigger") as trigger_mock:
                if wait_type:
                    worker.wait_for_devices(instructions)

                if wait_type == "move":
                    idle_mock.assert_called_once_with(instructions)
                elif wait_type == "read":
                    read_mock.assert_called_once_with(instructions)
                elif wait_type == "trigger":
                    trigger_mock.assert_called_once_with(instructions)
                else:
                    with pytest.raises(DeviceMessageError) as exc_info:
                        worker.wait_for_devices(instructions)
                    assert exc_info.value.args[0] == "Unknown wait command"


@pytest.mark.parametrize(
    "instructions",
    [
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="complete",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 3},
            )
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="complete",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 3},
            )
        ),
        (
            messages.DeviceInstructionMessage(
                device=["samx", "samy"],
                action="complete",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 3},
            )
        ),
    ],
)
def test_complete_devices(scan_worker_mock, instructions):
    worker = scan_worker_mock
    with mock.patch.object(worker, "_wait_for_status") as wait_for_status_mock:
        with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
            worker.complete_devices(instructions)
            if instructions.content["device"]:
                devices = instructions.content["device"]
                if isinstance(devices, str):
                    devices = [devices]
            else:
                devices = [dev.name for dev in worker.device_manager.devices.enabled_devices]

            wait_for_status_mock.assert_called_once_with(devices, instructions.metadata)
            send_mock.assert_called_once_with(
                MessageEndpoints.device_instructions(),
                messages.DeviceInstructionMessage(
                    device=devices, action="complete", parameter={}, metadata=instructions.metadata
                ),
            )


@pytest.mark.parametrize(
    "instructions",
    [
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="pre_scan",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 3},
            )
        )
    ],
)
def test_pre_scan(scan_worker_mock, instructions):
    worker = scan_worker_mock
    with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
        with mock.patch.object(worker, "_wait_for_status") as wait_for_status_mock:
            worker.pre_scan(instructions)
            devices = [dev.name for dev in worker.device_manager.devices.enabled_devices]

            wait_for_status_mock.assert_called_once_with(devices, instructions.metadata)
            send_mock.assert_called_once_with(
                MessageEndpoints.device_instructions(),
                messages.DeviceInstructionMessage(
                    device=devices, action="pre_scan", parameter={}, metadata=instructions.metadata
                ),
            )


@pytest.mark.parametrize(
    "device_status,devices,instr,abort",
    [
        (
            [
                messages.DeviceReqStatusMessage(
                    device="samx",
                    success=True,
                    metadata={
                        "readout_priority": "monitored",
                        "DIID": 3,
                        "scan_id": "scan_id",
                        "RID": "requestID",
                    },
                )
            ],
            [("samx", 4)],
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            False,
        ),
        (
            [
                messages.DeviceReqStatusMessage(
                    device="samx",
                    success=False,
                    metadata={
                        "readout_priority": "monitored",
                        "DIID": 3,
                        "scan_id": "scan_id",
                        "RID": "request",
                    },
                )
            ],
            [("samx", 4)],
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            False,
        ),
        (
            [
                messages.DeviceReqStatusMessage(
                    device="samx",
                    success=False,
                    metadata={
                        "readout_priority": "monitored",
                        "DIID": 4,
                        "scan_id": "scan_id",
                        "RID": "requestID",
                    },
                )
            ],
            [("samx", 4)],
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            True,
        ),
        (
            [
                messages.DeviceReqStatusMessage(
                    device="samx",
                    success=False,
                    metadata={
                        "readout_priority": "monitored",
                        "DIID": 3,
                        "scan_id": "scan_id",
                        "RID": "requestID",
                    },
                )
            ],
            [("samx", 4)],
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            False,
        ),
    ],
)
def test_check_for_failed_movements(scan_worker_mock, device_status, devices, instr, abort):
    worker = scan_worker_mock
    worker.device_manager.connector = ConnectorMock()
    if abort:
        with pytest.raises(ScanAbortion):
            worker.device_manager.connector._get_buffer[
                MessageEndpoints.device_readback("samx").endpoint
            ] = messages.DeviceMessage(signals={"samx": {"value": 4}}, metadata={})
            worker._check_for_failed_movements(device_status, devices, instr)
    else:
        worker._check_for_failed_movements(device_status, devices, instr)


@pytest.mark.parametrize(
    "msg1,msg2,req_msg",
    [
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 10, "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceReqStatusMessage(
                device="samx",
                success=False,
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 10, "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceReqStatusMessage(
                device="samx",
                success=True,
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 10, "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceReqStatusMessage(
                device="samx",
                success=True,
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 5,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
        ),
    ],
)
def test_wait_for_idle(scan_worker_mock, msg1, msg2, req_msg: messages.DeviceReqStatusMessage):
    worker = scan_worker_mock
    worker.device_manager.connector = ConnectorMock()

    with mock.patch.object(
        worker.validate, "get_device_status", return_value=[req_msg]
    ) as device_status:
        worker.device_manager.connector._get_buffer[
            MessageEndpoints.device_readback("samx").endpoint
        ] = messages.DeviceMessage(signals={"samx": {"value": 4}}, metadata={})

        worker._add_wait_group(msg1)
        if req_msg.content["success"]:
            worker._wait_for_idle(msg2)
        else:
            with pytest.raises(ScanAbortion):
                worker._wait_for_idle(msg2)


@pytest.mark.parametrize(
    "msg1,msg2,req_msg",
    [
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="set",
                parameter={"value": 10, "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            messages.DeviceStatusMessage(
                device="samx",
                status=0,
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
        )
    ],
)
def test_wait_for_read(scan_worker_mock, msg1, msg2, req_msg: messages.DeviceReqStatusMessage):
    worker = scan_worker_mock
    worker.device_manager.connector = ConnectorMock()

    with mock.patch.object(
        worker.validate, "get_device_status", return_value=[req_msg]
    ) as device_status:
        with mock.patch.object(worker, "_check_for_interruption") as interruption_mock:
            assert worker._groups == {}
            worker._groups["scan_motor"] = {"samx": 3, "samy": 4}
            worker.device_manager.connector._get_buffer[
                MessageEndpoints.device_readback("samx").endpoint
            ] = messages.DeviceMessage(signals={"samx": {"value": 4}}, metadata={})
            worker._add_wait_group(msg1)
            worker._wait_for_read(msg2)
            assert worker._groups == {"scan_motor": {"samy": 4}}
            interruption_mock.assert_called_once()


@pytest.mark.parametrize(
    "instr",
    [
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="set",
                parameter={"time": 0.1},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            )
        )
    ],
)
def test_wait_for_trigger(scan_worker_mock, instr):
    worker = scan_worker_mock
    worker._last_trigger = instr

    with mock.patch.object(worker.validate, "get_device_status") as status_mock:
        with mock.patch.object(worker, "_check_for_interruption") as interruption_mock:
            status_mock.return_value = [
                messages.DeviceReqStatusMessage(
                    device="eiger",
                    success=True,
                    metadata={
                        "readout_priority": "async",
                        "DIID": 3,
                        "scan_id": "scan_id",
                        "RID": "requestID",
                    },
                ),
                messages.DeviceReqStatusMessage(
                    device="monitor_async",
                    success=True,
                    metadata={
                        "readout_priority": "async",
                        "DIID": 3,
                        "scan_id": "scan_id",
                        "RID": "requestID",
                    },
                ),
            ]
            worker._wait_for_trigger(instr)
            assert status_mock.call_count == 1
            assert "eiger" in status_mock.call_args[0][1]
            assert "monitor_async" in status_mock.call_args[0][1]


def test_wait_for_stage(scan_worker_mock):
    worker = scan_worker_mock
    devices = ["samx", "samy"]
    with mock.patch.object(worker.validate, "get_device_status") as status_mock:
        with mock.patch.object(worker, "_check_for_interruption") as interruption_mock:
            worker._wait_for_stage(True, devices, {})
            status_mock.assert_called_once_with(MessageEndpoints.device_staged, devices)
            interruption_mock.assert_called_once()


def test_wait_for_device_server(scan_worker_mock):
    worker = scan_worker_mock
    with mock.patch.object(worker.parent, "wait_for_service") as service_mock:
        worker._wait_for_device_server()
        service_mock.assert_called_once_with("DeviceServer")


@pytest.mark.parametrize(
    "instr",
    [
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="set",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            )
        )
    ],
)
def test_set_devices(scan_worker_mock, instr):
    worker = scan_worker_mock
    with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
        worker.set_devices(instr)
        send_mock.assert_called_once_with(MessageEndpoints.device_instructions(), instr)


@pytest.mark.parametrize(
    "instr",
    [
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="trigger",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            )
        )
    ],
)
def test_trigger_devices(scan_worker_mock, instr):
    worker = scan_worker_mock
    with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
        worker.trigger_devices(instr)
        devices = [
            dev.name for dev in worker.device_manager.devices.get_software_triggered_devices()
        ]

        send_mock.assert_called_once_with(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=devices,
                action="trigger",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
        )


@pytest.mark.parametrize(
    "instr",
    [
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="trigger",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            )
        )
    ],
)
def test_send_rpc(scan_worker_mock, instr):
    worker = scan_worker_mock
    with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
        worker.send_rpc(instr)
        send_mock.assert_called_once_with(MessageEndpoints.device_instructions(), instr)


@pytest.mark.parametrize(
    "instr",
    [
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="read",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            )
        ),
        (
            messages.DeviceInstructionMessage(
                device=[],
                action="read",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            )
        ),
    ],
)
def test_read_devices(scan_worker_mock, instr):
    worker = scan_worker_mock
    instr_devices = instr.content["device"]
    if instr_devices is None:
        instr_devices = []
    worker.readout_priority.update({"monitored": instr_devices})
    devices = [dev.name for dev in worker._get_devices_from_instruction(instr)]
    with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
        worker.read_devices(instr)

        if instr.content.get("device"):
            send_mock.assert_called_once_with(
                MessageEndpoints.device_instructions(),
                messages.DeviceInstructionMessage(
                    device=["samx"],
                    action="read",
                    parameter=instr.content["parameter"],
                    metadata=instr.metadata,
                ),
            )
        else:
            send_mock.assert_called_once_with(
                MessageEndpoints.device_instructions(),
                messages.DeviceInstructionMessage(
                    device=devices,
                    action="read",
                    parameter=instr.content["parameter"],
                    metadata=instr.metadata,
                ),
            )


@pytest.mark.parametrize(
    "instr, devices, parameter, metadata",
    [
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="trigger",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            ["samx"],
            {"value": 10, "wait_group": "scan_motor", "time": 30},
            {"readout_priority": "monitored", "DIID": 3, "scan_id": "scan_id", "RID": "requestID"},
        )
    ],
)
def test_kickoff_devices(scan_worker_mock, instr, devices, parameter, metadata):
    worker = scan_worker_mock
    with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
        worker.kickoff_devices(instr)
        send_mock.assert_called_once_with(
            MessageEndpoints.device_instructions(),
            messages.DeviceInstructionMessage(
                device=devices, action="kickoff", parameter=parameter, metadata=metadata
            ),
        )


@pytest.mark.parametrize(
    "instr, devices",
    [
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="trigger",
                parameter={"value": 10, "wait_group": "scan_motor", "time": 30},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 3,
                    "scan_id": "scan_id",
                    "RID": "requestID",
                },
            ),
            None,
        )
    ],
)
def test_publish_readback(scan_worker_mock, instr, devices):
    worker = scan_worker_mock
    with mock.patch.object(worker, "_get_readback", return_value=[{}]) as get_readback:
        with mock.patch.object(worker.device_manager, "connector") as connector_mock:
            worker._publish_readback(instr)

            get_readback.assert_called_once_with(["samx"])
            pipe = connector_mock.pipeline()
            msg = messages.DeviceMessage(signals={}, metadata=instr.metadata)
            connector_mock.set_and_publish.assert_called_once_with(
                MessageEndpoints.device_read("samx"), msg, pipe
            )


def test_get_readback(scan_worker_mock):
    worker = scan_worker_mock
    devices = ["samx"]
    with mock.patch.object(worker.device_manager, "connector") as connector_mock:
        worker._get_readback(devices)
        pipe = connector_mock.pipeline()
        connector_mock.get.assert_called_once_with(
            MessageEndpoints.device_readback("samx"), pipe=pipe
        )
        connector_mock.execute_pipeline.assert_called_once()


def test_publish_data_as_read(scan_worker_mock):
    worker = scan_worker_mock
    instr = messages.DeviceInstructionMessage(
        device=["samx"],
        action="publish_data_as_read",
        parameter={"data": {}},
        metadata={
            "readout_priority": "monitored",
            "DIID": 3,
            "scan_id": "scan_id",
            "RID": "requestID",
        },
    )
    with mock.patch.object(worker.device_manager, "connector") as connector_mock:
        worker.publish_data_as_read(instr)
        msg = messages.DeviceMessage(
            signals=instr.content["parameter"]["data"], metadata=instr.metadata
        )
        connector_mock.set_and_publish.assert_called_once_with(
            MessageEndpoints.device_read("samx"), msg
        )


def test_publish_data_as_read_multiple(scan_worker_mock):
    worker = scan_worker_mock
    data = [{"samx": {}}, {"samy": {}}]
    devices = ["samx", "samy"]
    instr = messages.DeviceInstructionMessage(
        device=devices,
        action="publish_data_as_read",
        parameter={"data": data},
        metadata={
            "readout_priority": "monitored",
            "DIID": 3,
            "scan_id": "scan_id",
            "RID": "requestID",
        },
    )
    with mock.patch.object(worker.device_manager, "connector") as connector_mock:
        worker.publish_data_as_read(instr)
        mock_calls = []
        for device, dev_data in zip(devices, data):
            msg = messages.DeviceMessage(signals=dev_data, metadata=instr.metadata)
            mock_calls.append(mock.call(MessageEndpoints.device_read(device), msg))
        assert connector_mock.set_and_publish.mock_calls == mock_calls


def test_check_for_interruption(scan_worker_mock):
    worker = scan_worker_mock
    worker.status = InstructionQueueStatus.STOPPED
    with pytest.raises(ScanAbortion) as exc_info:
        worker._check_for_interruption()


@pytest.mark.parametrize(
    "instr, corr_num_points, scan_id",
    [
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="open_scan",
                parameter={
                    "num_points": 150,
                    "scan_motors": ["samx", "samy"],
                    "scan_name": "test_scan",
                },
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 18,
                    "scan_id": "12345",
                    "scan_def_id": 100,
                    "point_id": 50,
                    "RID": 11,
                },
            ),
            201,
            False,
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="open_scan",
                parameter={"num_points": 150, "scan_name": "test_scan"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 18,
                    "scan_id": "12345",
                    "RID": 11,
                },
            ),
            150,
            True,
        ),
    ],
)
def test_open_scan(scan_worker_mock, instr, corr_num_points, scan_id):
    worker = scan_worker_mock

    if not scan_id:
        assert worker.scan_id == None
    else:
        worker.scan_id = 111
        worker.scan_motors = ["bpm4i"]

    if "point_id" in instr.metadata:
        worker.max_point_id = instr.metadata["point_id"]

    assert worker.parent.connector.get(MessageEndpoints.scan_number()) == None

    with mock.patch("bec_server.scan_server.scan_worker.DataStore"):
        with mock.patch(
            "bec_server.scan_server.scan_worker.bec_scan_info_to_blissdata_scan_info",
            return_value=({}, {}),
        ):
            with mock.patch.object(worker, "current_instruction_queue_item") as queue_mock:
                with mock.patch.object(
                    worker.scan_report_instructions, "append"
                ) as instr_append_mock:
                    with mock.patch.object(worker, "_send_scan_status") as send_mock:
                        with mock.patch.object(
                            worker.current_instruction_queue_item.parent.queue_manager,
                            "send_queue_status",
                        ) as queue_status_mock:
                            active_rb = queue_mock.active_request_block
                            active_rb.scan_report_instructions = []
                            worker.open_scan(instr)

                            if not scan_id:
                                assert worker.scan_id == instr.metadata.get("scan_id")
                                assert worker.scan_motors == [
                                    worker.device_manager.devices["samx"],
                                    worker.device_manager.devices["samy"],
                                ]
                            else:
                                assert worker.scan_id == 111
                                assert worker.scan_motors == ["bpm4i"]
                            assert active_rb.scan_report_instructions == [
                                {"scan_progress": corr_num_points}
                            ]
                            queue_status_mock.assert_called_once()
                            send_mock.assert_called_once_with("open")


@pytest.mark.parametrize(
    "msg",
    [
        messages.ScanQueueMessage(
            scan_type="grid_scan",
            parameter={
                "args": {"samx": (-5, 5, 5), "samy": (-1, 1, 2)},
                "kwargs": {"exp_time": 1, "relative": True},
                "num_points": 10,
            },
            queue="primary",
            metadata={"RID": "something"},
        )
    ],
)
def test_initialize_scan_info(scan_worker_mock, msg):
    worker = scan_worker_mock
    scan_server = scan_worker_mock.parent
    rb = RequestBlock(msg, assembler=ScanAssembler(parent=scan_server))
    assert rb.metadata == {"RID": "something"}

    with mock.patch.object(worker, "current_instruction_queue_item"):
        worker.scan_motors = ["samx"]
        worker.readout_priority = {
            "monitored": ["samx"],
            "baseline": [],
            "async": [],
            "continuous": [],
            "on_request": [],
        }
        open_scan_msg = list(rb.scan.open_scan())[0]
        worker._initialize_scan_info(rb, open_scan_msg, msg.content["parameter"].get("num_points"))

        assert worker.current_scan_info["RID"] == "something"
        assert worker.current_scan_info["scan_number"] == 2
        assert worker.current_scan_info["dataset_number"] == 3
        assert worker.current_scan_info["scan_report_devices"] == rb.scan.scan_report_devices
        assert worker.current_scan_info["num_points"] == 10
        assert worker.current_scan_info["scan_msgs"] == []
        assert worker.current_scan_info["monitor_sync"] == "bec"
        assert worker.current_scan_info["frames_per_trigger"] == 1
        assert worker.current_scan_info["args"] == {"samx": (-5, 5, 5), "samy": (-1, 1, 2)}
        assert worker.current_scan_info["kwargs"] == {"exp_time": 1, "relative": True}
        assert "samx" in worker.current_scan_info["readout_priority"]["monitored"]
        assert "samy" in worker.current_scan_info["readout_priority"]["baseline"]


@pytest.mark.parametrize(
    "msg,scan_id,max_point_id,exp_num_points",
    [
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="close_scan",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 18, "scan_id": "12345"},
            ),
            "12345",
            19,
            20,
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="close_scan",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 18, "scan_id": "12345"},
            ),
            "0987",
            200,
            19,
        ),
    ],
)
def test_close_scan(scan_worker_mock, msg, scan_id, max_point_id, exp_num_points):
    worker = scan_worker_mock
    worker.scan_id = scan_id
    worker.current_scan_info["num_points"] = 19

    reset = bool(worker.scan_id == msg.metadata["scan_id"])
    with mock.patch.object(worker, "_send_scan_status") as send_scan_status_mock:
        worker.close_scan(msg, max_point_id=max_point_id)
        if reset:
            send_scan_status_mock.assert_called_with("closed")
            assert worker.scan_id == None
        else:
            assert worker.scan_id == scan_id
    assert worker.current_scan_info["num_points"] == exp_num_points


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionMessage(
            device=None,
            action="stage",
            parameter={},
            metadata={"readout_priority": "async", "DIID": 18, "scan_id": "12345"},
        )
    ],
)
def test_stage_device(scan_worker_mock, msg):
    worker = scan_worker_mock
    worker.device_manager.devices["eiger"]._config["readoutPriority"] = "async"
    worker.device_manager.devices["flyer_sim"]._config["readoutPriority"] = "on_request"

    with mock.patch.object(worker, "_wait_for_stage") as wait_mock:
        with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
            worker.stage_devices(msg)
            on_request_device_names = [
                dev.name for dev in worker.device_manager.devices.on_request_devices()
            ]

            async_devices = worker.device_manager.devices.async_devices()
            async_device_names = [dev.name for dev in async_devices]
            excluded_devices = async_devices
            excluded_devices.extend(worker.device_manager.devices.on_request_devices())
            excluded_devices.extend(worker.device_manager.devices.continuous_devices())
            devices = [
                dev.name
                for dev in worker.device_manager.devices.enabled_devices
                if dev not in excluded_devices
            ]

            for dev in [
                *worker.device_manager.devices.monitored_devices(),
                *worker.device_manager.devices.baseline_devices(),
                *worker.device_manager.devices.async_devices(),
            ]:
                assert dev.name in worker._staged_devices
            for async_dev in async_devices:
                assert (
                    mock.call(
                        MessageEndpoints.device_instructions(),
                        messages.DeviceInstructionMessage(
                            device=async_dev.name,
                            action="stage",
                            parameter=msg.content["parameter"],
                            metadata=msg.metadata,
                        ),
                    )
                    in send_mock.mock_calls
                )
            assert (
                mock.call(
                    MessageEndpoints.device_instructions(),
                    messages.DeviceInstructionMessage(
                        device=devices,
                        action="stage",
                        parameter=msg.content["parameter"],
                        metadata=msg.metadata,
                    ),
                )
                in send_mock.mock_calls
            )
            assert (
                mock.call(staged=True, devices=devices, metadata=msg.metadata)
                in wait_mock.mock_calls
            )
            assert (
                mock.call(staged=True, devices=async_device_names, metadata=msg.metadata)
                in wait_mock.mock_calls
            )
            for dev in on_request_device_names:
                assert dev not in worker._staged_devices


@pytest.mark.parametrize(
    "msg, devices, parameter, metadata, cleanup",
    [
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="close_scan",
                parameter={"parameter": "param"},
                metadata={"readout_priority": "monitored", "DIID": 18, "scan_id": "12345"},
            ),
            ["samx"],
            {"parameter": "param"},
            {"readout_priority": "monitored", "DIID": 18, "scan_id": "12345"},
            False,
        ),
        (None, None, {}, {}, False),
        (None, None, {}, {}, True),
    ],
)
def test_unstage_device(scan_worker_mock, msg, devices, parameter, metadata, cleanup):
    worker = scan_worker_mock
    if not devices:
        devices = [dev.name for dev in worker.device_manager.devices.enabled_devices]

    with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
        with mock.patch.object(worker, "_wait_for_stage") as wait_mock:
            worker.unstage_devices(msg, devices, cleanup)

            send_mock.assert_called_once_with(
                MessageEndpoints.device_instructions(),
                messages.DeviceInstructionMessage(
                    device=devices, action="unstage", parameter=parameter, metadata=metadata
                ),
            )
            if cleanup:
                wait_mock.assert_not_called()
            else:
                wait_mock.assert_called_once_with(staged=False, devices=devices, metadata=metadata)


@pytest.mark.parametrize("status,expire", [("open", None), ("closed", 1800), ("aborted", 1800)])
def test_send_scan_status(scan_worker_mock, status, expire):
    worker = scan_worker_mock
    worker.device_manager.connector = ConnectorMock()
    worker.current_scan_id = str(uuid.uuid4())
    worker._send_scan_status(status)
    scan_info_msgs = [
        msg
        for msg in worker.device_manager.connector.message_sent
        if msg["queue"]
        == MessageEndpoints.public_scan_info(scan_id=worker.current_scan_id).endpoint
    ]
    assert len(scan_info_msgs) == 1
    assert scan_info_msgs[0]["expire"] == expire


@pytest.mark.parametrize("abortion", [False, True])
def test_process_instructions(scan_worker_mock, abortion):
    worker = scan_worker_mock
    scan_server = scan_worker_mock.parent
    scan_queue = ScanQueue(QueueManager(scan_server))
    queue = InstructionQueueMock(
        parent=scan_queue, assembler=ScanAssembler(parent=scan_server), worker=worker
    )

    with mock.patch.object(worker, "_wait_for_device_server") as wait_mock:
        with mock.patch.object(worker, "reset") as reset_mock:
            with mock.patch.object(worker, "_check_for_interruption") as interruption_mock:
                with mock.patch.object(queue.queue, "active_rb") as rb_mock:
                    with mock.patch.object(worker, "_instruction_step") as step_mock:
                        if abortion:
                            interruption_mock.side_effect = ScanAbortion
                            with pytest.raises(ScanAbortion) as exc_info:
                                worker._process_instructions(queue)
                        else:
                            worker._process_instructions(queue)

                        assert worker.max_point_id == 0
                        wait_mock.assert_called_once()

                        if not abortion:
                            assert interruption_mock.call_count == 4
                            assert worker._exposure_time == getattr(rb_mock.scan, "exp_time", None)
                            assert step_mock.call_count == 4
                            assert queue.is_active == False
                            assert queue.status == InstructionQueueStatus.COMPLETED
                            assert worker.current_instruction_queue_item == None
                            reset_mock.assert_called_once()

                        else:
                            assert worker._groups == {}
                            assert queue.stopped == True
                            assert interruption_mock.call_count == 1
                            assert queue.is_active == True
                            assert queue.status == InstructionQueueStatus.PENDING
                            assert worker.current_instruction_queue_item == queue


@pytest.mark.parametrize(
    "msg,method",
    [
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="open_scan",
                parameter={"readout_priority": {"monitored": [], "baseline": [], "on_request": []}},
                metadata={"readout_priority": "monitored", "DIID": 18, "scan_id": "12345"},
            ),
            "open_scan",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="close_scan",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 18, "scan_id": "12345"},
            ),
            "close_scan",
        ),
        (
            messages.DeviceInstructionMessage(
                device=["samx"],
                action="wait",
                parameter={"type": "move", "wait_group": "scan_motor"},
                metadata={
                    "readout_priority": "monitored",
                    "DIID": 4,
                    "scan_id": "12345",
                    "RID": "123456",
                },
            ),
            "wait_for_devices",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="trigger",
                parameter={"group": "trigger"},
                metadata={"readout_priority": "monitored", "DIID": 20, "point_id": 0},
            ),
            "trigger_devices",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1.3681828686580249, "wait_group": "scan_motor"},
                metadata={"readout_priority": "monitored", "DIID": 24},
            ),
            "set_devices",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="read",
                parameter={"group": "primary", "wait_group": "readout_primary"},
                metadata={"readout_priority": "monitored", "DIID": 30, "point_id": 1},
            ),
            "read_devices",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="stage",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 17},
            ),
            "stage_devices",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="unstage",
                parameter={},
                metadata={"readout_priority": "monitored", "DIID": 17},
            ),
            "unstage_devices",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="rpc",
                parameter={
                    "device": "lsamy",
                    "func": "readback.get",
                    "rpc_id": "61a7376c-36cf-41af-94b1-76c1ba821d47",
                    "args": [],
                    "kwargs": {},
                },
                metadata={"readout_priority": "monitored", "DIID": 9},
            ),
            "send_rpc",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx", action="kickoff", parameter={}, metadata={}
            ),
            "kickoff_devices",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="baseline_reading",
                parameter={},
                metadata={"readout_priority": "baseline", "DIID": 15},
            ),
            "baseline_reading",
        ),
        (
            messages.DeviceInstructionMessage(device=None, action="close_scan_def", parameter={}),
            "close_scan",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None, action="publish_data_as_read", parameter={}
            ),
            "publish_data_as_read",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None, action="scan_report_instruction", parameter={}
            ),
            "process_scan_report_instruction",
        ),
        (
            messages.DeviceInstructionMessage(device=None, action="pre_scan", parameter={}),
            "pre_scan",
        ),
        (
            messages.DeviceInstructionMessage(device=None, action="complete", parameter={}),
            "complete_devices",
        ),
    ],
)
def test_instruction_step(scan_worker_mock, msg, method):
    worker = scan_worker_mock
    with mock.patch(
        f"bec_server.scan_server.scan_worker.ScanWorker.{method}"
    ) as instruction_method:
        worker._instruction_step(msg)
        instruction_method.assert_called_once()


def test_reset(scan_worker_mock):
    worker = scan_worker_mock
    worker._gropus = 1
    worker.current_scan_id = 1
    worker.current_scan_info = 1
    worker.scan_id = 1
    worker.interception_msg = 1
    worker.scan_motors = 1

    worker.reset()

    assert worker._groups == {}
    assert worker.current_scan_id == ""
    assert worker.current_scan_info == {}
    assert worker.scan_id == None
    assert worker.interception_msg == None
    assert worker.scan_motors == []


def test_cleanup(scan_worker_mock):
    worker = scan_worker_mock
    with mock.patch.object(worker, "unstage_devices") as unstage_mock:
        worker.cleanup()
        unstage_mock.assert_called_once_with(devices=list(worker._staged_devices), cleanup=True)


def test_shutdown(scan_worker_mock):
    worker = scan_worker_mock
    with mock.patch.object(worker.signal_event, "set") as set_mock:
        worker._started = mock.MagicMock()
        worker._started.is_set.return_value = True
        with mock.patch.object(worker, "join") as join_mock:
            worker.shutdown()
            set_mock.assert_called_once()
            join_mock.assert_called_once()
