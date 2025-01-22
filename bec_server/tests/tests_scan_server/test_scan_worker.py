# pylint: skip-file
import os
import uuid
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.utils import ConnectorMock
from bec_server.scan_server.errors import ScanAbortion
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


def test_wait_for_device_server(scan_worker_mock):
    worker = scan_worker_mock
    with mock.patch.object(worker.parent, "wait_for_service") as service_mock:
        worker._wait_for_device_server()
        service_mock.assert_called_once_with("DeviceServer")


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
                parameter={"num_points": 150, "scan_motors": ["samx", "samy"]},
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
                parameter={"num_points": 150},
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

    with mock.patch.object(worker, "current_instruction_queue_item") as queue_mock:
        with mock.patch.object(worker, "_initialize_scan_info") as init_mock:
            with mock.patch.object(worker.scan_report_instructions, "append") as instr_append_mock:
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
                        init_mock.assert_called_once_with(active_rb, instr, corr_num_points)
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
                "kwargs": {
                    "exp_time": 1,
                    "relative": True,
                    "system_config": {"file_suffix": None, "file_directory": None},
                },
                "num_points": 10,
            },
            queue="primary",
            metadata={
                "RID": "something",
                "system_config": {"file_suffix": None, "file_directory": None},
            },
        ),
        messages.ScanQueueMessage(
            scan_type="grid_scan",
            parameter={
                "args": {"samx": (-5, 5, 5), "samy": (-1, 1, 2)},
                "kwargs": {
                    "exp_time": 1,
                    "relative": True,
                    "system_config": {"file_suffix": "test", "file_directory": "tmp"},
                },
                "num_points": 10,
            },
            queue="primary",
            metadata={
                "RID": "something",
                "system_config": {"file_suffix": "test", "file_directory": "tmp"},
            },
        ),
        messages.ScanQueueMessage(
            scan_type="grid_scan",
            parameter={
                "args": {"samx": (-5, 5, 5), "samy": (-1, 1, 2)},
                "kwargs": {
                    "exp_time": 1,
                    "relative": True,
                    "system_config": {"file_suffix": "test", "file_directory": None},
                },
                "num_points": 10,
            },
            queue="primary",
            metadata={
                "RID": "something",
                "system_config": {"file_suffix": "test", "file_directory": None},
            },
        ),
    ],
)
def test_initialize_scan_info(scan_worker_mock, msg):
    worker = scan_worker_mock
    scan_server = scan_worker_mock.parent
    rb = RequestBlock(msg, assembler=ScanAssembler(parent=scan_server))
    assert rb.metadata == msg.metadata

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
        assert worker.current_scan_info["kwargs"] == msg.parameter["kwargs"]
        assert "samx" in worker.current_scan_info["readout_priority"]["monitored"]
        assert "samy" in worker.current_scan_info["readout_priority"]["baseline"]

        base_path = worker.parent._service_config.service_config["file_writer"]["base_path"]
        scan_nr = worker.current_scan_info["scan_number"]
        file_dir = msg.parameter["kwargs"]["system_config"]["file_directory"]
        suffix = msg.parameter["kwargs"]["system_config"]["file_suffix"]
        if file_dir is None:
            if suffix is None:
                file_dir = "S00000-00999/S00002"
            else:
                file_dir = f"S00000-00999/S00002_{suffix}"
        file_components = os.path.join(base_path, "data", file_dir, "S00002"), "h5"
        assert worker.current_scan_info["file_components"] == file_components


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


# @pytest.mark.parametrize(
#     "msg",
#     [
#         messages.DeviceInstructionMessage(
#             device=None,
#             action="stage",
#             parameter={},
#             metadata={"readout_priority": "async", "DIID": 18, "scan_id": "12345"},
#         )
#     ],
# )
# def test_stage_device(scan_worker_mock, msg):
#     worker = scan_worker_mock
#     worker.device_manager.devices["eiger"]._config["readoutPriority"] = "async"
#     worker.device_manager.devices["flyer_sim"]._config["readoutPriority"] = "on_request"

#     with mock.patch.object(worker, "_wait_for_stage") as wait_mock:
#         with mock.patch.object(worker.device_manager.connector, "send") as send_mock:
#             worker.stage_devices(msg)
#             on_request_device_names = [
#                 dev.name for dev in worker.device_manager.devices.on_request_devices()
#             ]

#             async_devices = worker.device_manager.devices.async_devices()
#             async_device_names = [dev.name for dev in async_devices]
#             excluded_devices = async_devices
#             excluded_devices.extend(worker.device_manager.devices.on_request_devices())
#             excluded_devices.extend(worker.device_manager.devices.continuous_devices())
#             devices = [
#                 dev.name
#                 for dev in worker.device_manager.devices.enabled_devices
#                 if dev not in excluded_devices
#             ]

#             for dev in [
#                 *worker.device_manager.devices.monitored_devices(),
#                 *worker.device_manager.devices.baseline_devices(),
#                 *worker.device_manager.devices.async_devices(),
#             ]:
#                 assert dev.name in worker._staged_devices
#             for async_dev in async_devices:
#                 assert (
#                     mock.call(
#                         MessageEndpoints.device_instructions(),
#                         messages.DeviceInstructionMessage(
#                             device=async_dev.name,
#                             action="stage",
#                             parameter=msg.content["parameter"],
#                             metadata=msg.metadata,
#                         ),
#                     )
#                     in send_mock.mock_calls
#                 )
#             assert (
#                 mock.call(
#                     MessageEndpoints.device_instructions(),
#                     messages.DeviceInstructionMessage(
#                         device=devices,
#                         action="stage",
#                         parameter=msg.content["parameter"],
#                         metadata=msg.metadata,
#                     ),
#                 )
#                 in send_mock.mock_calls
#             )
#             assert (
#                 mock.call(staged=True, devices=devices, metadata=msg.metadata)
#                 in wait_mock.mock_calls
#             )
#             assert (
#                 mock.call(staged=True, devices=async_device_names, metadata=msg.metadata)
#                 in wait_mock.mock_calls
#             )
#             for dev in on_request_device_names:
#                 assert dev not in worker._staged_devices


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
                metadata={"readout_priority": "monitored", "scan_id": "12345"},
            ),
            "open_scan",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="close_scan",
                parameter={},
                metadata={"readout_priority": "monitored", "scan_id": "12345"},
            ),
            "close_scan",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="trigger",
                parameter={"group": "trigger"},
                metadata={"readout_priority": "monitored", "point_id": 0},
            ),
            "forward_instruction",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx",
                action="set",
                parameter={"value": 1.3681828686580249},
                metadata={"readout_priority": "monitored"},
            ),
            "forward_instruction",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="read",
                parameter={"group": "monitored"},
                metadata={"readout_priority": "monitored", "point_id": 1},
            ),
            "forward_instruction",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="stage",
                parameter={},
                metadata={"readout_priority": "monitored"},
            ),
            "forward_instruction",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="unstage",
                parameter={},
                metadata={"readout_priority": "monitored"},
            ),
            "forward_instruction",
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
                metadata={"readout_priority": "monitored"},
            ),
            "forward_instruction",
        ),
        (
            messages.DeviceInstructionMessage(
                device="samx", action="kickoff", parameter={}, metadata={}
            ),
            "forward_instruction",
        ),
        (
            messages.DeviceInstructionMessage(
                device=None,
                action="baseline_reading",
                parameter={},
                metadata={"readout_priority": "baseline"},
            ),
            "forward_instruction",
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
            "forward_instruction",
        ),
        (
            messages.DeviceInstructionMessage(device=None, action="complete", parameter={}),
            "forward_instruction",
        ),
    ],
)
def test_instruction_step(scan_worker_mock, msg, method):
    worker = scan_worker_mock
    with mock.patch(
        f"bec_server.scan_server.scan_worker.ScanWorker.{method}"
    ) as instruction_method:
        with mock.patch.object(worker, "update_instr_with_scan_report") as update_mock:
            worker._instruction_step(msg)
            instruction_method.assert_called_once()
            if method == "set":
                update_mock.assert_called_once_with(msg)


def test_reset(scan_worker_mock):
    worker = scan_worker_mock
    worker.current_scan_id = 1
    worker.current_scan_info = 1
    worker.scan_id = 1
    worker.interception_msg = 1
    worker.scan_motors = 1

    worker.reset()

    assert worker.current_scan_id == ""
    assert worker.current_scan_info == {}
    assert worker.scan_id == None
    assert worker.interception_msg == None
    assert worker.scan_motors == []


def test_cleanup(scan_worker_mock):
    worker = scan_worker_mock
    with mock.patch.object(worker, "forward_instruction") as forward_mock:
        worker.cleanup()
        sent_message = forward_mock.mock_calls[0].args[0]
        diid = sent_message.metadata["device_instr_id"]
        devices = sent_message.device
        msg = messages.DeviceInstructionMessage(
            device=devices, action="unstage", parameter={}, metadata={"device_instr_id": diid}
        )
        forward_mock.assert_called_once_with(msg)


def test_shutdown(scan_worker_mock):
    worker = scan_worker_mock
    with mock.patch.object(worker.signal_event, "set") as set_mock:
        worker._started = mock.MagicMock()
        worker._started.is_set.return_value = True
        with mock.patch.object(worker, "join") as join_mock:
            worker.shutdown()
            set_mock.assert_called_once()
            join_mock.assert_called_once()


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionMessage(
            device=["samx"], action="set", parameter={"value": 1}, metadata={"scan_id": "scan_id"}
        )
    ],
)
def test_worker_update_instr_with_scan_report_no_update(msg, scan_worker_mock):
    worker = scan_worker_mock
    worker.current_instruction_queue_item = mock.MagicMock(spec=InstructionQueueItem)
    arb = worker.current_instruction_queue_item.active_request_block = mock.MagicMock(
        spec=RequestBlock
    )
    arb.scan_report_instructions = []
    with mock.patch.object(worker, "forward_instruction") as forward_mock:
        worker._instruction_step(msg)
        worker.update_instr_with_scan_report(msg)
        forward_mock.assert_called_once_with(msg)


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionMessage(
            device=["samx"], action="set", parameter={"value": 1}, metadata={"scan_id": "scan_id"}
        )
    ],
)
def test_worker_update_instr_with_scan_report_no_update_with_report(msg, scan_worker_mock):
    worker = scan_worker_mock
    worker.current_instruction_queue_item = mock.MagicMock(spec=InstructionQueueItem)
    arb = worker.current_instruction_queue_item.active_request_block = mock.MagicMock(
        spec=RequestBlock
    )
    arb.scan_report_instructions = [{"scan_progress": 10}]
    with mock.patch.object(worker, "forward_instruction") as forward_mock:
        worker._instruction_step(msg)
        worker.update_instr_with_scan_report(msg)
        forward_mock.assert_called_once_with(msg)


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionMessage(
            device=["samx"], action="set", parameter={"value": 1}, metadata={"scan_id": "scan_id"}
        )
    ],
)
def test_worker_update_instr_with_scan_report_update(msg, scan_worker_mock):
    worker = scan_worker_mock
    worker.current_instruction_queue_item = mock.MagicMock(spec=InstructionQueueItem)
    arb = worker.current_instruction_queue_item.active_request_block = mock.MagicMock(
        spec=RequestBlock
    )
    arb.scan_report_instructions = [
        {"readback": {"RID": "rid", "devices": ["samx"], "start": [0], "end": [1]}}
    ]
    with mock.patch.object(worker, "forward_instruction") as forward_mock:
        worker._instruction_step(msg)
        worker.update_instr_with_scan_report(msg)
        forward_mock.assert_called_once_with(msg)
        assert msg.metadata["response"] is True
