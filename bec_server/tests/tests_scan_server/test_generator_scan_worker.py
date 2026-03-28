# pylint: skip-file
import os
import uuid
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.fixtures import dm_with_devices
from bec_lib.tests.utils import ConnectorMock
from bec_server.scan_server.errors import ScanAbortion, UserScanInterruption
from bec_server.scan_server.generator_scan_worker import GeneratorScanWorker
from bec_server.scan_server.scan_queue import (
    InstructionQueueItem,
    InstructionQueueStatus,
    RequestBlock,
)
from bec_server.scan_server.scan_worker import ScanWorker


@pytest.fixture
def scan_worker_mock(dm_with_devices) -> ScanWorker:
    dm_with_devices.connector = mock.MagicMock()
    dm_with_devices._rpc_method = mock.MagicMock()
    queue_manager = SimpleNamespace(instruction_handler=mock.MagicMock(), queues={"primary": mock.MagicMock()})
    parent = SimpleNamespace(
        device_manager=dm_with_devices,
        connector=mock.MagicMock(),
        queue_manager=queue_manager,
        wait_for_service=mock.MagicMock(),
        _service_config=SimpleNamespace(config={"file_writer": {"base_path": "/tmp/data"}}),
        scan_number=2,
        dataset_number=3,
    )
    scan_worker = ScanWorker(parent=parent)
    yield scan_worker


@pytest.fixture
def generator_worker_mock(scan_worker_mock) -> GeneratorScanWorker:
    return GeneratorScanWorker(worker=scan_worker_mock)


class InstructionQueueMock:
    def __init__(self):
        self.status = InstructionQueueStatus.PENDING
        self.idx = 1
        self.is_active = False
        self.stopped = False
        self.return_to_start = True
        self.queue_id = "queue-id"
        self.scan_msgs = []
        self.parent = SimpleNamespace(queue_manager=SimpleNamespace(send_queue_status=mock.MagicMock()))
        self.active_request_block = mock.MagicMock()
        self.active_request_block.scan = mock.MagicMock()
        self.active_request_block.scan.exp_time = 1
        self.active_request_block.scan.stubs._rpc_call = mock.MagicMock()
        self.active_request_block.scan.move_to_start.return_value = []
        self.active_request_block.scan_report_instructions = []
        self.active_request_block.metadata = {}
        self.queue = SimpleNamespace(request_blocks=[mock.MagicMock(scan=mock.MagicMock(stubs=SimpleNamespace(_rpc_call=mock.MagicMock())) )], active_rb=SimpleNamespace(scan_id="scan-id"))

    def append_to_queue_history(self):
        pass

    def stop(self):
        pass

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

        raise StopIteration

    def __iter__(self):
        return self


def test_wait_for_device_server(generator_worker_mock):
    worker = generator_worker_mock
    with mock.patch.object(worker.worker.parent, "wait_for_service") as service_mock:
        worker._wait_for_device_server()
        service_mock.assert_called_once_with("DeviceServer")


def test_publish_data_as_read(generator_worker_mock):
    worker = generator_worker_mock
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
    with mock.patch.object(worker.worker.device_manager, "connector") as connector_mock:
        worker.publish_data_as_read(instr)
        msg = messages.DeviceMessage(
            signals=instr.content["parameter"]["data"], metadata=instr.metadata
        )
        connector_mock.set_and_publish.assert_called_once_with(
            MessageEndpoints.device_read("samx"), msg
        )


def test_publish_data_as_read_multiple(generator_worker_mock):
    worker = generator_worker_mock
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
    with mock.patch.object(worker.worker.device_manager, "connector") as connector_mock:
        worker.publish_data_as_read(instr)
        mock_calls = []
        for device, dev_data in zip(devices, data):
            msg = messages.DeviceMessage(signals=dev_data, metadata=instr.metadata)
            mock_calls.append(mock.call(MessageEndpoints.device_read(device), msg))
        assert connector_mock.set_and_publish.mock_calls == mock_calls


def test_check_for_interruption(generator_worker_mock):
    worker = generator_worker_mock
    worker.worker.status = InstructionQueueStatus.STOPPED
    with pytest.raises(ScanAbortion):
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
def test_open_scan(generator_worker_mock, instr, corr_num_points, scan_id):
    worker = generator_worker_mock

    if not scan_id:
        assert worker.scan_id is None
    else:
        worker.scan_id = 111
        worker.scan_motors = ["bpm4i"]

    if "point_id" in instr.metadata:
        worker.max_point_id = instr.metadata["point_id"]

    queue_mock = mock.MagicMock()
    queue_mock.active_request_block.scan_report_instructions = []
    queue_mock.active_request_block.scan.show_live_table = True
    queue_mock.active_request_block.scan.use_scan_progress_report = True
    queue_mock.parent.queue_manager.send_queue_status = mock.MagicMock()
    worker.worker.current_instruction_queue_item = queue_mock

    with mock.patch.object(worker, "_initialize_scan_info") as init_mock:
        with mock.patch.object(worker, "_send_scan_status") as send_mock:
            worker.open_scan(instr)

            if not scan_id:
                assert worker.scan_id == instr.metadata.get("scan_id")
                assert worker.scan_motors == [
                    worker.worker.device_manager.devices["samx"],
                    worker.worker.device_manager.devices["samy"],
                ]
            else:
                assert worker.scan_id == 111
                assert worker.scan_motors == ["bpm4i"]
            init_mock.assert_called_once_with(queue_mock.active_request_block, instr, corr_num_points)
            assert queue_mock.active_request_block.scan_report_instructions == [
                {"scan_progress": {"points": corr_num_points, "show_table": True}}
            ]
            queue_mock.parent.queue_manager.send_queue_status.assert_called_once()
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
def test_initialize_scan_info(generator_worker_mock, msg):
    worker = generator_worker_mock
    rb = SimpleNamespace(
        metadata=msg.metadata,
        scan=SimpleNamespace(
            frames_per_trigger=1,
            settling_time=0,
            readout_time=0,
            scan_report_devices=[],
            monitor_sync="bec",
            scan_parameters=msg.parameter["kwargs"],
            request_inputs=msg.parameter,
            parameter=msg.parameter,
        ),
    )

    worker.worker.current_instruction_queue_item = mock.MagicMock(scan_msgs=[])
    worker.scan_motors = ["samx"]
    worker.readout_priority = {
        "monitored": ["samx"],
        "baseline": [],
        "async": [],
        "continuous": [],
        "on_request": [],
    }
    open_scan_msg = messages.DeviceInstructionMessage(
        device=None,
        action="open_scan",
        parameter={"num_points": msg.content["parameter"].get("num_points")},
        metadata=msg.metadata,
    )
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

    base_path = worker.worker.parent._service_config.config["file_writer"]["base_path"]
    file_dir = msg.parameter["kwargs"]["system_config"]["file_directory"]
    suffix = msg.parameter["kwargs"]["system_config"]["file_suffix"]
    if file_dir is None:
        if suffix is None:
            file_dir = "S00000-00999/S00002"
        else:
            file_dir = f"S00000-00999/S00002_{suffix}"
    file_components = os.path.abspath(os.path.join(base_path, file_dir, "S00002")), "h5"
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
def test_close_scan(generator_worker_mock, msg, scan_id, max_point_id, exp_num_points):
    worker = generator_worker_mock
    worker.scan_id = scan_id
    worker.current_scan_info["num_points"] = 19

    reset = bool(worker.scan_id == msg.metadata["scan_id"])
    with mock.patch.object(worker, "_send_scan_status") as send_scan_status_mock:
        worker.close_scan(msg, max_point_id=max_point_id)
        if reset:
            send_scan_status_mock.assert_called_with("closed")
            assert worker.scan_id is None
        else:
            assert worker.scan_id == scan_id
    assert worker.current_scan_info["num_points"] == exp_num_points


@pytest.mark.parametrize("status,expire", [("open", None), ("closed", 1800), ("aborted", 1800)])
def test_send_scan_status(generator_worker_mock, status, expire):
    worker = generator_worker_mock
    worker.worker.device_manager.connector = ConnectorMock()
    worker.current_scan_id = str(uuid.uuid4())
    worker.current_scan_info = {"scan_number": 5}
    worker._send_scan_status(status)
    scan_info_msgs = [
        msg
        for msg in worker.worker.device_manager.connector.message_sent
        if msg["queue"] == MessageEndpoints.public_scan_info(scan_id=worker.current_scan_id).endpoint
    ]
    assert len(scan_info_msgs) == 1
    assert scan_info_msgs[0]["expire"] == expire


@pytest.mark.parametrize("abortion", [False, True])
def test_process_instructions(generator_worker_mock, abortion):
    worker = generator_worker_mock
    queue = InstructionQueueMock()
    worker.worker.device_manager._rpc_method.return_value = mock.MagicMock(
        __enter__=mock.MagicMock(return_value=None),
        __exit__=mock.MagicMock(return_value=None),
    )

    with mock.patch.object(worker, "_wait_for_device_server") as wait_mock:
        with mock.patch.object(worker, "reset") as reset_mock:
            with mock.patch.object(worker, "_check_for_interruption") as interruption_mock:
                queue.queue.request_blocks.append(mock.MagicMock())
                with mock.patch.object(queue.queue, "active_rb") as rb_mock:
                    with mock.patch.object(worker, "_instruction_step") as step_mock:
                        if abortion:
                            interruption_mock.side_effect = ScanAbortion
                            with pytest.raises(ScanAbortion):
                                worker.process_instructions(queue)
                        else:
                            worker.process_instructions(queue)

                        assert worker.max_point_id == 0
                        wait_mock.assert_called_once()

                        if not abortion:
                            assert interruption_mock.call_count == 4
                            assert worker._exposure_time == 1
                            assert step_mock.call_count == 4
                            assert queue.is_active is False
                            assert queue.status == InstructionQueueStatus.COMPLETED
                            assert worker.worker.current_instruction_queue_item is None
                            reset_mock.assert_called_once()

                        else:
                            assert queue.stopped is True
                            assert interruption_mock.call_count == 1
                            assert queue.is_active is True
                            assert queue.status == InstructionQueueStatus.PENDING
                            assert worker.worker.current_instruction_queue_item == queue


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
            messages.DeviceInstructionMessage(device="samx", action="kickoff", parameter={}, metadata={}),
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
            messages.DeviceInstructionMessage(device=None, action="publish_data_as_read", parameter={}),
            "publish_data_as_read",
        ),
        (
            messages.DeviceInstructionMessage(device=None, action="scan_report_instruction", parameter={}),
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
def test_instruction_step(generator_worker_mock, msg, method):
    worker = generator_worker_mock
    with mock.patch(
        f"bec_server.scan_server.generator_scan_worker.GeneratorScanWorker.{method}"
    ) as instruction_method:
        with mock.patch.object(worker, "update_instr_with_scan_report") as update_mock:
            worker._instruction_step(msg)
            instruction_method.assert_called_once()
            if method == "set":
                update_mock.assert_called_once_with(msg)


def test_reset(generator_worker_mock):
    worker = generator_worker_mock
    worker.current_scan_id = 1
    worker.current_scan_info = 1
    worker.scan_id = 1
    worker.interception_msg = 1
    worker.scan_motors = 1
    worker.worker.current_instruction_queue_item = 1

    worker.reset()

    assert worker.current_scan_id == ""
    assert worker.current_scan_info == {}
    assert worker.scan_id is None
    assert worker.interception_msg is None
    assert worker.scan_motors == []
    assert worker.worker.current_instruction_queue_item is None


def test_cleanup(generator_worker_mock):
    worker = generator_worker_mock
    with mock.patch.object(worker, "forward_instruction") as forward_mock:
        worker.cleanup()
        sent_message = forward_mock.mock_calls[0].args[0]
        diid = sent_message.metadata["device_instr_id"]
        devices = sent_message.device
        msg = messages.DeviceInstructionMessage(
            device=devices, action="unstage", parameter={}, metadata={"device_instr_id": diid}
        )
        forward_mock.assert_called_once_with(msg)


@pytest.mark.parametrize(
    "msg",
    [
        messages.DeviceInstructionMessage(
            device=["samx"], action="set", parameter={"value": 1}, metadata={"scan_id": "scan_id"}
        )
    ],
)
def test_worker_update_instr_with_scan_report_no_update(msg, generator_worker_mock):
    worker = generator_worker_mock
    worker.worker.current_instruction_queue_item = mock.MagicMock(spec=InstructionQueueItem)
    arb = worker.worker.current_instruction_queue_item.active_request_block = mock.MagicMock(
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
def test_worker_update_instr_with_scan_report_no_update_with_report(msg, generator_worker_mock):
    worker = generator_worker_mock
    worker.worker.current_instruction_queue_item = mock.MagicMock(spec=InstructionQueueItem)
    arb = worker.worker.current_instruction_queue_item.active_request_block = mock.MagicMock(
        spec=RequestBlock
    )
    arb.scan_report_instructions = [{"scan_progress": {"points": 10, "show_table": True}}]
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
def test_worker_update_instr_with_scan_report_update(msg, generator_worker_mock):
    worker = generator_worker_mock
    worker.worker.current_instruction_queue_item = mock.MagicMock(spec=InstructionQueueItem)
    arb = worker.worker.current_instruction_queue_item.active_request_block = mock.MagicMock(
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


@pytest.mark.parametrize(
    "base_path, current_account_msg, expected_path, raises_error",
    [
        (
            "/data/$account/raw",
            messages.VariableMessage(value="test_account"),
            "/data/test_account/raw",
            False,
        ),
        ("/data/$account/raw", None, "/data/raw", False),
        (
            "/data/raw",
            messages.VariableMessage(value="test_account"),
            "/data/raw/test_account",
            False,
        ),
        ("/data/raw", None, "/data/raw", False),
        (
            "/data/$account/$sub_dir/raw",
            messages.VariableMessage(value="test_account"),
            "/data/test_account/$sub_dir/raw",
            True,
        ),
    ],
)
def test_worker_get_file_base_path(
    generator_worker_mock, base_path, current_account_msg, expected_path, raises_error
):
    worker = generator_worker_mock
    file_writer_base_path_orig = worker.worker.parent._service_config.config["file_writer"]["base_path"]
    try:
        worker.worker.parent._service_config.config["file_writer"]["base_path"] = base_path
        with mock.patch.object(worker.worker.connector, "get_last", return_value=current_account_msg):
            if raises_error:
                with pytest.raises(ValueError):
                    worker._get_file_base_path()
            else:
                file_path = worker._get_file_base_path()
                assert file_path == expected_path
                worker.worker.connector.get_last.assert_called_once_with(
                    MessageEndpoints.account(), "data"
                )
    finally:
        worker.worker.parent._service_config.config["file_writer"]["base_path"] = (
            file_writer_base_path_orig
        )


@pytest.mark.parametrize(
    "scan_info, out",
    [
        (None, {}),
        ({}, {}),
        ({"scan_id": "12345"}, {"scan_id": "12345"}),
        ({"scan_number": 1}, {"scan_number": 1}),
        ({"scan_id": "12345", "scan_number": 1}, {"scan_id": "12345", "scan_number": 1}),
    ],
)
def test_worker_get_metadata_for_alarm(generator_worker_mock, scan_info, out):
    worker = generator_worker_mock
    worker.current_scan_info = scan_info
    metadata = worker.get_metadata_for_alarm()
    assert metadata == out


def test_handle_scan_abortion(generator_worker_mock):
    worker = generator_worker_mock
    queue = mock.MagicMock(spec=InstructionQueueItem)
    queue.exit_info = None
    queue.queue_id = "id-12345"
    with mock.patch.object(worker, "_send_scan_status") as send_status_mock:
        abortion = ScanAbortion()
        worker._handle_scan_abortion(queue, abortion)
        send_status_mock.assert_called_once_with("aborted", reason="alarm")


def test_handle_scan_halt(generator_worker_mock):
    worker = generator_worker_mock
    queue = mock.MagicMock(spec=InstructionQueueItem)
    queue.exit_info = None
    queue.queue_id = "id-12345"
    with mock.patch.object(worker, "_send_scan_status") as send_status_mock:
        abortion = ScanAbortion()
        queue.return_to_start = False
        worker._handle_scan_abortion(queue, abortion)
        send_status_mock.assert_called_once_with("halted", reason="alarm")


def test_handle_user_scan_interruption(generator_worker_mock):
    worker = generator_worker_mock
    queue = mock.MagicMock(spec=InstructionQueueItem)
    queue.exit_info = None
    queue.queue_id = "id-12345"
    with mock.patch.object(worker, "_send_scan_status") as send_status_mock:
        interruption = UserScanInterruption(exit_info=("user_completed", "user"))
        worker._handle_scan_abortion(queue, interruption)
        send_status_mock.assert_called_once_with("user_completed", reason="user")


def test_handle_user_scan_interruption_followed_by_abortion(generator_worker_mock):
    worker = generator_worker_mock
    queue = mock.MagicMock(spec=InstructionQueueItem)
    queue.exit_info = "user_completed"
    queue.queue_id = "id-12345"
    with mock.patch.object(worker, "_send_scan_status") as send_status_mock:
        interruption = UserScanInterruption(exit_info=("user_completed", "user"))
        abortion = ScanAbortion()
        worker._handle_scan_abortion(queue, interruption)
        worker._handle_scan_abortion(queue, abortion)
        send_status_mock.mock_calls[0].assert_called_with("user_completed", reason="user")
        send_status_mock.mock_calls[1].assert_called_with("user_completed", reason="user")
