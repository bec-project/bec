import os
import threading
from dataclasses import dataclass
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.device import ReadoutPriority
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.fixtures import dm_with_devices  # noqa: F401
from bec_lib.tests.utils import ConnectorMock
from bec_server.scan_server.instruction_handler import InstructionHandler
from bec_server.scan_server.scan_stubs import ScanStubStatus
from bec_server.scan_server.scans.scans_v4 import ScanBase


class _TestScan(ScanBase):
    scan_name = "_v4_test_scan"
    scan_type = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scan_info.scan_number = 1
        self.scan_info.dataset_number = 2
        self.scan_info.scan_report_devices = ["samx"]
        self.scan_info.readout_priority_modification = {
            "baseline": [],
            "monitored": [],
            "on_request": [],
            "async": [],
            "continuous": [],
        }
        self.scan_info.scan_report_instructions = []
        self.update_scan_info(
            num_points=3,
            num_monitored_readouts=0,
            exp_time=0.1,
            frames_per_trigger=1,
            settling_time=0.2,
            relative=False,
            run_on_exception_hook=False,
        )
        self.scan_info.readout_time = 0.3


@dataclass
class _ActionContext:
    actions: object
    connector: object
    device_manager: object
    scan: ScanBase


class _TestServiceConfig:
    def __init__(self, base_path: str):
        self.config = {"file_writer": {"base_path": base_path}}


class _TestParent:
    def __init__(self, base_path: str):
        self._service_config = _TestServiceConfig(base_path)


@pytest.fixture
def action_context(dm_with_devices):
    def _build(connector=None):
        connector = connector or ConnectorMock("")
        instruction_handler = InstructionHandler(connector)
        dm_with_devices.connector = connector
        scan = _TestScan(
            scan_id="scan-id-test",
            request_inputs={},
            system_config={},
            redis_connector=connector,
            device_manager=dm_with_devices,
            instruction_handler=instruction_handler,
        )
        return _ActionContext(
            actions=scan.actions, connector=connector, device_manager=dm_with_devices, scan=scan
        )

    return _build


def _sent_device_instructions(ctx, action):
    return [
        entry["msg"]
        for entry in ctx.connector.message_sent
        if getattr(entry["msg"], "action", None) == action
    ]


def _last_device_instruction(ctx, action):
    return _sent_device_instructions(ctx, action)[-1]


def _enabled_device_names(ctx):
    return [dev.root.name for dev in ctx.device_manager.devices.enabled_devices]


def _reading(device_name, value):
    return {
        device_name: {"value": value},
        f"{device_name}_setpoint": {"value": value},
        f"{device_name}_motor_is_moving": {"value": 0},
    }


def _set_readout_priority(ctx, **readout_groups):
    for device in ctx.device_manager.devices.values():
        device.root._config["readoutPriority"] = ReadoutPriority.ON_REQUEST
    for priority, device_names in readout_groups.items():
        readout_priority = ReadoutPriority[priority.upper()]
        for device_name in device_names:
            ctx.device_manager.devices[device_name].root._config[
                "readoutPriority"
            ] = readout_priority


def _set_software_triggered(ctx, *device_names):
    software_triggered = set(device_names)
    for device in ctx.device_manager.devices.values():
        device.root._config["softwareTrigger"] = device.root.name in software_triggered


def test_open_close_scan_send_scan_status(action_context):
    ctx = action_context()
    ctx.actions.check_for_unchecked_statuses = mock.MagicMock()
    ctx.actions._send_scan_status = mock.MagicMock()

    ctx.actions.open_scan()
    ctx.actions.close_scan()

    assert ctx.actions._send_scan_status.mock_calls == [mock.call("open"), mock.call("closed")]
    ctx.actions.check_for_unchecked_statuses.assert_called_once_with()


def test_build_scan_status_message(action_context):
    ctx = action_context()
    _set_readout_priority(
        ctx, monitored=["samx"], baseline=["samy"], on_request=["bpm4i"], **{"async": ["samz"]}
    )

    msg = ctx.actions._build_scan_status_message("open")

    assert msg.scan_id == "scan-id-test"
    assert msg.scan_name == "_v4_test_scan"
    assert msg.scan_number == 1
    assert msg.dataset_number == 2
    assert msg.num_points == 3
    assert msg.num_monitored_readouts == 0
    assert msg.scan_type is None
    assert msg.scan_parameters == {
        "exp_time": 0.1,
        "frames_per_trigger": 1,
        "settling_time": 0.2,
        "readout_time": 0.3,
        "relative": False,
        "system_config": {},
    }
    assert msg.readout_priority["monitored"] == ["samx"]
    assert msg.readout_priority["baseline"] == ["samy"]
    assert msg.readout_priority["async"] == ["samz"]
    assert "bpm4i" in msg.readout_priority["on_request"]


def test_device_instruction_actions_emit_expected_messages(action_context):
    ctx = action_context()
    ctx.scan.scan_info.metadata["RID"] = "rid-123"
    ctx.scan.scan_info.metadata["queue_id"] = "queue-123"

    stage_status = ctx.actions.stage("samx", wait=False)
    pre_scan_status = ctx.actions.pre_scan(wait=False)
    kickoff_status = ctx.actions.kickoff("samx", parameters={"frames": 3}, wait=False)
    complete_status = ctx.actions.complete("samy", wait=False)
    unstage_status = ctx.actions.unstage("samz", wait=False)

    stage_msg = _last_device_instruction(ctx, "stage")
    pre_scan_msg = _last_device_instruction(ctx, "pre_scan")
    kickoff_msg = _last_device_instruction(ctx, "kickoff")
    complete_msg = _last_device_instruction(ctx, "complete")
    unstage_msg = _last_device_instruction(ctx, "unstage")

    assert stage_msg.device == "samx"
    assert stage_msg.metadata["device_instr_id"] == stage_status._device_instr_id
    assert stage_msg.metadata["scan_id"] == "scan-id-test"
    assert stage_msg.metadata["RID"] == "rid-123"
    assert stage_msg.metadata["queue_id"] == "queue-123"
    assert pre_scan_msg.device == sorted(_enabled_device_names(ctx))
    assert pre_scan_msg.metadata["device_instr_id"] == pre_scan_status._device_instr_id
    assert kickoff_msg.device == "samx"
    assert kickoff_msg.parameter == {"configure": {"frames": 3}}
    assert kickoff_msg.metadata["device_instr_id"] == kickoff_status._device_instr_id
    assert complete_msg.device == "samy"
    assert complete_msg.metadata["device_instr_id"] == complete_status._device_instr_id
    assert unstage_msg.device == "samz"
    assert unstage_msg.metadata["device_instr_id"] == unstage_status._device_instr_id


def test_set_emits_one_instruction_per_device(action_context):
    ctx = action_context()

    status = ctx.actions.set(["samx", "samy"], [1.5, 2.5], wait=False)

    set_messages = _sent_device_instructions(ctx, "set")[-2:]
    assert (
        status._sub_status_objects[0]._device_instr_id
        == set_messages[0].metadata["device_instr_id"]
    )
    assert (
        status._sub_status_objects[1]._device_instr_id
        == set_messages[1].metadata["device_instr_id"]
    )
    assert [(msg.device, msg.parameter) for msg in set_messages] == [
        ("samx", {"value": 1.5}),
        ("samy", {"value": 2.5}),
    ]


def test_set_rejects_mismatched_device_and_value_counts(action_context):
    ctx = action_context()

    with pytest.raises(ValueError, match="number of devices and values"):
        ctx.actions.set(["samx", "samy"], [1.5], wait=False)


def test_read_actions_emit_expected_messages_and_point_ids(action_context):
    ctx = action_context()
    _set_readout_priority(ctx, baseline=["samz"], monitored=["samx", "samy"])

    baseline_status = ctx.actions.read_baseline_devices(wait=False)
    monitored_status_1 = ctx.actions.read_monitored_devices(wait=False)
    monitored_status_2 = ctx.actions.read_monitored_devices(wait=False)

    read_messages = _sent_device_instructions(ctx, "read")
    baseline_msg = read_messages[-3]
    monitored_msg_1 = read_messages[-2]
    monitored_msg_2 = read_messages[-1]
    assert baseline_msg.device == ["samz"]
    assert baseline_msg.metadata["readout_priority"] == "baseline"
    assert baseline_msg.metadata["device_instr_id"] == baseline_status._device_instr_id
    assert monitored_msg_1.device == ["samx", "samy"]
    assert monitored_msg_1.metadata["point_id"] == 0
    assert monitored_msg_1.metadata["device_instr_id"] == monitored_status_1._device_instr_id
    assert monitored_msg_2.metadata["point_id"] == 1
    assert monitored_msg_2.metadata["device_instr_id"] == monitored_status_2._device_instr_id


def test_empty_read_and_trigger_actions_return_done_status(action_context):
    ctx = action_context()
    _set_readout_priority(ctx)
    _set_software_triggered(ctx)

    baseline_status = ctx.actions.read_baseline_devices(wait=False)
    monitored_status = ctx.actions.read_monitored_devices(wait=False)
    trigger_status = ctx.actions.trigger_all_devices(wait=False)

    assert baseline_status.done
    assert monitored_status.done
    assert trigger_status.done
    assert not _sent_device_instructions(ctx, "read")
    assert not _sent_device_instructions(ctx, "trigger")


def test_trigger_all_devices_emits_software_triggered_devices(action_context):
    ctx = action_context()
    _set_software_triggered(ctx, "samy", "samx")

    status = ctx.actions.trigger_all_devices(wait=False)

    trigger_msg = _last_device_instruction(ctx, "trigger")
    assert trigger_msg.device == ["samx", "samy"]
    assert trigger_msg.metadata["device_instr_id"] == status._device_instr_id


def test_complete_and_unstage_all_devices_emit_enabled_devices(action_context):
    ctx = action_context()

    complete_status = ctx.actions.complete_all_devices(wait=False)
    unstage_status = ctx.actions.unstage_all_devices(wait=False)

    complete_msg = _last_device_instruction(ctx, "complete")
    unstage_msg = _last_device_instruction(ctx, "unstage")
    assert complete_msg.device == _enabled_device_names(ctx)
    assert complete_msg.metadata["device_instr_id"] == complete_status._device_instr_id
    assert unstage_msg.device == _enabled_device_names(ctx)
    assert unstage_msg.metadata["device_instr_id"] == unstage_status._device_instr_id


def test_stage_all_devices_stages_async_and_sync_devices(action_context):
    ctx = action_context()
    async_dev = ctx.device_manager.devices["samx"]
    on_request_dev = ctx.device_manager.devices["bpm4i"]
    continuous_dev = ctx.device_manager.devices["samz"]
    enabled_devices = [
        ctx.device_manager.devices["samx"],
        ctx.device_manager.devices["samy"],
        ctx.device_manager.devices["samz"],
        ctx.device_manager.devices["bpm4i"],
    ]
    container_status = ScanStubStatus(
        ctx.scan._instruction_handler,
        shutdown_event=threading.Event(),
        registry={},
        is_container=True,
        name="stage_all_devices",
    )
    container_status.add_status = mock.MagicMock(wraps=container_status.add_status)
    container_status.wait = mock.MagicMock()
    async_status = ScanStubStatus(
        ctx.scan._instruction_handler,
        shutdown_event=threading.Event(),
        registry={},
        name="stage_samx",
    )
    sync_status = ScanStubStatus(
        ctx.scan._instruction_handler,
        shutdown_event=threading.Event(),
        registry={},
        name="stage_sync_devices",
    )

    ctx.actions._create_status = mock.MagicMock(return_value=container_status)
    ctx.actions.stage = mock.MagicMock(side_effect=[async_status, sync_status])

    with (
        mock.patch.object(
            type(ctx.device_manager.devices), "async_devices", return_value=[async_dev]
        ),
        mock.patch.object(
            type(ctx.device_manager.devices), "on_request_devices", return_value=[on_request_dev]
        ),
        mock.patch.object(
            type(ctx.device_manager.devices), "continuous_devices", return_value=[continuous_dev]
        ),
        mock.patch.object(
            type(ctx.device_manager.devices),
            "enabled_devices",
            new_callable=mock.PropertyMock,
            return_value=enabled_devices,
        ),
    ):
        status = ctx.actions.stage_all_devices(wait=True)

    assert status is container_status
    assert ctx.actions.stage.mock_calls == [
        mock.call(async_dev, status_name="stage_samx", wait=False),
        mock.call(["samy"], status_name="stage_sync_devices", wait=False),
    ]
    assert container_status.add_status.mock_calls == [
        mock.call(async_status),
        mock.call(sync_status),
    ]
    container_status.wait.assert_called_once_with()


def test_report_instructions_update_scan_info_and_queue(action_context):
    ctx = action_context()
    ctx.actions._update_queue_info_callback = mock.MagicMock()

    ctx.actions.add_scan_report_instruction_readback(["samx"], [0], [1], "rid")
    ctx.actions.add_scan_report_instruction_device_progress("samy")
    ctx.actions.add_scan_report_instruction_scan_progress(points=5, show_table=False)

    assert ctx.scan.scan_info.scan_report_instructions == [
        {"readback": {"RID": "rid", "devices": ["samx"], "start": [0], "end": [1]}},
        {"device_progress": ["samy"]},
        {"scan_progress": {"points": 5, "show_table": False}},
    ]
    assert "samx" in ctx.actions._devices_with_required_response
    assert ctx.actions._update_queue_info_callback.call_count == 3


def test_rpc_call_returns_result_or_status(action_context):
    ctx = action_context()
    status = ScanStubStatus(
        ctx.scan._instruction_handler,
        device_instr_id="device-instr-id",
        shutdown_event=threading.Event(),
        registry={},
        name="rpc_samx_kickoff",
    )
    status.set_done({"ok": True})
    status.wait = mock.MagicMock()
    status._result_is_status = False
    ctx.actions._create_status = mock.MagicMock(return_value=status)
    ctx.actions._send = mock.MagicMock()

    result = ctx.actions.rpc_call("samx", "kickoff", 1, test=True)

    assert result == {"ok": True}
    sent_msg = ctx.actions._send.call_args.args[0]
    assert sent_msg.device == "samx"
    assert sent_msg.action == "rpc"
    assert sent_msg.parameter["device"] == "samx"
    assert sent_msg.parameter["func"] == "kickoff"
    assert sent_msg.parameter["args"] == (1,)
    assert sent_msg.parameter["kwargs"] == {"test": True}
    assert sent_msg.metadata["device_instr_id"] == "device-instr-id"
    status.wait.assert_called_once_with(resolve_on_known_type=True)

    status._result_is_status = True
    status.wait.reset_mock()
    ctx.actions._send.reset_mock()

    result = ctx.actions.rpc_call("samx", "kickoff")

    assert result is status
    status.wait.assert_called_once_with(resolve_on_known_type=True)


def test_send_scan_status_publishes_message(action_context):
    ctx = action_context()
    pipe = mock.MagicMock()
    ctx.connector.pipeline = mock.MagicMock(return_value=pipe)
    ctx.connector.set = mock.MagicMock()
    ctx.connector.set_and_publish = mock.MagicMock()
    status_msg = messages.ScanStatusMessage(scan_id="scan-id-test", status="closed", info={})
    ctx.actions._build_scan_status_message = mock.MagicMock(return_value=status_msg)

    ctx.actions._send_scan_status("closed", reason="alarm")

    ctx.actions._build_scan_status_message.assert_called_once_with(status="closed", reason="alarm")
    ctx.connector.set.assert_called_once_with(
        MessageEndpoints.public_scan_info("scan-id-test"), status_msg, pipe=pipe, expire=1800
    )
    ctx.connector.set_and_publish.assert_called_once_with(
        MessageEndpoints.scan_status(), status_msg, pipe=pipe
    )
    pipe.execute.assert_called_once_with()


def test_get_file_base_path_uses_account_and_templates(action_context):
    ctx = action_context()
    ctx.device_manager.parent = _TestParent("/tmp/data")
    ctx.connector.get_last = mock.MagicMock(
        return_value=messages.VariableMessage(value="test_account")
    )

    assert ctx.actions._get_file_base_path() == os.path.abspath("/tmp/data/test_account")

    ctx.device_manager.parent._service_config.config["file_writer"][
        "base_path"
    ] = "/tmp/$account/raw"
    assert ctx.actions._get_file_base_path() == os.path.abspath("/tmp/test_account/raw")


def test_get_file_base_path_rejects_invalid_account_and_template(action_context):
    ctx = action_context()
    ctx.device_manager.parent = _TestParent("/tmp/$missing/raw")
    ctx.connector.get_last = mock.MagicMock(
        return_value=messages.VariableMessage(value="bad/account")
    )

    with pytest.raises(ValueError, match="cannot contain a slash"):
        ctx.actions._get_file_base_path()

    ctx.connector.get_last = mock.MagicMock(return_value=None)
    with pytest.raises(ValueError, match="Invalid template variable"):
        ctx.actions._get_file_base_path()


def test_required_response_flag_is_added_for_registered_device(action_context):
    ctx = action_context()
    ctx.actions.add_device_with_required_response("samx")

    ctx.actions.stage(["samx", "samy"], wait=False)

    stage_msg = _last_device_instruction(ctx, "stage")
    assert stage_msg.metadata["response"] is True


def test_set_device_readout_priority_warns_after_reads(action_context):
    ctx = action_context()
    _set_readout_priority(ctx, monitored=["samx"])
    ctx.connector.raise_alarm = mock.MagicMock()

    ctx.actions.read_monitored_devices(wait=False)
    ctx.actions.set_device_readout_priority(["samy"], priority="monitored")

    assert ctx.scan.scan_info.readout_priority_modification["monitored"] == ["samy"]
    ctx.connector.raise_alarm.assert_called_once()


def test_check_for_unchecked_statuses_raises_cleanup_warnings(action_context):
    ctx = action_context()
    ctx.connector.raise_alarm = mock.MagicMock()
    unchecked_status = ctx.actions.stage("samx", wait=False)
    remaining_status = ctx.actions.complete("samy", wait=False)
    remaining_status.wait = mock.MagicMock()
    unchecked_status.set_done()

    ctx.actions.check_for_unchecked_statuses()

    assert ctx.connector.raise_alarm.call_count == 2
    alarm_types = [
        call.kwargs["info"].exception_type for call in ctx.connector.raise_alarm.mock_calls
    ]
    assert alarm_types == ["UncheckedStatusObjectsWarning", "ScanCleanupWarning"]
    remaining_status.wait.assert_called_once_with()


def test_read_manually_sends_read_with_return_result(action_context):
    ctx = action_context()

    status = ctx.actions.read_manually(["samy", "samx"], wait=False)

    read_messages = _sent_device_instructions(ctx, "read")
    msg = read_messages[-1]
    assert msg.device == ["samx", "samy"]
    assert msg.parameter == {"return_result": True}
    assert msg.metadata["device_instr_id"] == status._device_instr_id
    assert "point_id" not in msg.metadata


def test_publish_manual_read_validates_and_increments_point_id(action_context):
    ctx = action_context()
    _set_readout_priority(ctx, monitored=["samx", "samy"])
    readings = {"samy": _reading("samy", 2), "samx": _reading("samx", 1)}
    ctx.connector.pipeline = mock.MagicMock(wraps=ctx.connector.pipeline)

    ctx.actions.publish_manual_read(readings, wait=False)
    ctx.actions.publish_manual_read(
        [{"samy": _reading("samy", 4)}, {"samx": _reading("samx", 3)}], wait=False
    )

    assert not _sent_device_instructions(ctx, "publish_data_as_read")
    samx_read_messages = [
        entry["msg"]
        for entry in ctx.connector.message_sent
        if entry["queue"] == MessageEndpoints.device_read("samx").endpoint
    ]
    samy_read_messages = [
        entry["msg"]
        for entry in ctx.connector.message_sent
        if entry["queue"] == MessageEndpoints.device_read("samy").endpoint
    ]
    samx_readback_messages = [
        entry["msg"]
        for entry in ctx.connector.message_sent
        if entry["queue"] == MessageEndpoints.device_readback("samx").endpoint
    ]
    assert ctx.connector.pipeline.call_count == 2
    assert samx_read_messages[-2].signals == _reading("samx", 1)
    assert samy_read_messages[-2].signals == _reading("samy", 2)
    assert samx_read_messages[-2].metadata["point_id"] == 0
    assert samx_read_messages[-1].signals == _reading("samx", 3)
    assert samy_read_messages[-1].signals == _reading("samy", 4)
    assert samx_read_messages[-1].metadata["point_id"] == 1
    assert not samx_readback_messages


def test_publish_manual_read_uses_pipeline_with_fakeredis(action_context, connected_connector):
    ctx = action_context(connector=connected_connector)
    _set_readout_priority(ctx, monitored=["samx", "samy"])

    ctx.actions.publish_manual_read(
        {"samx": _reading("samx", 1), "samy": _reading("samy", 2)}, wait=False
    )

    samx_msg = connected_connector.get(MessageEndpoints.device_read("samx"))
    samy_msg = connected_connector.get(MessageEndpoints.device_read("samy"))
    assert samx_msg.signals == _reading("samx", 1)
    assert samx_msg.metadata["point_id"] == 0
    assert samy_msg.signals == _reading("samy", 2)
    assert samy_msg.metadata["point_id"] == 0
    assert connected_connector.get(MessageEndpoints.device_readback("samx")) is None


def test_publish_manual_read_rejects_wrong_devices(action_context):
    ctx = action_context()
    _set_readout_priority(ctx, monitored=["samx", "samy"])

    with pytest.raises(ValueError, match=r"Missing devices: \['samy'\]"):
        ctx.actions.publish_manual_read({"samx": _reading("samx", 1)}, wait=False)


def test_publish_manual_read_rejects_missing_signals(action_context):
    ctx = action_context()
    _set_readout_priority(ctx, monitored=["samx", "samy"])

    readings = {"samx": {"other_signal": {"value": 1}}, "samy": _reading("samy", 2)}
    with pytest.raises(ValueError, match=r"Missing signals: .*'samx': .*'samx'"):
        ctx.actions.publish_manual_read(readings, wait=False)
