# pylint: skip-file
import os
import threading
import time
from pathlib import Path
from unittest import mock

import numpy as np
import pytest

import bec_lib
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import MessageObject
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.file_writer import FileWriterManager
from bec_server.file_writer.async_writer import AsyncWriter
from bec_server.file_writer.file_writer import HDF5FileWriter
from bec_server.file_writer.file_writer_manager import ScanStorage

# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

dir_path = os.path.dirname(bec_lib.__file__)


@pytest.fixture
def scan_storage_mock():
    storage = ScanStorage(10, "scan_id")
    storage.start_time = time.time()
    storage.end_time = time.time()
    storage.num_points = 10
    storage.metadata = {"dataset_number": 10, "status": "closed", "scan_name": "line_scan"}
    yield storage


@pytest.fixture
def file_writer_manager_mock(dm_with_devices):
    connector_cls = ConnectorMock
    config = ServiceConfig(
        redis={"host": "dummy", "port": 6379},
        service_config={
            "file_writer": {"plugin": "default_NeXus_format", "base_path": "./"},
            "log_writer": {"base_path": "./"},
        },
    )

    def _start_device_manager(self):
        self.device_manager = dm_with_devices

    with (
        mock.patch.object(FileWriterManager, "_start_device_manager", _start_device_manager),
        mock.patch.object(FileWriterManager, "wait_for_service"),
    ):
        file_writer_manager_mock = FileWriterManager(config=config, connector_cls=connector_cls)
        try:
            yield file_writer_manager_mock
        finally:
            file_writer_manager_mock.shutdown()
            bec_logger.logger.remove()
            bec_logger._reset_singleton()


def test_scan_segment_callback(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    msg = messages.ScanMessage(
        point_id=1, scan_id="scan_id", data={"data": "data"}, metadata={"scan_number": 1}
    )
    msg_bundle = messages.BundleMessage()
    msg_bundle.append(msg)
    msg_raw = MessageObject(value=msg_bundle, topic="scan_segment")

    with mock.patch.object(
        file_writer_manager_mock, "check_storage_status"
    ) as mock_check_storage_status:
        file_manager._scan_segment_callback(msg_raw)
        assert mock_check_storage_status.call_args == mock.call(scan_id="scan_id")
        assert file_manager.scan_storage["scan_id"].scan_segments[1] == {"data": "data"}


def test_scan_status_callback(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    msg = messages.ScanStatusMessage(
        scan_id="scan_id",
        status="closed",
        scan_number=1,
        scan_type="step",
        num_points=1,
        info={"DIID": "DIID", "stream": "stream", "enforce_sync": True},
    )
    msg_raw = MessageObject(value=msg, topic="scan_status")

    with mock.patch.object(
        file_writer_manager_mock, "check_storage_status"
    ) as mock_check_storage_status:
        file_manager._scan_status_callback(msg_raw)
        assert mock_check_storage_status.call_args == mock.call(scan_id="scan_id")
        assert file_manager.scan_storage["scan_id"].status_msg == msg
        assert file_manager.scan_storage["scan_id"].scan_finished is True


def test_storage_copy_request_callback_invokes_plugin(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    file_manager._run_storage_copy_plugin = mock.Mock()
    msg = messages.StorageCopyRequestMessage(
        source_file="/tmp/source.h5", scope="alignment", subdir="safe/path"
    )
    msg_raw = MessageObject(value=msg, topic="storage/copy_request")

    file_manager._storage_copy_request_callback(msg_raw)

    file_manager._run_storage_copy_plugin.assert_called_once_with(msg)


def test_storage_copy_request_callback_ignores_invalid_message(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    file_manager._run_storage_copy_plugin = mock.Mock()
    msg_raw = MessageObject(value="bad-message", topic="storage/copy_request")

    with mock.patch("bec_server.file_writer.file_writer_manager.logger.error") as mock_error:
        file_manager._storage_copy_request_callback(msg_raw)

    file_manager._run_storage_copy_plugin.assert_not_called()
    mock_error.assert_called_once()


def test_storage_copy_request_callback_skips_when_plugin_runner_missing(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    file_manager._run_storage_copy_plugin = None
    msg = messages.StorageCopyRequestMessage(
        source_file="/tmp/source.h5", scope="alignment", subdir="safe/path"
    )
    msg_raw = MessageObject(value=msg, topic="storage/copy_request")

    with mock.patch("bec_server.file_writer.file_writer_manager.logger.error") as mock_error:
        file_manager._storage_copy_request_callback(msg_raw)

    mock_error.assert_called_once_with(
        "Storage copy request received but no storage copy plugin is installed."
    )


def test_load_storage_copy_plugin_uses_active_account(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    plugin = mock.Mock()

    with mock.patch(
        "bec_server.file_writer.file_writer_manager.plugin_helper.get_file_writer_storage_copy_plugin",
        return_value=plugin,
    ):
        runner = file_manager._load_storage_copy_plugin()

    with mock.patch.object(
        file_manager.connector, "get_last", return_value=messages.VariableMessage(value="p12345")
    ) as mock_get_last:
        runner(
            messages.StorageCopyRequestMessage(
                source_file="/tmp/source.h5", scope="alignment", subdir="../unsafe/folder"
            )
        )

    mock_get_last.assert_called_once_with(MessageEndpoints.account(), "data")
    plugin.assert_called_once_with("/tmp/source.h5", "alignment", "p12345", "unsafe/folder")


def test_load_storage_copy_plugin_returns_none_when_unavailable(file_writer_manager_mock):
    file_manager = file_writer_manager_mock

    with mock.patch(
        "bec_server.file_writer.file_writer_manager.plugin_helper.get_file_writer_storage_copy_plugin",
        return_value=None,
    ):
        runner = file_manager._load_storage_copy_plugin()

    assert runner is None


@pytest.mark.parametrize("account_msg", [None, messages.VariableMessage(value=1234)])
def test_load_storage_copy_plugin_defaults_empty_account(file_writer_manager_mock, account_msg):
    file_manager = file_writer_manager_mock
    plugin = mock.Mock()

    with mock.patch(
        "bec_server.file_writer.file_writer_manager.plugin_helper.get_file_writer_storage_copy_plugin",
        return_value=plugin,
    ):
        runner = file_manager._load_storage_copy_plugin()

    with mock.patch.object(file_manager.connector, "get_last", return_value=account_msg):
        runner(
            messages.StorageCopyRequestMessage(
                source_file="/tmp/source.h5", scope="alignment", subdir="~/results"
            )
        )

    plugin.assert_called_once_with("/tmp/source.h5", "alignment", "", "results")


def test_check_storage_status(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock

    with (
        mock.patch.object(scan_storage_mock, "ready_to_write") as mock_ready_to_write,
        mock.patch.object(file_manager, "write_file") as mock_write_file,
    ):
        mock_ready_to_write.return_value = True
        file_manager.check_storage_status(scan_id="scan_id")
        assert mock_ready_to_write.called
        assert mock_write_file.call_args == mock.call("scan_id")


class MockWriter(HDF5FileWriter):
    def __init__(self, file_writer_manager):
        super().__init__(file_writer_manager)
        self.write_called = False

    def write(
        self,
        file_path: str,
        data,
        configuration_data,
        mode="w",
        file_handle=None,
        written_async_signals=None,
    ):
        self.write_called = True


def test_write_file(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock

    with mock.patch("bec_server.file_writer.file_writer_manager.get_full_path") as mock_filename:
        mock_filename.return_value = "path"
        # replace NexusFileWriter with MockWriter
        file_manager.file_writer = MockWriter(file_manager)
        file_manager.write_file("scan_id")
        assert file_manager.file_writer.write_called is True


def test_write_file_forwards_written_async_signals(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    scan_storage_mock.async_writer = mock.Mock(
        written_signals={"waveform": ["waveform_data"]}, file_handle=None, error_info=None
    )
    file_manager.scan_storage["scan_id"] = scan_storage_mock

    with mock.patch("bec_server.file_writer.file_writer_manager.get_full_path") as mock_filename:
        mock_filename.return_value = "path"
        file_manager.file_writer.write = mock.Mock()
        file_manager.write_file("scan_id")

    file_manager.file_writer.write.assert_called_once()
    assert file_manager.file_writer.write.call_args.kwargs["written_async_signals"] == {
        "waveform": ["waveform_data"]
    }


def test_write_file_invalid_scan_id(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    with mock.patch("bec_server.file_writer.file_writer_manager.get_full_path") as mock_filename:
        file_manager.write_file("scan_id1")
        mock_filename.assert_not_called()


def test_write_file_invalid_scan_number(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    file_manager.scan_storage["scan_id"].scan_number = None
    with mock.patch("bec_server.file_writer.file_writer_manager.get_full_path") as mock_filename:
        file_manager.write_file("scan_id")
        mock_filename.assert_not_called()


def test_write_file_raises_alarm_on_error(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    with mock.patch("bec_server.file_writer.file_writer_manager.get_full_path") as mock_filename:
        with mock.patch.object(file_manager, "connector") as mock_connector:
            mock_filename.return_value = "path"
            # replace NexusFileWriter with MockWriter
            file_manager.file_writer = MockWriter(file_manager)
            file_manager.file_writer.write = mock.Mock(side_effect=Exception("error"))
            file_manager.write_file("scan_id")
            mock_connector.raise_alarm.assert_called_once()


def test_write_file_renames_tmp_file(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock

    with mock.patch("bec_server.file_writer.file_writer_manager.get_full_path") as mock_filename:
        mock_filename.return_value = "test_scan.h5"
        # replace NexusFileWriter with MockWriter
        file_manager.file_writer = MockWriter(file_manager)

        # Mock os.rename to track its calls
        with mock.patch("os.rename") as mock_rename, mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True  # Simulate that the .tmp file exists
            file_manager.write_file("scan_id")
            tmp_file_path = "test_scan.tmp"
            final_file_path = "test_scan.h5"
            mock_rename.assert_called_once_with(tmp_file_path, final_file_path)


def test_write_file_renames_tmp_file_on_exception(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock

    with mock.patch("bec_server.file_writer.file_writer_manager.get_full_path") as mock_filename:
        mock_filename.return_value = "test_scan.h5"
        # replace NexusFileWriter with MockWriter
        file_manager.file_writer = MockWriter(file_manager)

        # Mock os.rename to track its calls
        with mock.patch("os.rename") as mock_rename, mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True  # Simulate that the .tmp file exists
            # Force an exception during writing
            file_manager.file_writer.write = mock.Mock(side_effect=Exception("error"))
            try:
                file_manager.write_file("scan_id")
            except Exception:
                pass  # Ignore the exception for this test
            tmp_file_path = "test_scan.tmp"
            final_file_path = "test_scan.h5"
            mock_rename.assert_called_once_with(tmp_file_path, final_file_path)


def test_scan_storage_append(scan_storage_mock):
    storage = scan_storage_mock
    storage.append(1, {"data": "data"})
    assert storage.scan_segments[1] == {"data": "data"}
    assert storage.scan_finished is False


def test_scan_storage_ready_to_write(scan_storage_mock):
    storage = scan_storage_mock
    storage.num_monitored_readouts = 1
    storage.scan_finished = True
    storage.append(1, {"data": "data"})
    assert storage.ready_to_write() is True


def test_update_scan_storage_with_status_ignores_none(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    file_manager.update_scan_storage_with_status(
        messages.ScanStatusMessage(scan_id=None, status="closed", info={})
    )
    assert file_manager.scan_storage == {}


def test_update_scan_storage_with_status_waits_for_v4_sync_segments(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    storage = ScanStorage(1, "scan_id")
    storage.scan_segments = {point_id: {"data": point_id} for point_id in range(99)}
    file_manager.scan_storage["scan_id"] = storage
    msg = messages.ScanStatusMessage(
        scan_id="scan_id",
        status="closed",
        scan_number=1,
        scan_type="software_triggered",
        num_points=100,
        num_monitored_readouts=100,
        readout_priority={"monitored": ["samx"]},
        info={"scan_number": 1, "monitor_sync": None},
    )

    with (mock.patch.object(file_manager, "write_file") as write_file,):
        file_manager.update_scan_storage_with_status(msg)

    assert storage.enforce_sync is True
    assert storage.ready_to_write() is False
    write_file.assert_not_called()


def test_ready_to_write(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    scan_storage_mock.status_msg = messages.ScanStatusMessage(
        scan_id="scan_id", status="closed", info={}, readout_priority={"monitored": ["samx"]}
    )
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    file_manager.scan_storage["scan_id"].scan_finished = True
    file_manager.scan_storage["scan_id"].num_monitored_readouts = 1
    file_manager.scan_storage["scan_id"].scan_segments = {"0": {"data": np.zeros((10, 10))}}
    assert file_manager.scan_storage["scan_id"].ready_to_write() is True
    file_manager.scan_storage["scan_id1"] = scan_storage_mock
    file_manager.scan_storage["scan_id1"].scan_finished = True
    file_manager.scan_storage["scan_id1"].num_monitored_readouts = 2
    file_manager.scan_storage["scan_id1"].scan_segments = {"0": {"data": np.zeros((10, 10))}}
    assert file_manager.scan_storage["scan_id1"].ready_to_write() is False
    scan_storage_mock.status_msg = messages.ScanStatusMessage(
        scan_id="scan_id", status="closed", info={}, readout_priority={"monitored": ["samx"]}
    )
    assert file_manager.scan_storage["scan_id1"].ready_to_write() is False


def test_ready_to_write_forced(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = ScanStorage(10, "scan_id")
    file_manager.scan_storage["scan_id"].status_msg = messages.ScanStatusMessage(
        scan_id="scan_id", status="closed", info={}, readout_priority={"monitored": ["samx"]}
    )
    file_manager.scan_storage["scan_id"].scan_finished = False
    file_manager.scan_storage["scan_id"].forced_finish = True
    assert file_manager.scan_storage["scan_id"].ready_to_write() is True

    # Test case with scan finished, but not forced and no moniotred devices
    file_manager.scan_storage["scan_id"].forced_finish = False
    file_manager.scan_storage["scan_id"].scan_finished = True
    file_manager.scan_storage["scan_id"].status_msg = messages.ScanStatusMessage(
        scan_id="scan_id", status="closed", info={}, readout_priority={"monitored": []}
    )
    assert file_manager.scan_storage["scan_id"].ready_to_write() is True
    # Test enforce_sync is False
    file_manager.scan_storage["scan_id"].scan_finished = True
    file_manager.scan_storage["scan_id"].enforce_sync = False
    assert file_manager.scan_storage["scan_id"].ready_to_write() is True


def test_file_writer_manager_update_configuration(file_writer_manager_mock):
    msg = messages.DeviceMessage(signals={"samx_velocity": {"value": 1}})
    msg_obj = MessageObject(
        topic=MessageEndpoints.device_read_configuration("samx").endpoint, value=msg
    )
    with mock.patch.object(file_writer_manager_mock, "update_device_configuration") as mock_update:
        file_writer_manager_mock._device_configuration_callback(msg_obj)
        mock_update.assert_called_once_with("samx", msg)


def test_file_writer_manager_update_available_beamline_states(file_writer_manager_mock):
    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State1",
                state_type="DeviceWithinLimitsState",
                parameters={
                    "name": "State1",
                    "device": "samx",
                    "low_limit": 0.0,
                    "high_limit": 10.0,
                },
            )
        ]
    )

    file_writer_manager_mock._update_available_beamline_states({"data": msg})
    assert "State1" in file_writer_manager_mock.beamline_state_subscriptions

    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State2",
                state_type="DeviceWithinLimitsState",
                parameters={
                    "name": "State2",
                    "device": "samx",
                    "low_limit": 0.0,
                    "high_limit": 10.0,
                },
            )
        ]
    )
    file_writer_manager_mock._update_available_beamline_states({"data": msg})
    assert "State1" not in file_writer_manager_mock.beamline_state_subscriptions
    assert "State2" in file_writer_manager_mock.beamline_state_subscriptions


def test_file_writer_manager_updates_scan_storage_with_state(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    scan_storage = ScanStorage(10, "scan_id")
    scan_storage.status_msg = messages.ScanStatusMessage(
        scan_id="scan_id", status="open", info={}, readout_priority={"monitored": ["samx"]}
    )
    scan_storage.metadata["status"] = "open"
    file_manager.scan_storage["scan_id"] = scan_storage

    state_msg = messages.BeamlineStateMessage(name="State1", status="valid", label="Within limits")

    file_manager.update_beamline_state(state_msg)
    assert file_manager.scan_storage["scan_id"].beamline_states["State1"] == [state_msg]

    # verify that the latest state is kept in the file writer manager
    assert file_manager.beamline_states["State1"] == state_msg

    state_msg2 = messages.BeamlineStateMessage(name="State1", status="valid", label="Within limits")
    file_manager.update_beamline_state(state_msg2)
    assert file_manager.scan_storage["scan_id"].beamline_states["State1"] == [state_msg, state_msg2]
    assert file_manager.beamline_states["State1"] == state_msg2


def test_file_writer_manager_removes_beamline_state_subscription(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    scan_storage = ScanStorage(10, "scan_id")
    scan_storage.status_msg = messages.ScanStatusMessage(
        scan_id="scan_id", status="open", info={}, readout_priority={"monitored": ["samx"]}
    )
    scan_storage.metadata["status"] = "open"
    file_manager.scan_storage["scan_id"] = scan_storage
    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State1",
                state_type="DeviceWithinLimitsState",
                parameters={
                    "name": "State1",
                    "device": "samx",
                    "low_limit": 0.0,
                    "high_limit": 10.0,
                },
            )
        ]
    )
    file_manager._update_available_beamline_states({"data": msg})
    assert "State1" in file_manager.beamline_state_subscriptions

    state_msg = messages.BeamlineStateMessage(name="State1", status="valid", label="Within limits")

    file_manager.update_beamline_state(state_msg)
    assert file_manager.scan_storage["scan_id"].beamline_states["State1"] == [state_msg]

    # Remove the state by sending an empty list of states
    msg = messages.AvailableBeamlineStatesMessage(states=[])
    file_manager._update_available_beamline_states({"data": msg})
    assert "State1" not in file_manager.beamline_state_subscriptions
    assert "State1" not in file_manager.beamline_states


@pytest.mark.parametrize("enforce_sync,monitored", [(True, ["samx"]), (True, []), (False, [])])
def test_ready_to_write_waits_for_baseline(enforce_sync, monitored):
    storage = ScanStorage(1, "scan_id")
    storage.status_msg = messages.ScanStatusMessage(
        scan_id="scan_id",
        status="closed",
        info={"baseline_readout_requested": True},
        readout_priority={"monitored": monitored, "baseline": ["samz"]},
    )
    storage.enforce_sync = enforce_sync
    storage.scan_finished = True
    storage.num_monitored_readouts = 0

    assert storage.baseline_ready is False
    assert storage.ready_to_write() is False

    storage.baseline = {"samz": {"samz": {"value": 42, "timestamp": 1}}}

    assert storage.baseline_ready is True
    assert storage.ready_to_write() is True


@pytest.mark.parametrize("arrival_order", ["baseline_first", "baseline_last"])
def test_baseline_arrival_finalizes_scan_once(file_writer_manager_mock, arrival_order):
    manager = file_writer_manager_mock
    manager.scan_storage["scan_id"] = ScanStorage(1, "scan_id")
    baseline_msg = messages.ScanBaselineMessage(
        scan_id="scan_id", data={"samz": {"samz": {"value": 42, "timestamp": 1}}}
    )
    baseline_event = MessageObject(
        topic=MessageEndpoints.scan_baseline().endpoint, value=baseline_msg
    )
    closed_msg = messages.ScanStatusMessage(
        scan_id="scan_id",
        status="closed",
        scan_number=1,
        scan_type="software_triggered",
        num_points=1,
        num_monitored_readouts=1,
        readout_priority={"monitored": ["samx"], "baseline": ["samz"]},
        info={"scan_number": 1, "monitor_sync": "bec", "baseline_readout_requested": True},
    )
    segment = messages.ScanMessage(
        scan_id="scan_id", point_id=0, data={"samx": {"samx": {"value": 1, "timestamp": 1}}}
    )
    written_baselines = []

    def write_file(scan_id):
        written_baselines.append(manager.scan_storage.pop(scan_id).baseline)
        manager._finished_scan_ids.append(scan_id)

    with (
        mock.patch.object(manager.connector, "get", return_value=None) as get,
        mock.patch.object(manager, "write_file", side_effect=write_file) as write,
    ):
        if arrival_order == "baseline_first":
            manager._scan_baseline_callback(baseline_event)
            write.assert_not_called()

        manager.insert_to_scan_storage(segment)
        write.assert_not_called()
        manager.update_scan_storage_with_status(closed_msg)

        if arrival_order == "baseline_last":
            write.assert_not_called()
            manager._scan_baseline_callback(baseline_event)

        write.assert_called_once_with("scan_id")
        assert written_baselines == [baseline_msg.data]
        get.assert_not_called()
        assert "scan_id" not in manager.scan_storage

        manager._scan_baseline_callback(baseline_event)
        write.assert_called_once_with("scan_id")
        assert "scan_id" not in manager.scan_storage
        assert manager._pending_baseline_writes == {}


def test_baseline_callback_does_not_create_scan_storage(file_writer_manager_mock):
    manager = file_writer_manager_mock
    baseline = messages.ScanBaselineMessage(scan_id="unknown_scan", data={"samz": {}})
    with mock.patch.object(manager, "write_file") as write:
        manager._scan_baseline_callback(
            MessageObject(topic=MessageEndpoints.scan_baseline().endpoint, value=baseline)
        )
    write.assert_not_called()
    assert manager.scan_storage == {}
    assert manager._pending_baseline_writes == {"unknown_scan": baseline.data}


@pytest.mark.parametrize("status", ["aborted", "halted", "user_completed"])
def test_interrupted_scan_does_not_wait_for_baseline(file_writer_manager_mock, status):
    manager = file_writer_manager_mock
    storage = ScanStorage(1, "scan_id")
    manager.scan_storage["scan_id"] = storage
    status_msg = messages.ScanStatusMessage(
        scan_id="scan_id",
        status=status,
        scan_number=1,
        scan_type="software_triggered",
        num_monitored_readouts=1,
        readout_priority={"monitored": ["samx"], "baseline": ["samz"]},
        info={"scan_number": 1, "monitor_sync": "bec", "baseline_readout_requested": True},
    )
    with (
        mock.patch.object(manager.connector, "get", return_value=None),
        mock.patch.object(manager, "write_file") as write,
    ):
        manager.update_scan_storage_with_status(status_msg)
        assert storage.baseline_ready is False
        write.assert_called_once_with("scan_id")


@pytest.mark.parametrize("baseline_devices", [[], ["samz"]])
@pytest.mark.parametrize("requested_info", [{}, {"baseline_readout_requested": False}])
def test_scan_without_baseline_request_does_not_wait_for_baseline(
    file_writer_manager_mock, baseline_devices, requested_info
):
    manager = file_writer_manager_mock
    manager.scan_storage["scan_id"] = ScanStorage(1, "scan_id")
    closed_msg = messages.ScanStatusMessage(
        scan_id="scan_id",
        status="closed",
        scan_number=1,
        scan_type="software_triggered",
        num_monitored_readouts=0,
        readout_priority={"monitored": [], "baseline": baseline_devices},
        info={"scan_number": 1, "monitor_sync": "bec", **requested_info},
    )
    with (
        mock.patch.object(manager.connector, "get", return_value=None),
        mock.patch.object(manager, "write_file") as write,
    ):
        manager.update_scan_storage_with_status(closed_msg)
        write.assert_called_once_with("scan_id")


@pytest.mark.parametrize("first_update", ["status", "segment"])
def test_pending_notifications_are_flushed_when_storage_is_created(
    file_writer_manager_mock, first_update
):
    manager = file_writer_manager_mock
    baseline = messages.ScanBaselineMessage(
        scan_id="scan_id", data={"samz": {"samz": {"value": 42, "timestamp": 1}}}
    )
    files = {
        name: messages.FileMessage(file_path=f"/{name}.h5", done=True, successful=True)
        for name in ["eiger", "pilatus"]
    }
    manager._scan_baseline_callback(
        MessageObject(topic=MessageEndpoints.scan_baseline().endpoint, value=baseline)
    )
    for name, file_msg in files.items():
        manager._file_reference_callback(
            MessageObject(
                topic=MessageEndpoints.public_file("scan_id", name).endpoint, value=file_msg
            )
        )
    manager._pending_baseline_writes["other_scan"] = {"samz": {}}
    manager._pending_file_references["other_scan"] = {"eiger": files["eiger"]}
    assert manager.scan_storage == {}

    with mock.patch.object(manager, "write_file") as write:
        if first_update == "status":
            manager.update_scan_storage_with_status(
                messages.ScanStatusMessage(
                    scan_id="scan_id",
                    status="paused",
                    info={"scan_number": 1},
                    readout_priority={"baseline": ["samz"]},
                )
            )
        else:
            manager.insert_to_scan_storage(
                messages.ScanMessage(
                    scan_id="scan_id", point_id=0, data={}, metadata={"scan_number": 1}
                )
            )
        write.assert_not_called()

    storage = manager.scan_storage["scan_id"]
    assert storage.baseline == baseline.data
    assert storage.file_references == files
    assert "scan_id" not in manager._pending_baseline_writes
    assert "scan_id" not in manager._pending_file_references
    assert manager._pending_baseline_writes == {"other_scan": {"samz": {}}}
    assert manager._pending_file_references == {"other_scan": {"eiger": files["eiger"]}}


def test_baseline_before_first_status_allows_closed_scan_to_finalize(file_writer_manager_mock):
    manager = file_writer_manager_mock
    baseline = messages.ScanBaselineMessage(scan_id="scan_id", data={"samz": {}})
    manager._scan_baseline_callback(
        MessageObject(topic=MessageEndpoints.scan_baseline().endpoint, value=baseline)
    )
    with mock.patch.object(manager, "write_file") as write:
        manager.update_scan_storage_with_status(
            messages.ScanStatusMessage(
                scan_id="scan_id",
                status="closed",
                scan_type="software_triggered",
                num_monitored_readouts=0,
                readout_priority={"monitored": [], "baseline": ["samz"]},
                info={"scan_number": 1, "monitor_sync": "bec", "baseline_readout_requested": True},
            )
        )
        write.assert_called_once_with("scan_id")
    assert manager.scan_storage["scan_id"].baseline == baseline.data
    assert manager._pending_baseline_writes == {}


def test_file_reference_updates_active_storage(file_writer_manager_mock):
    manager = file_writer_manager_mock
    manager.scan_storage["scan_id"] = ScanStorage(1, "scan_id")
    first = messages.FileMessage(file_path="/eiger.h5", done=False, successful=False)
    finished = messages.FileMessage(file_path="/eiger.h5", done=True, successful=True)
    for file_msg in [first, finished]:
        manager._file_reference_callback(
            MessageObject(
                topic=MessageEndpoints.public_file("scan_id", "eiger").endpoint, value=file_msg
            )
        )
    assert manager.scan_storage["scan_id"].file_references == {"eiger": finished}
    assert manager._pending_file_references == {}


@pytest.mark.parametrize(
    "name,is_master_file", [("master", False), ("master", True), ("other", True)]
)
def test_master_file_notifications_are_not_buffered(file_writer_manager_mock, name, is_master_file):
    manager = file_writer_manager_mock
    file_msg = messages.FileMessage(
        file_path="/master.h5", done=True, successful=True, is_master_file=is_master_file
    )
    manager._file_reference_callback(
        MessageObject(topic=MessageEndpoints.public_file("scan_id", name).endpoint, value=file_msg)
    )
    assert manager.scan_storage == {}
    assert manager._pending_file_references == {}


def test_finished_scan_does_not_buffer_late_notifications(
    file_writer_manager_mock, scan_storage_mock
):
    manager = file_writer_manager_mock
    manager.scan_storage["scan_id"] = scan_storage_mock
    with (
        mock.patch(
            "bec_server.file_writer.file_writer_manager.get_full_path", return_value="scan.h5"
        ),
        mock.patch.object(manager.file_writer, "write"),
    ):
        manager.write_file("scan_id")

    baseline = messages.ScanBaselineMessage(scan_id="scan_id", data={"samz": {}})
    file_msg = messages.FileMessage(file_path="/eiger.h5", done=True, successful=True)
    manager._scan_baseline_callback(
        MessageObject(topic=MessageEndpoints.scan_baseline().endpoint, value=baseline)
    )
    manager._file_reference_callback(
        MessageObject(
            topic=MessageEndpoints.public_file("scan_id", "eiger").endpoint, value=file_msg
        )
    )
    assert manager.scan_storage == {}
    assert manager._pending_baseline_writes == {}
    assert manager._pending_file_references == {}


def test_subscriptions_buffer_early_notifications(file_writer_manager_mock, connected_connector):
    manager = file_writer_manager_mock
    received = threading.Event()
    baseline = messages.ScanBaselineMessage(scan_id="scan_id", data={"samz": {}})
    file_msg = messages.FileMessage(file_path="/eiger.h5", done=True, successful=True)

    def file_reference_callback(msg):
        manager._file_reference_callback(msg)
        received.set()

    connected_connector.register(
        MessageEndpoints.scan_baseline(), cb=manager._scan_baseline_callback
    )
    connected_connector.register(
        patterns=MessageEndpoints.public_file("*", "*"), cb=file_reference_callback
    )
    pipe = connected_connector.pipeline()
    connected_connector.set_and_publish(MessageEndpoints.scan_baseline(), baseline, pipe=pipe)
    connected_connector.set_and_publish(
        MessageEndpoints.public_file("scan_id", "eiger"), file_msg, pipe=pipe
    )
    pipe.execute()
    assert received.wait(5), "File reference subscription did not deliver its notification"
    assert manager.scan_storage == {}
    assert manager._pending_baseline_writes == {"scan_id": baseline.data}
    assert manager._pending_file_references == {"scan_id": {"eiger": file_msg}}

    with mock.patch.object(manager, "write_file") as write:
        manager.update_scan_storage_with_status(
            messages.ScanStatusMessage(
                scan_id="scan_id",
                status="closed",
                scan_type="software_triggered",
                num_monitored_readouts=0,
                readout_priority={"monitored": [], "baseline": ["samz"]},
                info={"scan_number": 1, "monitor_sync": "bec", "baseline_readout_requested": True},
            )
        )
        write.assert_called_once_with("scan_id")
    assert manager.scan_storage["scan_id"].baseline == baseline.data
    assert manager.scan_storage["scan_id"].file_references == {"eiger": file_msg}
    assert manager._pending_baseline_writes == {}
    assert manager._pending_file_references == {}


@pytest.mark.parametrize("failure_stage", ["poll", "write", "final_poll", None])
@pytest.mark.parametrize("finalization_fails", [False, True])
def test_async_failure_survives_finalization(
    file_writer_manager_mock,
    scan_storage_mock,
    tmp_path,
    failure_stage,
    finalization_fails,
    request,
):
    manager = file_writer_manager_mock
    path = str(tmp_path / "master.h5")
    writer = AsyncWriter(path, "scan_id", 10, manager.connector, ["det"], [])

    def close_file():
        if writer.file_handle is not None:
            writer.file_handle.close()

    request.addfinalizer(close_file)
    scan_storage_mock.async_writer = writer
    manager.scan_storage["scan_id"] = scan_storage_mock
    data = {
        "det": [
            messages.DeviceMessage(
                signals={"det": {"value": [1], "timestamp": 1}},
                metadata={"async_update": {"type": "add", "max_shape": [None]}},
            )
        ]
    }
    failure = RuntimeError("async acquisition failed")

    def poll(poll_timeout=500):
        if writer.written_signals:
            if failure_stage in ("poll", "final_poll"):
                raise failure
            writer.stop()
            return None
        return data

    original_write = writer.write_data

    def write(*args, **kwargs):
        original_write(*args, **kwargs)
        if failure_stage == "write":
            raise failure
        if failure_stage == "final_poll":
            writer.stop()

    with (
        mock.patch.object(writer, "poll_data", side_effect=poll),
        mock.patch.object(writer, "write_data", side_effect=write),
        mock.patch.object(manager.connector, "raise_alarm"),
    ):
        # Error propagation does not require a background thread or a timing deadline.
        writer.run()
    assert writer.written_signals == {"det": ["det"]}
    assert writer.file_handle["/entry/collection/devices/det/det/value"][:].tolist() == [1]
    if failure_stage:
        assert "async acquisition failed" in writer.error_info.compact_error_message
    else:
        assert writer.error_info is None
    with (
        mock.patch("bec_server.file_writer.file_writer_manager.get_full_path", return_value=path),
        mock.patch.object(
            manager.file_writer,
            "write",
            side_effect=RuntimeError("metadata failed") if finalization_fails else None,
        ) as finalize,
        mock.patch.object(manager.connector, "set_and_publish") as publish,
        mock.patch.object(manager.connector, "xadd") as xadd,
        mock.patch.object(manager.connector, "raise_alarm"),
    ):
        manager.write_file("scan_id")
    finalize.assert_called_once()
    outcome = publish.call_args.args[1]
    assert outcome.done is True
    assert outcome.successful is (failure_stage is None and not finalization_fails)
    # The logger may also publish through this connector, depending on test order.
    history_calls = [
        call
        for call in xadd.call_args_list
        if call.kwargs.get("topic") == MessageEndpoints.scan_history()
    ]
    if outcome.successful:
        assert len(history_calls) == 1
        assert history_calls[0].kwargs["msg_dict"]["data"].file_path == path
    else:
        assert not history_calls
    assert not writer.file_handle.id.valid
    assert Path(path).exists()
