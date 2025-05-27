# pylint: skip-file
import os
import time
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
    storage.metadata = {"dataset_number": 10, "exit_status": "closed", "scan_name": "line_scan"}
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
        file_manager._scan_segment_callback(msg_raw, parent=file_manager)
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
        file_manager._scan_status_callback(msg_raw, parent=file_manager)
        assert mock_check_storage_status.call_args == mock.call(scan_id="scan_id")
        assert file_manager.scan_storage["scan_id"].status_msg == msg
        assert file_manager.scan_storage["scan_id"].scan_finished is True


def test_check_storage_status(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock

    with (
        mock.patch.object(scan_storage_mock, "ready_to_write") as mock_ready_to_write,
        mock.patch.object(file_manager, "update_baseline_reading") as mock_update_baseline,
        mock.patch.object(file_manager, "update_file_references") as mock_update_file_references,
        mock.patch.object(file_manager, "write_file") as mock_write_file,
    ):
        mock_ready_to_write.return_value = True
        file_manager.check_storage_status(scan_id="scan_id")
        assert mock_ready_to_write.called
        assert mock_update_baseline.call_args == mock.call("scan_id")
        assert mock_update_file_references.call_args == mock.call("scan_id")
        assert mock_write_file.call_args == mock.call("scan_id")


class MockWriter(HDF5FileWriter):
    def __init__(self, file_writer_manager):
        super().__init__(file_writer_manager)
        self.write_called = False

    def write(self, file_path: str, data, configuration_data, mode="w", file_handle=None):
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


def test_update_baseline_reading(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    with mock.patch.object(file_manager, "connector") as mock_connector:
        mock_connector.get.return_value = messages.ScanBaselineMessage(
            scan_id="scan_id", data={"data": "data"}
        )
        file_manager.update_baseline_reading("scan_id")
        assert file_manager.scan_storage["scan_id"].baseline == {"data": "data"}
        mock_connector.get.assert_called_once_with(MessageEndpoints.public_scan_baseline("scan_id"))


def test_scan_storage_append(scan_storage_mock):
    storage = scan_storage_mock
    storage.append(1, {"data": "data"})
    assert storage.scan_segments[1] == {"data": "data"}
    assert storage.scan_finished is False


def test_scan_storage_ready_to_write(scan_storage_mock):
    storage = scan_storage_mock
    storage.num_points = 1
    storage.scan_finished = True
    storage.append(1, {"data": "data"})
    assert storage.ready_to_write() is True


def test_update_file_references(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    with mock.patch.object(file_manager, "connector") as mock_connector:
        file_manager.update_file_references("scan_id")
        mock_connector.keys.assert_not_called()


def test_update_file_references_gets_keys(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    with mock.patch.object(file_manager, "connector") as mock_connector:
        file_manager.update_file_references("scan_id")
        mock_connector.keys.assert_called_once_with(MessageEndpoints.public_file("scan_id", "*"))


def test_update_scan_storage_with_status_ignores_none(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    file_manager.update_scan_storage_with_status(
        messages.ScanStatusMessage(scan_id=None, status="closed", info={})
    )
    assert file_manager.scan_storage == {}


def test_ready_to_write(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    scan_storage_mock.status_msg = messages.ScanStatusMessage(
        scan_id="scan_id", status="closed", info={}, readout_priority={"monitored": ["samx"]}
    )
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    file_manager.scan_storage["scan_id"].scan_finished = True
    file_manager.scan_storage["scan_id"].num_points = 1
    file_manager.scan_storage["scan_id"].scan_segments = {"0": {"data": np.zeros((10, 10))}}
    assert file_manager.scan_storage["scan_id"].ready_to_write() is True
    file_manager.scan_storage["scan_id1"] = scan_storage_mock
    file_manager.scan_storage["scan_id1"].scan_finished = True
    file_manager.scan_storage["scan_id1"].num_points = 2
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
        file_writer_manager_mock._device_configuration_callback(
            msg_obj, parent=file_writer_manager_mock
        )
        mock_update.assert_called_once_with("samx", msg)
