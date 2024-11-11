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
def file_writer_manager_mock():
    connector_cls = ConnectorMock
    config = ServiceConfig(
        redis={"host": "dummy", "port": 6379},
        service_config={
            "file_writer": {"plugin": "default_NeXus_format", "base_path": "./"},
            "log_writer": {"base_path": "./"},
        },
    )
    with (
        mock.patch.object(FileWriterManager, "_start_device_manager", return_value=None),
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

    file_manager._scan_segment_callback(msg_raw, parent=file_manager)
    assert file_manager.scan_storage["scan_id"].scan_segments[1] == {"data": "data"}


def test_scan_status_callback(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    msg = messages.ScanStatusMessage(
        scan_id="scan_id",
        status="closed",
        info={
            "scan_number": 1,
            "DIID": "DIID",
            "stream": "stream",
            "scan_type": "step",
            "num_points": 1,
            "enforce_sync": True,
        },
    )
    msg_raw = MessageObject(value=msg, topic="scan_status")

    file_manager._scan_status_callback(msg_raw, parent=file_manager)
    assert file_manager.scan_storage["scan_id"].scan_finished is True


class MockWriter(HDF5FileWriter):
    def __init__(self, file_writer_manager):
        super().__init__(file_writer_manager)
        self.write_called = False

    def write(self, file_path: str, data, mode="w", file_handle=None):
        self.write_called = True


def test_write_file(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock

    with mock.patch.object(
        file_manager.writer_mixin, "compile_full_filename"
    ) as mock_create_file_path:
        mock_create_file_path.return_value = "path"
        # replace NexusFileWriter with MockWriter
        file_manager.file_writer = MockWriter(file_manager)
        file_manager.write_file("scan_id")
        assert file_manager.file_writer.write_called is True


def test_write_file_invalid_scan_id(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    with mock.patch.object(
        file_manager.writer_mixin, "compile_full_filename"
    ) as mock_create_file_path:
        file_manager.write_file("scan_id1")
        mock_create_file_path.assert_not_called()


def test_write_file_invalid_scan_number(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    file_manager.scan_storage["scan_id"].scan_number = None
    with mock.patch.object(
        file_manager.writer_mixin, "compile_full_filename"
    ) as mock_create_file_path:
        file_manager.write_file("scan_id")
        mock_create_file_path.assert_not_called()


def test_write_file_raises_alarm_on_error(file_writer_manager_mock, scan_storage_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = scan_storage_mock
    with mock.patch.object(
        file_manager.writer_mixin, "compile_full_filename"
    ) as mock_compile_filename:
        with mock.patch.object(file_manager, "connector") as mock_connector:
            mock_compile_filename.return_value = "path"
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


def test_ready_to_write_forced(file_writer_manager_mock):
    file_manager = file_writer_manager_mock
    file_manager.scan_storage["scan_id"] = ScanStorage(10, "scan_id")
    file_manager.scan_storage["scan_id"].scan_finished = False
    file_manager.scan_storage["scan_id"].forced_finish = True
    assert file_manager.scan_storage["scan_id"].ready_to_write() is True
