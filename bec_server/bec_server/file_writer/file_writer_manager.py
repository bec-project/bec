from __future__ import annotations

import threading
import time
import traceback

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_service import BECService
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import FileWriter
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import MessageObject, RedisConnector
from bec_lib.service_config import ServiceConfig
from bec_server.file_writer.async_writer import AsyncWriter
from bec_server.file_writer.file_writer import HDF5FileWriter

logger = bec_logger.logger


class ScanStorage:
    def __init__(self, scan_number: int, scan_id: str) -> None:
        """
        Helper class to store scan data until it is ready to be written to file.

        Args:
            scan_number (int): Scan number
            scan_id (str): Scan ID
        """
        self.scan_number = scan_number
        self.scan_id = scan_id
        self.scan_segments = {}
        self.scan_finished = False
        self.num_points = None
        self.baseline = {}
        self.async_data = {}
        self.metadata = {}
        self.file_references = {}
        self.start_time = None
        self.end_time = None
        self.enforce_sync = True
        self.forced_finish = False

    def append(self, point_id, data):
        """
        Append data to the scan storage.

        Args:
            point_id (int): Point ID
            data (dict): Data to be stored
        """
        self.scan_segments[point_id] = data

    def ready_to_write(self) -> bool:
        """
        Check if the scan is ready to be written to file.
        """
        if self.forced_finish:
            return True
        if self.enforce_sync:
            return self.scan_finished and (self.num_points == len(self.scan_segments))
        return self.scan_finished and self.scan_number is not None


class FileWriterManager(BECService):
    def __init__(self, config: ServiceConfig, connector_cls: RedisConnector) -> None:
        """
        Service to write scan data to file.

        Args:
            config (ServiceConfig): Service config
            connector_cls (RedisConnector): Connector class
        """
        super().__init__(config, connector_cls, unique_service=True)
        self._lock = threading.RLock()
        self.file_writer_config = self._service_config.service_config.get("file_writer")
        self._start_device_manager()
        self.connector.register(
            MessageEndpoints.scan_segment(), cb=self._scan_segment_callback, parent=self
        )
        self.connector.register(
            MessageEndpoints.scan_status(), cb=self._scan_status_callback, parent=self
        )

        self.async_writer = None
        self.scan_storage = {}
        self.writer_mixin = FileWriter(
            service_config=self.file_writer_config, connector=self.connector
        )
        self.file_writer = HDF5FileWriter(self)

        self.status = messages.BECStatus.RUNNING

    def _start_device_manager(self):
        self.wait_for_service("DeviceServer")
        self.device_manager = DeviceManagerBase(self)
        self.device_manager.initialize([self.bootstrap_server])

    def _scan_segment_callback(self, msg: MessageObject, *, parent: FileWriterManager):
        msgs = msg.value
        for scan_msg in msgs:
            parent.insert_to_scan_storage(scan_msg)

    @staticmethod
    def _scan_status_callback(msg, *, parent):
        msg = msg.value
        parent.update_scan_storage_with_status(msg)

    def update_scan_storage_with_status(self, msg: messages.ScanStatusMessage) -> None:
        """
        Update the scan storage with the scan status.

        Args:
            msg (messages.ScanStatusMessage): Scan status message
        """
        scan_id = msg.content.get("scan_id")
        if scan_id is None:
            return

        if not self.scan_storage.get(scan_id):
            self.scan_storage[scan_id] = ScanStorage(
                scan_number=msg.content["info"].get("scan_number"), scan_id=scan_id
            )
        metadata = msg.content.get("info").copy()
        metadata.pop("DIID", None)
        metadata.pop("stream", None)

        scan_storage = self.scan_storage[scan_id]
        scan_storage.metadata.update(metadata)
        status = msg.content.get("status")
        if status:
            scan_storage.metadata["exit_status"] = status
        if status == "open" and not scan_storage.start_time:
            scan_storage.start_time = msg.content.get("timestamp")
            self.async_writer = AsyncWriter(
                self.writer_mixin.compile_full_filename(suffix="master"),
                scan_id=scan_id,
                connector=self.connector,
                devices=self.device_manager.devices.async_devices(),
            )
            self.async_writer.start()

        if status in ["closed", "aborted", "halted"]:
            if status in ["aborted", "halted"]:
                scan_storage.forced_finish = True
            if not scan_storage.end_time:
                scan_storage.end_time = msg.content.get("timestamp")

            scan_storage.scan_finished = True
            info = msg.content.get("info")
            if info:
                scan_storage.num_points = info["num_points"]
                if info["scan_type"] == "step":
                    scan_storage.enforce_sync = True
                else:
                    scan_storage.enforce_sync = info["monitor_sync"] == "bec"

            if self.async_writer:
                self.async_writer.stop()
                self.async_writer.join()

            self.check_storage_status(scan_id=scan_id)

    def insert_to_scan_storage(self, msg: messages.ScanMessage) -> None:
        """
        Insert scan data to the scan storage.

        Args:
            msg (messages.ScanMessage): Scan message
        """
        scan_id = msg.content.get("scan_id")
        if scan_id is None:
            return
        if not self.scan_storage.get(scan_id):
            self.scan_storage[scan_id] = ScanStorage(
                scan_number=msg.metadata.get("scan_number"), scan_id=scan_id
            )
        self.scan_storage[scan_id].append(
            point_id=msg.content.get("point_id"), data=msg.content.get("data")
        )
        logger.debug(msg.content.get("point_id"))
        self.check_storage_status(scan_id=scan_id)

    def update_baseline_reading(self, scan_id: str) -> None:
        """
        Update the baseline reading for the scan.

        Args:
            scan_id (str): Scan ID
        """
        if not self.scan_storage.get(scan_id):
            return
        if self.scan_storage[scan_id].baseline:
            return
        baseline = self.connector.get(MessageEndpoints.public_scan_baseline(scan_id))
        if not baseline:
            return
        self.scan_storage[scan_id].baseline = baseline.content["data"]
        return

    def update_file_references(self, scan_id: str) -> None:
        """
        Update the file references for the scan.
        All external files ought to be announced to the endpoint public_file before the scan finishes. This function
        retrieves the file references and adds them to the scan storage.

        Args:
            scan_id (str): Scan ID
        """
        if not self.scan_storage.get(scan_id):
            return
        msgs = self.connector.keys(MessageEndpoints.public_file(scan_id, "*"))
        if not msgs:
            return

        # extract name from 'public/<scan_id>/file/<name>'
        names = [msg.decode().split("/")[-1] for msg in msgs]
        file_msgs = [self.connector.get(msg.decode()) for msg in msgs]
        if not file_msgs:
            return
        for name, file_msg in zip(names, file_msgs):
            self.scan_storage[scan_id].file_references[name] = file_msg
        return

    def check_storage_status(self, scan_id: str) -> None:
        """
        Check if the scan storage is ready to be written to file and write it if it is.

        Args:
            scan_id (str): Scan ID
        """
        with self._lock:
            if not self.scan_storage.get(scan_id):
                return
            self.update_baseline_reading(scan_id)
            self.update_file_references(scan_id)
            if self.scan_storage[scan_id].ready_to_write():
                self.write_file(scan_id)

    def write_file(self, scan_id: str) -> None:
        """
        Write scan data to file.

        Args:
            scan_id (str): Scan ID
        """
        if not self.scan_storage.get(scan_id):
            return
        storage = self.scan_storage[scan_id]
        if storage.scan_number is None:
            return

        file_path = ""
        file_suffix = "master"

        start_time = time.time()

        try:
            file_path = self.writer_mixin.compile_full_filename(suffix=file_suffix)
            self.connector.set_and_publish(
                MessageEndpoints.public_file(scan_id, "master"),
                messages.FileMessage(file_path=file_path, done=False, successful=False),
            )
            successful = True
            logger.info(f"Writing to file {file_path}.")

            # If we've already written device data, we need to append to the file
            writte_devices = None if not self.async_writer else self.async_writer.written_devices
            write_mode = "w" if not writte_devices else "a"
            file_handle = self.async_writer.file_handle if self.async_writer else None
            self.file_writer.write(
                file_path=file_path, data=storage, mode=write_mode, file_handle=file_handle
            )
        # pylint: disable=broad-except
        # pylint: disable=unused-variable
        except Exception:
            content = traceback.format_exc()
            logger.error(f"Failed to write to file {file_path}. Error: {content}")
            self.connector.raise_alarm(
                severity=Alarms.MINOR,
                alarm_type="FileWriterError",
                source={"file_writer_manager": content},
                msg=f"Failed to write to file {file_path}. Error: {content}",
                metadata=self.scan_storage[scan_id].metadata,
            )
            successful = False
        finally:
            # make sure that the file is closed
            if file_handle:
                file_handle.close()
        logger.info(f"Writing to file {file_path} took {time.time() - start_time:.2f} seconds.")

        self.scan_storage.pop(scan_id)
        self.connector.set_and_publish(
            MessageEndpoints.public_file(scan_id, "master"),
            messages.FileMessage(file_path=file_path, done=True, successful=successful),
        )
        history_msg = messages.ScanHistoryMessage(
            scan_id=scan_id,
            scan_number=storage.scan_number,
            dataset_number=storage.metadata.get("dataset_number"),
            exit_status=storage.metadata.get("exit_status"),
            file_path=file_path,
            start_time=storage.start_time,
            end_time=storage.end_time,
            num_points=storage.num_points,
            scan_name=storage.metadata.get("scan_name"),
        )
        self.connector.xadd(
            topic=MessageEndpoints.scan_history(), msg_dict={"data": history_msg}, max_size=10000
        )
        if successful:
            logger.success(f"Finished writing file {file_path}.")
            return
