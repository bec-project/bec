from __future__ import annotations

import logging
import os
import threading
import traceback

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.async_data import AsyncDataHandler
from bec_lib.bec_service import BECService
from bec_lib.devicemanager import DeviceManagerBase
from bec_lib.endpoints import MessageEndpoints
from bec_lib.file_utils import FileWriter
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import MessageObject, RedisConnector
from bec_lib.service_config import ServiceConfig
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

        self.blissdata_subscriber = None
        self.file_writer_config = self._service_config.service_config.get("file_writer")

        if self.file_writer_config.get("enable_bliss_writer", "false") in (
            "False",
            "false",
            "no",
            "0",
        ):
            self._lock = threading.RLock()
            self._start_device_manager()
            self.connector.register(
                MessageEndpoints.scan_segment(), cb=self._scan_segment_callback, parent=self
            )
            self.connector.register(
                MessageEndpoints.scan_status(), cb=self._scan_status_callback, parent=self
            )
            self.scan_storage = {}
            self.writer_mixin = FileWriter(
                service_config=self.file_writer_config, connector=self.connector
            )
            self.file_writer = HDF5FileWriter(self)
        else:
            from blisswriter.subscribers.session_subscriber import NexusSessionSubscriber
            from blisswriter.utils.logging_utils import config as configure_logger

            configure_logger(logging.getLogger("blisswriter"), bec_logger.level)
            redis_data_url = f"redis://{self._service_config.redis_data}"
            os.environ["REDIS_DATA_URL"] = redis_data_url
            self.blissdata_subscriber = NexusSessionSubscriber("bec", writer_name="external")
            logger.info("Starting blissdata subscriber (listening to session 'bec')")
            self.blissdata_subscriber.start()
            # self.blissdata_subscriber.join() # wait is done in 'launch' script with a lock
        self.status = messages.BECStatus.RUNNING

    def shutdown(self):
        super().shutdown()
        if self.blissdata_subscriber is not None:
            logger.info("Stopping blissdata subscriber...")
            self.blissdata_subscriber.stop()
            logger.info("Goodbye")

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
            self.scan_storage[scan_id].file_references[name] = {
                "path": file_msg.content["file_path"],
                "done": file_msg.content["done"],
                "successful": file_msg.content["successful"],
                "metadata": file_msg.metadata,
            }
        return

    def update_async_data(self, scan_id: str) -> None:
        """
        Update the async data for the scan.
        All async data is sent to the endpoint MessageEndpoints.device_async_readback(scan_id, device_name)
        before the scan finishes. This function retrieves the async data and adds them to the scan storage.

        Args:
            scan_id (str): Scan ID
        """

        if not self.scan_storage.get(scan_id):
            return
        # get all async devices
        async_device_keys = self.connector.keys(
            MessageEndpoints.device_async_readback(scan_id, "*")
        )
        if not async_device_keys:
            return
        for device_key in async_device_keys:
            key = device_key.decode()
            device_name = key.split(MessageEndpoints.device_async_readback(scan_id, "").endpoint)[
                -1
            ].split(":")[0]
            msgs = self.connector.xrange(key, min="-", max="+")
            if not msgs:
                continue
            self._process_async_data(msgs, scan_id, device_name)

    def _process_async_data(self, msgs: list, scan_id: str, device_name: str):
        """
        Process the async data for the scan and add it to the scan storage. If needed, concatenate the data.

        Args:
            msgs (list): List of async data messages
            scan_id (str): Scan ID
            device_name (str): Device name
        """
        self.scan_storage[scan_id].async_data[device_name] = AsyncDataHandler.process_async_data(
            msgs
        )

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
                self.update_async_data(scan_id)
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

        try:
            file_path = self.writer_mixin.compile_full_filename()
            self.connector.set_and_publish(
                MessageEndpoints.public_file(scan_id, "master"),
                messages.FileMessage(file_path=file_path, done=False, successful=False),
            )
            successful = True
            logger.info(f"Starting writing to file {file_path}.")
            self.file_writer.write(file_path=file_path, data=storage)
        # pylint: disable=broad-except
        # pylint: disable=unused-variable
        except Exception:
            content = traceback.format_exc()
            logger.error(f"Failed to write to file {file_path}. Error: {content}")
            self.connector.raise_alarm(
                severity=Alarms.MINOR,
                alarm_type="FileWriterError",
                source="file_writer_manager",
                msg=f"Failed to write to file {file_path}. Error: {content}",
                metadata=self.scan_storage[scan_id].metadata,
            )
            successful = False
        self.scan_storage.pop(scan_id)
        self.connector.set_and_publish(
            MessageEndpoints.public_file(scan_id, "master"),
            messages.FileMessage(file_path=file_path, done=True, successful=successful),
        )
        if successful:
            logger.success(f"Finished writing file {file_path}.")
            return
