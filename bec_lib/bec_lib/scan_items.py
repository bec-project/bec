"""
This module contains the ScanItem and ScanStorage classes. The ScanItem class is used to store
information about a scan. The ScanStorage class is used to store scan items.
"""

from __future__ import annotations

import builtins
import datetime
import importlib
import threading
import time
from collections import defaultdict, deque
from typing import TYPE_CHECKING

from bec_lib.live_scan_data import LiveScanData
from bec_lib.logger import bec_logger
from bec_lib.scan_data_container import ScanDataContainer
from bec_lib.utils import threadlocked

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.scan_manager import ScanManager

    try:
        import pandas as pd
    except ImportError:
        logger.info("Unable to import `pandas` optional dependency")


class ScanItem:
    status: dict

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        queue_id: str,
        scan_number: list,
        scan_id: list,
        status: str,
        scan_manager: ScanManager | None = None,
        **_kwargs,
    ) -> None:
        self.scan_manager = scan_manager
        self._queue_id = queue_id
        self.scan_number = scan_number
        self.scan_id = scan_id
        self.status = status
        self.status_message = None
        self.data = ScanDataContainer()
        self.live_data = LiveScanData()
        self.open_scan_defs = set()
        self.open_queue_group = None
        self.num_points = None
        self.start_time = None
        self.end_time = None
        self.scan_report_instructions = []
        self._callbacks = []
        self._bec = builtins.__dict__.get("bec")
        self.public_files = {}

    @property
    def queue(self):
        """get the queue item for the current scan item"""
        return self.scan_manager.queue_storage.find_queue_item_by_ID(self._queue_id)

    def emit_data(self, scan_msg: messages.ScanMessage) -> None:
        self._bec.callbacks.run("scan_segment", scan_msg.content, scan_msg.metadata)
        self._run_request_callbacks("scan_segment", scan_msg.content, scan_msg.metadata)

    def emit_status(self, scan_status: messages.ScanStatusMessage) -> None:
        self._bec.callbacks.run("scan_status", scan_status.content, scan_status.metadata)
        self._run_request_callbacks("scan_status", scan_status.content, scan_status.metadata)

    def _run_request_callbacks(self, event_type: str, data: dict, metadata: dict):
        for rid in self.queue.requestIDs:
            req = self.scan_manager.request_storage.find_request_by_ID(rid)
            if req is None:
                continue
            req.callbacks.run(event_type, data, metadata)

    def poll_callbacks(self):
        for rid in self.queue.requestIDs:
            req = self.scan_manager.request_storage.find_request_by_ID(rid)
            if req is None:
                continue
            req.callbacks.poll()

    def _get_pandas(self):
        try:
            return importlib.import_module("pandas")
        except ImportError as exc:
            raise ImportError("Install `pandas` to use to_pandas() method") from exc

    def to_pandas(self) -> pd.DataFrame:
        """convert to pandas dataframe"""
        pd = self._get_pandas()
        tmp = defaultdict(list)
        for scan_msg in self.live_data.messages.values():
            scan_msg_data = scan_msg.content["data"]
            for dev, dev_data in scan_msg_data.items():
                for signal, signal_data in dev_data.items():
                    for key, value in signal_data.items():
                        tmp[(dev, signal, key)].append(value)
        return pd.DataFrame(tmp)

    def __eq__(self, other):
        return self.scan_id == other.scan_id

    def describe(self) -> str:
        """describe the scan item"""
        start_time = (
            f"\tStart time: {datetime.datetime.fromtimestamp(self.start_time).strftime('%c')}\n"
            if self.start_time
            else ""
        )
        end_time = (
            f"\tEnd time: {datetime.datetime.fromtimestamp(self.end_time).strftime('%c')}\n"
            if self.end_time
            else ""
        )
        elapsed_time = (
            f"\tElapsed time: {(self.end_time-self.start_time):.1f} s\n"
            if self.end_time and self.start_time
            else ""
        )
        scan_id = f"\tScan ID: {self.scan_id}\n" if self.scan_id else ""
        scan_number = f"\tScan number: {self.scan_number}\n" if self.scan_number else ""
        num_points = f"\tNumber of points: {self.num_points}\n" if self.num_points else ""
        public_file = ""
        for file_path in self.public_files:
            file_name = file_path.split("/")[-1]
            if "_master" in file_name:
                public_file = "\tFile: " + file_path + "\n"
        details = (
            start_time + end_time + elapsed_time + scan_id + scan_number + num_points + public_file
        )
        return details

    def __str__(self) -> str:
        return f"ScanItem:\n {self.describe()}"


class ScanStorage:
    """stores scan items"""

    def __init__(self, scan_manager: ScanManager, maxlen=5, init_scan_number=0) -> None:
        self.scan_manager = scan_manager
        self.storage = deque(maxlen=maxlen)
        self.last_scan_number = init_scan_number
        self._lock = threading.RLock()

    @property
    def current_scan_info(self) -> dict:
        """get the current scan info from the scan queue"""
        scan_queue = self.scan_manager.queue_storage.current_scan_queue
        if not scan_queue or not scan_queue["primary"].get("info"):
            return None
        return scan_queue["primary"].get("info")[0]

    @property
    def current_scan(self) -> ScanItem | None:
        """get the current scan item"""
        if not self.current_scan_id:
            return None
        return self.find_scan_by_ID(scan_id=self.current_scan_id[0])

    @property
    def current_scan_id(self) -> str | None:
        """get the current scan_id"""
        if self.current_scan_info is None:
            return None
        return self.current_scan_info.get("scan_id")

    @threadlocked
    def find_scan_by_ID(self, scan_id: str) -> ScanItem | None:
        """find a scan item based on its scan_id"""
        for scan in self.storage:
            if scan_id == scan.scan_id:
                return scan
        return None

    def update_with_scan_status(self, scan_status: messages.ScanStatusMessage) -> None:
        """update scan item in storage with a new ScanStatusMessage"""

        scan_id = scan_status.content["scan_id"]
        if not scan_id:
            return

        scan_number = scan_status.content["info"].get("scan_number")
        if scan_number:
            self.last_scan_number = scan_number

        while True:
            scan_item = self.find_scan_by_ID(scan_id=scan_status.content["scan_id"])
            if scan_item:
                break
            time.sleep(0.1)

        # update timestamps
        if scan_status.content.get("status") == "open":
            scan_item.start_time = scan_status.content.get("timestamp")
        elif scan_status.content.get("status") == "closed":
            scan_item.end_time = scan_status.content.get("timestamp")

        # update status message
        scan_item.status = scan_status.content.get("status")
        scan_item.status_message = scan_status

        # update total number of points
        if scan_status.content["info"].get("num_points"):
            scan_item.num_points = scan_status.content["info"].get("num_points")

        # update scan number
        if scan_item.scan_number is None:
            scan_item.scan_number = scan_number

        # add scan report info
        scan_item.scan_report_instructions = scan_status.content["info"].get(
            "scan_report_instructions"
        )

        # add scan def id
        scan_def_id = scan_status.content["info"].get("scan_def_id")
        if scan_def_id:
            if scan_status.content.get("status") != "open":
                scan_item.open_scan_defs.remove(scan_def_id)
            else:
                scan_item.open_scan_defs.add(scan_def_id)

        # add queue group
        scan_item.open_queue_group = scan_status.content["info"].get("queue_group")

        # run status callbacks
        scan_item.emit_status(scan_status)

    def add_scan_segment(self, scan_msg: messages.ScanMessage) -> None:
        """update a scan item with a new scan segment"""
        logger.info(
            f"Received scan segment {scan_msg.content['point_id']} for scan"
            f" {scan_msg.metadata['scan_id']}: "
        )
        while True:
            with self._lock:
                for scan_item in self.storage:
                    if scan_item.scan_id == scan_msg.metadata["scan_id"]:
                        scan_item.live_data.set(scan_msg.content["point_id"], scan_msg)
                        scan_item.emit_data(scan_msg)
                        return
            time.sleep(0.01)

    def add_public_file(self, scan_id: str, msg: messages.FileMessage) -> None:
        """add a public file to the scan item"""
        while True:
            with self._lock:
                for scan_item in self.storage:
                    if scan_item.scan_id == scan_id:
                        # if we receive the master file, we can create the data container
                        if "_master" in msg.content["file_path"] and msg.done:
                            scan_item.data.set_file(file_path=msg.content["file_path"])
                        file_path = msg.content["file_path"]
                        done_state = msg.content["done"]
                        successful = msg.content["successful"]
                        scan_item.public_files[file_path] = {
                            "done_state": done_state,
                            "successful": successful,
                        }
                        return
            time.sleep(0.01)

    @threadlocked
    def add_scan_item(self, queue_id: str, scan_number: list, scan_id: list, status: str):
        """append new scan item to scan storage"""
        self.storage.append(
            ScanItem(
                scan_manager=self.scan_manager,
                queue_id=queue_id,
                scan_number=scan_number,
                scan_id=scan_id,
                status=status,
            )
        )

    @threadlocked
    def update_with_queue_status(self, queue_msg: messages.ScanQueueStatusMessage):
        """create new scan items based on their existence in the queue info"""
        queue_info = queue_msg.content["queue"]["primary"].get("info")
        for queue_item in queue_info:
            # append = True
            # for scan_obj in self.storage:
            #     if len(set(scan_obj.scan_id) & set(queue_item["scan_id"])) > 0:
            #         append = False
            if not any(queue_item["is_scan"]):
                continue

            for ii, scan in enumerate(queue_item["scan_id"]):
                if self.find_scan_by_ID(scan):
                    continue

                logger.debug(f"Appending new scan: {queue_item}")
                self.add_scan_item(
                    queue_id=queue_item["queue_id"],
                    scan_number=queue_item["scan_number"][ii],
                    scan_id=queue_item["scan_id"][ii],
                    status=queue_item["status"],
                )
