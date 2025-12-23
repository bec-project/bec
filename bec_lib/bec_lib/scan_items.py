"""
This module contains the ScanItem and ScanStorage classes. The ScanItem class is used to store
information about a scan. The ScanStorage class is used to store scan items.
"""

from __future__ import annotations

import builtins
import datetime
import importlib
import threading
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any, Literal

from bec_lib.live_scan_data import LiveScanData
from bec_lib.logger import bec_logger
from bec_lib.scan_data_container import ScanDataContainer
from bec_lib.utils import threadlocked

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages
    from bec_lib.scan_manager import ScanManager

    try:
        import pandas as pd
    except ImportError:
        logger.info("Unable to import `pandas` optional dependency")


class ScanItem:
    """
    Represents a single scan with its associated data, metadata, and status.

    A ScanItem stores comprehensive information about a scan including its status, timing,
    data segments, and callbacks. It provides methods to emit data and status updates,
    convert data to pandas DataFrames, and describe the scan details.

    Attributes:
        scan_manager: The scan manager instance that manages this scan item.
        scan_number: Scan number associated with this scan.
        scan_id: Scan ID associated with this scan.
        status: Current status of the scan ("open", "closed", "aborted", "halted", or "paused").
        status_message: Latest status message for this scan.
        data: Container for the scan's data points.
        live_data: Container for live/streaming scan data.
        open_scan_defs: Set of open scan definition IDs.
        open_queue_group: Queue group this scan belongs to.
        num_points: Total number of data points in the scan.
        start_time: Unix timestamp when the scan started.
        end_time: Unix timestamp when the scan ended.
        scan_report_instructions: Instructions for generating scan reports.
        public_files: Dictionary of public files associated with this scan.

    Example:
        >>> scan_item = ScanItem(
        ...     queue_id="queue_123",
        ...     scan_number=[42],
        ...     scan_id=["scan_abc"],
        ...     status="open",
        ...     scan_manager=manager
        ... )
        >>> print(scan_item.describe())
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        queue_id: str,
        scan_number: int,
        scan_id: str,
        status: Literal["open", "closed", "aborted", "halted", "paused"],
        scan_manager: ScanManager | None = None,
        **_kwargs,
    ) -> None:
        self.scan_manager = scan_manager
        self._queue_id = queue_id
        self.scan_number = scan_number
        self.scan_id = scan_id
        self.status = status
        self.status_message: messages.ScanStatusMessage | None = None
        self.data = ScanDataContainer()
        self.live_data = LiveScanData()
        self.open_scan_defs = set()
        self.open_queue_group = None
        self.num_points: int | None = None
        self.start_time: float | None = None
        self.end_time: float | None = None
        self.scan_report_instructions: list[dict] = []
        self._callbacks = []
        self._bec = builtins.__dict__.get("bec")
        self.public_files = {}

    @property
    def queue(self):
        """Get the queue item associated with this scan.

        Returns:
            The queue item that contains this scan, or None if not found.
        """
        if self.scan_manager is None:
            return None
        return self.scan_manager.queue_storage.find_queue_item_by_ID(self._queue_id)

    def emit_data(self, scan_msg: messages.ScanMessage) -> None:
        """Emit scan data to registered callbacks.

        Broadcasts scan segment data to both global BEC callbacks and request-specific callbacks.

        Args:
            scan_msg: The scan message containing data to emit.
        """
        if self._bec is None:
            return
        self._bec.callbacks.run("scan_segment", scan_msg.content, scan_msg.metadata)
        self._run_request_callbacks("scan_segment", scan_msg.content, scan_msg.metadata)

    def emit_status(self, scan_status: messages.ScanStatusMessage) -> None:
        """Emit scan status updates to registered callbacks.

        Broadcasts scan status changes to both global BEC callbacks and request-specific callbacks.

        Args:
            scan_status: The scan status message containing the status update.
        """
        if self._bec is None:
            return
        self._bec.callbacks.run("scan_status", scan_status.content, scan_status.metadata)
        self._run_request_callbacks("scan_status", scan_status.content, scan_status.metadata)

    def _run_request_callbacks(self, event_type: str, data: dict, metadata: dict):
        queue = self.queue
        if queue is None or self.scan_manager is None:
            return
        for rid in queue.requestIDs:
            req = self.scan_manager.request_storage.find_request_by_ID(rid)
            if req is None:
                continue
            req.callbacks.run(event_type, data, metadata)

    def poll_callbacks(self):
        """Poll all request-specific callbacks for this scan.

        Iterates through all requests associated with this scan's queue and polls
        their callbacks to process any pending events.
        """
        if self.queue is None or self.scan_manager is None:
            return
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
        """Convert scan data to a pandas DataFrame.

        Extracts all live scan data and organizes it into a pandas DataFrame with
        multi-level columns: (device_name, signal_name, data_key).

        Returns:
            A pandas DataFrame containing the scan data.

        Raises:
            ImportError: If pandas is not installed.

        Example:
            >>> df = scan_item.to_pandas()
            >>> print(df.columns)  # MultiIndex with (device, signal, key)
        """
        pd = self._get_pandas()
        tmp = defaultdict(list)
        for scan_msg in self.live_data.messages.values():
            for dev, dev_data in scan_msg.data.items():
                for signal, signal_data in dev_data.items():
                    for key, value in signal_data.items():
                        tmp[(dev, signal, key)].append(value)
        return pd.DataFrame(tmp)

    def __eq__(self, other):
        return self.scan_id == other.scan_id

    def describe(self) -> str:
        """Generate a human-readable description of the scan.

        Creates a formatted string containing key scan information including start/end times,
        elapsed time, scan ID, scan number, number of points, and associated files.

        Returns:
            A formatted string describing the scan's key attributes.
        """
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
    """Thread-safe storage for managing scan items with automatic lifecycle handling.

    ScanStorage maintains a collection of scan items with thread-safe operations,
    automatic pending insert handling, and integration with the scan queue system.
    It provides properties to access current scan information and methods to update
    scans with status messages, data segments, and file associations.

    Attributes:
        scan_manager: The scan manager instance that owns this storage.
        storage: Double-ended queue (deque) storing scan items with maximum length.
        last_scan_number: The most recently assigned scan number.

    Args:
        scan_manager: The scan manager that will use this storage.
        maxlen: Maximum number of scan items to store (default: 100).
        init_scan_number: Initial scan number to start from (default: 0).
    """

    def __init__(self, scan_manager: ScanManager, maxlen=100, init_scan_number=0) -> None:
        self.scan_manager = scan_manager
        self.storage = deque(maxlen=maxlen)
        self.last_scan_number = init_scan_number
        self._lock = threading.RLock()
        self._pending_inserts = defaultdict(list[dict[Literal["func", "func_args"], Any]])

    @property
    def current_scan_info(self) -> messages.QueueInfoEntry | None:
        """Get the current scan information from the scan queue.

        Returns:
            The current QueueInfoEntry for the active scan, or None if no scan is active.
        """
        scan_queue = self.scan_manager.queue_storage.current_scan_queue

        target_queue = self.scan_manager.get_default_scan_queue()
        if not scan_queue or target_queue not in scan_queue:
            return None
        scan_queue = scan_queue[target_queue]

        if not scan_queue.info:
            return None

        return scan_queue.info[0]

    @property
    def current_scan(self) -> ScanItem | None:
        """Get the currently active scan item.

        Returns:
            The active ScanItem instance, or None if no scan is currently active.
        """
        if not self.current_scan_id:
            return None
        return self.find_scan_by_ID(scan_id=self.current_scan_id[0])

    @property
    def current_scan_id(self) -> list[str | None]:
        """Get the scan ID of the currently active scan.

        Returns:
            A list of scan IDs for the active scan, or an empty list if no scan is active.
        """
        if self.current_scan_info is None:
            return []

        return self.current_scan_info.scan_id

    @threadlocked
    def find_scan_by_ID(self, scan_id: str | None) -> ScanItem | None:
        """Find a scan item by its scan ID.

        This method is thread-safe and searches through the storage for a matching scan.

        Args:
            scan_id: The unique identifier of the scan to find.

        Returns:
            The matching ScanItem if found, otherwise None.
        """
        if scan_id is None:
            return None
        for scan in self.storage:
            if scan_id == scan.scan_id:
                return scan
        return None

    @threadlocked
    def update_with_scan_status(self, scan_status: messages.ScanStatusMessage) -> None:
        """Update a scan item with new status information.

        This method is thread-safe and updates various scan attributes based on the
        status message, including timestamps, scan number, number of points, and
        scan definition IDs. If the scan item doesn't exist yet, the update is queued
        for later processing.

        Args:
            scan_status: The status message containing updated scan information.
        """

        scan_id = scan_status.scan_id
        if not scan_id:
            return

        scan_number = scan_status.scan_number
        if scan_number:
            self.last_scan_number = scan_number

        scan_item = self.find_scan_by_ID(scan_id=scan_id)
        if not scan_item:
            self._pending_inserts[scan_id].append(
                {"func": "update_with_scan_status", "func_args": (scan_status,)}
            )
            return

        # update timestamps
        if scan_status.status == "open":
            scan_item.start_time = scan_status.timestamp
        elif scan_status.status == "closed":
            scan_item.end_time = scan_status.timestamp

        # update status message
        scan_item.status = scan_status.status
        scan_item.status_message = scan_status

        # update total number of points
        if scan_status.num_points:
            scan_item.num_points = scan_status.num_points

        # update scan number
        if scan_number is not None:
            scan_item.scan_number = scan_number

        # add scan report info
        scan_item.scan_report_instructions = scan_status.info.get("scan_report_instructions", [])

        # add scan def id
        scan_def_id = scan_status.info.get("scan_def_id")
        if scan_def_id:
            if scan_status.status != "open":
                scan_item.open_scan_defs.remove(scan_def_id)
            else:
                scan_item.open_scan_defs.add(scan_def_id)

        # add queue group
        scan_item.open_queue_group = scan_status.info.get("queue_group")

        # run status callbacks
        scan_item.emit_status(scan_status)

    @threadlocked
    def add_scan_segment(self, scan_msg: messages.ScanMessage) -> None:
        """Add a new data segment to a scan item.

        This method is thread-safe and adds scan data points to the appropriate scan item.
        If the scan item doesn't exist yet, the segment is queued for later processing.
        After adding the segment, data callbacks are triggered.

        Args:
            scan_msg: The scan message containing the data segment to add.
        """
        logger.info(f"Received scan segment {scan_msg.point_id} for scan {scan_msg.scan_id}: ")
        scan_id = scan_msg.scan_id
        scan_item = self.find_scan_by_ID(scan_id)
        if scan_item is None:
            self._pending_inserts[scan_id].append(
                {"func": "add_scan_segment", "func_args": (scan_msg,)}
            )
            return

        scan_item.live_data.set(scan_msg.point_id, scan_msg)
        scan_item.emit_data(scan_msg)

    @threadlocked
    def add_public_file(self, scan_id: str, msg: messages.FileMessage) -> None:
        """Associate a public file with a scan item.

        This method is thread-safe and adds file information to the scan item. If the file
        is a master file and marked as done, it initializes the scan's data container. If
        the scan item doesn't exist yet, the file association is queued for later processing.

        Args:
            scan_id: The unique identifier of the scan.
            msg: The file message containing file path and status information.
        """
        scan_item = self.find_scan_by_ID(scan_id)
        if scan_item is None:
            self._pending_inserts[scan_id].append(
                {"func": "add_public_file", "func_args": (scan_id, msg)}
            )
            return

        # if we receive the master file, we can create the data container
        if "_master" in msg.file_path and msg.done:
            scan_item.data.set_file(file_path=msg.file_path)
        file_path = msg.file_path
        done_state = msg.done
        successful = msg.successful
        scan_item.public_files[file_path] = {"done_state": done_state, "successful": successful}

    @threadlocked
    def add_scan_item(
        self,
        queue_id: str,
        scan_number: int,
        scan_id: str,
        status: Literal["open", "closed", "aborted", "halted", "paused"],
    ) -> None:
        """Create and add a new scan item to the storage.

        This method is thread-safe and creates a new ScanItem with the specified parameters,
        adds it to storage, and processes any pending operations that were queued for this scan.

        Args:
            queue_id: The unique identifier of the queue containing this scan.
            scan_number: List of scan numbers for this scan.
            scan_id: List of scan IDs for this scan.
            status: Current status of the scan.
        """
        self.storage.append(
            ScanItem(
                scan_manager=self.scan_manager,
                queue_id=queue_id,
                scan_number=scan_number,
                scan_id=scan_id,
                status=status,
            )
        )
        pending_inserts = self._pending_inserts.pop(scan_id, None)
        if not pending_inserts:
            return
        for insert in pending_inserts:
            getattr(self, insert["func"])(*insert["func_args"])

    @threadlocked
    def update_with_queue_status(self, queue_msg: messages.ScanQueueStatusMessage):
        """Create new scan items based on queue status information.

        This method is thread-safe and processes queue status messages to create ScanItem
        instances for any scans that don't already exist in storage. It limits processing
        to the most recent 20 queue items to avoid excessive memory usage.

        Args:
            queue_msg(messages.ScanQueueStatusMessage): The queue status message containing scan info.
        """
        for queue in queue_msg.queue.values():
            for ii, queue_item in enumerate(queue.info):

                if not any(queue_item.is_scan):
                    continue

                for scan_idx, scan in enumerate(queue_item.scan_id):
                    if self.find_scan_by_ID(scan):
                        continue

                    logger.debug(f"Appending new scan: {queue_item}")
                    self.add_scan_item(
                        queue_id=queue_item.queue_id,
                        scan_number=queue_item.scan_number[scan_idx],  # type: ignore[index]
                        scan_id=queue_item.scan_id[scan_idx],  # type: ignore[index]
                        status=queue_item.status,  # type: ignore[attr-defined]
                    )
                if ii > 20:
                    # only keep the last 20 queue items in storage to avoid
                    # evicting too many items just because of a large queue
                    break
