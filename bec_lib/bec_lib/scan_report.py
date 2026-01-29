"""
Scan Report class that provides a convenient way to access the status of a scan request. It is typically
the return value of a scan request.
"""

from __future__ import annotations

import time
from math import inf
from typing import TYPE_CHECKING

from bec_lib.bec_errors import ScanAbortion
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.scan_items import ScanItem

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages
    from bec_lib.client import BECClient
    from bec_lib.queue_items import QueueItem
    from bec_lib.request_items import RequestItem

logger = bec_logger.logger


class ScanReport:
    """Scan Report class that provides a convenient way to access the status of a scan request."""

    def __init__(self) -> None:
        self._client: BECClient | None = None
        self.request: RequestItem | None = None
        self._queue_item: QueueItem | None = None

    @classmethod
    def from_request(
        cls, request: messages.ScanQueueMessage, client: BECClient = None
    ) -> ScanReport:
        """
        Create a ScanReport from a request

        Args:
            request (messages.ScanQueueMessage): request to create the report from
            client (BECClient, optional): BECClient instance. Defaults to None.

        Returns:
            ScanReport: ScanReport instance
        """
        scan_report = cls()
        scan_report._client = client

        client.queue.request_storage.update_with_request(request)
        scan_report.request = client.queue.request_storage.find_request_by_ID(
            request.metadata["RID"]
        )
        return scan_report

    @property
    def scan(self) -> ScanItem | None:
        """get the scan item"""
        if not self.request:
            raise ValueError("Request is not set. Cannot get scan item.")
        return self.request.scan

    @property
    def status(self):
        """returns the current status of the request"""
        scan_type = self.request.request.content["scan_type"]
        status = self.queue_item.status
        if scan_type == "mv" and status == "COMPLETED":
            return "COMPLETED" if self._get_mv_status() else "RUNNING"
        return self.queue_item.status

    @property
    def queue_item(self):
        """get the queue item"""
        if not self._queue_item:
            self._queue_item = self._get_queue_item(timeout=10)
        return self._queue_item

    def _get_queue_item(self, timeout=None) -> QueueItem:
        """
        get the queue item from the queue storage

        Args:
            timeout (float, optional): timeout in seconds. Defaults to None.
        """
        timeout = timeout if timeout is not None else inf
        queue_item = None
        elapsed_time = 0
        sleep_time = 0.1
        while not queue_item:
            queue_item = self._client.queue.queue_storage.find_queue_item_by_requestID(
                self.request.requestID
            )
            elapsed_time += sleep_time
            time.sleep(sleep_time)
            if elapsed_time > timeout:
                raise TimeoutError
        return queue_item

    def _get_mv_status(self) -> bool:
        """get the status of a move request"""
        if not self.request:
            return False
        motors = list(self.request.request.content["parameter"]["args"].keys())
        request_status: list[dict[str, messages.DeviceReqStatusMessage]] | None = (
            self._client.device_manager.connector.xread(
                MessageEndpoints.device_req_status(self.request.requestID), from_start=True
            )
        )
        if request_status is None:
            return False
        if len(request_status) == len(motors):
            return True
        return False

    def wait(
        self, timeout: float | None = None, num_points: bool = False, file_written: bool = False
    ) -> ScanReport:
        """
        Wait for the request to complete

        Args:
            num_points (bool, optional): if True, wait for the number of points to be reached. Defaults to False.
            file_written (bool, optional): if True, wait for the master file to be written. Defaults to False.
            timeout (float, optional): timeout in seconds. Defaults to None.

        Raises:
            TimeoutError: if the timeout is reached

        Returns:
            ScanReport: ScanReport instance
        """
        sleep_time = 0.1
        if self.request is None:
            return self
        scan_type = self.request.request.content["scan_type"]
        try:
            if scan_type == "mv":
                self._wait_move(timeout, sleep_time)
            else:
                self._wait_scan(
                    timeout, sleep_time, num_points=num_points, file_written=file_written
                )
        except KeyboardInterrupt as exc:
            self.cancel()
            raise ScanAbortion("Aborted by user.") from exc

        return self

    def cancel(self) -> ScanReport:
        """
        Cancel the scan request
        """
        if self.request is None:
            raise ValueError("Request is not set. Cannot cancel the scan.")
        scan_type = self.request.request.content["scan_type"]
        if scan_type == "mv":
            motors = list(self.request.request.content["parameter"]["args"].keys())
            for motor in motors:
                try:
                    self._client.device_manager.devices.get(motor).stop()
                except Exception:  # pylint: disable=broad-except
                    logger.warning(f"Failed to stop motor {motor}.")
        else:
            self._client.queue.request_scan_abortion()
        return self

    def _check_timeout(self, timeout: float | None = None, elapsed_time: float = 0) -> None:
        """
        check if the timeout is reached

        Args:
            timeout (float, optional): timeout in seconds. Defaults to None.
            elapsed_time (float, optional): elapsed time in seconds. Defaults to 0.

        """
        if timeout is None:
            return
        if elapsed_time > timeout:
            raise TimeoutError(
                f"Timeout reached while waiting for request to complete. Timeout: {timeout} s."
            )

    def _wait_move(self, timeout: float | None = None, sleep_time: float = 0.1) -> None:
        """
        wait for a move request to complete

        Args:
            timeout (float, optional): timeout in seconds. Defaults to None.
            sleep_time (float, optional): sleep time in seconds. Defaults to 0.1.

        """
        elapsed_time = 0
        while True:
            if self._get_mv_status():
                break
            self._client.alarm_handler.raise_alarms()
            time.sleep(sleep_time)
            elapsed_time += sleep_time
            self._check_timeout(timeout, elapsed_time)

    def _wait_scan(
        self,
        timeout: float | None = None,
        sleep_time: float = 0.1,
        num_points: bool = False,
        file_written: bool = False,
    ) -> None:
        """
        wait for a scan request to complete

        Args:
            timeout (float, optional): timeout in seconds. Defaults to None.
            sleep_time (float, optional): sleep time in seconds. Defaults to 0.1.
        """
        elapsed_time = 0

        def conditions_are_met() -> bool:
            """Check if the conditions are met"""
            if num_points and not self._num_points_reached():
                return False
            if file_written and not self._file_written():
                return False
            return self.status == "COMPLETED"

        while True:
            if conditions_are_met():
                break
            if self.status == "STOPPED":
                raise ScanAbortion
            self._client.callbacks.poll()
            time.sleep(sleep_time)
            elapsed_time += sleep_time
            self._check_timeout(timeout, elapsed_time)

        # final poll to ensure all callbacks are processed
        self._client.callbacks.poll()

    def _file_written(self) -> bool:
        """
        Check if the master file was written
        Returns:
            bool: True if the file was written, False otherwise
        """
        if not self.scan:
            return False

        files_written = self.scan.public_files
        for file_path, state in files_written.items():
            file_name = file_path.split("/")[-1]
            if "_master" in file_name and state.get("done_state"):
                return True
        return False

    def _num_points_reached(self) -> bool:
        """
        Check if the number of points was reached
        Returns:
            bool: True if the number of points was reached, False otherwise
        """
        if not self.scan:
            return False
        return self.scan.num_points == len(self.scan.live_data)

    def __str__(self) -> str:
        separator = "--" * 10
        details = f"\tStatus: {self.status}\n"
        if self.scan:
            details += self.scan.describe()
        return f"ScanReport:\n{separator}\n{details}"
