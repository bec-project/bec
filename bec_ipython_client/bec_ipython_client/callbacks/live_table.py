from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from bec_ipython_client.prettytable import PrettyTable
from bec_ipython_client.progressbar import ScanProgressBar
from bec_lib.logger import bec_logger

from .utils import LiveUpdatesBase, check_alarms

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.client import BECClient

logger = bec_logger.logger


def sort_devices(devices, scan_devices) -> list:
    """sort the devices to ensure that the table starts with scan motors"""
    for scan_dev in list(scan_devices)[::-1]:
        root_dev = scan_dev.split(".")[0]
        if root_dev in devices:
            devices.remove(root_dev)
        devices.insert(0, scan_dev)
    return devices


class LiveUpdatesTable(LiveUpdatesBase):
    """Live updates for scans using a table and a scan progess bar.

    Args:
        bec (BECClient): client instance
        request (messages.ScanQueueMessage): The scan request that should be monitored

    Raises:
        TimeoutError: Raised if no queue item is added before reaching a predefined timeout.
        RuntimeError: Raised if more points than requested are returned.
        ScanRequestError: Raised if the scan was rejected by the server.
    """

    MAX_DEVICES = 10
    REPORT_TYPE = "scan_progress"

    def __init__(
        self,
        bec: BECClient,
        report_instruction: dict = None,
        request: messages.ScanQueueMessage = None,
        callbacks: list[Callable] = None,
        print_table_data=None,
    ) -> None:
        super().__init__(
            bec, report_instruction=report_instruction, request=request, callbacks=callbacks
        )
        self.scan_queue_request = None
        self.scan_item = None
        self.dev_values = None
        self.point_data = None
        self.point_id = 0
        self.table = None
        self.__print_table_data = None
        self._print_table_data = (
            print_table_data
            if print_table_data is not None
            else self.REPORT_TYPE == "scan_progress"
        )

    def wait_for_scan_to_start(self):
        """wait until the scan starts"""
        while True:
            queue_pos = self.scan_item.queue.queue_position
            self.check_alarms()
            if self.scan_item.status == "closed":
                break
            if queue_pos is None:
                logger.debug(f"Could not find queue entry for scan_id {self.scan_item.scan_id}")
                continue
            if queue_pos == 0:
                break
            print(
                f"Scan is enqueued and is waiting for execution. Current position in queue: {queue_pos + 1}.",
                end="\r",
                flush=True,
            )
            time.sleep(0.1)
        while not self.scan_item.scan_number:
            time.sleep(0.05)

    def wait_for_scan_item_to_finish(self):
        """wait for scan completion"""
        while True:
            if self.scan_item.end_time:
                if self.scan_item.open_queue_group:
                    break
                if self.scan_item.queue.queue_position is None:
                    break
            self.check_alarms()
            time.sleep(0.1)

    def check_alarms(self):
        """check for alarms"""
        check_alarms(self.bec)

    def resume(self, request: messages.ScanQueueMessage, report_instruction: str, callbacks):
        """resume the scan after a pause"""
        super().__init__(
            self.bec, request=request, report_instruction=report_instruction, callbacks=callbacks
        )
        self.process_request()

    @property
    def devices(self):
        """get the devices for the callback"""
        if self.point_data.metadata["scan_type"] == "step":
            return self.get_devices_from_scan_data(self.scan_item.live_data[0])
        if self.point_data.metadata["scan_type"] == "fly":
            devices = list(self.point_data.content["data"].keys())
            if len(devices) > self.MAX_DEVICES:
                return devices[0 : self.MAX_DEVICES]
            return devices
        return None

    def get_devices_from_scan_data(self, data: messages.ScanMessage) -> list:
        """extract interesting devices from a scan request"""
        device_manager = self.bec.device_manager
        scan_devices = data.metadata.get("scan_report_devices")
        monitored_devices = device_manager.devices.monitored_devices(
            [device_manager.devices[dev] for dev in scan_devices]
        )
        devices = [dev.name for dev in monitored_devices]
        devices = sort_devices(devices, scan_devices)
        if len(devices) > self.MAX_DEVICES:
            return devices[0 : self.MAX_DEVICES]
        return devices

    def _prepare_table(self) -> PrettyTable:
        """Prepares the custom table for the live updates."""
        header = self._get_header()
        max_len = max(len(head) for head in header)
        return PrettyTable(header, padding=max_len)

    def _get_header(self) -> list:
        """get the header for the table with up to self.MAX_DEVICES as entries

        Returns:
            list: header for the table
        """
        header = ["seq. num"]
        for dev in self.devices:
            if dev in self.bec.device_manager.devices:
                obj = self.bec.device_manager.devices[dev]
                header.extend(obj._hints)
            else:
                header.append(dev)
        return header

    def update_scan_item(self, timeout: float = 15):
        """get the current scan item"""
        start = time.time()
        while self.scan_queue_request.scan is None:
            self.check_alarms()
            time.sleep(0.1)
            if time.time() - start > timeout:
                raise TimeoutError("Could not find scan item.")
        self.scan_item = self.scan_queue_request.scan

    def core(self):
        """core function to run the live updates for the table"""
        req_ID = self.scan_queue_request.requestID
        while True:
            request_block = [
                req for req in self.scan_item.queue.request_blocks if req["RID"] == req_ID
            ][0]
            if not request_block["is_scan"]:
                break
            if request_block["report_instructions"]:
                break
            self.check_alarms()

        self._run_update(self.report_instruction[self.REPORT_TYPE])

    def _run_update(self, target_num_points: int):
        """run the update loop with the progress bar

        Args:
            target_num_points (int): number of points to be collected
        """
        with ScanProgressBar(
            scan_number=self.scan_item.scan_number, clear_on_exit=self._print_table_data
        ) as progressbar:
            while True:
                self.check_alarms()
                self.point_data = self.scan_item.live_data.get(self.point_id)
                if self.scan_item.num_points:
                    progressbar.max_points = self.scan_item.num_points
                    if target_num_points == 0:
                        target_num_points = self.scan_item.num_points

                progressbar.update(self.point_id)
                if self.point_data:
                    self.point_id += 1
                    self.print_table_data()
                    progressbar.update(self.point_id)

                    # process sync callbacks
                    self.bec.callbacks.poll()
                    self.scan_item.poll_callbacks()
                else:
                    logger.debug("waiting for new data point")
                    time.sleep(0.1)

                if not self.scan_item.num_points:
                    continue

                if self.point_id == target_num_points:
                    break
                if self.point_id > self.scan_item.num_points:
                    raise RuntimeError("Received more points than expected.")

    @property
    def _print_table_data(self) -> bool:
        """Checks if the table should be printed or not.

        Returns:
            bool: True/False
        """
        if not self.__print_table_data:
            return False
        try:
            # pylint: disable=protected-access
            # pylint: disable=undefined-variable
            if get_ipython().__class__.__name__ == "ZMQInteractiveShell":
                self.__print_table_data = False
                return False
        except Exception:
            pass
        return self.__print_table_data

    @_print_table_data.setter
    def _print_table_data(self, value: bool):
        """Set the print_table_data attribute.

        Args:
            value (bool): value to set
        """
        self.__print_table_data = value

    def print_table_data(self):
        """print the table data for the current point_id"""
        if not self._print_table_data:
            return

        if not self.table:
            self.dev_values = (len(self._get_header()) - 1) * [0]
            self.table = self._prepare_table()
            print(self.table.get_header_lines())

        if self.point_id % 100 == 0:
            print(self.table.get_header_lines())
        ind = 0
        for dev in self.devices:
            if dev in self.bec.device_manager.devices:
                obj = self.bec.device_manager.devices[dev]
                for hint in obj._hints:
                    signal = self.point_data.content["data"].get(obj.root.name, {}).get(hint)
                    if signal is None or signal.get("value") is None:
                        print_value = "N/A"
                    else:
                        try:
                            precision = getattr(obj, "precision")
                        except AttributeError:
                            precision = 2
                        value = signal.get("value")
                        if isinstance(value, (int, float)):
                            print_value = f"{value:.{precision}f}"
                        else:
                            print_value = str(value)
                    self.dev_values[ind] = print_value
                    ind += 1
            else:
                signal = self.point_data.content["data"].get(dev, {})
                value = signal.get("value")
                if value is not None:
                    if isinstance(value, (int, float)):
                        print_value = f"{value:.2f}"
                    else:
                        print_value = str(value)
                else:
                    print_value = "N/A"
                self.dev_values[ind] = print_value
                ind += 1
        print(self.table.get_row(str(self.point_id), *self.dev_values))
        # pylint: disable=protected-access
        self._print_client_msgs_asap()

    def close_table(self):
        """close the table and print the footer"""
        if not self.table:
            return
        elapsed_time = self.scan_item.end_time - self.scan_item.start_time
        print(
            self.table.get_footer(
                f"Scan {self.scan_item.scan_number} finished. Scan ID {self.scan_item.scan_id}. Elapsed time: {elapsed_time:.2f} s"
            )
        )
        # TODO #286
        # self._print_client_msgs_all()

    def process_request(self):
        """process the request and start the core loop for live updates"""
        if self.request.content["scan_type"] == "close_scan_def":
            self.wait_for_scan_item_to_finish()
            self.close_table()
            return

        self.wait_for_request_acceptance()
        self.update_scan_item(timeout=15)
        self.wait_for_scan_to_start()

        if self.table:
            self.table = None
        else:
            if self._print_table_data:
                print(f"\nStarting scan {self.scan_item.scan_number}.")

        self.core()

    def run(self):
        """run the live updates"""
        if self.request.content["scan_type"] == "open_scan_def":
            self.wait_for_request_acceptance()
            return
        self.process_request()
        self.wait_for_scan_item_to_finish()
        if self._print_table_data:
            self.close_table()
