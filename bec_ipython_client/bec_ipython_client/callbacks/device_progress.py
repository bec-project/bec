import time

from bec_ipython_client.progressbar import ScanProgressBar
from bec_lib.bec_errors import ScanInterruption, ScanRestart
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .live_table import LiveUpdatesTable

logger = bec_logger.logger


class LiveUpdatesDeviceProgress(LiveUpdatesTable):
    """Live updates for scans using a progress bar based on the progress of one or more devices"""

    REPORT_TYPE = "device_progress"

    def _check_scan_state(self) -> bool:
        """Check whether the scan has reached a terminal or exceptional state.

        Returns:
            bool: True if the scan should stop without error.
        """
        if not self.scan_item:
            return False

        restarted_msg = getattr(self.scan_item, "restarted_msg", None)
        if restarted_msg:
            raise ScanRestart(new_scan_msg=restarted_msg)

        if getattr(self.scan_item, "status", None) == "user_completed":
            print("Scan was set to 'completed' by user.")
            return True

        status_message = getattr(self.scan_item, "status_message", None)
        if status_message and getattr(status_message, "reason", None) == "user":
            raise ScanInterruption(
                f"Scan {getattr(self.scan_item, 'scan_number', None)} was aborted by user."
            )

        return False

    def core(self):
        """core function to run the live updates for the table"""
        self._wait_for_report_instructions()
        self._run_update(self.report_instruction[self.REPORT_TYPE])

    def _run_update(self, device_names: list[str]):
        """Run the update loop for the progress bar.

        Args:
            device_names (list[str]): The name of the device to monitor.
        """
        with ScanProgressBar(
            scan_number=self.scan_item.scan_number, clear_on_exit=False
        ) as progressbar:
            while True:
                if self._update_progressbar(progressbar, device_names):
                    break
                self._print_client_msgs_asap()
        self._print_client_msgs_all()

    def _update_progressbar(self, progressbar: ScanProgressBar, device_names: list[str]) -> bool:
        """Update the progressbar based on the device status message

        Args:
            progressbar (ScanProgressBar): The progressbar to update.
            device_names (str): The name of the device to monitor.
        Returns:
            bool: True if the scan is finished.
        """
        self.check_alarms()
        if self._check_scan_state():
            return True
        status = self.bec.connector.get(MessageEndpoints.device_progress(device_names[0]))
        if not status:
            logger.trace("waiting for new data point")
            time.sleep(0.1)
            return False
        if status.metadata.get("scan_id") != self.scan_item.scan_id:
            logger.trace("waiting for new data point")
            time.sleep(0.1)
            return False

        point_id = status.content.get("value")
        if point_id is None:
            logger.trace("waiting for new data point")
            time.sleep(0.1)
            return False

        max_value = status.content.get("max_value")
        if max_value and max_value != progressbar.max_points:
            progressbar.max_points = max_value

        progressbar.update(point_id)
        # process sync callbacks
        self.bec.callbacks.poll()
        self.scan_item.poll_callbacks()
        if self._check_scan_state():
            return True

        done = status.content.get("done")
        if point_id == max_value or done:
            return True
        return False
