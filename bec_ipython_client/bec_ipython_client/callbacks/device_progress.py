import time

from bec_ipython_client.progressbar import ScanProgressBar
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .live_table import LiveUpdatesTable

logger = bec_logger.logger


class LiveUpdatesDeviceProgress(LiveUpdatesTable):
    """Live updates for scans using a progress bar based on the progress of one or more devices"""

    REPORT_TYPE = "device_progress"

    def _run_update(self, device_names: str):
        """Run the update loop for the progress bar.

        Args:
            device_names (str): The name of the device to monitor.
        """
        with ScanProgressBar(
            scan_number=self.scan_item.scan_number, clear_on_exit=False
        ) as progressbar:
            while True:
                if self._update_progressbar(progressbar, device_names):
                    break
                self._print_client_msgs_asap()
        self._print_client_msgs_all()

    def _update_progressbar(self, progressbar: ScanProgressBar, device_names: str) -> bool:
        """Update the progressbar based on the device status message

        Args:
            progressbar (ScanProgressBar): The progressbar to update.
            device_names (str): The name of the device to monitor.
        Returns:
            bool: True if the scan is finished.
        """
        self.check_alarms()
        status = self.bec.connector.get(MessageEndpoints.device_progress(device_names[0]))
        if not status:
            logger.debug("waiting for new data point")
            time.sleep(0.1)
            return False
        if status.metadata.get("scan_id") != self.scan_item.scan_id:
            logger.debug("waiting for new data point")
            time.sleep(0.1)
            return False

        point_id = status.content.get("value")
        if point_id is None:
            logger.debug("waiting for new data point")
            time.sleep(0.1)
            return False

        max_value = status.content.get("max_value")
        if max_value and max_value != progressbar.max_points:
            progressbar.max_points = max_value

        progressbar.update(point_id)
        # process sync callbacks
        self.bec.callbacks.poll()
        self.scan_item.poll_callbacks()

        done = status.content.get("done")
        if point_id == max_value or done:
            return True
        return False
