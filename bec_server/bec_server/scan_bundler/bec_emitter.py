from __future__ import annotations

import threading
import time
from queue import Queue
from typing import TYPE_CHECKING, cast

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .emitter import EmitterBase

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_lib.redis_connector import MessageObject

    from .scan_bundler import ScanBundler


class BECEmitter(EmitterBase):
    def __init__(self, scan_bundler: ScanBundler) -> None:
        super().__init__(scan_bundler.connector)
        self._send_buffer = Queue()
        self.scan_bundler = scan_bundler
        self._buffered_connector_thread = None
        self._buffered_publisher_stop_event = threading.Event()
        self._start_buffered_connector()

    def _start_buffered_connector(self):
        self._buffered_connector_thread = threading.Thread(
            target=self._buffered_publish, daemon=True, name="buffered_publisher"
        )
        self._buffered_connector_thread.start()

    def add_message(self, msg: messages.BECMessage, endpoint: str, public: str = None):
        self._send_buffer.put((msg, endpoint, public))

    def _buffered_publish(self):
        try:
            while not self._buffered_publisher_stop_event.is_set():
                self._publish_data()
        except Exception as e:
            logger.error(f"Error in buffered publisher: {e}")
            raise

    def _get_messages_from_buffer(self) -> list:
        msgs_to_send = []
        while not self._send_buffer.empty():
            msgs_to_send.append(self._send_buffer.get())
        return msgs_to_send

    def _publish_data(self) -> None:
        msgs_to_send = self._get_messages_from_buffer()

        if not msgs_to_send:
            time.sleep(0.1)
            return

        pipe = self.connector.pipeline()
        msgs = messages.BundleMessage()
        _, endpoint, _ = msgs_to_send[0]
        for msg, endpoint, public in msgs_to_send:
            msg_dump = msg
            msgs.append(msg_dump)
            if public:
                self.connector.set(public, msg_dump, pipe=pipe, expire=1800)
        self.connector.send(endpoint, msgs, pipe=pipe)
        pipe.execute()

    def on_scan_point_emit(self, scan_id: str, point_id: int):
        self._send_bec_scan_point(scan_id, point_id)

    def on_baseline_emit(self, scan_id: str):
        self._send_baseline(scan_id)

    def _send_bec_scan_point(self, scan_id: str, point_id: int) -> None:
        sb = self.scan_bundler

        if scan_id not in sb.sync_storage:
            logger.warning(f"Cannot send scan point: Scan {scan_id} not found in sync storage.")
            return

        info = sb.sync_storage[scan_id]["info"]
        msg = messages.ScanMessage(
            point_id=point_id,
            scan_id=scan_id,
            data=sb.sync_storage[scan_id][point_id],
            metadata={
                "scan_id": scan_id,
                "scan_type": info.get("scan_type"),
                "scan_report_devices": info.get("scan_report_devices"),
            },
        )
        self.add_message(
            msg,
            MessageEndpoints.scan_segment(),
            MessageEndpoints.public_scan_segment(scan_id=scan_id, point_id=point_id),
        )
        if not sb.sync_storage[scan_id].get("device_progress_sub"):
            self._update_scan_progress(scan_id, point_id)

    def _update_scan_progress(self, scan_id: str, point_id: int, done=False) -> None:
        if scan_id not in self.scan_bundler.sync_storage:
            logger.warning(
                f"Cannot update scan progress: Scan {scan_id} not found in sync storage."
            )
            return
        info = self.scan_bundler.sync_storage[scan_id]["info"]

        num_monitored_readouts = info.get("num_monitored_readouts", info.get("num_points", 0))
        value = point_id + 1
        max_value = num_monitored_readouts or point_id + 1
        self.send_scan_progress(scan_id, value=value, max_value=max_value, done=done)

    def send_scan_progress(self, scan_id: str, value: float, max_value: float, done=False) -> None:
        """
        Send a scan progress update.

        Args:
            scan_id (str): The ID of the scan.
            value (float): The current progress value.
            max_value (float): The maximum progress value.
            done (bool): Whether the scan is done.
        """
        storage = self.scan_bundler.sync_storage.get(scan_id)
        if not storage:
            return
        info = storage["info"]
        msg = messages.ProgressMessage(
            value=value,
            max_value=max_value,
            done=done,
            metadata={
                "scan_id": scan_id,
                "RID": info.get("RID", ""),
                "queue_id": info.get("queue_id", ""),
                "status": storage["status"],
            },
        )
        storage["last_progress_sent"] = msg
        self.scan_bundler.connector.set_and_publish(MessageEndpoints.scan_progress(), msg)

    def _send_baseline(self, scan_id: str) -> None:
        sb = self.scan_bundler

        if scan_id not in sb.sync_storage:
            logger.warning(f"Cannot send baseline: Scan {scan_id} not found in sync storage.")
            return

        msg = messages.ScanBaselineMessage(
            scan_id=scan_id,
            data=sb.sync_storage[scan_id]["baseline"],
            metadata=sb.sync_storage[scan_id]["info"],
        )
        pipe = sb.connector.pipeline()
        sb.connector.set(
            MessageEndpoints.public_scan_baseline(scan_id=scan_id), msg, expire=1800, pipe=pipe
        )
        sb.connector.set_and_publish(MessageEndpoints.scan_baseline(), msg, pipe=pipe)
        pipe.execute()

    def on_scan_status_update(self, status_msg: messages.ScanStatusMessage):
        sb = self.scan_bundler
        if status_msg.scan_id not in sb.sync_storage:
            logger.warning(
                f"Cannot update scan progress: Scan {status_msg.scan_id} not found in sync storage."
            )
            return

        if status_msg.status == "open":
            # Update progress subscription:
            # - If the scan report instruction contains "scan_progress", we simply emit
            #  progress updates as they come in.
            # - If the scan report instruction contains "device_progress", we subscribe
            #  to the progress of the first device and use that as the progress for the whole scan.
            self._update_device_progress_subscription(status_msg.scan_id)
            return

        num_points = max(status_msg.info.get("num_points", 0) - 1, 0)
        num_monitored_readouts = status_msg.info.get("num_monitored_readouts", num_points)
        if status_msg.status == "closed":
            storage = sb.sync_storage[status_msg.scan_id]
            device_sub = storage.get("device_progress_sub")
            if not device_sub:
                self._update_scan_progress(status_msg.scan_id, num_monitored_readouts, done=True)
                return

            self.connector.unregister(**storage["device_progress_sub"])
            self._emit_last_progress(status_msg.scan_id)
            return

        # Scan is not open or closed but instead in ["aborted", "halted", "user_completed"]
        storage = sb.sync_storage[status_msg.scan_id]
        if storage.get("device_progress_sub"):
            self.connector.unregister(**storage["device_progress_sub"])
            self._emit_last_progress(status_msg.scan_id)
            return
        sent_vals = storage.get("sent", {0}) or {0}
        max_point = max(sent_vals)
        self._update_scan_progress(status_msg.scan_id, max_point, done=True)

    def on_cleanup(self, scan_id: str):
        if scan_id in self.scan_bundler.sync_storage:
            device_progress_sub = self.scan_bundler.sync_storage[scan_id].get("device_progress_sub")
            if device_progress_sub:
                self.connector.unregister(**device_progress_sub)

    def shutdown(self):
        if self._buffered_connector_thread:
            self._buffered_publisher_stop_event.set()
            self._buffered_connector_thread.join()
            self._buffered_connector_thread = None

    #############################################################
    ################# Device Progress Helpers ###################
    #############################################################

    def _update_device_progress_subscription(self, scan_id: str):
        sb = self.scan_bundler
        instructions = sb.scan_report_instructions.get(scan_id, [])
        if sb.sync_storage[scan_id].get("device_progress_sub"):
            return
        for instruction in instructions:
            if "device_progress" in instruction:
                device = instruction["device_progress"][0]
                sub = sb.sync_storage[scan_id]["device_progress_sub"] = {
                    "topics": MessageEndpoints.device_progress(device=device),
                    "cb": self._on_device_progress,
                    "scan_id": scan_id,
                }

                self.connector.register(**sub)

    def _emit_last_progress(self, scan_id: str):
        storage = self.scan_bundler.sync_storage.get(scan_id, {})
        msg = storage.get("last_progress_sent")
        value = msg.value if msg else 0
        max_value = msg.max_value if msg else 0
        self.send_scan_progress(scan_id, value=value, max_value=max_value, done=True)

    def _on_device_progress(self, msg_obj: MessageObject, scan_id: str):
        msg = cast(messages.ProgressMessage, msg_obj.value)
        if msg.done:
            sub_info = self.scan_bundler.sync_storage.get(scan_id, {}).get("device_progress_sub")
            if sub_info:
                self.connector.unregister(**sub_info)
        self.send_scan_progress(scan_id, value=msg.value, max_value=msg.max_value, done=msg.done)
