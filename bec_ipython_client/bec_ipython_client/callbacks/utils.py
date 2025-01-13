from __future__ import annotations

import abc
import threading
import time
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING

from bec_lib.logger import bec_logger
from bec_lib.request_items import RequestItem

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.client import BECClient

logger = bec_logger.logger


class ScanRequestError(Exception):
    """Error raised when a scan request is rejected"""


def set_event_delayed(event: threading.Event, delay: int) -> None:
    """Set event with a delay

    Args:
        event (threading.Event): event that should be set
        delay (int): delay time in seconds

    """

    def call_set():
        time.sleep(delay)
        event.set()

    thread = threading.Thread(target=call_set, daemon=True)
    thread.start()


def check_alarms(bec):
    """check for alarms and raise them if needed"""
    bec.alarm_handler.raise_alarms()


class LiveUpdatesBase(abc.ABC):
    def __init__(
        self,
        bec: BECClient,
        report_instruction: dict = None,
        request: messages.ScanQueueMessage = None,
        callbacks: list[Callable] = None,
    ) -> None:
        """Base class for live updates

        Args:
            bec (BECClient): BECClient instance
            report_instruction (dict, optional): report instruction. Defaults to None.
            request (messages.ScanQueueMessage, optional): scan queue request. Defaults to None.
            callbacks (list[Callable], optional): list of callback functions. Defaults to None.
        """
        self.bec = bec
        self.request = request
        self.RID = request.metadata["RID"]
        self.scan_queue_request = None
        self.report_instruction = report_instruction
        if callbacks is None:
            self.callbacks = []
        self.callbacks = callbacks if isinstance(callbacks, list) else [callbacks]

    def wait_for_request_acceptance(self):
        scan_request = ScanRequestMixin(self.bec, self.RID)
        scan_request.wait()
        self.scan_queue_request = scan_request.scan_queue_request

    @abc.abstractmethod
    def run(self):
        pass

    def emit_point(self, data: dict, metadata: dict = None):
        for cb in self.callbacks:
            if not cb:
                continue
            try:
                cb(data, metadata=metadata)
            except Exception:
                content = traceback.format_exc()
                logger.warning(f"Failed to run callback function: {content}")

    def _print_client_msgs_asap(self):
        """Print client messages flagged as show_asap"""
        # pylint: disable=protected-access
        if self.scan_queue_request is None:
            return
        msgs = self.scan_queue_request.queue.get_client_messages(only_asap=True)
        if not msgs:
            return
        if self.bec.live_updates_config.print_client_messages is False:
            return
        for msg in msgs:
            print(self.scan_queue_request.queue.format_client_msg(msg))

    def _print_client_msgs_all(self):
        """Print summary of client messages"""
        # pylint: disable=protected-access
        if self.scan_queue_request is None:
            return
        msgs = self.scan_queue_request.queue.get_client_messages()
        if self.bec.live_updates_config.print_client_messages is False:
            return
        if not msgs:
            return
        print("------------------------")
        print("Summary of client messages")
        print("------------------------")
        # pylint: disable=protected-access
        for msg in msgs:
            print(self.scan_queue_request.queue.format_client_msg(msg))
        print("------------------------")


class ScanRequestMixin:
    def __init__(self, bec: BECClient, RID: str) -> None:
        """Mixin to handle scan request acceptance

        Args:
            bec (BECClient): BECClient instance
            RID (str): request ID
        """
        self.bec = bec
        self.request_storage = self.bec.queue.request_storage
        self.RID = RID
        self.scan_queue_request = None

    def _wait_for_scan_request(self) -> RequestItem:
        """wait for scan queuest

        Returns:
            RequestItem: scan queue request
        """
        logger.debug("Waiting for request ID")
        start = time.time()
        while self.request_storage.find_request_by_ID(self.RID) is None:
            time.sleep(0.1)
            check_alarms(self.bec)
        logger.debug(f"Waiting for request ID finished after {time.time()-start} s.")
        return self.request_storage.find_request_by_ID(self.RID)

    def _wait_for_scan_request_decision(self):
        """wait for a scan queuest decision"""
        logger.debug("Waiting for decision")
        start = time.time()
        while self.scan_queue_request.decision_pending:
            time.sleep(0.1)
            check_alarms(self.bec)
        logger.debug(f"Waiting for decision finished after {time.time()-start} s.")

    def wait(self):
        """wait for the request acceptance"""
        self.scan_queue_request = self._wait_for_scan_request()

        self._wait_for_scan_request_decision()
        check_alarms(self.bec)

        while self.scan_queue_request.accepted is None:
            time.sleep(0.1)
            check_alarms(self.bec)

        if not self.scan_queue_request.accepted[0]:
            raise ScanRequestError(
                "Scan was rejected by the server:"
                f" {self.scan_queue_request.response.content.get('message')}"
            )

        while self.scan_queue_request.queue is None:
            time.sleep(0.1)
            check_alarms(self.bec)
