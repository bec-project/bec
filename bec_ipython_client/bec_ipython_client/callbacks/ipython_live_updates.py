from __future__ import annotations

import collections
import time
from typing import TYPE_CHECKING, Any

from bec_ipython_client.callbacks.device_progress import LiveUpdatesDeviceProgress
from bec_lib.bec_errors import ScanInterruption
from bec_lib.logger import bec_logger

from .live_table import LiveUpdatesTable
from .move_device import LiveUpdatesReadbackProgressbar
from .utils import ScanRequestMixin, check_alarms

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.client import BECClient
    from bec_lib.queue_items import QueueItem

logger = bec_logger.logger


class IPythonLiveUpdates:
    def __init__(self, client: BECClient) -> None:
        """Class to handle live updates for IPython, also works in Jupyterlab.

        Args:
            client (BECClient): The BECClient instance.
        """
        self.client = client
        self._interrupted_request = None
        self._active_callback = None
        self._processed_instructions = 0
        self._active_request = None
        self._user_callback = None
        self._request_block_index = collections.defaultdict(lambda: 0)
        self._request_block_id = None
        self._current_queue = None

    @property
    def print_table_data(self):
        return self.client.live_updates_config.print_live_table

    def _process_report_instructions(self, report_instructions: list) -> None:
        """Process instructions for the live updates.

        Args:
            report_instructions (list): The list of report instructions.
        """
        scan_type = self._active_request.content["scan_type"]
        if scan_type in ["open_scan_def", "close_scan_def"]:
            self._process_instruction({"scan_progress": {"points": 0, "show_table": True}})
            return
        if scan_type == "close_scan_group":
            return

        if not report_instructions:
            return

        remaining_report_instructions = report_instructions[self._processed_instructions :]
        if not remaining_report_instructions:
            return

        for instr in remaining_report_instructions:
            self._process_instruction(instr)
            self._processed_instructions += 1

    def _process_instruction(self, instr: dict):
        """Process the received instruction based on scan_report_type.

        Args:
            instr (dict): The instruction to process.
        """
        scan_report_type = list(instr.keys())[0]
        scan_def_id = self.client.scans._scan_def_id
        interactive_scan = self.client.scans._interactive_scan
        if scan_def_id is None or interactive_scan:
            if scan_report_type == "readback":
                LiveUpdatesReadbackProgressbar(
                    self.client,
                    report_instruction=instr,
                    request=self._active_request,
                    callbacks=self._user_callback,
                ).run()
            elif scan_report_type == "scan_progress":
                LiveUpdatesTable(
                    self.client,
                    report_instruction=instr,
                    request=self._active_request,
                    callbacks=self._user_callback,
                    print_table_data=self.print_table_data,
                ).run()
            elif scan_report_type == "device_progress":
                LiveUpdatesDeviceProgress(
                    self.client,
                    report_instruction=instr,
                    request=self._active_request,
                    callbacks=self._user_callback,
                ).run()
            else:
                raise ValueError(f"Unknown scan report type: {scan_report_type}")
        else:
            if self._active_callback:
                if scan_report_type == "readback":
                    LiveUpdatesReadbackProgressbar(
                        self.client,
                        report_instruction=instr,
                        request=self._active_request,
                        callbacks=self._user_callback,
                    ).run()
                else:
                    self._active_callback.resume(
                        request=self._active_request,
                        report_instruction=instr,
                        callbacks=self._user_callback,
                    )

                return

            self._active_callback = LiveUpdatesTable(
                self.client,
                report_instruction=instr,
                request=self._active_request,
                callbacks=self._user_callback,
                print_table_data=self.print_table_data,
            )
            self._active_callback.run()

    def _available_req_blocks(self, queue: QueueItem, request: messages.ScanQueueMessage):
        """Get the available request blocks.

        Args:
            queue (QueueItem): The queue item.
            request (messages.ScanQueueMessage): The request message.
        """
        available_blocks = [
            req_block
            for req_block in queue.request_blocks
            if req_block["RID"] == request.metadata["RID"]
        ]
        return available_blocks

    def process_request(self, request: messages.ScanQueueMessage, callbacks: Any) -> None:
        """Process the request and report instructions."""
        # pylint: disable=protected-access
        try:
            with self.client._sighandler:
                # pylint: disable=protected-access
                self._active_request = request
                self._user_callback = callbacks
                scan_request = ScanRequestMixin(self.client, request.metadata["RID"])
                scan_request.wait()

                # get the corresponding queue item
                while not scan_request.request_storage.storage[-1].queue:
                    time.sleep(0.01)

                self._current_queue = queue = scan_request.request_storage.storage[-1].queue
                self._request_block_id = req_id = self._active_request.metadata.get("RID")

                while queue.status not in ["COMPLETED", "ABORTED", "HALTED"]:
                    if self._process_queue(queue, request, req_id):
                        break

                available_blocks = self._available_req_blocks(queue, request)
                req_block = available_blocks[self._request_block_index[req_id]]
                report_instructions = req_block.get("report_instructions", [])
                self._process_report_instructions(report_instructions)

            self._reset()

        except ScanInterruption as scan_interr:
            self._interrupted_request = (request,)
            if self._current_queue and self.client._service_config.abort_on_ctrl_c:
                self._wait_for_cleanup()
            self._reset(forced=True)
            raise scan_interr

    def _wait_for_cleanup(self):
        """Wait for the scan to be cleaned up."""
        try:
            if not self._element_in_queue():
                return
            print("Waiting for the scan to be cleaned up...")
            while self._element_in_queue():
                time.sleep(0.1)
        except KeyboardInterrupt:
            self.client.queue.request_scan_halt()

    def _element_in_queue(self) -> bool:
        queue = self.client.queue.queue_storage.current_scan_queue.get("primary", {}).get(
            "info", []
        )
        if not queue:
            return False
        return self._current_queue.queue_id in queue[0].get("queue_id")

    def _process_queue(
        self, queue: QueueItem, request: messages.ScanQueueMessage, req_id: str
    ) -> bool:
        """
        Process the queue and return True if the scan is completed.

        Args:
            queue(QueueItem): The queue item to process.
            request(messages.ScanQueueMessage): The request message.
            req_id(str): The request ID.

        Returns:
            bool: True if the scan is completed.
        """
        check_alarms(self.client)
        if not queue.request_blocks or not queue.status or queue.queue_position is None:
            return False
        if queue.status == "PENDING" and queue.queue_position > 0:
            status = self.client.queue.queue_storage.current_scan_queue.get("primary", {}).get(
                "status"
            )
            print(
                "Scan is enqueued and is waiting for execution. Current position in queue:"
                f" {queue.queue_position + 1}. Queue status: {status}.",
                end="\r",
                flush=True,
            )
        available_blocks = self._available_req_blocks(queue, request)
        req_block = available_blocks[self._request_block_index[req_id]]
        if req_block["content"]["scan_type"] in [
            "open_scan_def",
            "mv",
        ]:  # TODO: make this more general for all scan types that don't have report instructions
            return True

        report_instructions = req_block["report_instructions"]
        if not report_instructions:
            return False
        self._process_report_instructions(report_instructions)

        complete_rbl = len(available_blocks) == self._request_block_index[req_id] + 1
        if self._active_callback and complete_rbl:
            return True

        if complete_rbl and self.client.scans._interactive_scan:
            return True

        if not queue.active_request_block:
            return True

        return False

    def _reset(self, forced=False):
        """Reset the active request and callback.

        Args:
            forced(bool): If True, the reset is forced.
        """
        self._interrupted_request = None

        self._current_queue = None
        self._user_callback = None
        self._processed_instructions = 0
        scan_closed = forced or (self._active_request.content["scan_type"] == "close_scan_def")
        self._active_request = None

        if self.client.scans._scan_def_id and not scan_closed:
            self._request_block_index[self._request_block_id] += 1
            return

        if scan_closed:
            self._active_callback = None

    def continue_request(self):
        """Continue the interrupted request."""
        if not self._interrupted_request:
            return
        self.process_request(*self._interrupted_request, self._user_callback)
