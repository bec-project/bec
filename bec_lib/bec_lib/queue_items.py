"""
This module contains the QueueItem and QueueStorage classes.
"""

from __future__ import annotations

import functools
import threading
from collections import deque
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

from bec_lib.endpoints import MessageEndpoints
from bec_lib.utils import threadlocked

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages
    from bec_lib.request_items import RequestItem
    from bec_lib.scan_items import ScanItem
    from bec_lib.scan_manager import ScanManager


def update_queue(fcn):
    """Decorator to update the queue item"""

    @functools.wraps(fcn)
    def wrapper(self, *args, **kwargs):
        self._update_with_buffer()
        return fcn(self, *args, **kwargs)

    return wrapper


class QueueItem:
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        scan_manager: ScanManager,
        queue_id: str,
        request_blocks: list,
        status: str,
        active_request_block: dict,
        scan_id: list[str],
        client_messages: list | None = None,
        **_kwargs,
    ) -> None:
        self.scan_manager = scan_manager
        self.queue_id = queue_id
        self.request_blocks = request_blocks
        self._status = status
        self.active_request_block = active_request_block
        self.scan_ids = scan_id
        if client_messages is None:
            client_messages = []
        self.client_messages = client_messages

    @property
    @update_queue
    def scans(self) -> list[ScanItem]:
        """get the scans items assigned to the current queue item"""
        return [
            self.scan_manager.scan_storage.find_scan_by_ID(scan_id) for scan_id in self.scan_ids
        ]

    @property
    @update_queue
    def requestIDs(self):
        return [request_block["RID"] for request_block in self.request_blocks]

    @property
    @update_queue
    def requests(self) -> list[RequestItem]:
        """get the request items assigned to the current queue item"""
        return [
            self.scan_manager.request_storage.find_request_by_ID(requestID)
            for requestID in self.requestIDs
        ]

    @property
    @update_queue
    def status(self):
        return self._status

    def _update_with_buffer(self):
        current_queue = self.scan_manager.queue_storage.current_scan_queue
        queue_info = current_queue["primary"].get("info")
        for queue_item in queue_info:
            if queue_item["queue_id"] == self.queue_id:
                self.update_queue_item(queue_item)
                return
        history = self.scan_manager.queue_storage.queue_history
        for queue_item in history:
            if queue_item.content["queue_id"] == self.queue_id:
                self.update_queue_item(queue_item.content["info"])
                return

    def update_queue_item(self, queue_item):
        """update the queue item"""
        self.request_blocks = queue_item.get("request_blocks")
        self._status = queue_item.get("status")
        self.active_request_block = queue_item.get("active_request_block")
        self.scan_ids = queue_item.get("scan_id")

    def update_with_client_message(self, message: messages.ClientInfoMessage):
        """append a client message to the queue item"""
        self.client_messages.append(message)

    @property
    def queue_position(self) -> int | None:
        """get the current queue position"""
        current_queue = self.scan_manager.queue_storage.current_scan_queue
        for queue_group in current_queue.values():
            if not isinstance(queue_group, dict):
                continue
            for queue_position, queue in enumerate(queue_group["info"]):
                if self.queue_id == queue["queue_id"]:
                    return queue_position
        return None

    def get_client_messages(self, only_asap: bool = False) -> list[messages.ClientInfoMessage]:
        """Get all client messages from the queue item.

        Args:
            only_asap (bool): If True, only return the asap messages.
        """
        msgs = []
        for ii, msg in enumerate(self.client_messages):
            if only_asap is True and msg.show_asap is False:
                continue
            msgs.append(msg)
            self.client_messages.pop(ii)
        return msgs

    @staticmethod
    def format_client_msg(msg: messages.ClientInfoMessage) -> str:
        """Pop messages from the client message handler.

        Args:
            msg (messages.ClientInfoMessage): client message
        """
        source = msg.source if msg.source else ""
        rtr = f"Client info ({source}) : {msg.message}"
        return rtr


class QueueStorage:
    """stores queue items"""

    def __init__(self, scan_manager: ScanManager, maxlen=100) -> None:
        self.storage: deque[QueueItem] = deque(maxlen=maxlen)
        self._lock = threading.RLock()
        self.scan_manager = scan_manager
        self.current_scan_queue = None
        self.queue_history = None

    def _update_queue_history(self):
        """get the queue history from redis"""
        self.queue_history = self.scan_manager.connector.lrange(
            MessageEndpoints.scan_queue_history(), 0, 5
        )

    def _update_current_scan_queue(self):
        """get the current scan queue from redis"""
        msg = self.scan_manager.connector.get(MessageEndpoints.scan_queue_status())
        if msg:
            self.current_scan_queue = msg.content["queue"]

    def _update_queue(self):
        self._update_current_scan_queue()
        self._update_queue_history()

    def describe_queue(self):
        """create a rich.table description of the current scan queue"""
        queue_tables = []
        self._update_queue()
        console = Console()
        for queue_name, scan_queue in self.current_scan_queue.items():
            table = Table(title=f"{queue_name} queue / {scan_queue.get('status')}")
            table.add_column("queue_id", justify="center")
            table.add_column("scan_id", justify="center")
            table.add_column("is_scan", justify="center")
            table.add_column("type", justify="center")
            table.add_column("scan_number", justify="center")
            table.add_column("IQ status", justify="center")

            for instruction_queue in scan_queue.get("info"):
                scan_msgs = [
                    msg.get("content") for msg in instruction_queue.get("request_blocks", [])
                ]
                table.add_row(
                    instruction_queue.get("queue_id"),
                    ", ".join([str(s) for s in instruction_queue.get("scan_id")]),
                    ", ".join([str(s) for s in instruction_queue.get("is_scan")]),
                    ", ".join([msg["scan_type"] for msg in scan_msgs]),
                    ", ".join([str(s) for s in instruction_queue.get("scan_number")]),
                    instruction_queue.get("status"),
                )
            with console.capture() as capture:
                console.print(table)
            queue_tables.append(capture.get())
        return queue_tables

    @threadlocked
    def update_with_status(self, queue_msg: messages.ScanQueueStatusMessage) -> None:
        """update a queue item with a new ScanQueueStatusMessage / queue message"""
        self.current_scan_queue = queue_msg.content["queue"]
        self._update_queue_history()
        queue_info = queue_msg.content["queue"]["primary"].get("info")
        for ii, queue_item in enumerate(queue_info):
            queue = self.find_queue_item_by_ID(queue_id=queue_item["queue_id"])
            if queue:
                queue.update_queue_item(queue_item)
                continue
            self.storage.append(QueueItem(scan_manager=self.scan_manager, **queue_item))
            if ii > 20:
                # only keep the last 20 queue items
                break

    @threadlocked
    def find_queue_item_by_ID(self, queue_id: str) -> QueueItem | None:
        """find a queue item based on its queue_id"""
        for queue_item in self.storage:
            if queue_item.queue_id == queue_id:
                return queue_item
        return None

    @threadlocked
    def find_queue_item_by_requestID(self, requestID: str) -> QueueItem | None:
        """find a queue item based on its requestID"""
        for queue_item in self.storage:
            if requestID in queue_item.requestIDs:
                return queue_item
        return None

    @threadlocked
    def find_queue_item_by_scan_id(self, scan_id: str) -> QueueItem | None:
        """find a queue item based on its scan_id"""
        for queue_item in self.storage:
            if scan_id in queue_item.scans:
                return queue_item
        return None

    def update_with_client_message(self, client_message: messages.ClientInfoMessage) -> None:
        """Update the queue item with a new ClientInfoMessage"""
        queue_info = self.current_scan_queue["primary"].get("info")
        if not queue_info:
            return
        queue_item = self.find_queue_item_by_ID(queue_info[0]["queue_id"])
        if not queue_item:
            return
        queue_item.update_with_client_message(client_message)
