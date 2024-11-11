"""
This module provides a class that provides a convenient way to interact with the scan queue as well
as the requests and scans that are currently running or have been completed.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Literal

from typeguard import typechecked

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.queue_items import QueueStorage
from bec_lib.request_items import RequestStorage
from bec_lib.scan_items import ScanStorage

logger = bec_logger.logger

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector


class ScanManager:
    def __init__(self, connector: RedisConnector):
        """
        ScanManager is a class that provides a convenient way to interact with the scan queue as well
        as the requests and scans that are currently running or have been completed.
        It also contains storage container for the queue, requests and scans.

        Args:
            connector (BECConnector): BECConnector instance
        """
        self.connector = connector
        self.queue_storage = QueueStorage(scan_manager=self)
        self.request_storage = RequestStorage(scan_manager=self)
        self.scan_storage = ScanStorage(scan_manager=self)

        self.connector.register(
            topics=MessageEndpoints.scan_queue_status(), cb=self._scan_queue_status_callback
        )
        self.connector.register(
            topics=MessageEndpoints.scan_queue_request(), cb=self._scan_queue_request_callback
        )
        self.connector.register(
            topics=MessageEndpoints.scan_queue_request_response(),
            cb=self._scan_queue_request_response_callback,
        )
        self.connector.register(
            topics=MessageEndpoints.scan_status(), cb=self._scan_status_callback
        )

        self.connector.register(
            topics=MessageEndpoints.scan_segment(), cb=self._scan_segment_callback
        )

        self.connector.register(topics=MessageEndpoints.client_info(), cb=self._client_msg_callback)

        self.connector.register(
            patterns=MessageEndpoints.public_file("*", "*"), cb=self._public_file_callback
        )

    def update_with_queue_status(self, queue: messages.ScanQueueStatusMessage) -> None:
        """update storage with a new queue status message"""
        self.queue_storage.update_with_status(queue)
        self.scan_storage.update_with_queue_status(queue)

    def request_scan_interruption(self, deferred_pause=True, scan_id: str = None) -> None:
        """request a scan interruption

        Args:
            deferred_pause (bool, optional): Request a deferred pause. If False, a pause will be requested. Defaults to True.
            scan_id (str, optional): ScanID. Defaults to None.

        """
        if scan_id is None:
            scan_id = self.scan_storage.current_scan_id
        if not any(scan_id):
            return self.request_scan_abortion()

        action = "deferred_pause" if deferred_pause else "pause"
        logger.info(f"Requesting {action}")
        return self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(scan_id=scan_id, action=action, parameter={}),
        )

    def request_scan_abortion(self, scan_id=None):
        """request a scan abortion

        Args:
            scan_id (str, optional): ScanID. Defaults to None.

        """
        if scan_id is None:
            scan_id = self.scan_storage.current_scan_id
        logger.info("Requesting scan abortion")
        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(scan_id=scan_id, action="abort", parameter={}),
        )

    def request_scan_halt(self, scan_id=None):
        """request a scan halt

        Args:
            scan_id (str, optional): ScanID. Defaults to None.

        """
        if scan_id is None:
            scan_id = self.scan_storage.current_scan_id
        logger.info("Requesting scan halt")
        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(scan_id=scan_id, action="halt", parameter={}),
        )

    def request_scan_continuation(self, scan_id=None):
        """request a scan continuation

        Args:
            scan_id (str, optional): ScanID. Defaults to None.

        """
        if scan_id is None:
            scan_id = self.scan_storage.current_scan_id
        logger.info("Requesting scan continuation")
        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(scan_id=scan_id, action="continue", parameter={}),
        )

    def request_queue_reset(self):
        """request a scan queue reset"""
        logger.info("Requesting a queue reset")
        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(scan_id=None, action="clear", parameter={}),
        )

    def request_scan_restart(self, scan_id=None, requestID=None, replace=True) -> str:
        """request to restart a scan"""
        if scan_id is None:
            scan_id = self.scan_storage.current_scan_id
        if requestID is None:
            requestID = str(uuid.uuid4())
        logger.info("Requesting to abort and repeat a scan")
        position = "replace" if replace else "append"

        self.connector.send(
            MessageEndpoints.scan_queue_modification_request(),
            messages.ScanQueueModificationMessage(
                scan_id=scan_id,
                action="restart",
                parameter={"position": position, "RID": requestID},
            ),
        )
        return requestID

    @typechecked
    def request_queue_order_modification(
        self,
        scan_id: str,
        action: Literal["move_up", "move_down", "move_top", "move_bottom", "move_to"],
        position: int | None = None,
        queue: str = "primary",
        wait_for_response: bool = False,
    ) -> messages.RequestResponseMessage | None:
        """
        Request to modify the order of a scan in the queue.

        Args:
            scan_id (str): ScanID
            action (Literal["move_up", "move_down", "move_top", "move_bottom", "move_to"]): Action to perform
            position (int, optional): Position to move to. Required if action is "move_to". Defaults to None.
            queue (str, optional): Queue to modify. Defaults to "primary".
            wait_for_response (bool, optional): Wait for a response. Defaults to False.

        Returns:
            dict: Response message if wait_for_response is True
        """
        logger.info(f"Requesting to {action} a scan in the queue")

        if action == "move_to" and position is None:
            raise ValueError("Position must be provided when action is 'move_to'")

        if wait_for_response:
            update = {}
            self.connector.register(
                topics=MessageEndpoints.scan_queue_order_change_response(),
                cb=self._request_response_callback,
                update=update,
            )
        self.connector.send(
            MessageEndpoints.scan_queue_order_change_request(),
            messages.ScanQueueOrderMessage(
                scan_id=scan_id, action=action, target_position=position, queue=queue
            ),
        )
        if wait_for_response:
            while "response" not in update:
                time.sleep(0.1)
            self.connector.unregister(
                topics=MessageEndpoints.scan_queue_order_change_response(),
                cb=self._request_response_callback,
            )
            return update["response"]

    @staticmethod
    def _request_response_callback(msg, update):
        response = msg.value
        update["response"] = response

    @property
    def next_scan_number(self):
        """get the next scan number from redis"""
        msg = self.connector.get(MessageEndpoints.scan_number())
        if msg is None:
            logger.warning("Failed to retrieve scan number from redis.")
            return -1
        if not isinstance(msg, messages.VariableMessage):
            # this is a temporary fix for providing backwards compatibility
            return int(msg)
        return int(msg.value)

    @next_scan_number.setter
    @typechecked
    def next_scan_number(self, val: int):
        """set the next scan number in redis"""
        msg = messages.VariableMessage(value=val)
        return self.connector.set(MessageEndpoints.scan_number(), msg)

    @property
    def next_dataset_number(self):
        """get the next dataset number from redis"""
        msg = self.connector.get(MessageEndpoints.dataset_number())
        if msg is None:
            logger.warning("Failed to retrieve dataset number from redis.")
            return -1
        if not isinstance(msg, messages.VariableMessage):
            # this is a temporary fix for providing backwards compatibility
            return int(msg)
        return int(msg.value)

    @next_dataset_number.setter
    @typechecked
    def next_dataset_number(self, val: int):
        """set the next dataset number in redis"""
        msg = messages.VariableMessage(value=val)
        return self.connector.set(MessageEndpoints.dataset_number(), msg)

    def _scan_queue_status_callback(self, msg, **_kwargs) -> None:
        queue_status = msg.value
        if not queue_status:
            return
        self.update_with_queue_status(queue_status)

    def _scan_queue_request_callback(self, msg, **_kwargs) -> None:
        request = msg.value
        self.request_storage.update_with_request(request)

    def _scan_queue_request_response_callback(self, msg, **_kwargs) -> None:
        response = msg.value
        logger.debug(response)
        self.request_storage.update_with_response(response)

    def _client_msg_callback(self, msg: dict, **_kwargs) -> None:
        message = msg["data"]
        self.request_storage.update_with_client_message(message)

    def _scan_status_callback(self, msg, **_kwargs) -> None:
        scan = msg.value
        self.scan_storage.update_with_scan_status(scan)

    def _scan_segment_callback(self, msg, **_kwargs) -> None:
        scan_msgs = msg.value
        if not isinstance(scan_msgs, list):
            scan_msgs = [scan_msgs]
        for scan_msg in scan_msgs:
            self.scan_storage.add_scan_segment(scan_msg)

    @typechecked
    def add_scan_to_queue_schedule(
        self, schedule_name: str, msg: messages.ScanQueueMessage
    ) -> None:
        """
        Add a scan to the queue schedule

        Args:
            schedule_name (str): name of the queue schedule
            msg (messages.ScanQueueMessage): scan message
        """
        self.connector.rpush(MessageEndpoints.scan_queue_schedule(schedule_name=schedule_name), msg)

    @typechecked
    def get_scan_queue_schedule(self, schedule_name: str) -> list:
        """
        Get the scan queue schedule

        Args:
            schedule_name (str): name of the queue schedule

        Returns:
            list: list of scan messages
        """
        return self.connector.lrange(
            MessageEndpoints.scan_queue_schedule(schedule_name=schedule_name), 0, -1
        )

    @typechecked
    def clear_scan_queue_schedule(self, schedule_name: str) -> None:
        """
        Clear the scan queue schedule

        Args:
            schedule_name (str): name of the queue schedule
        """
        self.connector.delete(MessageEndpoints.scan_queue_schedule(schedule_name=schedule_name))

    def get_scan_queue_schedule_names(self) -> list:
        """
        Get the names of the scan queue schedules

        Returns:
            list: list of schedule names
        """
        keys = self.connector.keys(MessageEndpoints.scan_queue_schedule(schedule_name="*"))
        if not keys:
            return []
        return [key.decode().split("/")[-1] for key in keys]

    def clear_all_scan_queue_schedules(self) -> None:
        """
        Clear all scan queue schedules
        """
        keys = self.get_scan_queue_schedule_names()
        for key in keys:
            self.clear_scan_queue_schedule(key)

    def _public_file_callback(self, msg, **_kwargs) -> None:
        topic = msg.topic
        value = msg.value
        scan_id = topic.split("/")[1]
        self.scan_storage.add_public_file(scan_id, value)

    def __str__(self) -> str:
        try:
            return "\n".join(self.queue_storage.describe_queue())
        except Exception:
            # queue_storage.describe_queue() can fail,
            # for example if there is no current scan queue (None)
            return super().__str__()

    def shutdown(self):
        pass
