"""Validate scan requests and route accepted requests to scan execution."""

from __future__ import annotations

import re
import traceback
import uuid
from typing import TYPE_CHECKING, Any, cast

from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .scan_queue.coordinator import QueueClosedError

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.scan_server.scan_server import ScanServer


class ScanRejection(Exception):
    """Indicate that a scan request failed validation."""


class ScanStatus:
    """Store the acceptance decision and explanation for a scan request."""

    def __init__(self, accepted: bool = True, message: str = "") -> None:
        """Initialize the acceptance decision and its explanation.

        Args:
            accepted (bool): Whether the request was accepted.
            message (str): Explanation of the acceptance decision.
        """
        self.accepted = accepted
        self.message = message


class ScanGuard:
    """Validate scan requests and admit accepted requests to the queue manager."""

    def __init__(self, *, parent: ScanServer) -> None:
        """Initialize request validation and register request subscriptions.

        Args:
            parent (ScanServer): Owning scan server.
        """
        self.parent = parent
        self.device_manager = self.parent.device_manager
        self.connector = self.parent.connector

        self.connector.register(
            patterns=MessageEndpoints.scan_queue_request("*"), cb=self._scan_queue_request_callback
        )

        self.connector.register(
            MessageEndpoints.scan_queue_modification_request(),
            cb=self._scan_queue_modification_request_callback,
        )
        self.connector.register(
            MessageEndpoints.scan_queue_order_change_request(), cb=self._scan_queue_order_callback
        )

    def shutdown(self) -> None:
        """Unregister request ingress before shutting down queue admission."""
        self.connector.unregister(
            patterns=MessageEndpoints.scan_queue_request("*"), cb=self._scan_queue_request_callback
        )
        self.connector.unregister(
            topics=MessageEndpoints.scan_queue_modification_request(),
            cb=self._scan_queue_modification_request_callback,
        )
        self.connector.unregister(
            topics=MessageEndpoints.scan_queue_order_change_request(),
            cb=self._scan_queue_order_callback,
        )

    #############################################
    ############### Helper Methods ##############
    #############################################

    def _is_valid_scan_request(
        self, request: messages.ScanQueueMessage, username: str
    ) -> ScanStatus:
        """Perform validity checks on the scan request.

        Args:
            request (messages.ScanQueueMessage): A scan queue message
            username (str): The username associated with the request

        Returns:
            ScanStatus: Acceptance decision and validation error, if any.
        """
        try:
            self._check_valid_request(request, username)
            self._check_valid_scan(request)
            self._check_baton(request)
            self._check_motors_movable(request)
        # pylint: disable=broad-except
        except Exception:
            content = traceback.format_exc()
            return ScanStatus(False, str(content))
        return ScanStatus()

    def _check_valid_request(self, request: messages.ScanQueueMessage, username: str) -> None:
        """Validate the request message and submitting username.

        Require the topic username to match the client metadata so users cannot submit
        requests on behalf of another user.

        Args:
            request (messages.ScanQueueMessage): A scan queue message
            username (str): The username associated with the request

        Raises:
            ScanRejection: If the request is invalid
        """
        if request is None:
            raise ScanRejection("Invalid request.")
        client_info = request.metadata.get("client_info")
        if not client_info:
            raise ScanRejection("Missing client info in request metadata.")

        # Note: the default redis user does not have an acl username
        acl_user = client_info.get("acl_user") or "default"
        if acl_user != username:
            raise ScanRejection("Username in topic does not match client info.")

    def _check_valid_scan(self, request: messages.ScanQueueMessage) -> None:
        """Check if the scan is valid and known.

        Args:
            request (messages.ScanQueueMessage): A scan queue message

        Raises:
            ScanRejection: If the scan is invalid
        """
        avail_scans = self.connector.get(MessageEndpoints.available_scans())
        scan_type = request.content.get("scan_type")
        if scan_type not in avail_scans.resource:
            raise ScanRejection(f"Unknown scan type {scan_type}.")

        if scan_type == "device_rpc":
            # ensure that the requested rpc is allowed for this particular device
            device, func = self._extract_device_rpc_target(request)
            if not self._device_rpc_is_valid(device=device, func=func):
                raise ScanRejection(f"Rejected rpc: {request.content}")

    def _device_rpc_is_valid(self, device: str | list[str] | None, func: str) -> bool:
        # pylint: disable=unused-argument
        # TODO: make sure the device rpc is valid and not exceeding the scope
        """Check whether a device RPC request specifies a target device.

        Args:
            device (str | list[str] | None): Device name or names targeted by the RPC request.
            func (str): Unused compatibility parameter.

        Returns:
            bool: Whether the request specifies a device target.
        """
        if not device:
            return False
        return True

    def _check_baton(self, request: messages.ScanQueueMessage) -> None:
        # TODO: Implement baton handling
        """Reserve the validation hook for future baton checks.

        Args:
            request (messages.ScanQueueMessage): Unused compatibility parameter.
        """
        pass

    def _check_motors_movable(self, request: messages.ScanQueueMessage) -> None:
        """Check if the motors involved in the scan request are movable.

        Args:
            request (messages.ScanQueueMessage): A scan queue message

        Raises:
            ScanRejection: If any motor is not enabled or movable
        """
        parameter = request.parameter
        if request.scan_type == "device_rpc":
            device, _ = self._extract_device_rpc_target(request)
            if not isinstance(device, list):
                device = [device]
            for dev in device:
                if dev not in self.device_manager.devices:
                    raise ScanRejection(f"Device {dev} is not known.")
                if not self.device_manager.devices[dev].enabled:
                    raise ScanRejection(f"Device {dev} is not enabled.")
            return
        motor_args = parameter.get("args")
        if not motor_args:
            return
        for motor in motor_args:
            if not motor:
                continue
            if not isinstance(motor, str):
                continue
            if motor not in self.device_manager.devices:
                continue
            if not self.device_manager.devices[motor].enabled:
                raise ScanRejection(f"Device {motor} is not enabled.")

    def _scan_queue_request_callback(self, msg: MessageObject[messages.ScanQueueMessage]) -> None:
        """Read the submitting username and validate its scan request.

        Args:
            msg (MessageObject[messages.ScanQueueMessage]): Request or event message to handle.
        """
        scan_msg = cast(messages.ScanQueueMessage, msg.value)
        content = scan_msg.content
        username_regex = f"^{MessageEndpoints.scan_queue_request('([^/]+)').endpoint}$"
        result = re.match(username_regex, msg.topic)
        if not result:
            raise ScanRejection("Could not extract username from topic.")
        username = result.group(1)

        logger.info(f"Receiving scan request: {content} from user {username}")

        self._handle_scan_request(scan_msg, username=username)

    def _scan_queue_modification_request_callback(
        self, msg: MessageObject[messages.ScanQueueModificationMessage]
    ) -> None:
        """Handle a queue modification request from Redis.

        Args:
            msg (MessageObject[messages.ScanQueueModificationMessage]): Request or event message to
                handle.
        """
        mod_msg = cast(messages.ScanQueueModificationMessage | None, msg.value)
        if mod_msg is None:
            logger.warning("Failed to parse scan queue modification message.")
            return
        content = mod_msg.content
        logger.info(f"Receiving scan modification request: {content}")

        self._handle_scan_modification_request(mod_msg)

    def _send_scan_request_response(
        self, scan_status: ScanStatus, metadata: dict[str, Any]
    ) -> None:
        """Send a scan request response message.

        Args:
            scan_status (ScanStatus): ScanStatus object
            metadata (dict[str, Any]): Metadata dict
        """
        sqrr = MessageEndpoints.scan_queue_request_response()
        rrm = messages.RequestResponseMessage(
            accepted=scan_status.accepted, message=scan_status.message, metadata=metadata
        )
        self.device_manager.connector.send(sqrr, rrm)

    def _handle_scan_request(self, msg: messages.ScanQueueMessage, username: str) -> None:
        """Validate a scan request and report its acceptance decision.

        Admit scans before acknowledging them. Read-only device RPC requests execute directly.

        Args:
            msg (messages.ScanQueueMessage): A scan queue message
            username (str): The username associated with the request
        """
        scan_status = self._is_valid_scan_request(msg, username=username)

        if not scan_status.accepted:
            self._send_scan_request_response(scan_status, msg.metadata)
            logger.info(f"Request was rejected: {scan_status.message}")
            return

        if msg.scan_type == "device_rpc":
            _, func = self._extract_device_rpc_target(msg)
            if func in ["get", "read"] or func.endswith(".get") or func.endswith(".read"):
                logger.info("Scan request is a read operation, not enqueuing.")
                self._send_scan_request_response(scan_status, msg.metadata)
                self._direct_device_rpc(msg)
                return
        try:
            if not self._append_to_scan_queue(msg):
                scan_status = ScanStatus(False, "Scan queue rejected the request")
        except QueueClosedError:
            scan_status = ScanStatus(False, "Scan queue is shutting down")
        self._send_scan_request_response(scan_status, msg.metadata)

    @staticmethod
    def _extract_device_rpc_target(
        msg: messages.ScanQueueMessage,
    ) -> tuple[str | list[str] | None, str]:
        """Extract the target device and function from a device RPC request.

        Args:
            msg (messages.ScanQueueMessage): Request or event message to handle.

        Returns:
            tuple[str | list[str] | None, str]: Target device or devices and the requested function
                name.
        """
        params = msg.content.get("parameter", {})
        rpc_kwargs = params.get("kwargs", {})
        return rpc_kwargs.get("device"), rpc_kwargs.get("func", "")

    def _direct_device_rpc(self, msg: messages.ScanQueueMessage) -> None:
        """Directly send a device RPC request without enqueuing.

        Args:
            msg (messages.ScanQueueMessage): ScanQueueMessage containing the RPC request
        """
        device, _ = self._extract_device_rpc_target(msg)
        if not device:
            logger.error("No device specified for RPC request.")
            return
        logger.info(f"Directly sending device RPC request for device {device}")
        params = msg.content.get("parameter", {})
        if not params:
            logger.error("No parameters provided for device RPC request.")
            return

        rpc_kwargs = params.get("kwargs", {})
        params = {
            "device": device,
            "rpc_id": rpc_kwargs.get("rpc_id"),
            "func": rpc_kwargs.get("func"),
            "args": rpc_kwargs.get("func_args", []),
            "kwargs": rpc_kwargs.get("func_kwargs", {}),
        }
        instr = messages.DeviceInstructionMessage(
            device=device,
            action="rpc",
            parameter=params,
            metadata={"device_instr_id": str(uuid.uuid4())},
        )

        self.connector.send(MessageEndpoints.device_instructions(), instr)

    def _handle_scan_modification_request(self, msg: messages.ScanQueueModificationMessage) -> None:
        """Forward a queue modification and acknowledge restart requests.

        Args:
            msg (messages.ScanQueueModificationMessage): Queue modification to forward to the queue
                manager.
        """
        mod_msg = msg

        if mod_msg.action == "restart":
            RID = mod_msg.content["parameter"].get("RID")
            if RID:
                mod_msg.metadata["RID"] = RID
                self._send_scan_request_response(ScanStatus(), mod_msg.metadata)

        sqm = MessageEndpoints.scan_queue_modification()
        self.device_manager.connector.send(sqm, mod_msg)

    def _append_to_scan_queue(self, msg: messages.ScanQueueMessage) -> bool:
        """Admit a validated request before acknowledging it to its client.

        Args:
            msg (messages.ScanQueueMessage): Request or event message to handle.

        Returns:
            bool: Whether the queue manager admitted the request.

        Raises:
            QueueClosedError: The queue manager has closed request admission.
        """
        logger.info("Appending new scan to queue")
        return self.parent.queue_manager.add_to_queue(msg.queue, msg.model_copy(deep=True))

    def _scan_queue_order_callback(
        self, msg: MessageObject[messages.ScanQueueOrderMessage]
    ) -> None:
        """Forward a queue order request to its handler.

        Args:
            msg (MessageObject[messages.ScanQueueOrderMessage]): Request or event message to
                handle.
        """
        self._handle_scan_order_change(cast(messages.ScanQueueOrderMessage, msg.value))

    def _handle_scan_order_change(self, msg: messages.ScanQueueOrderMessage) -> None:
        """Handle the scan queue order change request.

        Args:
            msg (messages.ScanQueueOrderMessage): ScanQueueOrderMessage
        """
        logger.info("Handling scan queue order change")
        sqoc = MessageEndpoints.scan_queue_order_change()
        target_queue = msg.queue
        queue_manager = self.parent.queue_manager
        queue = queue_manager.export_queue().get(target_queue)
        queue_exists = queue is not None
        queue_paused = queue is not None and queue.status == "PAUSED"
        scan_found = queue is not None and any(msg.scan_id in item.scan_id for item in queue.info)
        if not queue_exists:
            logger.error(f"Invalid queue: {target_queue}")
            self._send_scan_queue_order_change_response(False, f"Invalid queue: {target_queue}")
            return
        if not queue_paused:
            logger.error(f"Queue {target_queue} is not paused.")
            self._send_scan_queue_order_change_response(
                False, f"Queue {target_queue} is not paused. Cannot move scans."
            )
            return
        if msg.action == "move_to" and msg.target_position is None:
            logger.error("Missing target_position")
            self._send_scan_queue_order_change_response(
                False, "Missing target_position for move_to"
            )
            return

        if not scan_found:
            logger.error(f"Scan {msg.scan_id} not found in queue {target_queue}")
            self._send_scan_queue_order_change_response(
                False, f"Scan {msg.scan_id} not found in queue {target_queue}"
            )
            return
        self.device_manager.connector.send(sqoc, msg)
        self._send_scan_queue_order_change_response(True, "Order change accepted")

    def _send_scan_queue_order_change_response(self, accepted: bool, message: str) -> None:
        """Send a response to the scan queue order change request.

        Args:
            accepted (bool): Whether the request was accepted.
            message (str): Explanation of the acceptance decision.
        """
        logger.info(f"Sending scan queue order change response: {message}")
        sqocp = MessageEndpoints.scan_queue_order_change_response()
        msg = messages.RequestResponseMessage(accepted=accepted, message=message)
        self.device_manager.connector.send(sqocp, msg)
