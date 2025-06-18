from __future__ import annotations

from typing import TYPE_CHECKING

from ophyd import OphydObject
from ophyd_devices.utils import bec_signals as bms

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.device_server.devices.devicemanager import DeviceManagerDS


class BECMessageHandler:
    """
    A message handler for the BEC device server that processes and emits messages related to device signals.
    """

    def __init__(self, device_manager: DeviceManagerDS):
        """
        Initialize the BECMessageHandler with a DeviceManagerDS instance.
        This handler is responsible for processing and emitting messages related to device signals.

        Args:
            device_manager (DeviceManagerDS): The device manager instance that manages devices and their signals.
        """
        self.device_manager = device_manager
        self.connector: RedisConnector = device_manager.connector
        self.devices = device_manager.devices

    def emit(self, obj: OphydObject, message: messages.BECMessage):
        """
        Emit a message for the given object.
        This method handles different types of signals and publishes the appropriate messages to Redis.

        Args:
            obj (OphydObject): The object that emitted the signal.
            message (messages.BECMessage): The message to be published.

        Raises:
            TypeError: If the object is not a supported signal type.
        """

        if isinstance(obj, bms.FileEventSignal):
            return self._handle_file_event_signal(obj, message)
        if isinstance(obj, bms.ProgressSignal):
            return self._handle_progress_signal(obj, message)
        if isinstance(obj, bms.PreviewSignal):
            return self._handle_preview_signal(obj, message)
        if isinstance(obj, bms.AsyncSignal):
            return self._handle_async_signal(obj, message)

        raise TypeError(f"Unsupported signal type: {type(obj)}")

    def _handle_file_event_signal(self, obj: bms.FileEventSignal, message: messages.BECMessage):
        if not isinstance(message, messages.FileMessage):
            raise TypeError(f"Expected FileMessage, got {type(message)}")

        device_name = obj.root.name
        scan_id = self._get_current_scan_id()
        if scan_id is None:
            logger.warning(
                f"Scan ID is None for device {device_name} and file event signal. Cannot emit file event."
            )
            return

        pipe = self.connector.pipeline()
        self.connector.set_and_publish(MessageEndpoints.file_event(device_name), message, pipe=pipe)
        self.connector.set_and_publish(
            MessageEndpoints.public_file(scan_id=scan_id, name=device_name), message, pipe=pipe
        )
        pipe.execute()

    def _handle_progress_signal(self, obj: bms.ProgressSignal, message: messages.BECMessage):
        if not isinstance(message, messages.ProgressMessage):
            raise TypeError(f"Expected ProgressMessage, got {type(message)}")

        device_name = obj.root.name
        scan_status_msg = self._get_scan_status_message()
        if scan_status_msg is None:
            logger.warning(
                f"Scan status message is None for device {device_name} and progress signal."
            )
            return

        if message.metadata is None:
            message.metadata = {}
        message.metadata.update(
            {"scan_id": scan_status_msg.scan_id, "status": scan_status_msg.status}
        )
        self.connector.set_and_publish(MessageEndpoints.device_progress(device_name), message)

    def _handle_preview_signal(self, obj: bms.PreviewSignal, message: messages.BECMessage):
        if not isinstance(message, messages.DevicePreviewMessage):
            raise TypeError(f"Expected DevicePreviewMessage, got {type(message)}")

        data = message.data
        # Convert sizes from bytes to MB
        dsize = len(data.tobytes()) / 1e6
        max_size = 1000
        if dsize > max_size:
            logger.warning(
                f"Data size of single message is too large to send, current max_size {max_size}."
            )
            return

        stream_msg = {"data": message}
        self.connector.xadd(
            MessageEndpoints.device_preview(device=obj.root.name, signal=message.signal),
            stream_msg,
            max_size=min(100, int(max_size // dsize)),
        )

    def _handle_async_signal(self, obj: bms.AsyncSignal, message: messages.BECMessage):
        if not isinstance(message, messages.DeviceMessage):
            raise TypeError(f"Expected DeviceMessage, got {type(message)}")

        device_name = obj.root.name
        signal_name = obj.name

        scan_status_msg = self._get_scan_status_message()
        if scan_status_msg is None:
            logger.warning(
                f"Scan status message is None for device {device_name} and async signal {signal_name}."
            )
            return
        scan_id = scan_status_msg.scan_id
        if scan_id is None:
            logger.warning(
                f"Scan ID is None for device {device_name} and async signal {signal_name}."
            )
            return
        status = scan_status_msg.status
        if status in ["closed", "aborted", "halted", None]:
            logger.warning(
                f"Cannot emit async signal {signal_name} for device {device_name} with status {status}."
            )
            return

        if obj.signal_metadata is None:
            logger.warning(
                f"Signal metadata is None for device {device_name} and signal {signal_name}."
            )
            return
        self.connector.xadd(
            MessageEndpoints.device_async_signal(
                scan_id=scan_id, device=device_name, signal=signal_name
            ),
            {"data": message},
            max_size=obj.signal_metadata.get("max_size", 1000),
            expire=obj.signal_metadata.get("expire", 600),  # default to 10 minutes (600 seconds)
        )

    def _get_scan_status_message(self) -> messages.ScanStatusMessage | None:
        """
        Get the current scan status message from the device manager.

        Returns:
            messages.ScanStatusMessage | None: The current scan status message, or None if not available.
        """
        if self.device_manager.scan_info is None or self.device_manager.scan_info.msg is None:
            return None
        return self.device_manager.scan_info.msg

    def _get_current_scan_id(self) -> str | None:
        """
        Get the current scan ID from the device manager's scan info.

        Returns:
            str | None: The current scan ID, or None if not available.
        """
        scan_status_msg = self._get_scan_status_message()
        if scan_status_msg is None or scan_status_msg.scan_id is None:
            return None
        return scan_status_msg.scan_id
