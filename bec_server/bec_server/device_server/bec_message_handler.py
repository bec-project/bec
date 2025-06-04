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
        # Note: It is fine to use the metadata from the device object here,
        # as it is safe to assume that the file event is emitted before a new scan starts.
        metadata = self.devices[device_name].metadata
        scan_id = metadata.get("scan_id")

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
        signal_name = obj.attr_name
        metadata = self.devices[device_name].metadata
        scan_id = metadata.get("scan_id")
        if scan_id is None:
            logger.warning(f"Scan ID is None for device {device_name} and signal {signal_name}.")
            return
        if not isinstance(scan_id, str):
            logger.warning(
                f"Scan ID is not a string for device {device_name} and signal {signal_name}."
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
