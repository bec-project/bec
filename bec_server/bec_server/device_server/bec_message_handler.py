from __future__ import annotations

from collections import OrderedDict
from threading import Lock
from typing import TYPE_CHECKING, Iterable

import numpy as np
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
        self._event_lock = Lock()
        self._async_signal_indices: OrderedDict[tuple[str, str, str], dict[str, int]] = (
            OrderedDict()
        )
        self._pending_files: OrderedDict[tuple[str, str], tuple[str, int | None]] = OrderedDict()

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
        if isinstance(obj, bms.DynamicSignal):
            return self._handle_async_signal(obj, message)

        logger.error(f"Unsupported signal type: {type(obj)}")
        raise TypeError(f"Unsupported signal type: {type(obj)}")

    def _handle_file_event_signal(self, obj: bms.FileEventSignal, message: messages.BECMessage):
        if not isinstance(message, messages.FileMessage):
            raise TypeError(f"Expected FileMessage, got {type(message)}")

        device_name = obj.root.name
        file_key = (device_name, message.file_path)
        with self._event_lock:
            status = self._get_scan_status_message()
            active_scan_id = status.scan_id if status else None
            captured = self._pending_files.get(file_key)
            scan_id = message.metadata.get("scan_id") or (
                captured[0] if message.done and captured else active_scan_id
            )
            owns_pending = captured is not None and captured[0] == scan_id
            entry_index = message.metadata.get("entry_index")
            if entry_index is None:
                if owns_pending:
                    entry_index = captured[1]
                elif status and active_scan_id == scan_id:
                    entry_index = status.info.get("entry_index")
            if message.done:
                if owns_pending:
                    del self._pending_files[file_key]
            elif scan_id and (not captured or owns_pending or scan_id == active_scan_id):
                # A delayed announcement can be published without changing the current owner.
                self._pending_files[file_key] = (scan_id, entry_index)
                if len(self._pending_files) > 4096:
                    self._pending_files.popitem(last=False)
        if scan_id is None:
            logger.warning(
                f"Scan ID is None for device {device_name} and file event signal. Cannot emit file event."
            )
            return

        if entry_index is not None:
            message.metadata.update(scan_id=scan_id, entry_index=entry_index)
        pipe = self.connector.pipeline()
        self.connector.set_and_publish(
            MessageEndpoints.file_event(device_name), message, pipe=pipe, expire=3600
        )
        self.connector.set_and_publish(
            MessageEndpoints.public_file(scan_id=scan_id, name=device_name),
            message,
            pipe=pipe,
            expire=3600,
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
        self.connector.set_and_publish(
            MessageEndpoints.device_progress(device_name), message, expire=3600
        )

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
            expire=3600,
        )

    def _handle_async_signal(self, obj: bms.AsyncSignal, message: messages.BECMessage):
        if not isinstance(message, messages.DeviceMessage):
            raise TypeError(f"Expected DeviceMessage, got {type(message)}")

        device_name = obj.root.name
        signal_name = obj.name

        scan_status_msg = self._get_scan_status_message()
        captured_id = message.metadata.get("scan_id")
        scan_id = captured_id or (scan_status_msg.scan_id if scan_status_msg else None)
        if scan_id is None:
            logger.warning(
                f"Scan ID is None for device {device_name} and async signal {signal_name}."
            )
            return
        status = scan_status_msg.status if scan_status_msg else None
        if not captured_id and status in ["closed", "aborted", "halted", "user_completed", None]:
            logger.warning(
                f"Cannot emit async signal {signal_name} for device {device_name} with status {status}."
            )
            return

        if obj.signal_metadata is None:
            logger.warning(
                f"Signal metadata is None for device {device_name} and signal {signal_name}."
            )
            return

        indices = self._get_async_signal_indices(
            obj=obj, scan_id=scan_id, signal_names=message.signals.keys()
        )

        # we augment the message metadata with the async signal indices so that
        # downstream consumers can know which message corresponds to which async signal update
        message.metadata["async_indices"] = indices

        pipe = self.connector.pipeline()
        self.connector.xadd(
            MessageEndpoints.device_async_signal(
                scan_id=scan_id, device=device_name, signal=signal_name
            ),
            {"data": message},
            max_size=obj.signal_metadata.get("max_size", 1000),
            pipe=pipe,
            expire=obj.signal_metadata.get("expire", 600),  # default to 10 minutes (600 seconds)
        )

        # NOTE: We are storing the shape information. This means that
        # - a single value update will have shape [] (scalar)
        # - an empty array will have shape [0]
        # - a 1D array of length N will have shape [N]
        # - a 2D array of shape (M, N) will have shape [M, N]
        index_message = messages.DeviceAsyncSignalIndexMessage(
            scan_id=scan_id,
            device=device_name,
            signal=signal_name,
            shapes={
                current_signal_name: list(np.shape(signal_data["value"]))
                for current_signal_name, signal_data in message.signals.items()
            },
            indices=indices,
            async_update=messages.DeviceAsyncUpdate.model_validate(
                message.metadata["async_update"]
            ),
        )
        self.connector.rpush(
            MessageEndpoints.device_async_signal_index(
                scan_id=scan_id, device=device_name, signal=signal_name
            ),
            index_message,
            pipe=pipe,
            expire=3600,  # expire after 1 hour; note that this is longer than the async signal message itself
        )
        pipe.execute()

    def _get_async_signal_indices(
        self, obj: bms.DynamicSignal, scan_id: str, signal_names: Iterable[str]
    ) -> dict[str, int]:
        state_key = (scan_id, obj.root.name, obj.name)
        with self._event_lock:
            indices = self._async_signal_indices.setdefault(state_key, {})
            self._async_signal_indices.move_to_end(state_key)
            if len(self._async_signal_indices) > 4096:
                self._async_signal_indices.popitem(last=False)
            out = {}
            for signal_name in signal_names:
                current_index = indices.get(signal_name, -1) + 1
                indices[signal_name] = current_index
                out[signal_name] = current_index
            return out

    def _get_scan_status_message(self) -> messages.ScanStatusMessage | None:
        """
        Get the current scan status message from the device manager.

        Returns:
            messages.ScanStatusMessage | None: The current scan status message, or None if not available.
        """
        if self.device_manager.scan_info is None or self.device_manager.scan_info.msg is None:
            return None
        return self.device_manager.scan_info.msg
