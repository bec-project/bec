from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, TypedDict

import numpy as np

from bec_lib import messages
from bec_lib.device_backend import DeviceEvent, EventKind, MessageChannel
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.device_server.devices.devicemanager import DeviceManagerDS


class _AsyncSignalIndexState(TypedDict):
    scan_id: str
    indices: dict[str, int]


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
        self._async_signal_index_state: dict[tuple[str, str], _AsyncSignalIndexState] = {}

    def emit(self, obj: object, message: messages.BECMessage):
        """Compatibility callback; descriptions identify BEC channels without HAL types."""
        handler = self.device_manager.handler_for(obj)
        info = handler.runtime_info().get("")
        if info is None:
            raise TypeError(f"Unsupported signal type: {type(obj)}")
        channel = {
            "file_event": MessageChannel.FILE,
            "progress": MessageChannel.PROGRESS,
            "preview": MessageChannel.PREVIEW,
        }.get(info.role, MessageChannel.DYNAMIC)
        self.emit_event(
            DeviceEvent(
                handler.identity,
                EventKind.MESSAGE,
                message=message,
                channel=channel,
                signal_metadata=info.signal_metadata,
            )
        )

    def emit_event(self, event: DeviceEvent) -> None:
        """Publish a typed provider push event using existing BEC endpoints."""
        dispatch = {
            MessageChannel.FILE: self._handle_file_event_signal,
            MessageChannel.PROGRESS: self._handle_progress_signal,
            MessageChannel.PREVIEW: self._handle_preview_signal,
            MessageChannel.DYNAMIC: self._handle_async_signal,
        }
        if event.channel not in dispatch or event.message is None:
            raise TypeError(f"Unsupported BEC push channel: {event.channel}")
        dispatch[event.channel](event, event.message)

    def _handle_file_event_signal(self, obj: DeviceEvent, message: messages.BECMessage):
        if not isinstance(message, messages.FileMessage):
            raise TypeError(f"Expected FileMessage, got {type(message)}")

        device_name = obj.source.root
        scan_id = self._get_current_scan_id()
        if scan_id is None:
            logger.warning(
                f"Scan ID is None for device {device_name} and file event signal. Cannot emit file event."
            )
            return

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

    def _handle_progress_signal(self, obj: DeviceEvent, message: messages.BECMessage):
        if not isinstance(message, messages.ProgressMessage):
            raise TypeError(f"Expected ProgressMessage, got {type(message)}")

        device_name = obj.source.root
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

    def _handle_preview_signal(self, obj: DeviceEvent, message: messages.BECMessage):
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
            MessageEndpoints.device_preview(device=obj.source.root, signal=message.signal),
            stream_msg,
            max_size=min(100, int(max_size // dsize)),
            expire=3600,
        )

    def _handle_async_signal(self, obj: DeviceEvent, message: messages.BECMessage):
        if not isinstance(message, messages.DeviceMessage):
            raise TypeError(f"Expected DeviceMessage, got {type(message)}")

        device_name = obj.source.root
        signal_name = obj.source.name

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
        if status in ["closed", "aborted", "halted", "user_completed", None]:
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
        self, obj: DeviceEvent, scan_id: str, signal_names: Iterable[str]
    ) -> dict[str, int]:
        state_key = (obj.source.root, obj.source.name)
        state = self._async_signal_index_state.get(state_key)
        if state is None or state["scan_id"] != scan_id:
            state = _AsyncSignalIndexState(scan_id=scan_id, indices={})
            self._async_signal_index_state[state_key] = state

        out = {}
        indices = state["indices"]
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
