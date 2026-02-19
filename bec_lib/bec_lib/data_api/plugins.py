from __future__ import annotations

import uuid
import weakref
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Callable, Literal, Tuple

import louie
from pydantic import BaseModel, ConfigDict

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import DeviceAsyncUpdate

CallbackRef = louie.saferef.BoundMethodWeakref | weakref.ReferenceType[Callable[[dict, dict], Any]]


class DataAPIPlugin(ABC):
    """Base class for DataAPI plugins."""

    def connect(self) -> None:
        """
        Connection setup for the plugin.
        """

    def disconnect(self) -> None:
        """
        Disconnect and clean up resources for the plugin.
        """

    @abstractmethod
    def has_scan_data(self, scan_id: str) -> bool:
        """
        Check if the plugin has data for the given scan ID.

        Args:
            scan_id: Identifier for the scan.
        Returns:
            True if the plugin has data for the scan ID, False otherwise.
        """

    @abstractmethod
    def can_provide(self, device_name: str, device_entry: str, scan_id: str) -> bool:
        """
        Check if the plugin can provide data for the given device and entry.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.

        Returns:
            True if the plugin can provide the data, False otherwise.
        """

    def get_info(self) -> dict:
        """Return plugin metadata such as name and priority."""
        return {}

    @abstractmethod
    def subscribe(
        self,
        device_name: str,
        device_entry: str,
        scan_id: str,
        callback: Callable[[dict, dict], Any],
    ) -> str:
        """
        Subscribe to data updates.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.
            callback: Function to call on data update. The function should accept two dicts:
                      one for the data and one for the metadata.

        Returns:
            A unique subscription ID.
        """

    @abstractmethod
    def unsubscribe(
        self,
        subscription_id: str | None = None,
        scan_id: str | None = None,
        callback: Callable[[dict, dict], Any] | None = None,
    ) -> None:
        """
        Unsubscribe from data updates by either subscription ID, scan ID and callback, or both.

        Args:
            subscription_id: The ID of the subscription to cancel.
            scan_id: Identifier for the scan.
            callback: Function that was used for subscription.
        """


class _MonitoredSubscription(BaseModel):
    scan_id: str
    callback_ref: CallbackRef
    devices: list[Tuple[str, str]]  # List of (device_name, device_entry) tuples

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _AsyncSubscription(BaseModel):
    """Tracks a single async signal subscription shared by multiple callbacks."""

    scan_id: str
    device_name: str
    device_entry: str
    callback_refs: list[CallbackRef]
    connector_id: Any  # ID returned by client.connector.register

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _DataBuffer(BaseModel):
    """Buffer for storing data updates until all sources are synchronized."""

    device_name: str
    device_entry: str
    data: list[dict]  # List of data points with value and timestamp
    source_type: Literal["monitored", "async_signal"]

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _CallbackBuffer(BaseModel):
    """Tracks buffered data for a specific callback across all its subscribed devices."""

    callback_ref: CallbackRef
    scan_id: str
    buffers: dict[tuple[str, str], _DataBuffer]  # (device_name, device_entry) -> buffer
    min_length: int = 0  # Minimum data length across all buffers for this callback
    monitored_indices: dict[tuple[str, str], int] = (
        {}
    )  # Track last processed index for monitored devices

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _SubscriptionInfo(BaseModel):
    """Information about a single subscription."""

    subscription_id: str
    scan_id: str
    device_name: str
    device_entry: str
    callback_ref: CallbackRef
    subscription_type: Literal["monitored", "async_signal"]

    model_config = ConfigDict(arbitrary_types_allowed=True)


class BECLiveDataPlugin(DataAPIPlugin):
    """
    Plugin to access live BEC data.
    Provides real-time data from the BEC client if available. It fetches live data from
    the storage of the BEC client as well as from async updates.
    """

    def __init__(self, client: BECClient):
        self.client = client
        # Subscription tracking: sub_id -> subscription info
        self._subscriptions: dict[str, _SubscriptionInfo] = {}
        # Scan-level grouping for monitored devices: scan_id -> {callback_ref -> devices}
        self._monitored_subscriptions: dict[str, dict[CallbackRef, _MonitoredSubscription]] = {}
        # Async signal grouping: (scan_id, device_name, device_entry) -> _AsyncSubscription
        self._async_subscriptions: dict[tuple[str, str, str], _AsyncSubscription] = {}
        # Data buffers for synchronization: callback_ref -> _CallbackBuffer
        self._callback_buffers: dict[CallbackRef, _CallbackBuffer] = {}
        self._connect_id = None

    def connect(self):
        """Connect to client signals for live data updates."""
        self._connect_id = self.client.callbacks.register(
            "scan_segment", self._handle_scan_segment_update
        )

    def disconnect(self):
        """Disconnect from client signals."""
        if self._connect_id is not None:
            self.client.callbacks.remove(self._connect_id)
            self._connect_id = None

        # Unregister all async signal subscriptions from redis connector
        for async_sub in self._async_subscriptions.values():
            self.client.connector.unregister(async_sub.connector_id)
        self._async_subscriptions.clear()

    def has_scan_data(self, scan_id: str) -> bool:
        """
        Check if live data is available for the given scan ID.

        Args:
            scan_id: Identifier for the scan.
        Returns:
            True if live data is available, False otherwise.
        """
        if not self.client.started:
            return False
        if self.client.queue is None:
            return False

        scan_item = self.client.queue.scan_storage.find_scan_by_ID(scan_id)
        if scan_item is None:
            return False

        if scan_item.status in ["closed", "aborted", "halted"]:
            # We skip closed scans and instead rely on historical data plugin
            return False
        return True

    def can_provide(self, device_name: str, device_entry: str, scan_id: str) -> bool:
        """
        Check if live data is available for the given device and entry.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.

        Returns:
            True if live data is available, False otherwise.
        """
        mode = self._get_device_mode(device_name, device_entry, scan_id)
        return mode is not None

    @lru_cache(maxsize=128)
    def _get_device_mode(
        self, device_name: str, device_entry: str, scan_id: str
    ) -> Literal["monitored", "async_signal", None]:
        """
        Get the mode of the device entry for the given scan ID.
        As the mode does not change during a scan, we cache the results for performance.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.

        Returns:
            "monitored" if live data is available as monitored device,
            "async_signal" if live data is available as async signal,
            None otherwise.
        """
        # Pre-checks; mostly for type checks
        if not self.client.started or self.client.queue is None:
            return None

        scan_item = self.client.queue.scan_storage.find_scan_by_ID(scan_id)
        if scan_item is None:
            return None

        if self._device_entry_is_monitored(device_name, device_entry, scan_item):
            return "monitored"

        if self._device_entry_is_async_signal(device_name, device_entry):
            return "async_signal"
        return None

    def _device_entry_is_monitored(self, device_name: str, device_entry: str, scan_item) -> bool:
        """
        Check if the device entry is a monitored devices in the scan item.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_item: The scan item to check against.

        Returns:
            True if the device entry is monitored, False otherwise.
        """
        if scan_item.status_message is None:
            return False

        readout_priority = scan_item.status_message.readout_priority or {}
        if device_name in readout_priority.get("monitored", []):
            return True

        # FIXME: we should also check that the device_entry is actually part of the monitored device
        return False

    def _device_entry_is_async_signal(self, device_name: str, device_entry: str) -> bool:
        """
        Check if the device entry is an async signal.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
        Returns:
            True if the device entry is an async signal, False otherwise.
        """
        async_signal_info = self._get_async_signal_info(device_name, device_entry)
        return async_signal_info is not None

    def _get_async_signal_info(self, device_name: str, device_entry: str) -> dict | None:
        """
        Get the async signal information for the given device and entry.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
        Returns:
            The async signal information dict if found, None otherwise.
        """
        if not self.client.device_manager:
            return None
        async_signals = self.client.device_manager.get_bec_signals(
            ["AsyncSignal", "AsyncMultiSignal", "DynamicSignal"]
        )
        for dev_name, _, entry_info in async_signals:
            if entry_info.get("obj_name") == device_entry and dev_name == device_name:
                return entry_info
        return None

    def subscribe(
        self,
        device_name: str,
        device_entry: str,
        scan_id: str,
        callback: Callable[[dict, dict], Any],
    ) -> str:
        """
        Subscribe to live data updates for the given device and entry.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.
            callback: Function to call on data update. The function should accept two dicts:
                      one for the data and one for the metadata.
        Returns:
            A unique subscription ID.
        """

        match self._get_device_mode(device_name, device_entry, scan_id):
            case "monitored":
                return self._subscribe_to_monitored_device(
                    device_name, device_entry, scan_id, callback
                )

            case "async_signal":
                return self._subscribe_to_async_signal(device_name, device_entry, scan_id, callback)
            case None:
                raise ValueError(
                    f"Cannot subscribe to device '{device_name}' entry '{device_entry}' for scan '{scan_id}'."
                )
            case _:
                raise ValueError(
                    f"Cannot subscribe to device '{device_name}' entry '{device_entry}' for scan '{scan_id}': unknown mode."
                )

    def unsubscribe(
        self,
        subscription_id: str | None = None,
        scan_id: str | None = None,
        callback: Callable[[dict, dict], Any] | None = None,
    ) -> None:
        """
        Unsubscribe from live data updates by either subscription ID, scan ID and callback, or both.

        Args:
            subscription_id: The ID of the subscription to cancel.
            scan_id: Identifier for the scan.
            callback: Function that was used for subscription.
        """

        if subscription_id is not None:
            self._unsubscribe_by_id(subscription_id)
            return
        if scan_id is not None and callback is not None:
            # find all subscriptions matching scan_id and callback
            callback_ref = louie.saferef.safe_ref(callback)
            to_remove = []
            for sub_id, sub_info in self._subscriptions.items():
                if sub_info.scan_id == scan_id and sub_info.callback_ref == callback_ref:
                    to_remove.append(sub_id)
            for sub_id in to_remove:
                self._unsubscribe_by_id(sub_id)
            return
        if scan_id is not None:
            # find all subscriptions matching scan_id
            to_remove = []
            for sub_id, sub_info in self._subscriptions.items():
                if sub_info.scan_id == scan_id:
                    to_remove.append(sub_id)
            for sub_id in to_remove:
                self._unsubscribe_by_id(sub_id)
            return
        if callback is not None:
            # find all subscriptions matching callback
            callback_ref = louie.saferef.safe_ref(callback)
            to_remove = []
            for sub_id, sub_info in self._subscriptions.items():
                if sub_info.callback_ref == callback_ref:
                    to_remove.append(sub_id)
            for sub_id in to_remove:
                self._unsubscribe_by_id(sub_id)
            return

    def _unsubscribe_by_id(self, subscription_id: str) -> None:
        """
        Unsubscribe from live data updates by subscription ID.
        Args:
            subscription_id: The ID of the subscription to cancel.
        """

        # Look up subscription info
        if subscription_id not in self._subscriptions:
            return

        sub_info = self._subscriptions[subscription_id]

        # Handle based on subscription type
        if sub_info.subscription_type == "monitored":
            self._unsubscribe_monitored(sub_info)
        elif sub_info.subscription_type == "async_signal":
            self._unsubscribe_async_signal(sub_info)

        # Remove from main subscription tracking
        del self._subscriptions[subscription_id]

    def _unsubscribe_monitored(self, sub_info: _SubscriptionInfo) -> None:
        """Unsubscribe from monitored device updates."""
        scan_id = sub_info.scan_id
        device_name = sub_info.device_name
        device_entry = sub_info.device_entry
        callback_ref = sub_info.callback_ref

        if scan_id not in self._monitored_subscriptions:
            return

        subscriptions = self._monitored_subscriptions[scan_id]

        # Find the subscription for this callback
        if callback_ref in subscriptions:
            sub = subscriptions[callback_ref]

            # Remove the device from the callback's device list
            if (device_name, device_entry) in sub.devices:
                sub.devices.remove((device_name, device_entry))

            # Remove from buffer if exists
            if callback_ref in self._callback_buffers:
                buffer_key = (device_name, device_entry)
                if buffer_key in self._callback_buffers[callback_ref].buffers:
                    del self._callback_buffers[callback_ref].buffers[buffer_key]

                # Clean up callback buffer if empty
                if not self._callback_buffers[callback_ref].buffers:
                    del self._callback_buffers[callback_ref]

            # If callback has no more devices, remove it entirely
            if not sub.devices:
                del subscriptions[callback_ref]

        # Clean up scan if no more subscriptions
        if not subscriptions:
            del self._monitored_subscriptions[scan_id]

    def _unsubscribe_async_signal(self, sub_info: _SubscriptionInfo) -> None:
        """Unsubscribe from async signal updates."""
        scan_id = sub_info.scan_id
        device_name = sub_info.device_name
        device_entry = sub_info.device_entry
        callback_ref = sub_info.callback_ref

        key = (scan_id, device_name, device_entry)

        if key not in self._async_subscriptions:
            return

        async_sub = self._async_subscriptions[key]

        # Remove the callback from the list
        if callback_ref in async_sub.callback_refs:
            async_sub.callback_refs.remove(callback_ref)

        # Remove from buffer if exists
        if callback_ref in self._callback_buffers:
            buffer_key = (device_name, device_entry)
            if buffer_key in self._callback_buffers[callback_ref].buffers:
                del self._callback_buffers[callback_ref].buffers[buffer_key]

            # Clean up callback buffer if empty
            if not self._callback_buffers[callback_ref].buffers:
                del self._callback_buffers[callback_ref]

        # If no more callbacks, unregister from redis connector and clean up
        if not async_sub.callback_refs:
            self.client.connector.unregister(async_sub.connector_id)
            del self._async_subscriptions[key]

    def _subscribe_to_monitored_device(
        self,
        device_name: str,
        device_entry: str,
        scan_id: str,
        callback: Callable[[dict, dict], Any],
    ) -> str:
        """
        Subscribe to monitored device data updates.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.
            callback: Function to call on data update.

        Returns:
            A unique subscription ID.
        """
        # Generate unique subscription ID
        sub_id = str(uuid.uuid4())

        callback_ref = louie.saferef.safe_ref(callback)

        # Store subscription info
        sub_info = _SubscriptionInfo(
            subscription_id=sub_id,
            scan_id=scan_id,
            device_name=device_name,
            device_entry=device_entry,
            callback_ref=callback_ref,
            subscription_type="monitored",
        )
        self._subscriptions[sub_id] = sub_info

        # Update monitored subscriptions grouping
        available_subscriptions = self._monitored_subscriptions.get(scan_id)

        if available_subscriptions is None:
            sub = _MonitoredSubscription(
                scan_id=scan_id, callback_ref=callback_ref, devices=[(device_name, device_entry)]
            )
            self._monitored_subscriptions[scan_id] = {callback_ref: sub}
            return sub_id

        for callback_ref_existing, sub in available_subscriptions.items():
            if callback_ref_existing == callback_ref:
                # Found existing subscription for this callback
                if (device_name, device_entry) not in sub.devices:
                    sub.devices.append((device_name, device_entry))
                return sub_id

        # New callback for this scan
        sub = _MonitoredSubscription(
            scan_id=scan_id, callback_ref=callback_ref, devices=[(device_name, device_entry)]
        )
        self._monitored_subscriptions[scan_id][callback_ref] = sub
        return sub_id

    def _handle_scan_segment_update(self, _scan_segment: dict, metadata: dict) -> None:
        """
        Handle scan segment updates from the client. We do not use the scan_segment directly,
        but use it as a trigger to fetch data for all subscribed monitored devices from the live update storage.

        Args:
            scan_segment: The scan segment data (content from ScanMessage).
            metadata: Metadata associated with the scan segment.
        """
        scan_id = _scan_segment.get("scan_id")
        if scan_id is None:
            return

        if scan_id not in self._monitored_subscriptions:
            return

        if self.client.queue is None:
            return

        scan_item = self.client.queue.scan_storage.find_scan_by_ID(scan_id)
        if scan_item is None:
            return

        subscriptions = self._monitored_subscriptions[scan_id]

        for callback_ref, sub in subscriptions.items():
            callback = callback_ref()
            if callback is None:
                continue

            # Get or initialize callback buffer
            if callback_ref not in self._callback_buffers:
                self._callback_buffers[callback_ref] = _CallbackBuffer(
                    callback_ref=callback_ref, scan_id=scan_id, buffers={}
                )

            callback_buffer = self._callback_buffers[callback_ref]

            # Prepare data for this subscription
            for device_name, device_entry in sub.devices:
                # live_data returns lists of all values and timestamps
                values = (
                    scan_item.live_data.get(device_name, {}).get(device_entry, {}).get("val", None)
                )
                timestamps = (
                    scan_item.live_data.get(device_name, {})
                    .get(device_entry, {})
                    .get("timestamp", None)
                )
                if values is None and timestamps is None:
                    continue

                if not isinstance(values, list):
                    values = [values]
                if not isinstance(timestamps, list):
                    timestamps = [timestamps]

                # Track which index we've already processed for this device
                key = (device_name, device_entry)
                last_processed_index = callback_buffer.monitored_indices.get(key, 0)

                # Only add new data points we haven't processed yet
                for idx in range(last_processed_index, len(values)):
                    if idx < len(timestamps):
                        self._add_to_buffer(
                            callback_ref,
                            scan_id,
                            device_name,
                            device_entry,
                            values[idx],
                            timestamps[idx],
                            "monitored",
                        )

                # Update the last processed index
                callback_buffer.monitored_indices[key] = len(values)

            # Check if we can emit synchronized data
            self._check_and_emit_synchronized_data(callback_ref, scan_id)

    def _subscribe_to_async_signal(
        self,
        device_name: str,
        device_entry: str,
        scan_id: str,
        callback: Callable[[dict, dict], Any],
    ) -> str:
        """
        Subscribe to async signal data updates.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.
            callback: Function to call on data update.

        Returns:
            A unique subscription ID.
        """
        # Generate unique subscription ID
        sub_id = str(uuid.uuid4())

        callback_ref = louie.saferef.safe_ref(callback)

        # Store subscription info
        sub_info = _SubscriptionInfo(
            subscription_id=sub_id,
            scan_id=scan_id,
            device_name=device_name,
            device_entry=device_entry,
            callback_ref=callback_ref,
            subscription_type="async_signal",
        )
        self._subscriptions[sub_id] = sub_info

        # Check if we already have a subscription for this device/entry/scan combination
        key = (scan_id, device_name, device_entry)

        if key in self._async_subscriptions:
            # Reuse existing subscription, just add the callback
            async_sub = self._async_subscriptions[key]
            if callback_ref not in async_sub.callback_refs:
                async_sub.callback_refs.append(callback_ref)
        else:
            # Create new redis connector subscription
            async_signal_info = self._get_async_signal_info(device_name, device_entry)
            if async_signal_info is None:
                raise ValueError(
                    f"Cannot subscribe to async signal '{device_name}' entry '{device_entry}': signal not found."
                )
            connector_id = self.client.connector.register(
                MessageEndpoints.device_async_signal(
                    scan_id=scan_id,
                    device=device_name,
                    signal=async_signal_info.get("storage_name"),
                ),
                cb=self._async_signal_sync_callback,
                from_start=True,
                parent=self,
                scan_id=scan_id,
                device_name=device_name,
                device_entry=device_entry,
            )

            # Create subscription tracking entry
            async_sub = _AsyncSubscription(
                scan_id=scan_id,
                device_name=device_name,
                device_entry=device_entry,
                callback_refs=[callback_ref],
                connector_id=connector_id,
            )
            self._async_subscriptions[key] = async_sub

        return sub_id

    @staticmethod
    def _async_signal_sync_callback(
        msg: dict, parent: BECLiveDataPlugin, scan_id: str, device_name: str, device_entry: str
    ):
        """Callback for async signal updates from the client. Broadcasts to all subscribers."""

        msg_obj = msg.get("data")
        if not isinstance(msg_obj, messages.DeviceMessage):
            return

        signals = msg_obj.signals
        timestamp = msg_obj.metadata.get("timestamp")

        # Get all callbacks for this device/entry/scan combination
        key = (scan_id, device_name, device_entry)
        if key not in parent._async_subscriptions:
            return

        async_sub = parent._async_subscriptions[key]

        # Add data to buffer for each subscriber
        for callback_ref in async_sub.callback_refs:
            callback = callback_ref()
            if callback is None:
                continue

            # Add to buffer
            parent._add_to_buffer(
                callback_ref, scan_id, device_name, device_entry, signals, timestamp, "async_signal"
            )

            # Check if we can emit synchronized data
            parent._check_and_emit_synchronized_data(callback_ref, scan_id)

    def _add_to_buffer(
        self,
        callback_ref: CallbackRef,
        scan_id: str,
        device_name: str,
        device_entry: str,
        value: Any,
        timestamp: Any,
        source_type: Literal["monitored", "async_signal"],
    ) -> None:
        """
        Add data to the buffer for a specific callback and device.

        Args:
            callback_ref: Weak reference to the callback
            scan_id: Scan identifier
            device_name: Name of the device
            device_entry: Device entry
            value: Data value
            timestamp: Data timestamp
            source_type: Type of data source (monitored or async_signal)
        """
        # Initialize callback buffer if not exists
        if callback_ref not in self._callback_buffers:
            self._callback_buffers[callback_ref] = _CallbackBuffer(
                callback_ref=callback_ref, scan_id=scan_id, buffers={}
            )

        callback_buffer = self._callback_buffers[callback_ref]
        key = (device_name, device_entry)

        # Initialize device buffer if not exists
        if key not in callback_buffer.buffers:
            callback_buffer.buffers[key] = _DataBuffer(
                device_name=device_name, device_entry=device_entry, data=[], source_type=source_type
            )

        # Add data point to buffer
        data_point = {"value": value, "timestamp": timestamp}
        callback_buffer.buffers[key].data.append(data_point)

    def _get_expected_device_count(self, callback_ref: CallbackRef, scan_id: str) -> int:
        """
        Get the total number of devices (monitored + async) that this callback is subscribed to.

        Args:
            callback_ref: Weak reference to the callback
            scan_id: Scan identifier

        Returns:
            Total count of subscribed devices for this callback
        """
        count = 0

        # Count monitored devices
        if scan_id in self._monitored_subscriptions:
            if callback_ref in self._monitored_subscriptions[scan_id]:
                count += len(self._monitored_subscriptions[scan_id][callback_ref].devices)

        # Count async signals
        for (sub_scan_id, _, _), async_sub in self._async_subscriptions.items():
            if sub_scan_id == scan_id and callback_ref in async_sub.callback_refs:
                count += 1

        return count

    def _check_and_emit_synchronized_data(self, callback_ref: CallbackRef, scan_id: str) -> None:
        """
        Check if all buffers for a callback have data of equal length and emit synchronized data.

        Args:
            callback_ref: Weak reference to the callback
            scan_id: Scan identifier
        """
        if callback_ref not in self._callback_buffers:
            return

        callback_buffer = self._callback_buffers[callback_ref]

        if not callback_buffer.buffers:
            return

        # Determine how many devices this callback is subscribed to
        expected_device_count = self._get_expected_device_count(callback_ref, scan_id)

        # Wait until all subscribed devices have buffers
        if len(callback_buffer.buffers) < expected_device_count:
            return

        # Find minimum length across all buffers
        min_length = min(len(buffer.data) for buffer in callback_buffer.buffers.values())

        # If no data is available in all buffers yet, return
        if min_length == 0:
            return

        # Only emit new data (data beyond min_length we've already emitted)
        if min_length <= callback_buffer.min_length:
            return

        callback = callback_ref()
        if callback is None:
            return

        # Emit data from min_length onward up to the new min_length
        for idx in range(callback_buffer.min_length, min_length):
            data = {}
            for (device_name, device_entry), buffer in callback_buffer.buffers.items():
                data_point = buffer.data[idx]
                if device_name not in data:
                    data[device_name] = {}
                data[device_name][device_entry] = {
                    "value": data_point["value"],
                    "timestamp": data_point["timestamp"],
                }

            # Call the callback with synchronized data
            callback(
                data,
                {
                    "scan_id": scan_id,
                    "async_update": DeviceAsyncUpdate(type="replace").model_dump(),
                },
            )

        # Update the min_length to track what we've already emitted
        callback_buffer.min_length = min_length
