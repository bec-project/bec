from __future__ import annotations

from typing import Any, Callable

from bec_lib.logger import bec_logger

from .plugins import BECLiveDataPlugin, DataAPIPlugin

logger = bec_logger.logger


class DataSubscription:
    """
    A subscription object that manages synchronized data updates for multiple device/signal pairs.

    The subscription automatically handles synchronization across all subscribed device/signal pairs
    and provides methods to dynamically add/remove devices and reload data. The subscription is
    automatically cleaned up when the object is destroyed.

    Example:
        >>> subscription = data_api.subscribe(scan_id="my_scan")
        >>> subscription.add_device("samx", "samx")
        >>> subscription.add_device("detector1", "async_sig1")
        >>> subscription.set_callback(my_callback_function)
        >>> # Later: update the device list
        >>> subscription.remove_device("samx", "samx")
        >>> subscription.add_device("samy", "samy")
        >>> # Reload all data
        >>> subscription.reload()
        >>> # Cleanup happens automatically when object is destroyed or explicitly:
        >>> subscription.close()
    """

    def __init__(self, data_api: DataAPI, scan_id: str, buffered: bool = False):
        """
        Initialize a data subscription.

        Args:
            data_api: The DataAPI instance that manages this subscription.
            scan_id: Identifier for the scan.
            buffered: If True, re-emit the entire accumulated data buffer on each update.
                     If False (default), only emit new synchronized data blocks.
        """
        self._data_api = data_api
        self._scan_id = scan_id
        self._devices: dict[tuple[str, str], str | None] = {}  # (device, entry) -> subscription_id
        self._callback: Callable[[dict, dict], Any] | None = None
        self._user_callback: Callable[[dict, dict], Any] | None = None
        self._is_closed = False
        self._buffered = buffered
        self._data_buffer: dict[str, dict[str, list[dict]]] = (
            {}
        )  # device_name -> device_entry -> list of {value, timestamp}

    @property
    def scan_id(self) -> str:
        """Get the scan ID for this subscription."""
        return self._scan_id

    @property
    def devices(self) -> list[tuple[str, str]]:
        """Get the list of subscribed (device_name, device_entry) pairs."""
        return list(self._devices.keys())

    @property
    def buffered(self) -> bool:
        """Get whether this subscription is in buffered mode."""
        return self._buffered

    def set_buffered(self, buffered: bool) -> DataSubscription:
        """
        Change the buffering mode of the subscription.

        Args:
            buffered: If True, re-emit the entire accumulated data buffer on each update.
                     If False, only emit new synchronized data blocks.

        Returns:
            self for method chaining.
        """
        if self._is_closed:
            raise RuntimeError("Cannot change buffered mode on a closed subscription")

        if buffered == self._buffered:
            return self

        self._buffered = buffered

        # Clear buffer when switching modes
        if not buffered:
            self._data_buffer.clear()

        return self

    def _buffering_callback(self, data: dict, metadata: dict) -> None:
        """
        Internal callback wrapper that handles buffering logic.

        Args:
            data: Data dictionary from the plugin.
            metadata: Metadata dictionary from the plugin.
        """
        if self._user_callback is None:
            return

        if not self._buffered:
            # Pass through directly without buffering
            self._user_callback(data, metadata)
            return

        # Buffered mode: accumulate data and re-emit entire buffer
        for device_name, device_data in data.items():
            if device_name not in self._data_buffer:
                self._data_buffer[device_name] = {}

            for device_entry, signal_data in device_data.items():
                if device_entry not in self._data_buffer[device_name]:
                    self._data_buffer[device_name][device_entry] = []

                self._data_buffer[device_name][device_entry].append(signal_data)

        # Re-emit the entire buffer
        buffered_data = {}
        for device_name, device_entries in self._data_buffer.items():
            buffered_data[device_name] = {}
            for device_entry, signal_list in device_entries.items():
                buffered_data[device_name][device_entry] = signal_list

        self._user_callback(buffered_data, metadata)

    def set_scan_id(self, scan_id: str) -> DataSubscription:
        """
        Update the scan ID and resubscribe all devices to the new scan.

        Args:
            scan_id: New scan identifier.

        Returns:
            self for method chaining.
        """
        if self._is_closed:
            raise RuntimeError("Cannot change scan_id on a closed subscription")

        if scan_id == self._scan_id:
            return self

        old_scan_id = self._scan_id
        self._scan_id = scan_id

        # Clear buffer when changing scans
        self._data_buffer.clear()

        # If we have devices and a callback, resubscribe to new scan
        if self._devices and self._callback is not None:
            logger.info(
                f"Changing scan_id from {old_scan_id} to {scan_id}, resubscribing all devices"
            )
            self._resubscribe_all()

        return self

    def set_callback(self, callback: Callable[[dict, dict], Any]) -> DataSubscription:
        """
        Set or update the callback function for data updates.

        Args:
            callback: Function to call on data update. Receives (data_dict, metadata_dict).
                     In non-buffered mode, receives individual synchronized data blocks.
                     In buffered mode, receives the entire accumulated data buffer.

        Returns:
            self for method chaining.
        """
        if self._is_closed:
            raise RuntimeError("Cannot set callback on a closed subscription")

        old_user_callback = self._user_callback
        self._user_callback = callback

        # The internal callback is always the buffering wrapper
        new_internal_callback = self._buffering_callback
        old_internal_callback = self._callback
        self._callback = new_internal_callback

        # If we already have devices subscribed and callback changed, we need to resubscribe
        if old_internal_callback is not None and old_user_callback != callback and self._devices:
            self._resubscribe_all()

        return self

    def add_device(self, device_name: str, device_entry: str) -> DataSubscription:
        """
        Add a device/signal pair to the synchronized subscription.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry/signal of the device.

        Returns:
            self for method chaining.
        """
        if self._is_closed:
            raise RuntimeError("Cannot add device to a closed subscription")

        key = (device_name, device_entry)
        if key in self._devices:
            logger.debug(f"Device {device_name}/{device_entry} already subscribed")
            return self

        if self._callback is None:
            # Store the device but don't subscribe yet
            self._devices[key] = None
            logger.debug(f"Device {device_name}/{device_entry} queued, waiting for callback")
        else:
            # Subscribe immediately
            sub_id = self._data_api.subscribe(
                device_name, device_entry, self._scan_id, self._callback
            )
            self._devices[key] = sub_id

        return self

    def remove_device(self, device_name: str, device_entry: str) -> DataSubscription:
        """
        Remove a device/signal pair from the subscription.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry/signal of the device.

        Returns:
            self for method chaining.
        """
        if self._is_closed:
            raise RuntimeError("Cannot remove device from a closed subscription")

        key = (device_name, device_entry)
        sub_id = self._devices.pop(key, None)

        if sub_id is not None:
            self._data_api.unsubscribe(subscription_id=sub_id)

        return self

    def reload(self) -> DataSubscription:
        """
        Reload data for all subscribed devices by resubscribing.

        Returns:
            self for method chaining.
        """
        if self._is_closed:
            raise RuntimeError("Cannot reload a closed subscription")

        if self._callback is None:
            logger.warning("Cannot reload without a callback set")
            return self

        self._resubscribe_all()
        return self

    def close(self) -> None:
        """
        Close the subscription and unsubscribe from all devices.

        This is called automatically when the object is destroyed.
        """
        if self._is_closed:
            return

        # Unsubscribe from all devices
        for sub_id in self._devices.values():
            if sub_id is not None:
                self._data_api.unsubscribe(subscription_id=sub_id)

        self._devices.clear()
        self._callback = None
        self._user_callback = None
        self._data_buffer.clear()
        self._is_closed = True

    def _resubscribe_all(self) -> None:
        """Resubscribe to all devices (used when callback changes or reload is requested)."""
        if self._callback is None:
            return

        # Unsubscribe from all
        for sub_id in self._devices.values():
            if sub_id is not None:
                self._data_api.unsubscribe(subscription_id=sub_id)

        # Resubscribe with new callback
        for device_name, device_entry in list(self._devices.keys()):
            sub_id = self._data_api.subscribe(
                device_name, device_entry, self._scan_id, self._callback
            )
            self._devices[(device_name, device_entry)] = sub_id

    def __del__(self):
        """Ensure cleanup when object is garbage collected."""
        self.close()

    def __enter__(self) -> DataSubscription:
        """Support context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support context manager protocol."""
        self.close()
        return False


class DataAPI:
    """
    DataAPI class that manages data retrieval through plugins.

    This is a singleton - only one instance exists globally.
    """

    _instance: DataAPI | None = None

    def __new__(cls, client):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, client):
        # Only initialize once
        if hasattr(self, "_initialized"):
            return

        self.client = client
        self.plugins: list[DataAPIPlugin] = []
        self._initialized = True
        self.register_plugin(BECLiveDataPlugin(self.client))

    @classmethod
    def clear_instance(cls) -> None:
        """Clear the singleton instance. Useful for testing."""
        cls._instance = None

    def register_plugin(self, plugin: DataAPIPlugin) -> None:
        """Register a new plugin."""
        plugin.connect()
        self.plugins.append(plugin)
        self.plugins.sort(key=lambda p: p.get_info().get("priority", 100))

    def create_subscription(self, scan_id: str, buffered: bool = False) -> DataSubscription:
        """
        Create a new subscription object for synchronized data updates.

        This is the recommended way to subscribe to data updates as it provides
        automatic lifecycle management and synchronization across multiple devices.

        Args:
            scan_id: Identifier for the scan.
            buffered: If True, re-emit the entire accumulated data buffer on each update.
                     If False (default), only emit new synchronized data blocks.

        Returns:
            A DataSubscription object that manages the subscription lifecycle.

        Example:
            >>> # Non-buffered mode (default): receive only new data blocks
            >>> sub = data_api.create_subscription("my_scan")
            >>> sub.add_device("samx", "samx").add_device("detector1", "async_sig1")
            >>> sub.set_callback(my_callback)
            >>>
            >>> # Buffered mode: receive entire accumulated buffer on each update
            >>> sub = data_api.create_subscription("my_scan", buffered=True)
            >>> sub.add_device("samx", "samx").set_callback(my_callback)
            >>> # Later:
            >>> sub.close()  # or use context manager: with data_api.create_subscription(...) as sub:
        """
        return DataSubscription(self, scan_id, buffered=buffered)

    def subscribe(
        self,
        device_name: str,
        device_entry: str,
        scan_id: str,
        callback: Callable[[dict, dict], Any],
    ) -> str | None:
        """
        Subscribe to data updates for a specific device and entry.

        Args:
            device_name: Name of the device.
            device_entry: Specific entry of the device.
            scan_id: Identifier for the scan.
            callback: Function to call on data update.

        Returns:
            A string subscription ID.
        """
        for plugin in self.plugins:
            if plugin.can_provide(device_name, device_entry, scan_id):
                return plugin.subscribe(device_name, device_entry, scan_id, callback)
        logger.warning(
            f"No plugin available to provide data for device '{device_name}', entry '{device_entry}', scan_id '{scan_id}'"
        )

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
        for plugin in self.plugins:
            plugin.unsubscribe(subscription_id, scan_id, callback)


if __name__ == "__main__":
    from bec_lib.client import BECClient

    def my_callback(data, metadata):
        print("Received data:", data)
        print("With metadata:", metadata)

    client = BECClient()
    data_api = DataAPI(client)
    sub = data_api.create_subscription("test_scan")
    sub.add_device(device_name="waveform", device_entry="waveform_data")
    sub.set_callback(my_callback)
