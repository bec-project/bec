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
        >>> subscription = data_api.create_subscription("my_scan")
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

    def __init__(self, data_api: DataAPI, scan_id: str | None = None, buffered: bool = False):
        """
        Initialize a data subscription.

        Args:
            data_api (DataAPI): DataAPI instance that manages this
                subscription.
            scan_id (str | None): Identifier for the scan. When ``None``, the
                subscription follows the current or next active scan
                automatically.
            buffered (bool): If ``True``, re-emit the entire accumulated data
                buffer on each update. If ``False``, emit only newly aligned
                synchronized data blocks.
        """
        self._data_api = data_api
        self._scan_id = scan_id
        self._follow_scan = scan_id is None
        self._devices: dict[tuple[str, str], str | None] = {}  # (device, entry) -> subscription_id
        self._callback: Callable[[dict, dict], Any] | None = None
        self._user_callback: Callable[[dict, dict], Any] | None = None
        self._is_closed = False
        self._buffered = buffered
        self._bundle_domain: tuple | None = None
        self._scan_status_callback_id: int | str | None = None
        self._data_buffer: dict[str, dict[str, list[dict]]] = (
            {}
        )  # device_name -> device_entry -> list of {value, timestamp}
        if self._follow_scan:
            self._scan_status_callback_id = self._data_api.client.callbacks.register(
                "scan_status", self._handle_scan_status_update
            )

    @property
    def scan_id(self) -> str | None:
        """
        Get the scan ID for this subscription.

        Returns:
            str | None: Current scan identifier, or ``None`` while the
                subscription is still waiting to bind to a scan.
        """
        return self._scan_id

    @property
    def devices(self) -> list[tuple[str, str]]:
        """
        Get the list of subscribed device-entry pairs.

        Returns:
            list[tuple[str, str]]: Subscribed ``(device_name, device_entry)``
                pairs.
        """
        return list(self._devices.keys())

    @property
    def buffered(self) -> bool:
        """
        Get whether this subscription is in buffered mode.

        Returns:
            bool: ``True`` if buffered mode is enabled, otherwise ``False``.
        """
        return self._buffered

    def set_buffered(self, buffered: bool) -> DataSubscription:
        """
        Change the buffering mode of the subscription.

        Args:
            buffered (bool): If ``True``, re-emit the entire accumulated data
                buffer on each update. If ``False``, emit only newly aligned
                synchronized data blocks.

        Returns:
            DataSubscription: This subscription instance for method chaining.

        Raises:
            RuntimeError: If the subscription is already closed.
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
            data (dict): Data dictionary emitted by the plugin.
            metadata (dict): Metadata dictionary emitted by the plugin.
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
            scan_id (str): New scan identifier.

        Returns:
            DataSubscription: This subscription instance for method chaining.

        Raises:
            RuntimeError: If the subscription is already closed.
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
            callback (Callable[[dict, dict], Any]): Function to call on data
                update. In non-buffered mode it receives individual synchronized
                data blocks. In buffered mode it receives the entire
                accumulated data buffer.

        Returns:
            DataSubscription: This subscription instance for method chaining.

        Raises:
            RuntimeError: If the subscription is already closed.
        """
        if self._is_closed:
            raise RuntimeError("Cannot set callback on a closed subscription")

        old_user_callback = self._user_callback
        self._user_callback = callback

        # The internal callback is always the buffering wrapper
        new_internal_callback = self._buffering_callback
        old_internal_callback = self._callback
        self._callback = new_internal_callback

        self._bind_to_current_scan_if_available()

        # If devices are already tracked, the first callback assignment should
        # activate queued subscriptions and later callback changes should
        # rewire them to the new callable.
        if self._devices and (old_internal_callback is None or old_user_callback != callback):
            self._resubscribe_all()

        return self

    def add_device(self, device_name: str, device_entry: str) -> DataSubscription:
        """
        Add a device/signal pair to the synchronized subscription.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry or signal of the device.

        Returns:
            DataSubscription: This subscription instance for method chaining.

        Raises:
            RuntimeError: If the subscription is already closed.
            ValueError: If the source is not bundle-compatible with the
                subscription.
        """
        if self._is_closed:
            raise RuntimeError("Cannot add device to a closed subscription")

        key = (device_name, device_entry)
        if key in self._devices:
            logger.debug(f"Device {device_name}/{device_entry} already subscribed")
            return self

        self._validate_bundle_compatibility(device_name, device_entry)

        if self._callback is None or self._scan_id is None:
            # Store the device but don't subscribe yet
            self._devices[key] = None
            logger.debug(
                "Device %s/%s queued, waiting for %s",
                device_name,
                device_entry,
                "scan binding and callback" if self._scan_id is None else "callback",
            )
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
            device_name (str): Name of the device.
            device_entry (str): Specific entry or signal of the device.

        Returns:
            DataSubscription: This subscription instance for method chaining.

        Raises:
            RuntimeError: If the subscription is already closed.
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
            DataSubscription: This subscription instance for method chaining.

        Raises:
            RuntimeError: If the subscription is already closed.
        """
        if self._is_closed:
            raise RuntimeError("Cannot reload a closed subscription")

        if self._callback is None:
            logger.warning("Cannot reload without a callback set")
            return self

        self._bind_to_current_scan_if_available()
        if self._scan_id is None:
            logger.warning("Cannot reload subscription before a scan is bound")
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
        if self._scan_status_callback_id is not None:
            self._data_api.client.callbacks.remove(self._scan_status_callback_id)
            self._scan_status_callback_id = None
        self._is_closed = True

    def _resubscribe_all(self) -> None:
        """
        Resubscribe every tracked source with the current callback and scan ID.

        This is used after callback changes and explicit reload requests.
        """
        if self._callback is None:
            return

        # Unsubscribe from all
        for sub_id in self._devices.values():
            if sub_id is not None:
                self._data_api.unsubscribe(subscription_id=sub_id)

        if self._scan_id is None:
            for key in self._devices:
                self._devices[key] = None
            return

        # Resubscribe with new callback
        for device_name, device_entry in list(self._devices.keys()):
            self._validate_bundle_compatibility(device_name, device_entry)
            sub_id = self._data_api.subscribe(
                device_name, device_entry, self._scan_id, self._callback
            )
            self._devices[(device_name, device_entry)] = sub_id

    def _validate_bundle_compatibility(self, device_name: str, device_entry: str) -> None:
        """
        Validate that a source is compatible with the subscription bundle.

        Args:
            device_name (str): Name of the device to validate.
            device_entry (str): Specific entry of the device to validate.

        Raises:
            ValueError: If the source resolves to a different bundle domain than
                the existing subscription.
        """
        source_domain = self._data_api.get_bundle_domain(device_name, device_entry, self._scan_id)
        source_signature = self._normalize_bundle_domain(source_domain)
        if self._bundle_domain is None:
            if source_signature is not None:
                self._bundle_domain = source_signature
            return

        if source_domain is None and self._data_api.allows_runtime_bundle_resolution(
            device_name, device_entry, self._scan_id
        ):
            return

        if source_signature != self._bundle_domain:
            raise ValueError(
                f"Cannot add device '{device_name}' entry '{device_entry}' to subscription bundle "
                f"{self._bundle_domain}; resolved bundle is {source_signature}."
            )

    def _normalize_bundle_domain(self, domain: tuple | None) -> tuple | None:
        """
        Normalize a bundle domain for compatibility checks across scans.

        Args:
            domain (tuple | None): Concrete bundle domain returned by a data
                plugin.

        Returns:
            tuple | None: Compatibility signature without scan-specific
                identifiers, or ``None`` when the domain is not yet known.
        """
        if domain is None:
            return None
        if len(domain) >= 2 and domain[0] == "monitored":
            return ("monitored",)
        if len(domain) >= 3 and domain[0] == "async_signal":
            return ("async_signal", domain[2])
        if len(domain) >= 4 and domain[0] == "standalone_async":
            return ("standalone_async", domain[2], domain[3])
        return domain

    def _bind_to_current_scan_if_available(self) -> None:
        """
        Bind an unbound follow-scan subscription to the current active scan.

        Returns:
            None: This method updates internal subscription state in place.
        """
        if not self._follow_scan or self._scan_id is not None:
            return

        queue = getattr(self._data_api.client, "queue", None)
        scan_storage = getattr(queue, "scan_storage", None)
        current_scan_ids = getattr(scan_storage, "current_scan_id", None)
        if not isinstance(current_scan_ids, (list, tuple)) or not current_scan_ids:
            return

        current_scan_id = current_scan_ids[0]
        if not isinstance(current_scan_id, str) or not current_scan_id:
            return

        scan_item = scan_storage.find_scan_by_ID(current_scan_id)
        if scan_item is None or getattr(scan_item, "status", None) in {
            "closed",
            "aborted",
            "halted",
            "user_completed",
        }:
            return

        self.set_scan_id(current_scan_id)

    def _handle_scan_status_update(self, scan_status: dict, _metadata: dict) -> None:
        """
        Rebind a follow-scan subscription when a new scan opens.

        Args:
            scan_status (dict): Scan-status payload emitted by the client
                callback handler.
            _metadata (dict): Metadata accompanying the scan-status update.

        Returns:
            None: This method updates internal subscription state in place.
        """
        if self._is_closed or not self._follow_scan:
            return

        scan_id = scan_status.get("scan_id")
        status = scan_status.get("status")
        if not scan_id or status != "open" or scan_id == self._scan_id:
            return

        self.set_scan_id(scan_id)

    def __del__(self):
        """
        Ensure cleanup when the subscription is garbage collected.
        """
        self.close()

    def __enter__(self) -> DataSubscription:
        """
        Enter the context manager for this subscription.

        Returns:
            DataSubscription: This subscription instance.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager and close the subscription.

        Args:
            exc_type: Exception type, if one was raised inside the context.
            exc_val: Exception value, if one was raised inside the context.
            exc_tb: Exception traceback, if one was raised inside the context.

        Returns:
            bool: Always ``False`` so exceptions are not suppressed.
        """
        self.close()
        return False


class DataAPI:
    """
    DataAPI class that manages data retrieval through plugins.

    This is a singleton - only one instance exists globally.
    """

    _instance: DataAPI | None = None

    def __new__(cls, client):
        """
        Create or return the singleton ``DataAPI`` instance.

        Args:
            client: Client object associated with the data API.

        Returns:
            DataAPI: Singleton instance of the data API.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, client):
        """
        Initialize the singleton data API instance.

        Args:
            client: Client object used by registered plugins.
        """
        # Only initialize once
        if hasattr(self, "_initialized"):
            return

        self.client = client
        self.plugins: list[DataAPIPlugin] = []
        self._initialized = True
        self.register_plugin(BECLiveDataPlugin(self.client))

    @classmethod
    def clear_instance(cls) -> None:
        """
        Clear the singleton instance.

        This is primarily useful for tests that need a fresh ``DataAPI``.
        """
        if cls._instance is not None:
            cls._instance.close()
        cls._instance = None

    def close(self) -> None:
        """
        Disconnect registered plugins and release held API resources.
        """
        for plugin in self.plugins:
            plugin.disconnect()
        self.plugins.clear()
        if hasattr(self, "_initialized"):
            delattr(self, "_initialized")

    def __del__(self):
        """
        Perform best-effort cleanup for plugin connections on destruction.
        """
        try:
            self.close()
        except Exception:  # pragma: no cover - destructor safety
            pass

    def register_plugin(self, plugin: DataAPIPlugin) -> None:
        """
        Register and connect a new data API plugin.

        Args:
            plugin (DataAPIPlugin): Plugin instance to register.
        """
        plugin.connect()
        self.plugins.append(plugin)
        self.plugins.sort(key=lambda p: p.get_info().get("priority", 100))

    def create_subscription(
        self, scan_id: str | None = None, buffered: bool = False
    ) -> DataSubscription:
        """
        Create a new subscription object for synchronized data updates.

        This is the recommended way to subscribe to data updates as it provides
        automatic lifecycle management and synchronization across multiple devices.

        Args:
            scan_id (str | None): Identifier for the scan. When omitted or
                ``None``, the subscription automatically follows the current or
                next active scan.
            buffered (bool): If ``True``, re-emit the entire accumulated data
                buffer on each update. If ``False``, emit only newly aligned
                synchronized data blocks.

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
        scan_id: str | None,
        callback: Callable[[dict, dict], Any],
    ) -> str | None:
        """
        Subscribe to data updates for a specific device and entry.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.
            callback (Callable[[dict, dict], Any]): Function to call on data
                update.

        Returns:
            str | None: Subscription identifier, or ``None`` if no plugin can
                provide the source.
        """
        for plugin in self.plugins:
            if plugin.can_provide(device_name, device_entry, scan_id):
                return plugin.subscribe(device_name, device_entry, scan_id, callback)
        logger.warning(
            f"No plugin available to provide data for device '{device_name}', entry '{device_entry}', scan_id '{scan_id}'"
        )

    def get_bundle_domain(
        self, device_name: str, device_entry: str, scan_id: str | None
    ) -> tuple | None:
        """
        Resolve a source bundle domain using the registered plugins.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            tuple | None: Resolved bundle domain, or ``None`` if no plugin can
                determine one statically.
        """
        for plugin in self.plugins:
            domain = plugin.get_bundle_domain(device_name, device_entry, scan_id)
            if domain is not None:
                return domain
        return None

    def allows_runtime_bundle_resolution(
        self, device_name: str, device_entry: str, scan_id: str | None
    ) -> bool:
        """
        Return whether a source may legitimately defer bundle resolution.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            bool: ``True`` if at least one plugin allows runtime bundle
                resolution for the source, otherwise ``False``.
        """
        return any(
            plugin.allows_runtime_bundle_resolution(device_name, device_entry, scan_id)
            for plugin in self.plugins
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
            subscription_id (str | None): Identifier of the subscription to
                cancel.
            scan_id (str | None): Identifier for the scan.
            callback (Callable[[dict, dict], Any] | None): Callback function
                that was used for subscription.
        """
        for plugin in self.plugins:
            plugin.unsubscribe(subscription_id, scan_id, callback)


if __name__ == "__main__":
    from bec_lib.client import BECClient

    def my_callback(data, metadata):
        """
        Print data and metadata received from the example subscription.

        Args:
            data (dict): Data dictionary emitted by the subscription.
            metadata (dict): Metadata dictionary emitted by the subscription.
        """
        print("Received data:", data)
        print("With metadata:", metadata)

    client = BECClient()
    data_api = DataAPI(client)
    sub = data_api.create_subscription("test_scan")
    sub.add_device(device_name="waveform", device_entry="waveform_data")
    sub.set_callback(my_callback)
