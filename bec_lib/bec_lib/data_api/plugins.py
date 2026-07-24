from __future__ import annotations

import copy
import uuid
import weakref
from abc import ABC, abstractmethod
from typing import Any, Callable, Literal, Tuple

import louie
from pydantic import BaseModel, ConfigDict

from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import DeviceAsyncUpdate, DeviceMessage

CallbackRef = louie.saferef.BoundMethodWeakref | weakref.ReferenceType[Callable[[dict, dict], Any]]
logger = bec_logger.logger


class DataAPIPlugin(ABC):
    """Base class for DataAPI plugins."""

    def connect(self) -> None:
        """
        Perform connection setup for the plugin.
        """

    def disconnect(self) -> None:
        """
        Disconnect the plugin and clean up its resources.
        """

    @abstractmethod
    def has_scan_data(self, scan_id: str | None) -> bool:
        """
        Check if the plugin has data for the given scan ID.

        Args:
            scan_id (str | None): Identifier for the scan.

        Returns:
            bool: ``True`` if the plugin has data for the scan ID, otherwise
                ``False``.
        """

    @abstractmethod
    def can_provide(self, device_name: str, device_entry: str, scan_id: str | None) -> bool:
        """
        Check if the plugin can provide data for the given device and entry.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            bool: ``True`` if the plugin can provide the data, otherwise
                ``False``.
        """

    def get_info(self) -> dict:
        """
        Return plugin metadata such as name and priority.

        Returns:
            dict: Plugin metadata dictionary.
        """
        return {}

    def get_bundle_domain(
        self, device_name: str, device_entry: str, scan_id: str | None
    ) -> tuple | None:
        """
        Return the bundle domain for a source if it can be determined.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            tuple | None: The resolved bundle domain, or ``None`` if the plugin
                cannot determine it statically.
        """
        return None

    def allows_runtime_bundle_resolution(
        self, device_name: str, device_entry: str, scan_id: str | None
    ) -> bool:
        """
        Return whether the source may resolve its bundle domain only at runtime.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            bool: ``True`` if runtime bundle resolution is allowed for the
                source, otherwise ``False``.
        """
        return False

    @abstractmethod
    def subscribe(
        self,
        device_name: str,
        device_entry: str,
        scan_id: str | None,
        callback: Callable[[dict, dict], Any],
    ) -> str:
        """
        Subscribe to data updates.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.
            callback (Callable[[dict, dict], Any]): Function to call on data
                update. The callback receives one dictionary for the data and
                one dictionary for the metadata.

        Returns:
            str: Unique subscription identifier.
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
            subscription_id (str | None): Identifier of the subscription to
                cancel.
            scan_id (str | None): Identifier for the scan.
            callback (Callable[[dict, dict], Any] | None): Callback function
                that was used for subscription.
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
    connector_endpoint: Any | None = None
    connector_callback: Callable | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _AsyncSourceState(BaseModel):
    """Tracks resolved state for a single async source within a scan."""

    update_type: Literal["add", "add_slice", "replace"] | None = None
    value: Any = None
    last_index: int | None = None
    incomplete: bool = False
    bundle_domain: tuple | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _DataBuffer(BaseModel):
    """Buffer for storing data updates until all sources are synchronized."""

    device_name: str
    device_entry: str
    data: list[dict]  # List of data points with value, timestamp, and optional metadata
    source_type: Literal["monitored", "async_signal"]

    model_config = ConfigDict(arbitrary_types_allowed=True)


class _CallbackBuffer(BaseModel):
    """Tracks buffered data for a specific callback across all its subscribed devices."""

    callback_ref: CallbackRef
    scan_id: str
    buffers: dict[tuple[str, str], _DataBuffer]  # (device_name, device_entry) -> buffer
    monitored_indices: dict[tuple[str, str], int] = (
        {}
    )  # Track last processed index for monitored devices
    bundle_domain: tuple | None = None
    incompatible_sources: set[tuple[str, str]] = set()

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

    MAX_PENDING_UPDATES = 1000

    def __init__(self, client: BECClient):
        """
        Initialize the live-data plugin.

        Args:
            client (BECClient): Client instance providing live scan storage,
                device metadata, callbacks, and connector access.
        """
        self.client = client
        # Subscription tracking: sub_id -> subscription info
        self._subscriptions: dict[str, _SubscriptionInfo] = {}
        # Scan-level grouping for monitored devices: scan_id -> {callback_ref -> devices}
        self._monitored_subscriptions: dict[str, dict[CallbackRef, _MonitoredSubscription]] = {}
        # Async signal grouping: (scan_id, device_name, device_entry) -> _AsyncSubscription
        self._async_subscriptions: dict[tuple[str, str, str], _AsyncSubscription] = {}
        # Resolved current state for async sources keyed by (scan_id, device_name, device_entry)
        self._async_source_states: dict[tuple[str, str, str], _AsyncSourceState] = {}
        # Data buffers for synchronization: callback_ref -> _CallbackBuffer
        self._callback_buffers: dict[CallbackRef, _CallbackBuffer] = {}
        self._connect_id = None

    def connect(self):
        """
        Connect the plugin to the client live-update callbacks.
        """
        self._connect_id = self.client.callbacks.register(
            "scan_segment", self._handle_scan_segment_update
        )

    def disconnect(self):
        """
        Disconnect the plugin from the client and clear async subscriptions.
        """
        if self._connect_id is not None:
            self.client.callbacks.remove(self._connect_id)
            self._connect_id = None

        # Unregister all async signal subscriptions from redis connector
        for async_sub in self._async_subscriptions.values():
            if (
                async_sub.connector_endpoint is not None
                and async_sub.connector_callback is not None
            ):
                self.client.connector.unregister(
                    topics=async_sub.connector_endpoint, cb=async_sub.connector_callback
                )
        self._async_subscriptions.clear()
        self._async_source_states.clear()

    def has_scan_data(self, scan_id: str | None) -> bool:
        """
        Check if live data is available for the given scan ID.

        Args:
            scan_id (str | None): Identifier for the scan.

        Returns:
            bool: ``True`` if live data is available, otherwise ``False``.
        """
        if not self.client.started:
            return False
        if self.client.queue is None:
            return False
        if scan_id is None:
            return False

        scan_item = self.client.queue.scan_storage.find_scan_by_ID(scan_id)
        if scan_item is None:
            return False

        if scan_item.status in ["closed", "aborted", "halted"]:
            # We skip closed scans and instead rely on historical data plugin
            return False
        return True

    def can_provide(self, device_name: str, device_entry: str, scan_id: str | None) -> bool:
        """
        Check if live data is available for the given device and entry.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            bool: ``True`` if live data is available, otherwise ``False``.
        """
        mode = self._get_device_mode(device_name, device_entry, scan_id)
        return mode is not None

    def get_bundle_domain(
        self, device_name: str, device_entry: str, scan_id: str | None
    ) -> tuple | None:
        """
        Resolve a source bundle domain when it is already known.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            tuple | None: The statically or scan-specifically known bundle
                domain, or ``None`` when runtime resolution is still required.
        """
        mode = self._get_device_mode(device_name, device_entry, scan_id)
        if mode == "monitored":
            if scan_id is None:
                return None
            return ("monitored", scan_id)
        if mode == "async_signal":
            async_signal_info = self._get_async_signal_info(device_name, device_entry)
            if async_signal_info is None:
                return None
            acquisition_group = async_signal_info.get("acquisition_group")
            if acquisition_group:
                return self._bundle_domain_from_acquisition_group(scan_id, acquisition_group)
        return None

    def allows_runtime_bundle_resolution(
        self, device_name: str, device_entry: str, scan_id: str | None
    ) -> bool:
        """
        Return whether the source may defer bundle resolution to runtime.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            bool: ``True`` when the source is an async signal without a static
                acquisition group, otherwise ``False``.
        """
        if self._get_device_mode(device_name, device_entry, scan_id) != "async_signal":
            return False
        async_signal_info = self._get_async_signal_info(device_name, device_entry)
        if async_signal_info is None:
            return False
        return not bool(async_signal_info.get("acquisition_group"))

    def _get_device_mode(
        self, device_name: str, device_entry: str, scan_id: str | None
    ) -> Literal["monitored", "async_signal", None]:
        """
        Get the mode of the device entry for the given scan ID.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.

        Returns:
            Literal["monitored", "async_signal", None]: Source mode for the
                device entry, or ``None`` if the plugin cannot provide it.
        """
        # Pre-checks; mostly for type checks
        if not self.client.started or self.client.queue is None:
            return None
        if scan_id is None:
            if self._device_entry_is_async_signal(device_name, device_entry):
                return "async_signal"
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
        Check if the device entry participates in monitored readout for a scan.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_item: Scan item to check against.

        Returns:
            bool: ``True`` if the device entry is monitored, otherwise
                ``False``.
        """
        if scan_item.status_message is None:
            return False

        readout_priority = scan_item.status_message.readout_priority or {}
        if device_name not in readout_priority.get("monitored", []):
            return False

        device_manager = getattr(self.client, "device_manager", None)
        devices = getattr(device_manager, "devices", None)
        device = devices.get(device_name) if hasattr(devices, "get") else None
        if device is None:
            # Fall back to the root signal name when device metadata is unavailable.
            return device_entry == device_name

        device_info = getattr(device, "_info", {})
        available_signals = device_info.get("signals", {}) if isinstance(device_info, dict) else {}
        if not isinstance(available_signals, dict) or not available_signals:
            return device_entry == device_name

        return any(
            signal_info.get("obj_name") == device_entry
            for signal_info in available_signals.values()
        )

    def _device_entry_is_async_signal(self, device_name: str, device_entry: str) -> bool:
        """
        Check if the device entry is an async signal.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.

        Returns:
            bool: ``True`` if the device entry is an async signal, otherwise
                ``False``.
        """
        async_signal_info = self._get_async_signal_info(device_name, device_entry)
        return async_signal_info is not None

    def _get_async_signal_info(self, device_name: str, device_entry: str) -> dict | None:
        """
        Get the async signal information for the given device and entry.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.

        Returns:
            dict | None: Async signal information dictionary if found,
                otherwise ``None``.
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

    def _bundle_domain_from_acquisition_group(
        self, scan_id: str | None, acquisition_group: str
    ) -> tuple:
        """
        Map an acquisition-group label to an internal bundle-domain tuple.

        Args:
            scan_id (str | None): Identifier for the scan.
            acquisition_group (str): Acquisition-group label from static device
                metadata or runtime update metadata.

        Returns:
            tuple: Normalized bundle-domain tuple used by the data API.
        """
        if acquisition_group == "monitored":
            return ("monitored", scan_id)
        return ("async_signal", scan_id, acquisition_group)

    def _resolve_async_bundle_domain(
        self, scan_id: str, device_name: str, device_entry: str, metadata: dict
    ) -> tuple[tuple, bool]:
        """
        Resolve the bundle domain for an async source update.

        The first resolved domain for a given async source is kept constant for
        the rest of the scan. Later updates that resolve to a different domain
        are treated as invalid and should be skipped by the caller.

        Args:
            scan_id (str): Identifier for the scan.
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            metadata (dict): Metadata attached to the incoming async update.

        Returns:
            tuple[tuple, bool]: A pair containing the resolved bundle domain and
                a flag indicating whether the incoming update is valid for that
                domain.
        """
        key = (scan_id, device_name, device_entry)
        async_signal_info = self._get_async_signal_info(device_name, device_entry) or {}
        acquisition_group = async_signal_info.get("acquisition_group") or metadata.get(
            "acquisition_group"
        )
        if acquisition_group:
            bundle_domain = self._bundle_domain_from_acquisition_group(scan_id, acquisition_group)
        else:
            bundle_domain = ("standalone_async", scan_id, device_name, device_entry)

        state = self._async_source_states.get(key)
        if state is None:
            state = _AsyncSourceState()
            self._async_source_states[key] = state

        if state.bundle_domain is None:
            state.bundle_domain = bundle_domain
            return bundle_domain, True

        if state.bundle_domain != bundle_domain:
            logger.warning(
                f"Skipping async update for {device_name}/{device_entry} in scan {scan_id} "
                f"because its bundle domain changed from {state.bundle_domain} to {bundle_domain}."
            )
            return state.bundle_domain, False

        return state.bundle_domain, True

    def _get_or_create_callback_buffer(
        self, callback_ref: CallbackRef, scan_id: str
    ) -> _CallbackBuffer:
        """
        Return the synchronization buffer for a callback, creating it if needed.

        Args:
            callback_ref (CallbackRef): Weak reference to the subscription
                callback.
            scan_id (str): Identifier for the scan.

        Returns:
            _CallbackBuffer: Buffer state associated with the callback.
        """
        if callback_ref not in self._callback_buffers:
            self._callback_buffers[callback_ref] = _CallbackBuffer(
                callback_ref=callback_ref, scan_id=scan_id, buffers={}
            )
        return self._callback_buffers[callback_ref]

    def _accept_update_for_callback_bundle(
        self,
        callback_ref: CallbackRef,
        scan_id: str,
        device_name: str,
        device_entry: str,
        source_domain: tuple,
    ) -> bool:
        """
        Check whether an incoming update belongs to the callback bundle domain.

        Args:
            callback_ref (CallbackRef): Weak reference to the subscription
                callback.
            scan_id (str): Identifier for the scan.
            device_name (str): Name of the device that produced the update.
            device_entry (str): Specific entry of the device that produced the
                update.
            source_domain (tuple): Bundle domain resolved for the incoming
                update.

        Returns:
            bool: ``True`` if the update may participate in the callback's
                bundle, otherwise ``False``.
        """
        callback_buffer = self._get_or_create_callback_buffer(callback_ref, scan_id)
        source_key = (device_name, device_entry)

        if callback_buffer.bundle_domain is None:
            callback_buffer.bundle_domain = source_domain

        if source_domain != callback_buffer.bundle_domain:
            callback_buffer.incompatible_sources.add(source_key)
            logger.warning(
                f"Skipping update for {device_name}/{device_entry} in scan {scan_id} because it "
                f"resolved to bundle {source_domain} while the subscription is bound to "
                f"{callback_buffer.bundle_domain}."
            )
            return False

        callback_buffer.incompatible_sources.discard(source_key)
        return True

    def subscribe(
        self,
        device_name: str,
        device_entry: str,
        scan_id: str | None,
        callback: Callable[[dict, dict], Any],
    ) -> str:
        """
        Subscribe to live data updates for the given device and entry.

        Args:
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str | None): Identifier for the scan.
            callback (Callable[[dict, dict], Any]): Function to call on data
                update. The callback receives one dictionary for the data and
                one dictionary for the metadata.

        Returns:
            str: Unique subscription identifier.

        Raises:
            ValueError: If the source cannot be subscribed or resolves to an
                unknown mode.
        """
        if scan_id is None:
            raise ValueError(
                f"Cannot subscribe to device '{device_name}' entry '{device_entry}' without a scan_id."
            )

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
            subscription_id (str | None): Identifier of the subscription to
                cancel.
            scan_id (str | None): Identifier for the scan.
            callback (Callable[[dict, dict], Any] | None): Callback function
                that was used for subscription.
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
            subscription_id (str): Identifier of the subscription to cancel.
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
        """
        Remove one monitored-source subscription from internal tracking.

        Args:
            sub_info (_SubscriptionInfo): Subscription record to remove.
        """
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
        """
        Remove one async-source subscription from internal tracking.

        Args:
            sub_info (_SubscriptionInfo): Subscription record to remove.
        """
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
            if (
                async_sub.connector_endpoint is not None
                and async_sub.connector_callback is not None
            ):
                self.client.connector.unregister(
                    topics=async_sub.connector_endpoint, cb=async_sub.connector_callback
                )
            del self._async_subscriptions[key]
            self._async_source_states.pop(key, None)

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
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str): Identifier for the scan.
            callback (Callable[[dict, dict], Any]): Function to call on data
                update.

        Returns:
            str: Unique subscription identifier.
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
            self._backfill_monitored_scan_data(scan_id)
            return sub_id

        for callback_ref_existing, sub in available_subscriptions.items():
            if callback_ref_existing == callback_ref:
                # Found existing subscription for this callback
                if (device_name, device_entry) not in sub.devices:
                    sub.devices.append((device_name, device_entry))
                self._backfill_monitored_scan_data(scan_id)
                return sub_id

        # New callback for this scan
        sub = _MonitoredSubscription(
            scan_id=scan_id, callback_ref=callback_ref, devices=[(device_name, device_entry)]
        )
        self._monitored_subscriptions[scan_id][callback_ref] = sub
        self._backfill_monitored_scan_data(scan_id)
        return sub_id

    def _backfill_monitored_scan_data(self, scan_id: str) -> None:
        """
        Re-process currently available monitored live data for a scan.

        This allows subscriptions created mid-acquisition to immediately align
        against data already present in scan storage instead of waiting for the
        next scan-segment event to arrive.

        Args:
            scan_id (str): Identifier for the scan whose monitored live data
                should be re-processed.
        """
        self._handle_scan_segment_update({"scan_id": scan_id}, {"scan_id": scan_id})

    def _handle_scan_segment_update(self, _scan_segment: dict, metadata: dict) -> None:
        """
        Handle one scan-segment trigger from the client.

        The scan-segment payload itself is not emitted directly. Instead it is
        used as a trigger to fetch newly available monitored values from the
        scan item's live-data storage.

        Args:
            _scan_segment (dict): Scan-segment content from the client
                callback.
            metadata (dict): Metadata associated with the scan segment.
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

            callback_buffer = self._get_or_create_callback_buffer(callback_ref, scan_id)
            callback_buffer.bundle_domain = callback_buffer.bundle_domain or ("monitored", scan_id)

            # Prepare data for this subscription
            for device_name, device_entry in sub.devices:
                if not self._accept_update_for_callback_bundle(
                    callback_ref, scan_id, device_name, device_entry, ("monitored", scan_id)
                ):
                    continue

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
            device_name (str): Name of the device.
            device_entry (str): Specific entry of the device.
            scan_id (str): Identifier for the scan.
            callback (Callable[[dict, dict], Any]): Function to call on data
                update.

        Returns:
            str: Unique subscription identifier.

        Raises:
            ValueError: If the async signal metadata cannot be found.
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
            endpoint = MessageEndpoints.device_async_signal(
                scan_id=scan_id, device=device_name, signal=async_signal_info.get("storage_name")
            )

            def connector_callback(
                msg, *, _scan_id=scan_id, _device_name=device_name, _device_entry=device_entry
            ):
                """
                Forward one connector message to the plugin async-update handler.

                Args:
                    msg: Raw connector payload for the async signal stream.
                    _scan_id: Bound scan identifier for the subscription.
                    _device_name: Bound device name for the subscription.
                    _device_entry: Bound device entry for the subscription.
                """
                self._handle_async_signal_update(msg, _scan_id, _device_name, _device_entry)

            self.client.connector.register(endpoint, cb=connector_callback, from_start=True)

            # Create subscription tracking entry
            async_sub = _AsyncSubscription(
                scan_id=scan_id,
                device_name=device_name,
                device_entry=device_entry,
                callback_refs=[callback_ref],
                connector_endpoint=endpoint,
                connector_callback=connector_callback,
            )
            self._async_subscriptions[key] = async_sub

        return sub_id

    def _handle_async_signal_update(
        self, msg: dict, scan_id: str, device_name: str, device_entry: str
    ) -> None:
        """
        Process one async update and route it to subscribed callbacks.

        Args:
            msg (dict): Connector payload containing a ``DeviceMessage`` under
                the ``"data"`` key.
            scan_id (str): Identifier for the scan.
            device_name (str): Name of the device that emitted the update.
            device_entry (str): Specific entry of the device that emitted the
                update.
        """

        msg_obj = msg.get("data")
        if not isinstance(msg_obj, DeviceMessage):
            return

        signals = msg_obj.signals
        signal_data = signals.get(device_entry)
        if signal_data is None:
            return

        value = signal_data.get("value")
        timestamp = signal_data.get("timestamp", msg_obj.metadata.get("timestamp"))
        metadata = dict(msg_obj.metadata)

        # Get all callbacks for this device/entry/scan combination
        key = (scan_id, device_name, device_entry)
        if key not in self._async_subscriptions:
            return

        async_sub = self._async_subscriptions[key]
        bundle_domain, is_valid_domain = self._resolve_async_bundle_domain(
            scan_id, device_name, device_entry, metadata
        )
        if not is_valid_domain:
            return
        resolved_value, resolved_metadata = self._resolve_async_signal_value(key, value, metadata)
        emitted_value = resolved_value
        if bundle_domain == ("monitored", scan_id):
            # For monitored bundles, each async update represents one aligned
            # progression step and should be emitted as that fragment rather
            # than as the cumulative reconstructed async state.
            emitted_value = copy.deepcopy(value)

        # Add data to buffer for each subscriber
        for callback_ref in async_sub.callback_refs:
            callback = callback_ref()
            if callback is None:
                continue
            if not self._accept_update_for_callback_bundle(
                callback_ref, scan_id, device_name, device_entry, bundle_domain
            ):
                continue

            # Add to buffer
            self._add_to_buffer(
                callback_ref,
                scan_id,
                device_name,
                device_entry,
                emitted_value,
                timestamp,
                "async_signal",
                resolved_metadata,
            )

            # Check if we can emit synchronized data
            self._check_and_emit_synchronized_data(callback_ref, scan_id)

    def _resolve_async_signal_value(
        self, key: tuple[str, str, str], value: Any, metadata: dict
    ) -> tuple[Any, dict]:
        """
        Resolve the current exposed value for one async source update.

        Args:
            key (tuple[str, str, str]): Source key as ``(scan_id, device_name,
                device_entry)``.
            value (Any): Raw payload from the async update.
            metadata (dict): Metadata attached to the async update.

        Returns:
            tuple[Any, dict]: The resolved current source value together with
                normalized metadata for downstream emission.
        """
        resolved_metadata = dict(metadata)
        async_update = DeviceAsyncUpdate.model_validate(resolved_metadata.get("async_update", {}))
        state = self._async_source_states.get(key)
        if state is None:
            state = _AsyncSourceState(update_type=async_update.type)
            self._async_source_states[key] = state

        if state.update_type is None:
            state.update_type = async_update.type
        elif state.update_type != async_update.type:
            raise ValueError(
                f"Async update type changed for source {key}: {state.update_type} -> {async_update.type}"
            )

        async_indices = resolved_metadata.get("async_indices", {})
        current_index = async_indices.get(key[2])
        if (
            async_update.type in {"add", "add_slice"}
            and current_index is not None
            and (
                (state.last_index is None and current_index != 0)
                or (state.last_index is not None and current_index != state.last_index + 1)
            )
        ):
            state.incomplete = True

        if async_update.type == "replace":
            state.value = copy.deepcopy(value)
        elif async_update.type == "add":
            state.value = self._resolve_add_value(state.value, value)
        elif async_update.type == "add_slice":
            state.value = self._resolve_add_slice_value(state.value, value, async_update.index)

        if current_index is not None:
            state.last_index = current_index

        if state.incomplete:
            resolved_metadata["async_state_incomplete"] = True

        return copy.deepcopy(state.value), resolved_metadata

    def _resolve_add_value(self, current_value: Any, new_value: Any) -> Any:
        """
        Append one ``add`` payload fragment to the current source state.

        Args:
            current_value (Any): Previously aggregated source value.
            new_value (Any): Newly received partial payload.

        Returns:
            Any: Aggregated source value after appending the new fragment.
        """
        if current_value is None:
            return copy.deepcopy(new_value)

        new_list = list(new_value) if isinstance(new_value, (list, tuple)) else [new_value]
        current_list = (
            list(current_value) if isinstance(current_value, (list, tuple)) else [current_value]
        )
        return current_list + new_list

    def _resolve_add_slice_value(
        self, current_value: Any, new_value: Any, row_index: int | None
    ) -> Any:
        """
        Merge one ``add_slice`` payload fragment into the current source state.

        Args:
            current_value (Any): Previously aggregated source value.
            new_value (Any): Newly received partial payload.
            row_index (int | None): Target row index for the slice fragment.

        Returns:
            Any: Aggregated source value after merging the slice.

        Raises:
            ValueError: If ``row_index`` is ``None``.
        """
        if row_index is None:
            raise ValueError("add_slice updates require an index")

        rows = copy.deepcopy(current_value) if current_value is not None else []
        while len(rows) <= row_index:
            rows.append([])

        row_update = list(new_value) if isinstance(new_value, (list, tuple)) else [new_value]
        rows[row_index].extend(row_update)
        return rows

    def _add_to_buffer(
        self,
        callback_ref: CallbackRef,
        scan_id: str,
        device_name: str,
        device_entry: str,
        value: Any,
        timestamp: Any,
        source_type: Literal["monitored", "async_signal"],
        metadata: dict | None = None,
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
            metadata: Optional source metadata associated with this data point
        """
        callback_buffer = self._get_or_create_callback_buffer(callback_ref, scan_id)
        key = (device_name, device_entry)

        # Initialize device buffer if not exists
        if key not in callback_buffer.buffers:
            callback_buffer.buffers[key] = _DataBuffer(
                device_name=device_name, device_entry=device_entry, data=[], source_type=source_type
            )

        # Add data point to buffer
        data_point = {"value": value, "timestamp": timestamp, "metadata": metadata or {}}
        callback_buffer.buffers[key].data.append(data_point)
        self._enforce_pending_backlog_limit(callback_buffer, key, scan_id, callback_ref)

    def _enforce_pending_backlog_limit(
        self,
        callback_buffer: _CallbackBuffer,
        key: tuple[str, str],
        scan_id: str,
        callback_ref: CallbackRef,
    ) -> None:
        """
        Enforce the maximum pending backlog for one buffered source.

        Args:
            callback_buffer (_CallbackBuffer): Per-callback synchronization
                buffer.
            key (tuple[str, str]): Source key as ``(device_name, device_entry)``.
            scan_id (str): Identifier for the scan.
            callback_ref (CallbackRef): Weak reference to the subscription
                callback.
        """
        data_buffer = callback_buffer.buffers[key]
        overflow = len(data_buffer.data) - self.MAX_PENDING_UPDATES
        if overflow <= 0:
            return

        del data_buffer.data[:overflow]
        logger.warning(
            f"Dropping {overflow} buffered updates for {key[0]}/{key[1]} in scan {scan_id} "
            f"because bundle alignment could not keep up for callback {callback_ref}."
        )

    def _get_expected_device_count(self, callback_ref: CallbackRef, scan_id: str) -> int:
        """
        Get the number of sources currently expected in the callback bundle.

        Args:
            callback_ref (CallbackRef): Weak reference to the callback.
            scan_id (str): Identifier for the scan.

        Returns:
            int: Number of subscribed sources expected to contribute to the
                aligned bundle.
        """
        callback_buffer = self._callback_buffers.get(callback_ref)
        bundle_domain = callback_buffer.bundle_domain if callback_buffer is not None else None
        incompatible_sources = (
            callback_buffer.incompatible_sources if callback_buffer is not None else set()
        )
        count = 0

        # Count monitored devices
        if scan_id in self._monitored_subscriptions:
            if callback_ref in self._monitored_subscriptions[scan_id]:
                for device_name, device_entry in self._monitored_subscriptions[scan_id][
                    callback_ref
                ].devices:
                    if (device_name, device_entry) in incompatible_sources:
                        continue
                    if bundle_domain is not None and bundle_domain != ("monitored", scan_id):
                        continue
                    count += 1

        # Count async signals
        for (
            sub_scan_id,
            device_name,
            device_entry,
        ), async_sub in self._async_subscriptions.items():
            if sub_scan_id == scan_id and callback_ref in async_sub.callback_refs:
                if (device_name, device_entry) in incompatible_sources:
                    continue
                count += 1

        return count

    def _check_and_emit_synchronized_data(self, callback_ref: CallbackRef, scan_id: str) -> None:
        """
        Emit aligned bundles once all subscribed sources have pending data.

        Args:
            callback_ref (CallbackRef): Weak reference to the callback.
            scan_id (str): Identifier for the scan.
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

        # Find the number of aligned pending bundles currently available.
        min_length = min(len(buffer.data) for buffer in callback_buffer.buffers.values())

        # If no data is available in all buffers yet, return
        if min_length == 0:
            return

        callback = callback_ref()
        if callback is None:
            return

        # Emit each newly aligned bundle once, then discard it from the active buffers.
        for idx in range(min_length):
            data = {}
            metadata = {"scan_id": scan_id}
            for (device_name, device_entry), buffer in callback_buffer.buffers.items():
                data_point = buffer.data[idx]
                if device_name not in data:
                    data[device_name] = {}
                data[device_name][device_entry] = {
                    "value": data_point["value"],
                    "timestamp": data_point["timestamp"],
                }
                if buffer.source_type == "async_signal":
                    point_metadata = data_point.get("metadata", {})
                    if point_metadata:
                        for key, value in point_metadata.items():
                            if key == "scan_id":
                                continue
                            metadata.setdefault(key, value)

            # Call the callback with synchronized data
            callback(data, metadata)
        for buffer in callback_buffer.buffers.values():
            del buffer.data[:min_length]
