"""
Tests for the data_api module and plugins.

These tests verify the functionality of the DataAPI and BECLiveDataPlugin classes,
including subscription management, data synchronization, and proper handling of
BECMessages (ScanMessage and DeviceMessage).
"""

import copy
from unittest import mock

import louie
import pytest

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.data_api.data_api import DataAPI
from bec_lib.data_api.plugins import BECLiveDataPlugin, _AsyncSubscription
from bec_lib.live_scan_data import LiveScanData
from bec_lib.scan_items import ScanItem

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name


@pytest.fixture
def mock_client(connected_connector):
    """Create a mock BECClient with necessary attributes."""
    client = mock.MagicMock(spec=BECClient)
    client.started = True
    client.connector = connected_connector
    client.callbacks = mock.MagicMock()
    client.callbacks.register = mock.MagicMock(return_value="callback_id")
    client.callbacks.remove = mock.MagicMock()

    # Setup queue and scan storage
    client.queue = mock.MagicMock()
    client.queue.scan_storage = mock.MagicMock()

    # Setup device manager
    client.device_manager = mock.MagicMock()

    return client


@pytest.fixture
def mock_callback():
    """Create a real callback function for testing (needed for louie.saferef)."""
    calls = []

    def callback(data, metadata):
        calls.append((data, metadata))

    callback.calls = calls
    callback.reset = lambda: calls.clear()
    return callback


@pytest.fixture
def data_api(mock_client):
    """Create a DataAPI instance for testing."""
    # Clear singleton before creating instance
    DataAPI.clear_instance()
    api = DataAPI(mock_client)
    yield api
    # Clean up after test
    DataAPI.clear_instance()


@pytest.fixture
def live_plugin(mock_client):
    """Create a BECLiveDataPlugin instance for testing."""
    plugin = BECLiveDataPlugin(mock_client)
    plugin.connect()
    yield plugin
    plugin.disconnect()


@pytest.fixture
def scan_item_with_monitored_devices(mock_client):
    """Create a scan item with monitored devices configured."""
    scan_item = ScanItem(
        queue_id="test_queue", scan_number=[1], scan_id=["test_scan_id"], status="open"
    )

    # Setup status message with monitored devices
    scan_item.status_message = messages.ScanStatusMessage(
        scan_id="test_scan_id", status="open", info={}
    )
    scan_item.status_message.readout_priority = {
        "monitored": ["samx", "samy"],
        "baseline": ["samz"],
    }

    # Initialize live_data with proper LiveScanData instance
    scan_item.live_data = LiveScanData()

    mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item
    return scan_item


class TestDataAPI:
    """Tests for the DataAPI class."""

    def test_singleton_pattern(self, mock_client):
        """Test that DataAPI follows singleton pattern."""
        DataAPI.clear_instance()
        api1 = DataAPI(mock_client)
        api2 = DataAPI(mock_client)
        assert api1 is api2
        DataAPI.clear_instance()

    def test_clear_instance(self, mock_client):
        """Test that clear_instance properly resets singleton."""
        DataAPI.clear_instance()
        api1 = DataAPI(mock_client)
        DataAPI.clear_instance()
        api2 = DataAPI(mock_client)
        assert api1 is not api2
        DataAPI.clear_instance()

    def test_register_plugin(self, data_api):
        """Test plugin registration."""
        mock_plugin = mock.MagicMock()
        mock_plugin.get_info.return_value = {"priority": 50}

        data_api.register_plugin(mock_plugin)

        mock_plugin.connect.assert_called_once()
        assert mock_plugin in data_api.plugins

    def test_plugin_priority_sorting(self, data_api):
        """Test that plugins are sorted by priority."""
        # Remove default plugin
        data_api.plugins = []

        plugin1 = mock.MagicMock()
        plugin1.get_info.return_value = {"priority": 100}
        plugin2 = mock.MagicMock()
        plugin2.get_info.return_value = {"priority": 50}
        plugin3 = mock.MagicMock()
        plugin3.get_info.return_value = {"priority": 75}

        data_api.register_plugin(plugin1)
        data_api.register_plugin(plugin2)
        data_api.register_plugin(plugin3)

        assert data_api.plugins[0] is plugin2  # priority 50
        assert data_api.plugins[1] is plugin3  # priority 75
        assert data_api.plugins[2] is plugin1  # priority 100

    def test_subscribe_with_capable_plugin(self, data_api):
        """Test subscription when a plugin can provide data."""
        mock_plugin = mock.MagicMock()
        mock_plugin.can_provide.return_value = True
        mock_plugin.subscribe.return_value = "sub_id_123"
        mock_plugin.get_info.return_value = {}

        data_api.plugins = [mock_plugin]

        callback = mock.MagicMock()
        sub_id = data_api.subscribe("samx", "value", "test_scan", callback)

        assert sub_id == "sub_id_123"
        mock_plugin.subscribe.assert_called_once_with("samx", "value", "test_scan", callback)

    def test_subscribe_no_capable_plugin(self, data_api):
        """Test subscription when no plugin can provide data."""
        mock_plugin = mock.MagicMock()
        mock_plugin.can_provide.return_value = False
        mock_plugin.get_info.return_value = {}

        data_api.plugins = [mock_plugin]

        callback = mock.MagicMock()
        sub_id = data_api.subscribe("samx", "value", "test_scan", callback)

        assert sub_id is None
        mock_plugin.subscribe.assert_not_called()

    def test_unsubscribe_by_id(self, data_api):
        """Test unsubscribe by subscription ID."""
        mock_plugin = mock.MagicMock()
        mock_plugin.get_info.return_value = {}
        data_api.plugins = [mock_plugin]

        data_api.unsubscribe(subscription_id="sub_123")

        mock_plugin.unsubscribe.assert_called_once_with("sub_123", None, None)

    def test_unsubscribe_by_scan_and_callback(self, data_api):
        """Test unsubscribe by scan ID and callback."""
        mock_plugin = mock.MagicMock()
        mock_plugin.get_info.return_value = {}
        data_api.plugins = [mock_plugin]

        callback = mock.MagicMock()
        data_api.unsubscribe(scan_id="test_scan", callback=callback)

        mock_plugin.unsubscribe.assert_called_once_with(None, "test_scan", callback)


class TestBECLiveDataPlugin:
    """Tests for the BECLiveDataPlugin class."""

    def test_connect_registers_callback(self, mock_client):
        """Test that connect registers scan_segment callback."""
        plugin = BECLiveDataPlugin(mock_client)
        plugin.connect()

        mock_client.callbacks.register.assert_called_once_with(
            "scan_segment", plugin._handle_scan_segment_update
        )
        assert plugin._connect_id == "callback_id"

    def test_disconnect_removes_callback(self, mock_client):
        """Test that disconnect removes callback and cleans up."""
        plugin = BECLiveDataPlugin(mock_client)
        plugin.connect()
        plugin.disconnect()

        mock_client.callbacks.remove.assert_called_once_with("callback_id")
        assert plugin._connect_id is None

    def test_disconnect_unregisters_async_subscriptions(self, mock_client):
        """Test that disconnect unregisters all async signal subscriptions."""
        plugin = BECLiveDataPlugin(mock_client)

        # Mock connector.unregister as a MagicMock
        mock_client.connector.unregister = mock.MagicMock()

        # Simulate having async subscriptions
        plugin._async_subscriptions[("scan1", "dev1", "sig1")] = _AsyncSubscription(
            scan_id="scan1",
            device_name="dev1",
            device_entry="sig1",
            callback_refs=[],
            connector_id="conn_id_1",
        )
        plugin._async_subscriptions[("scan2", "dev2", "sig2")] = _AsyncSubscription(
            scan_id="scan2",
            device_name="dev2",
            device_entry="sig2",
            callback_refs=[],
            connector_id="conn_id_2",
        )

        plugin.disconnect()

        assert mock_client.connector.unregister.call_count == 2
        assert len(plugin._async_subscriptions) == 0

    def test_has_scan_data_client_not_started(self, mock_client):
        """Test has_scan_data returns False when client not started."""
        mock_client.started = False
        plugin = BECLiveDataPlugin(mock_client)

        assert plugin.has_scan_data("test_scan") is False

    def test_has_scan_data_no_queue(self, mock_client):
        """Test has_scan_data returns False when queue is None."""
        mock_client.queue = None
        plugin = BECLiveDataPlugin(mock_client)

        assert plugin.has_scan_data("test_scan") is False

    def test_has_scan_data_scan_not_found(self, mock_client):
        """Test has_scan_data returns False when scan not found."""
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = None
        plugin = BECLiveDataPlugin(mock_client)

        assert plugin.has_scan_data("test_scan") is False

    def test_has_scan_data_scan_closed(self, mock_client):
        """Test has_scan_data returns False for closed scans."""
        scan_item = mock.MagicMock()
        scan_item.status = "closed"
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item

        plugin = BECLiveDataPlugin(mock_client)

        assert plugin.has_scan_data("test_scan") is False

    def test_has_scan_data_scan_open(self, mock_client):
        """Test has_scan_data returns True for open scans."""
        scan_item = mock.MagicMock()
        scan_item.status = "open"
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item

        plugin = BECLiveDataPlugin(mock_client)

        assert plugin.has_scan_data("test_scan") is True

    def test_device_entry_is_monitored(self, mock_client, scan_item_with_monitored_devices):
        """Test detection of monitored device entries."""
        plugin = BECLiveDataPlugin(mock_client)

        assert (
            plugin._device_entry_is_monitored("samx", "samx", scan_item_with_monitored_devices)
            is True
        )
        assert (
            plugin._device_entry_is_monitored("samz", "samz", scan_item_with_monitored_devices)
            is False
        )

    def test_device_entry_is_async_signal(self, mock_client):
        """Test detection of async signal device entries."""
        mock_client.device_manager.get_bec_signals.return_value = [
            ("detector1", None, {"obj_name": "async_sig1", "storage_name": "async_sig1"}),
            ("detector2", None, {"obj_name": "async_sig2", "storage_name": "async_sig2"}),
        ]

        plugin = BECLiveDataPlugin(mock_client)

        assert plugin._device_entry_is_async_signal("detector1", "async_sig1") is True
        assert plugin._device_entry_is_async_signal("detector2", "async_sig2") is True
        assert plugin._device_entry_is_async_signal("detector1", "async_sig2") is False

    def test_subscribe_to_monitored_device(
        self, mock_client, scan_item_with_monitored_devices, mock_callback
    ):
        """Test subscription to monitored device."""
        plugin = BECLiveDataPlugin(mock_client)

        sub_id = plugin._subscribe_to_monitored_device(
            "samx", "samx", "test_scan_id", mock_callback
        )

        assert sub_id is not None
        assert sub_id in plugin._subscriptions
        assert plugin._subscriptions[sub_id].device_name == "samx"
        assert plugin._subscriptions[sub_id].device_entry == "samx"
        assert plugin._subscriptions[sub_id].subscription_type == "monitored"

    def test_subscribe_to_monitored_device_multiple_callbacks(
        self, mock_client, scan_item_with_monitored_devices
    ):
        """Test multiple callbacks for same monitored device."""
        plugin = BECLiveDataPlugin(mock_client)

        def callback1(data, metadata):
            pass

        def callback2(data, metadata):
            pass

        sub_id1 = plugin._subscribe_to_monitored_device("samx", "samx", "test_scan_id", callback1)
        sub_id2 = plugin._subscribe_to_monitored_device("samx", "samx", "test_scan_id", callback2)

        assert sub_id1 != sub_id2
        assert len(plugin._monitored_subscriptions["test_scan_id"]) == 2

    def test_subscribe_to_async_signal(self, mock_client, mock_callback):
        """Test subscription to async signal."""
        mock_client.device_manager.get_bec_signals.return_value = [
            ("detector1", None, {"obj_name": "async_sig1", "storage_name": "async_sig1"})
        ]

        # Setup scan item
        scan_item = ScanItem(
            queue_id="test_queue", scan_number=1, scan_id="test_scan_id", status="open"
        )
        scan_item.status_message = messages.ScanStatusMessage(
            scan_id="test_scan_id", status="open", info={}
        )
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item

        plugin = BECLiveDataPlugin(mock_client)

        # Mock the connector.register method properly
        mock_client.connector.register = mock.MagicMock(return_value="redis_conn_id")

        sub_id = plugin._subscribe_to_async_signal(
            "detector1", "async_sig1", "test_scan_id", mock_callback
        )

        assert sub_id is not None
        assert sub_id in plugin._subscriptions
        assert plugin._subscriptions[sub_id].subscription_type == "async_signal"

        # Check that redis connector was registered
        mock_client.connector.register.assert_called_once()
        call_args = mock_client.connector.register.call_args
        assert call_args.kwargs["from_start"] is True
        assert call_args.kwargs["scan_id"] == "test_scan_id"
        assert call_args.kwargs["device_name"] == "detector1"
        assert call_args.kwargs["device_entry"] == "async_sig1"

    def test_subscribe_to_async_signal_shared_subscription(self, mock_client):
        """Test that multiple callbacks share the same async signal subscription."""
        mock_client.device_manager.get_bec_signals.return_value = [
            ("detector1", None, {"obj_name": "async_sig1", "storage_name": "async_sig1"})
        ]

        scan_item = ScanItem(
            queue_id="test_queue", scan_number=[1], scan_id=["test_scan_id"], status="open"
        )
        scan_item.status_message = messages.ScanStatusMessage(
            scan_id="test_scan_id", status="open", info={}
        )
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item

        plugin = BECLiveDataPlugin(mock_client)

        def callback1(data, metadata):
            pass

        def callback2(data, metadata):
            pass

        # Mock the connector.register method properly
        mock_client.connector.register = mock.MagicMock(return_value="redis_conn_id")

        sub_id1 = plugin._subscribe_to_async_signal(
            "detector1", "async_sig1", "test_scan_id", callback1
        )
        sub_id2 = plugin._subscribe_to_async_signal(
            "detector1", "async_sig1", "test_scan_id", callback2
        )

        # Should only register once with redis connector
        assert mock_client.connector.register.call_count == 1

        # Both subscriptions should share the same async subscription
        key = ("test_scan_id", "detector1", "async_sig1")
        assert key in plugin._async_subscriptions
        assert len(plugin._async_subscriptions[key].callback_refs) == 2

    def test_handle_scan_segment_update_monitored(
        self, mock_client, scan_item_with_monitored_devices, mock_callback
    ):
        """Test handling scan segment updates for monitored devices with proper ScanMessage."""
        plugin = BECLiveDataPlugin(mock_client)

        # Subscribe to monitored device
        plugin._subscribe_to_monitored_device("samx", "samx", "test_scan_id", mock_callback)

        # Create proper ScanMessages and set them in live_data
        # Each ScanMessage represents one point with single values
        scan_msgs = [
            messages.ScanMessage(
                point_id=0,
                scan_id="test_scan_id",
                data={"samx": {"samx": {"value": 1.0, "timestamp": 100.0}}},
                metadata={"scan_id": "test_scan_id"},
            ),
            messages.ScanMessage(
                point_id=1,
                scan_id="test_scan_id",
                data={"samx": {"samx": {"value": 2.0, "timestamp": 101.0}}},
                metadata={"scan_id": "test_scan_id"},
            ),
            messages.ScanMessage(
                point_id=2,
                scan_id="test_scan_id",
                data={"samx": {"samx": {"value": 3.0, "timestamp": 102.0}}},
                metadata={"scan_id": "test_scan_id"},
            ),
        ]

        # Set the messages in live_data
        for idx, msg in enumerate(scan_msgs):
            scan_item_with_monitored_devices.live_data.set(idx, msg)

        # Trigger the callback for each point
        for msg in scan_msgs:
            plugin._handle_scan_segment_update(msg.content, msg.metadata)

        # Callback should be called for each data point
        assert len(mock_callback.calls) >= 1  # At least one call should have been made

        # If called, check the structure of the first call
        if len(mock_callback.calls) > 0:
            call_data, call_metadata = mock_callback.calls[0]
            assert "samx" in call_data
            assert "samx" in call_data["samx"]
            assert call_metadata["scan_id"] == "test_scan_id"

    def test_async_signal_callback_with_device_message(self, mock_client, mock_callback):
        """Test async signal callback receives proper DeviceMessage."""
        plugin = BECLiveDataPlugin(mock_client)

        # Create async subscription manually
        callback_ref = louie.saferef.safe_ref(mock_callback)
        async_sub = _AsyncSubscription(
            scan_id="test_scan_id",
            device_name="detector1",
            device_entry="async_sig1",
            callback_refs=[callback_ref],
            connector_id="conn_id",
        )
        plugin._async_subscriptions[("test_scan_id", "detector1", "async_sig1")] = async_sub

        # Create a proper DeviceMessage
        device_msg = messages.DeviceMessage(
            signals={
                "async_sig1": {"value": 42.0, "timestamp": 123.456},
                "status": {"value": "ok", "timestamp": 123.456},
            },
            metadata={"timestamp": 123.456},
        )

        # Call the static callback method
        BECLiveDataPlugin._async_signal_sync_callback(
            {"data": device_msg}, plugin, "test_scan_id", "detector1", "async_sig1"
        )

        # Callback should be invoked with the data
        assert len(mock_callback.calls) == 1
        call_data, call_metadata = mock_callback.calls[0]
        assert "detector1" in call_data
        # The signals dict is stored as the value
        assert call_data["detector1"]["async_sig1"]["value"] == device_msg.signals
        assert call_data["detector1"]["async_sig1"]["timestamp"] == 123.456

    def test_data_synchronization_mixed_sources(
        self, mock_client, scan_item_with_monitored_devices, mock_callback
    ):
        """Test data synchronization between monitored and async sources."""
        mock_client.device_manager.get_bec_signals.return_value = [
            ("detector1", None, {"obj_name": "async_sig1", "storage_name": "async_sig1"})
        ]

        plugin = BECLiveDataPlugin(mock_client)

        # Subscribe to both monitored and async
        plugin._subscribe_to_monitored_device("samx", "samx", "test_scan_id", mock_callback)

        # Mock the connector.register method properly
        mock_client.connector.register = mock.MagicMock(return_value="redis_conn_id")
        plugin._subscribe_to_async_signal("detector1", "async_sig1", "test_scan_id", mock_callback)

        # Add monitored data via proper ScanMessage
        scan_msg = messages.ScanMessage(
            point_id=0,
            scan_id="test_scan_id",
            data={"samx": {"samx": {"value": 1.0, "timestamp": 100.0}}},
            metadata={"scan_id": "test_scan_id"},
        )
        scan_item_with_monitored_devices.live_data.set(0, scan_msg)

        plugin._handle_scan_segment_update(scan_msg.content, scan_msg.metadata)

        # At this point, callback should not be called yet (waiting for async data)
        assert len(mock_callback.calls) == 0

        # Now add async data
        device_msg = messages.DeviceMessage(
            signals={"async_sig1": {"value": 10.0, "timestamp": 100.0}}
        )

        BECLiveDataPlugin._async_signal_sync_callback(
            {"data": device_msg}, plugin, "test_scan_id", "detector1", "async_sig1"
        )

        # Callback should eventually be called with synchronized data
        # The synchronization requires all sources to have data
        assert len(mock_callback.calls) == 1

        # Check that the data contains both sources
        call_data, call_metadata = mock_callback.calls[0]
        assert "samx" in call_data
        assert "detector1" in call_data

    def test_unsubscribe_monitored_device(
        self, mock_client, scan_item_with_monitored_devices, mock_callback
    ):
        """Test unsubscribing from monitored device."""
        plugin = BECLiveDataPlugin(mock_client)

        sub_id = plugin._subscribe_to_monitored_device(
            "samx", "samx", "test_scan_id", mock_callback
        )

        # Verify subscription exists
        assert sub_id in plugin._subscriptions
        assert "test_scan_id" in plugin._monitored_subscriptions

        # Unsubscribe
        plugin.unsubscribe(subscription_id=sub_id)

        # Verify cleanup
        assert sub_id not in plugin._subscriptions
        assert "test_scan_id" not in plugin._monitored_subscriptions

    def test_unsubscribe_async_signal(self, mock_client, mock_callback):
        """Test unsubscribing from async signal."""
        mock_client.device_manager.get_bec_signals.return_value = [
            ("detector1", None, {"obj_name": "async_sig1", "storage_name": "async_sig1"})
        ]

        scan_item = ScanItem(
            queue_id="test_queue", scan_number=[1], scan_id=["test_scan_id"], status="open"
        )
        scan_item.status_message = messages.ScanStatusMessage(
            scan_id="test_scan_id", status="open", info={}
        )
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item

        plugin = BECLiveDataPlugin(mock_client)

        # Mock the connector methods properly
        mock_client.connector.register = mock.MagicMock(return_value="redis_conn_id")
        mock_client.connector.unregister = mock.MagicMock()

        sub_id = plugin._subscribe_to_async_signal(
            "detector1", "async_sig1", "test_scan_id", mock_callback
        )

        # Verify subscription exists
        assert sub_id in plugin._subscriptions
        key = ("test_scan_id", "detector1", "async_sig1")
        assert key in plugin._async_subscriptions

        # Unsubscribe
        plugin.unsubscribe(subscription_id=sub_id)

        # Verify cleanup
        assert sub_id not in plugin._subscriptions
        assert key not in plugin._async_subscriptions
        mock_client.connector.unregister.assert_called_once_with("redis_conn_id")

    def test_unsubscribe_by_scan_id(self, mock_client, scan_item_with_monitored_devices):
        """Test unsubscribing all subscriptions for a scan ID."""
        plugin = BECLiveDataPlugin(mock_client)

        def callback1(data, metadata):
            pass

        def callback2(data, metadata):
            pass

        sub_id1 = plugin._subscribe_to_monitored_device("samx", "samx", "test_scan_id", callback1)
        sub_id2 = plugin._subscribe_to_monitored_device("samy", "samy", "test_scan_id", callback2)

        # Unsubscribe all for scan
        plugin.unsubscribe(scan_id="test_scan_id")

        # Verify all cleaned up
        assert sub_id1 not in plugin._subscriptions
        assert sub_id2 not in plugin._subscriptions
        assert "test_scan_id" not in plugin._monitored_subscriptions

    def test_can_provide_monitored(self, mock_client, scan_item_with_monitored_devices):
        """Test can_provide returns True for monitored devices."""
        plugin = BECLiveDataPlugin(mock_client)

        assert plugin.can_provide("samx", "samx", "test_scan_id") is True
        assert plugin.can_provide("samz", "samz", "test_scan_id") is False

    def test_can_provide_async_signal(self, mock_client):
        """Test can_provide returns True for async signals."""
        mock_client.device_manager.get_bec_signals.return_value = [
            ("detector1", None, {"obj_name": "async_sig1", "storage_name": "async_sig1"})
        ]

        scan_item = ScanItem(
            queue_id="test_queue", scan_number=[1], scan_id=["test_scan_id"], status="open"
        )
        scan_item.status_message = messages.ScanStatusMessage(
            scan_id="test_scan_id", status="open", info={}
        )
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item

        plugin = BECLiveDataPlugin(mock_client)

        assert plugin.can_provide("detector1", "async_sig1", "test_scan_id") is True
        assert plugin.can_provide("detector1", "other_signal", "test_scan_id") is False

    def test_get_info(self, live_plugin):
        """Test get_info returns empty dict (base implementation)."""
        info = live_plugin.get_info()
        assert info == {}

    def test_cache_invalidation_on_new_scan(self, mock_client):
        """Test that device mode cache is properly scoped per scan."""
        plugin = BECLiveDataPlugin(mock_client)

        # Setup first scan with samx monitored
        scan_item1 = ScanItem(queue_id="queue1", scan_number=[1], scan_id=["scan_1"], status="open")
        scan_item1.status_message = messages.ScanStatusMessage(
            scan_id="scan_1", status="open", info={}
        )
        scan_item1.status_message.readout_priority = {"monitored": ["samx"]}

        # Setup second scan with samx as baseline (not monitored)
        scan_item2 = ScanItem(queue_id="queue2", scan_number=[2], scan_id=["scan_2"], status="open")
        scan_item2.status_message = messages.ScanStatusMessage(
            scan_id="scan_2", status="open", info={}
        )
        scan_item2.status_message.readout_priority = {"baseline": ["samx"]}

        def find_scan_side_effect(scan_id):
            if scan_id == "scan_1":
                return scan_item1
            elif scan_id == "scan_2":
                return scan_item2
            return None

        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = find_scan_side_effect

        # Check scan_1 - samx should be monitored
        mode1 = plugin._get_device_mode("samx", "samx", "scan_1")
        assert mode1 == "monitored"

        # Check scan_2 - samx should NOT be monitored (different scan)
        mode2 = plugin._get_device_mode("samx", "samx", "scan_2")
        assert mode2 is None


class TestDataSubscription:
    """Test suite for the DataSubscription class."""

    def test_create_subscription(self, data_api):
        """Test creating a subscription object."""
        sub = data_api.create_subscription("test_scan")
        assert sub.scan_id == "test_scan"
        assert sub.devices == []
        sub.close()

    def test_add_device_without_callback(self, data_api):
        """Test adding devices before setting a callback."""
        sub = data_api.create_subscription("test_scan")
        sub.add_device("samx", "samx")
        sub.add_device("samy", "samy")

        assert len(sub.devices) == 2
        assert ("samx", "samx") in sub.devices
        assert ("samy", "samy") in sub.devices
        sub.close()

    def test_add_device_with_callback(
        self, data_api, mock_callback, scan_item_with_monitored_devices
    ):
        """Test adding devices after setting a callback triggers immediate subscription."""
        sub = data_api.create_subscription("test_scan_id")
        sub.set_callback(mock_callback)
        sub.add_device("samx", "samx")

        assert len(sub.devices) == 1
        assert ("samx", "samx") in sub.devices
        sub.close()

    def test_set_callback_after_devices(
        self, data_api, mock_callback, scan_item_with_monitored_devices
    ):
        """Test setting callback after adding devices triggers subscription."""
        sub = data_api.create_subscription("test_scan_id")
        sub.add_device("samx", "samx")
        sub.add_device("samy", "samy")

        # Setting callback should subscribe all queued devices
        sub.set_callback(mock_callback)

        assert len(sub.devices) == 2
        sub.close()

    def test_method_chaining(self, data_api, mock_callback, scan_item_with_monitored_devices):
        """Test that methods support chaining."""
        sub = (
            data_api.create_subscription("test_scan_id")
            .add_device("samx", "samx")
            .add_device("samy", "samy")
            .set_callback(mock_callback)
        )

        assert len(sub.devices) == 2
        sub.close()

    def test_remove_device(self, data_api, mock_callback, scan_item_with_monitored_devices):
        """Test removing a device from subscription."""
        sub = data_api.create_subscription("test_scan_id")
        sub.set_callback(mock_callback)
        sub.add_device("samx", "samx")
        sub.add_device("samy", "samy")

        assert len(sub.devices) == 2

        sub.remove_device("samx", "samx")
        assert len(sub.devices) == 1
        assert ("samx", "samx") not in sub.devices
        assert ("samy", "samy") in sub.devices

        sub.close()

    def test_close_unsubscribes_all(
        self, data_api, mock_callback, scan_item_with_monitored_devices
    ):
        """Test that close() unsubscribes from all devices."""
        sub = data_api.create_subscription("test_scan_id")
        sub.set_callback(mock_callback)
        sub.add_device("samx", "samx")
        sub.add_device("samy", "samy")

        sub.close()

        # After close, devices should be cleared
        assert len(sub.devices) == 0

    def test_context_manager(self, data_api, mock_callback, scan_item_with_monitored_devices):
        """Test using subscription as a context manager."""
        with data_api.create_subscription("test_scan_id") as sub:
            sub.set_callback(mock_callback)
            sub.add_device("samx", "samx")
            assert len(sub.devices) == 1

        # After context exit, subscription should be closed
        with pytest.raises(RuntimeError, match="Cannot add device to a closed subscription"):
            sub.add_device("samy", "samy")

    def test_cannot_modify_closed_subscription(self, data_api, mock_callback):
        """Test that operations on closed subscription raise errors."""
        sub = data_api.create_subscription("test_scan")
        sub.close()

        with pytest.raises(RuntimeError, match="Cannot add device to a closed subscription"):
            sub.add_device("samx", "samx")

        with pytest.raises(RuntimeError, match="Cannot remove device from a closed subscription"):
            sub.remove_device("samx", "samx")

        with pytest.raises(RuntimeError, match="Cannot set callback on a closed subscription"):
            sub.set_callback(mock_callback)

        with pytest.raises(RuntimeError, match="Cannot reload a closed subscription"):
            sub.reload()

    def test_reload_resubscribes(self, data_api, mock_callback, scan_item_with_monitored_devices):
        """Test that reload() resubscribes to all devices."""
        sub = data_api.create_subscription("test_scan_id")
        sub.set_callback(mock_callback)
        sub.add_device("samx", "samx")

        # Reload should resubscribe
        sub.reload()

        assert len(sub.devices) == 1
        sub.close()

    def test_callback_change_resubscribes(self, data_api, scan_item_with_monitored_devices):
        """Test that changing callback resubscribes all devices."""
        calls1 = []
        calls2 = []

        def callback1(data, metadata):
            calls1.append((data, metadata))

        def callback2(data, metadata):
            calls2.append((data, metadata))

        sub = data_api.create_subscription("test_scan_id")
        sub.set_callback(callback1)
        sub.add_device("samx", "samx")

        # Change callback
        sub.set_callback(callback2)

        # Should still have the same device
        assert len(sub.devices) == 1
        sub.close()

    def test_add_duplicate_device(self, data_api, mock_callback, scan_item_with_monitored_devices):
        """Test adding the same device twice doesn't create duplicate subscriptions."""
        sub = data_api.create_subscription("test_scan_id")
        sub.set_callback(mock_callback)
        sub.add_device("samx", "samx")
        sub.add_device("samx", "samx")  # duplicate

        assert len(sub.devices) == 1
        sub.close()

    def test_remove_nonexistent_device(self, data_api, mock_callback):
        """Test removing a device that wasn't subscribed doesn't cause errors."""
        sub = data_api.create_subscription("test_scan")
        sub.set_callback(mock_callback)

        # Should not raise an error
        sub.remove_device("nonexistent", "signal")
        sub.close()

    def test_reload_without_callback(self, data_api):
        """Test that reload without callback logs warning and doesn't crash."""
        sub = data_api.create_subscription("test_scan")
        sub.add_device("samx", "samx")

        # Should not crash, just log warning
        sub.reload()
        sub.close()

    def test_destructor_cleanup(self, data_api, mock_callback, scan_item_with_monitored_devices):
        """Test that __del__ properly cleans up subscriptions."""
        sub = data_api.create_subscription("test_scan_id")
        sub.set_callback(mock_callback)
        sub.add_device("samx", "samx")

        # Explicitly delete the object
        del sub

        # Subscription should be cleaned up (this is implicitly tested by no errors)

    def test_set_scan_id(self, data_api, mock_callback, mock_client):
        """Test changing the scan_id."""
        # Setup two scan items
        scan_item1 = ScanItem(queue_id="queue1", scan_number=[1], scan_id=["scan_1"], status="open")
        scan_item1.status_message = messages.ScanStatusMessage(
            scan_id="scan_1", status="open", info={}
        )
        scan_item1.status_message.readout_priority = {"monitored": ["samx"]}
        scan_item1.live_data = LiveScanData()

        scan_item2 = ScanItem(queue_id="queue2", scan_number=[2], scan_id=["scan_2"], status="open")
        scan_item2.status_message = messages.ScanStatusMessage(
            scan_id="scan_2", status="open", info={}
        )
        scan_item2.status_message.readout_priority = {"monitored": ["samx"]}
        scan_item2.live_data = LiveScanData()

        def find_scan_side_effect(scan_id):
            if scan_id == "scan_1":
                return scan_item1
            elif scan_id == "scan_2":
                return scan_item2
            return None

        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = find_scan_side_effect

        sub = data_api.create_subscription("scan_1")
        sub.set_callback(mock_callback)
        sub.add_device("samx", "samx")

        assert sub.scan_id == "scan_1"

        # Change to new scan
        sub.set_scan_id("scan_2")
        assert sub.scan_id == "scan_2"

        # Devices should still be there
        assert len(sub.devices) == 1
        sub.close()

    def test_set_scan_id_without_callback(self, data_api):
        """Test changing scan_id when no callback is set yet."""
        sub = data_api.create_subscription("scan_1")
        sub.add_device("samx", "samx")

        # Should work without errors
        sub.set_scan_id("scan_2")
        assert sub.scan_id == "scan_2"
        assert len(sub.devices) == 1
        sub.close()

    def test_set_scan_id_same_value(self, data_api, mock_callback):
        """Test setting scan_id to the same value is a no-op."""
        sub = data_api.create_subscription("scan_1")
        sub.set_callback(mock_callback)

        # Should not trigger resubscription
        sub.set_scan_id("scan_1")
        assert sub.scan_id == "scan_1"
        sub.close()

    def test_set_scan_id_on_closed(self, data_api):
        """Test that changing scan_id on closed subscription raises error."""
        sub = data_api.create_subscription("scan_1")
        sub.close()

        with pytest.raises(RuntimeError, match="Cannot change scan_id on a closed subscription"):
            sub.set_scan_id("scan_2")

    def test_set_scan_id_method_chaining(self, data_api, mock_callback, mock_client):
        """Test that set_scan_id supports method chaining."""
        scan_item = ScanItem(queue_id="queue1", scan_number=[1], scan_id=["scan_2"], status="open")
        scan_item.status_message = messages.ScanStatusMessage(
            scan_id="scan_2", status="open", info={}
        )
        scan_item.status_message.readout_priority = {"monitored": ["samx"]}
        scan_item.live_data = LiveScanData()
        mock_client.queue.scan_storage.find_scan_by_ID.return_value = scan_item

        sub = (
            data_api.create_subscription("scan_1")
            .set_callback(mock_callback)
            .set_scan_id("scan_2")
            .add_device("samx", "samx")
        )

        assert sub.scan_id == "scan_2"
        assert len(sub.devices) == 1
        sub.close()


class TestDataSubscriptionBuffered:
    """Test suite for buffered mode in DataSubscription."""

    def test_create_buffered_subscription(self, data_api):
        """Test creating a buffered subscription."""
        sub = data_api.create_subscription("test_scan", buffered=True)
        assert sub.buffered is True
        assert sub.scan_id == "test_scan"
        sub.close()

    def test_create_non_buffered_subscription(self, data_api):
        """Test creating a non-buffered subscription (default)."""
        sub = data_api.create_subscription("test_scan")
        assert sub.buffered is False
        sub.close()

    def test_buffered_mode_accumulates_data(self, data_api, scan_item_with_monitored_devices):
        """Test that buffered mode accumulates and re-emits all data."""
        calls = []

        def callback(data, metadata):
            # Deep copy to capture state at callback time
            calls.append(copy.deepcopy(data))

        sub = data_api.create_subscription("test_scan_id", buffered=True)
        sub.set_callback(callback)
        sub.add_device("samx", "samx")

        # Simulate multiple data updates through the plugin
        plugin = data_api.plugins[0]

        # First data point
        scan_msg1 = messages.ScanMessage(
            point_id=0,
            scan_id="test_scan_id",
            data={"samx": {"samx": {"value": 1.0, "timestamp": 100.0}}},
            metadata={"scan_id": "test_scan_id"},
        )
        scan_item_with_monitored_devices.live_data.set(0, scan_msg1)
        plugin._handle_scan_segment_update(scan_msg1.content, scan_msg1.metadata)

        # Second data point
        scan_msg2 = messages.ScanMessage(
            point_id=1,
            scan_id="test_scan_id",
            data={"samx": {"samx": {"value": 2.0, "timestamp": 200.0}}},
            metadata={"scan_id": "test_scan_id"},
        )
        scan_item_with_monitored_devices.live_data.set(1, scan_msg2)
        plugin._handle_scan_segment_update(scan_msg2.content, scan_msg2.metadata)

        # In buffered mode, each callback should contain ALL accumulated data
        # The plugin processes all data in live_data each time, so we get multiple callbacks
        assert len(calls) >= 2

        # Find the last call - it should have all accumulated data
        last_call = calls[-1]
        assert "samx" in last_call
        assert "samx" in last_call["samx"]
        # Should be a list with all accumulated points
        assert isinstance(last_call["samx"]["samx"], list)
        assert len(last_call["samx"]["samx"]) >= 2
        # Verify the accumulated values are present
        values = [point["value"] for point in last_call["samx"]["samx"]]
        assert 1.0 in values
        assert 2.0 in values

        sub.close()

    def test_non_buffered_mode_emits_only_new(self, data_api, scan_item_with_monitored_devices):
        """Test that non-buffered mode only emits new data blocks."""
        calls = []

        def callback(data, metadata):
            calls.append(copy.deepcopy(data))

        sub = data_api.create_subscription("test_scan_id", buffered=False)
        sub.set_callback(callback)
        sub.add_device("samx", "samx")

        plugin = data_api.plugins[0]

        # First data point
        scan_msg1 = messages.ScanMessage(
            point_id=0,
            scan_id="test_scan_id",
            data={"samx": {"samx": {"value": 1.0, "timestamp": 100.0}}},
            metadata={"scan_id": "test_scan_id"},
        )
        scan_item_with_monitored_devices.live_data.set(0, scan_msg1)
        plugin._handle_scan_segment_update(scan_msg1.content, scan_msg1.metadata)

        # Second data point
        scan_msg2 = messages.ScanMessage(
            point_id=1,
            scan_id="test_scan_id",
            data={"samx": {"samx": {"value": 2.0, "timestamp": 200.0}}},
            metadata={"scan_id": "test_scan_id"},
        )
        scan_item_with_monitored_devices.live_data.set(1, scan_msg2)
        plugin._handle_scan_segment_update(scan_msg2.content, scan_msg2.metadata)

        # In non-buffered mode, each callback contains only individual data blocks
        assert len(calls) >= 2

        # All calls should have single value/timestamp dict (not a list)
        for call in calls:
            assert "samx" in call
            assert "samx" in call["samx"]
            # In non-buffered mode, data is NOT a list
            assert isinstance(call["samx"]["samx"], dict)
            assert "value" in call["samx"]["samx"]
            assert "timestamp" in call["samx"]["samx"]

        # Verify we got both values at some point
        values = [call["samx"]["samx"]["value"] for call in calls]
        assert 1.0 in values
        assert 2.0 in values

        sub.close()

    def test_set_buffered_mode(self, data_api):
        """Test changing buffered mode dynamically."""
        sub = data_api.create_subscription("test_scan", buffered=False)
        assert sub.buffered is False

        sub.set_buffered(True)
        assert sub.buffered is True

        sub.set_buffered(False)
        assert sub.buffered is False

        sub.close()

    def test_set_buffered_same_value_is_noop(self, data_api):
        """Test setting buffered to the same value is a no-op."""
        sub = data_api.create_subscription("test_scan", buffered=True)
        sub.set_buffered(True)  # No-op
        assert sub.buffered is True
        sub.close()

    def test_set_buffered_on_closed_raises_error(self, data_api):
        """Test that changing buffered mode on closed subscription raises error."""
        sub = data_api.create_subscription("test_scan")
        sub.close()

        with pytest.raises(
            RuntimeError, match="Cannot change buffered mode on a closed subscription"
        ):
            sub.set_buffered(True)

    def test_set_buffered_clears_buffer_when_disabling(
        self, data_api, scan_item_with_monitored_devices
    ):
        """Test that disabling buffered mode clears the accumulated buffer."""
        calls = []

        def callback(data, metadata):
            calls.append(copy.deepcopy(data))

        sub = data_api.create_subscription("test_scan_id", buffered=True)
        sub.set_callback(callback)
        sub.add_device("samx", "samx")

        plugin = data_api.plugins[0]

        # Add some data in buffered mode
        scan_msg1 = messages.ScanMessage(
            point_id=0,
            scan_id="test_scan_id",
            data={"samx": {"samx": {"value": 1.0, "timestamp": 100.0}}},
            metadata={"scan_id": "test_scan_id"},
        )
        scan_item_with_monitored_devices.live_data.set(0, scan_msg1)
        plugin._handle_scan_segment_update(scan_msg1.content, scan_msg1.metadata)

        assert len(calls) >= 1
        # Verify we got buffered data (as a list)
        assert isinstance(calls[-1]["samx"]["samx"], list)
        calls.clear()

        # Switch to non-buffered mode (should clear buffer)
        sub.set_buffered(False)

        # Add new data - should only get this new data, not accumulated buffer
        scan_msg2 = messages.ScanMessage(
            point_id=1,
            scan_id="test_scan_id",
            data={"samx": {"samx": {"value": 2.0, "timestamp": 200.0}}},
            metadata={"scan_id": "test_scan_id"},
        )
        scan_item_with_monitored_devices.live_data.set(1, scan_msg2)
        plugin._handle_scan_segment_update(scan_msg2.content, scan_msg2.metadata)

        assert len(calls) >= 1
        # Should have non-buffered data (dict, not list)
        assert isinstance(calls[-1]["samx"]["samx"], dict)
        assert calls[-1]["samx"]["samx"]["value"] == 2.0

        sub.close()

    def test_buffered_method_chaining(self, data_api, mock_callback):
        """Test that set_buffered supports method chaining."""
        sub = (
            data_api.create_subscription("test_scan").set_buffered(True).set_callback(mock_callback)
        )

        assert sub.buffered is True
        sub.close()

    def test_scan_id_change_clears_buffer(self, data_api, mock_client):
        """Test that changing scan_id clears the accumulated buffer."""
        calls = []

        def callback(data, metadata):
            calls.append(copy.deepcopy(data))

        # Setup two scans
        scan_item1 = ScanItem(queue_id="queue1", scan_number=[1], scan_id=["scan_1"], status="open")
        scan_item1.status_message = messages.ScanStatusMessage(
            scan_id="scan_1", status="open", info={}
        )
        scan_item1.status_message.readout_priority = {"monitored": ["samx"]}
        scan_item1.live_data = LiveScanData()

        scan_item2 = ScanItem(queue_id="queue2", scan_number=[2], scan_id=["scan_2"], status="open")
        scan_item2.status_message = messages.ScanStatusMessage(
            scan_id="scan_2", status="open", info={}
        )
        scan_item2.status_message.readout_priority = {"monitored": ["samx"]}
        scan_item2.live_data = LiveScanData()

        def find_scan_side_effect(scan_id):
            if scan_id == "scan_1":
                return scan_item1
            elif scan_id == "scan_2":
                return scan_item2
            return None

        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = find_scan_side_effect

        sub = data_api.create_subscription("scan_1", buffered=True)
        sub.set_callback(callback)
        sub.add_device("samx", "samx")

        plugin = data_api.plugins[0]

        # Add data to scan_1
        scan_msg1 = messages.ScanMessage(
            point_id=0,
            scan_id="scan_1",
            data={"samx": {"samx": {"value": 1.0, "timestamp": 100.0}}},
            metadata={"scan_id": "scan_1"},
        )
        scan_item1.live_data.set(0, scan_msg1)
        plugin._handle_scan_segment_update(scan_msg1.content, scan_msg1.metadata)

        assert len(calls) == 1
        calls.clear()

        # Change scan_id (should clear buffer)
        sub.set_scan_id("scan_2")

        # Add data to scan_2 - should start fresh buffer
        scan_msg2 = messages.ScanMessage(
            point_id=0,
            scan_id="scan_2",
            data={"samx": {"samx": {"value": 2.0, "timestamp": 200.0}}},
            metadata={"scan_id": "scan_2"},
        )
        scan_item2.live_data.set(0, scan_msg2)
        plugin._handle_scan_segment_update(scan_msg2.content, scan_msg2.metadata)

        assert len(calls) == 1
        # Should only have data from scan_2 (one point)
        assert len(calls[0]["samx"]["samx"]) == 1
        assert calls[0]["samx"]["samx"][0]["value"] == 2.0

        sub.close()
