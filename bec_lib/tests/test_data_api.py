"""Tests for the DataAPI v2 facade and live plugin."""

import gc
import time
import weakref
from unittest import mock

import numpy as np
import pytest

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.data_api import CorrelationGroupError, DataAPI
from bec_lib.endpoints import MessageEndpoints
from bec_lib.live_scan_data import LiveScanData
from bec_lib.scan_items import ScanItem

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name

ASYNC_WAVE_INFO = (
    "det",
    None,
    {
        "obj_name": "wave",
        "storage_name": "wave",
        "describe": {"signal_info": {"acquisition_group": "monitored"}},
    },
)


@pytest.fixture
def mock_client(connected_connector):
    client = mock.MagicMock(spec=BECClient)
    client.started = True
    client.connector = connected_connector
    client.callbacks = mock.MagicMock()
    client.callbacks.register = mock.MagicMock(return_value="callback_id")
    client.queue = mock.MagicMock()
    client.queue.scan_storage.current_scan_id = []
    client.device_manager = mock.MagicMock()
    client.device_manager.get_bec_signals.return_value = []
    return client


@pytest.fixture
def data_api(mock_client):
    DataAPI.clear_instance()
    api = DataAPI(mock_client)
    yield api
    DataAPI.clear_instance()


def make_scan_item(mock_client, scan_id="scan_1", monitored=("samx", "samy"), status="open"):
    item = ScanItem(queue_id="q", scan_number=[1], scan_id=[scan_id], status=status)
    item.status_message = messages.ScanStatusMessage(scan_id=scan_id, status=status, info={})
    item.status_message.readout_priority = {"monitored": list(monitored), "baseline": []}
    item.live_data = LiveScanData()
    return item


def register_scan(mock_client, items):
    mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: items.get(sid)


def seed_xy(item, scan_id, start, stop):
    for i in range(start, stop):
        msg = messages.ScanMessage(
            point_id=i,
            scan_id=scan_id,
            data={
                "samx": {"samx": {"value": float(i), "timestamp": 100.0 + i}},
                "samy": {"samy": {"value": float(i) + 100.0, "timestamp": 100.0 + i}},
            },
            metadata={"scan_id": scan_id},
        )
        item.live_data.set(i, msg)


def async_msg(entry, value, ordinal, update_type="add", ts=200.0, slice_index=None):
    if update_type == "add":
        update = messages.DeviceAsyncUpdate(type="add", max_shape=[None])
    elif update_type == "add_slice":
        update = messages.DeviceAsyncUpdate(
            type="add_slice", index=slice_index, max_shape=[None, None]
        )
    else:
        update = messages.DeviceAsyncUpdate(type="replace")
    return messages.DeviceMessage(
        signals={entry: {"value": value, "timestamp": ts}},
        metadata={
            "timestamp": ts,
            "async_indices": {entry: ordinal},
            "async_update": update.model_dump(),
        },
    )


class TestLiveSubscription:
    def _subscribe_xy(self, data_api, mock_client, scan_id="scan_1", **kwargs):
        item = make_scan_item(mock_client, scan_id)
        register_scan(mock_client, {scan_id: item})
        mock_client.queue.scan_storage.current_scan_id = [scan_id]
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
            **kwargs,
        )
        return sub, item, updates

    def test_backfill_on_subscribe(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        seed_xy(item, "scan_1", 0, 3)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
        )
        assert updates and updates[-1].reason == "backfill"
        cols = updates[-1].aligned()
        assert cols[("samx", "samx")] == (0.0, 1.0, 2.0)
        assert cols[("samy", "samy")] == (100.0, 101.0, 102.0)
        assert updates[-1].complete
        sub.close()

    def test_live_segments_extend_series(self, data_api, mock_client):
        sub, item, updates = self._subscribe_xy(data_api, mock_client)
        plugin = data_api.plugins[0]
        seed_xy(item, "scan_1", 0, 2)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        assert updates[-1].aligned_ordinals == (0, 1)
        assert updates[-1].reason == "live"
        sub.close()

    def test_async_join_by_ordinal_with_gap(self, data_api, mock_client):
        """Async ordinal 1 arriving late fills a hole instead of shifting the
        pairing — the core redesign guarantee."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
        )
        plugin = data_api.plugins[0]
        seed_xy(item, "scan_1", 0, 3)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})

        plugin._on_async_message({"data": async_msg("wave", 10.0, 0)}, "scan_1", "det")
        plugin._on_async_message({"data": async_msg("wave", 30.0, 2)}, "scan_1", "det")
        cols = updates[-1].aligned()
        assert updates[-1].aligned_ordinals == (0, 2)
        assert cols[("samx", "samx")] == (0.0, 2.0)
        assert cols[("det", "wave")] == (10.0, 30.0)
        assert not updates[-1].complete

        plugin._on_async_message({"data": async_msg("wave", 20.0, 1)}, "scan_1", "det")
        assert updates[-1].aligned_ordinals == (0, 1, 2)
        assert updates[-1].aligned()[("det", "wave")] == (10.0, 20.0, 30.0)
        assert updates[-1].complete
        sub.close()

    def test_add_slice_rows_accumulate(self, data_api, mock_client):
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("det", "wave")], scan="live", callback=updates.append, min_emit_interval=0
        )
        plugin = data_api.plugins[0]
        plugin._on_async_message(
            {"data": async_msg("wave", [1, 2], 0, "add_slice", slice_index=0)}, "scan_1", "det"
        )
        plugin._on_async_message(
            {"data": async_msg("wave", [3], 1, "add_slice", slice_index=0)}, "scan_1", "det"
        )
        plugin._on_async_message(
            {"data": async_msg("wave", [4], 2, "add_slice", slice_index=1)}, "scan_1", "det"
        )
        source = updates[-1].get("det", "wave")
        assert source.ordinals == (0, 1)
        assert source.values == ([1, 2, 3], [4])
        assert source.metadata["async_update_type"] == "add_slice"
        sub.close()

    def test_replace_source_is_single_point(self, data_api, mock_client):
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("det", "wave")], scan="live", callback=updates.append, min_emit_interval=0
        )
        plugin = data_api.plugins[0]
        plugin._on_async_message({"data": async_msg("wave", [1], 0, "replace")}, "scan_1", "det")
        plugin._on_async_message({"data": async_msg("wave", [1, 2], 1, "replace")}, "scan_1", "det")
        source = updates[-1].get("det", "wave")
        assert source.ordinals == (0,)
        assert source.values == ([1, 2],)
        sub.close()

    def test_mixed_sources_partition_into_groups(self, data_api, mock_client):
        """Monitored + non-monitored async sources are served as separate
        correlation groups, each with its own aligned emissions."""
        mock_client.device_manager.get_bec_signals.return_value = [
            (
                "det",
                None,
                {
                    "obj_name": "wave",
                    "storage_name": "wave",
                    "describe": {"signal_info": {"acquisition_group": "grp1"}},
                },
            )
        ]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
        )
        plugin = data_api.plugins[0]
        seed_xy(item, "scan_1", 0, 2)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        plugin._on_async_message({"data": async_msg("wave", 10.0, 0)}, "scan_1", "det")

        groups = {u.metadata.get("group") for u in updates}
        assert groups == {"scan", "async:grp1"}
        scan_updates = [u for u in updates if u.metadata.get("group") == "scan"]
        async_updates = [u for u in updates if u.metadata.get("group") == "async:grp1"]
        assert scan_updates[-1].aligned()[("samx", "samx")] == (0.0, 1.0)
        assert set(scan_updates[-1].sources) == {("samx", "samx")}
        assert async_updates[-1].aligned()[("det", "wave")] == (10.0,)
        sub.close()

    def test_set_sources_atomic_rebind(self, data_api, mock_client):
        sub, item, updates = self._subscribe_xy(data_api, mock_client)
        seed_xy(item, "scan_1", 0, 2)
        data_api.plugins[0]._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        sub.set_sources([("samx", "samx")])
        assert set(updates[-1].sources) == {("samx", "samx")}
        assert updates[-1].aligned()[("samx", "samx")] == (0.0, 1.0)
        sub.close()

    def test_rate_limit_delivers_trailing_state(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0.15,
        )
        plugin = data_api.plugins[0]
        for i in range(5):
            seed_xy(item, "scan_1", i, i + 1)
            plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        # Burst is coalesced, but the trailing timer must deliver the final state.
        deadline = time.time() + 2
        while time.time() < deadline:
            if updates and len(updates[-1].aligned_ordinals) == 5:
                break
            time.sleep(0.02)
        assert len(updates[-1].aligned_ordinals) == 5
        assert len(updates) < 6
        sub.close()

    def test_scan_rollover_and_terminal_flush(self, data_api, mock_client):
        item1 = make_scan_item(mock_client, "scan_1")
        item2 = make_scan_item(mock_client, "scan_2")
        register_scan(mock_client, {"scan_1": item1, "scan_2": item2})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
        )
        seed_xy(item1, "scan_1", 0, 1)
        data_api.plugins[0]._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})

        sub._on_scan_status({"scan_id": "scan_2", "status": "open"}, {})
        assert sub.scan_id == "scan_2"
        seed_xy(item2, "scan_2", 0, 2)
        data_api.plugins[0]._on_scan_segment({"scan_id": "scan_2"}, {"scan_id": "scan_2"})
        assert updates[-1].scan_id == "scan_2"
        assert updates[-1].aligned_ordinals == (0, 1)

        n_before = len(updates)
        sub._on_scan_status({"scan_id": "scan_2", "status": "closed"}, {})
        assert len(updates) == n_before + 1  # final flush
        sub.close()

    def test_unbound_sources_reported(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1", monitored=("samx",))
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("ghost", "ghost")], scan="live", callback=lambda u: None
        )
        assert sub.unbound_sources == [("ghost", "ghost")]
        sub.close()

    def test_unknown_fixed_scan_raises(self, data_api, mock_client):
        register_scan(mock_client, {})
        with pytest.raises(ValueError):
            data_api.subscribe(sources=[("samx", "samx")], scan="nope", callback=lambda u: None)

    def test_live_plugin_refuses_terminal_scan(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1", status="closed")
        register_scan(mock_client, {"scan_1": item})
        assert data_api.plugins[0].resolve([("samx", "samx")], "scan_1") is None


class TestLifecycle:
    def test_per_client_instances(self, mock_client):
        DataAPI.clear_instance()
        client_b = mock.MagicMock(spec=BECClient)
        client_b.callbacks = mock.MagicMock()
        client_b.queue = mock.MagicMock()
        client_b.device_manager = mock.MagicMock()
        api_a = DataAPI(mock_client)
        api_b = DataAPI(client_b)
        try:
            assert api_a is not api_b
            assert DataAPI(mock_client) is api_a
        finally:
            DataAPI.clear_instance()

    def test_close_removes_instance(self, mock_client):
        DataAPI.clear_instance()
        api = DataAPI(mock_client)
        api.close()
        fresh = DataAPI(mock_client)
        try:
            assert fresh is not api
            assert fresh.plugins
        finally:
            DataAPI.clear_instance()

    def test_subscription_collectable_after_del(self, data_api):
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="live", callback=lambda u: None)
        ref = weakref.ref(sub)
        del sub
        gc.collect()
        assert ref() is None

    def test_close_releases_plugin_request(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")], scan="live", callback=lambda u: None
        )
        plugin = data_api.plugins[0]
        assert plugin._requests
        sub.close()
        assert not plugin._requests


class TestLegacyAsyncAndAxis:
    def test_legacy_async_device_served_unindexed(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1", monitored=())
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        legacy_dev = mock.MagicMock()
        legacy_dev.readout_priority = "async"
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            legacy_dev if name == "mca" else default
        )

        updates = []
        sub = data_api.subscribe(
            sources=[("mca", "mca")], scan="live", callback=updates.append, min_emit_interval=0
        )
        assert sub.unbound_sources == []
        plugin = data_api.plugins[0]
        for i, value in enumerate([[1, 2], [3, 4]]):
            msg = messages.DeviceMessage(
                signals={"mca": {"value": value, "timestamp": 100.0 + i}}, metadata={}
            )
            plugin._on_async_message({"data": msg}, "scan_1", "mca")

        source = updates[-1].get("mca", "mca")
        assert source.kind == "unindexed"
        assert source.ordinals == (0, 1)
        assert source.values == ([1, 2], [3, 4])
        assert updates[-1].metadata["group"] == "standalone:mca/mca"
        sub.close()

    def test_axis_modes(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        seed_xy(item, "scan_1", 0, 3)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
        )
        update = updates[-1]
        assert update.axis("index") == (0, 1, 2)
        assert update.axis("device", ("samx", "samx")) == (0.0, 1.0, 2.0)
        assert update.axis("timestamp", ("samx", "samx")) == (100.0, 101.0, 102.0)
        sub.close()


class TestDeviceStreams:
    def _plugin(self, data_api):
        return next(p for p in data_api.plugins if p.priority == 90)

    def test_readback_stream(self, data_api, mock_client):
        motor = mock.MagicMock()
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            motor if name == "samx" else default
        )
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
            max_points=3,
        )
        assert sub.unbound_sources == []
        plugin = self._plugin(data_api)
        for i in range(5):
            msg = messages.DeviceMessage(
                signals={"samx": {"value": float(i), "timestamp": 100.0 + i}}, metadata={}
            )
            plugin._on_stream_message({"data": msg}, ("readback", "samx", "samx"))
        source = updates[-1].get("samx", "samx")
        # Retention cap keeps only the newest points.
        assert source.values == (2.0, 3.0, 4.0)
        assert source.ordinals == (2, 3, 4)
        assert source.kind == "unindexed"
        sub.close()
        assert not plugin._feeds

    def test_monitor_stream_sentinel_and_scan_metadata(self, data_api, mock_client):
        updates = []
        sub = data_api.subscribe(
            sources=[("waveform", "monitor_1d")],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
        )
        plugin = self._plugin(data_api)
        msg = mock.MagicMock()
        msg.data = [1, 2, 3]
        msg.metadata = {"scan_id": "scan_7"}
        plugin._on_stream_message({"data": msg}, ("monitor_1d", "waveform", "monitor_1d"))
        source = updates[-1].get("waveform", "monitor_1d")
        assert source.values == ([1, 2, 3],)
        assert source.metadata["scan_id"] == "scan_7"
        assert source.metadata["stream"] == "monitor_1d"
        sub.close()

    def test_preview_signal_classification(self, data_api, mock_client):
        dev = mock.MagicMock()
        dev._info = {
            "signals": {"preview": {"obj_name": "preview_img", "signal_class": "PreviewSignal"}}
        }
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            dev if name == "eiger" else default
        )
        plugin = self._plugin(data_api)
        specs = plugin.resolve([("eiger", "preview_img")], "")
        assert specs[0].storage_name == "preview"

        updates = []
        sub = data_api.subscribe(
            sources=[("eiger", "preview_img")],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
        )
        msg = mock.MagicMock()
        msg.data = [[1, 2], [3, 4]]
        msg.metadata = {}
        plugin._on_stream_message({"data": msg}, ("preview", "eiger", "preview_img"))
        assert updates[-1].get("eiger", "preview_img").values == ([[1, 2], [3, 4]],)
        sub.close()

    def test_unknown_device_unavailable(self, data_api, mock_client):
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: default
        sub = data_api.subscribe(sources=[("ghost", "ghost")], scan=None, callback=lambda u: None)
        assert sub.unbound_sources == [("ghost", "ghost")]
        sub.close()

    def test_scan_plugins_decline_device_scope(self, data_api, mock_client):
        assert data_api.plugins[0].resolve([("samx", "samx")], "") is None
        assert data_api.plugins[1].resolve([("samx", "samx")], "") is None


class TestDeviceStreamsThroughRealConnector:
    """Integration tests over the real (fakeredis) connector: they exercise the
    actual callback payload shapes, which differ between pubsub (MessageObject)
    and stream (dict) registrations."""

    def _pump(self, connector, predicate, timeout=5.0):
        """Drive the connector message loop until the predicate holds."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            try:
                connector.poll_messages(timeout=0.05)
            except Exception:
                time.sleep(0.02)
        return predicate()

    def test_readback_pubsub_payload_reaches_subscriber(self, data_api, mock_client):
        motor = mock.MagicMock()
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            motor if name == "samx" else default
        )
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan=None, callback=updates.append, min_emit_interval=0
        )

        for position in (1.5, 2.5):
            mock_client.connector.set_and_publish(
                MessageEndpoints.device_readback("samx"),
                messages.DeviceMessage(
                    signals={"samx": {"value": position, "timestamp": 100.0 + position}},
                    metadata={},
                ),
            )

        assert self._pump(
            mock_client.connector,
            lambda: bool(updates)
            and updates[-1].get("samx", "samx") is not None
            and len(updates[-1].get("samx", "samx").values) == 2,
        ), "readback positions never reached the subscriber"
        assert updates[-1].get("samx", "samx").values == (1.5, 2.5)
        sub.close()

    def test_monitor_stream_payload_reaches_subscriber(self, data_api, mock_client):
        updates = []
        sub = data_api.subscribe(
            sources=[("waveform", "monitor_1d")],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
        )
        mock_client.connector.xadd(
            MessageEndpoints.device_monitor_1d("waveform"),
            {"data": messages.DeviceMonitor1DMessage(device="waveform", data=np.array([1, 2, 3]))},
        )
        assert self._pump(
            mock_client.connector,
            lambda: bool(updates)
            and updates[-1].get("waveform", "monitor_1d") is not None
            and bool(updates[-1].get("waveform", "monitor_1d").values),
        ), "monitor payload never reached the subscriber"
        source = updates[-1].get("waveform", "monitor_1d")
        assert list(source.values[-1]) == [1, 2, 3]
        sub.close()
