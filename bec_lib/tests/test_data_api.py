"""Tests for the DataAPI v2 facade and live plugin."""

import gc
import time
import weakref
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

from bec_lib import messages
from bec_lib.callback_handler import CallbackHandler
from bec_lib.client import BECClient
from bec_lib.data_api import CorrelationGroupError, DataAPI
from bec_lib.data_api.data_path import BindContext
from bec_lib.data_api.models import CONTINUOUS_GROUP, SCAN_GROUP, CorrelationGroup
from bec_lib.data_api.paths.redis import RedisDataPath
from bec_lib.data_api.scenarios import Scenario, classify
from bec_lib.endpoints import MessageEndpoints
from bec_lib.live_scan_data import LiveScanData
from bec_lib.scan_items import ScanItem

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name

ASYNC_WAVE_INFO = ("det", None, {"obj_name": "wave", "storage_name": "wave"})


def multi_signal_info(device: str, storage: str, names: tuple[str, ...]) -> list[tuple]:
    """Device-manager entries of the sub-signals of one AsyncMultiSignal."""
    return [
        (
            device,
            f"{storage}.{name}",
            {
                "signal_class": "AsyncMultiSignal",
                "obj_name": f"{storage}_{name}",
                "storage_name": storage,
            },
        )
        for name in names
    ]


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
    client.history = None
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
        assert list(cols[("samx", "samx")]) == [0.0, 1.0, 2.0]
        assert list(cols[("samy", "samy")]) == [100.0, 101.0, 102.0]
        assert updates[-1].complete
        sub.close()

    def test_subscribe_vs_scan_segment_lock_order(self, data_api, mock_client):
        """ABBA guard: subscribing on the GUI thread while the dispatcher
        holds the scan-manager lock and delivers a segment must not deadlock
        (reproduces the mid-scan plot() freeze; scan-storage reads must happen
        before the DataAPI lock is taken)."""
        import threading

        item = make_scan_item(mock_client, "scan_1")
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        seed_xy(item, "scan_1", 0, 2)
        plugin = data_api.paths[0]

        scan_lock = threading.RLock()  # emulates the @threadlocked scan storage
        dispatcher_has_lock = threading.Event()
        main_in_subscribe = threading.Event()

        def locked_find(scan_id):
            if not scan_lock.acquire(timeout=5):
                raise TimeoutError("scan-storage lock not acquired: ABBA deadlock")
            try:
                return item if scan_id == "scan_1" else None
            finally:
                scan_lock.release()

        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = locked_find

        def dispatcher():
            with scan_lock:
                dispatcher_has_lock.set()
                main_in_subscribe.wait(2)
                time.sleep(0.05)  # let subscribe reach its lock acquisition
                plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})

        result = {}

        def gui_subscribe():
            dispatcher_has_lock.wait(2)
            main_in_subscribe.set()
            result["sub"] = data_api.subscribe(
                sources=[("samx", "samx"), ("samy", "samy")],
                scan="live",
                callback=lambda u: None,
                min_emit_interval=0,
            )

        threads = [
            threading.Thread(target=dispatcher, daemon=True),
            threading.Thread(target=gui_subscribe, daemon=True),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert all(not t.is_alive() for t in threads), "deadlocked"
        assert "sub" in result, "subscribe failed (lock-order inversion)"
        result["sub"].close()

    def test_estimate_bytes_vs_scan_segment_lock_order(self, data_api, mock_client):
        """Same ABBA guard for ``estimate_bytes``: it must read scan storage
        before taking the DataAPI lock, otherwise a GUI-thread size check
        deadlocks against a dispatcher segment delivered mid-scan."""
        import threading

        item = make_scan_item(mock_client, "scan_1")
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        seed_xy(item, "scan_1", 0, 2)
        plugin = data_api.paths[0]

        scan_lock = threading.RLock()
        dispatcher_has_lock = threading.Event()
        main_in_estimate = threading.Event()

        def locked_find(scan_id):
            if not scan_lock.acquire(timeout=5):
                raise TimeoutError("scan-storage lock not acquired: ABBA deadlock")
            try:
                return item if scan_id == "scan_1" else None
            finally:
                scan_lock.release()

        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = locked_find
        # A bound subscription is what makes the dispatcher need the DataAPI lock.
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=lambda u: None, min_emit_interval=0
        )

        def dispatcher():
            with scan_lock:
                dispatcher_has_lock.set()
                main_in_estimate.wait(2)
                time.sleep(0.05)
                plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})

        result = {}

        def gui_estimate():
            dispatcher_has_lock.wait(2)
            main_in_estimate.set()
            result["estimate"] = data_api.estimate_bytes([("samx", "samx")], "scan_1")

        threads = [
            threading.Thread(target=dispatcher, daemon=True),
            threading.Thread(target=gui_estimate, daemon=True),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert all(not t.is_alive() for t in threads), "deadlocked"
        assert "estimate" in result, "estimate_bytes failed (lock-order inversion)"
        sub.close()

    def test_bind_reads_scan_storage_once_before_the_lock(self, data_api, mock_client):
        """Deterministic ABBA reproduction for the bind path itself: the
        dispatcher takes the scan-manager lock right after subscribe's
        prefetch. If ``resolve``/``open`` re-read scan storage under the
        DataAPI lock, subscribe deadlocks against the segment callback; with
        the bind context the storage is read exactly once, before the lock."""
        import threading

        item = make_scan_item(mock_client, "scan_1")
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        seed_xy(item, "scan_1", 0, 2)
        register_scan(mock_client, {"scan_1": item})
        plugin = data_api.paths[0]
        # An already-bound subscription makes the dispatcher need the DataAPI lock.
        first = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=lambda u: None, min_emit_interval=0
        )

        scan_lock = threading.RLock()
        dispatcher_has_lock = threading.Event()
        gui_read_under_lock = threading.Event()
        gui_reads = []

        def locked_find(scan_id):
            if threading.current_thread().name == "gui":
                gui_reads.append(scan_id)
                if len(gui_reads) > 1:
                    # A second GUI-thread read can only come from under the
                    # DataAPI lock: hand the scan lock to the dispatcher first.
                    gui_read_under_lock.set()
                    dispatcher_has_lock.wait(2)
            if not scan_lock.acquire(timeout=5):
                raise TimeoutError("scan-storage lock not acquired: ABBA deadlock")
            try:
                return item if scan_id == "scan_1" else None
            finally:
                scan_lock.release()

        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = locked_find

        def dispatcher():
            gui_read_under_lock.wait(0.5)  # returns early only when the bug is present
            with scan_lock:
                dispatcher_has_lock.set()
                plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})

        result = {}

        def gui():
            result["sub"] = data_api.subscribe(
                sources=[("samy", "samy")],
                scan="scan_1",
                callback=lambda u: None,
                min_emit_interval=0,
            )

        threads = [
            threading.Thread(target=dispatcher, name="dispatcher", daemon=True),
            threading.Thread(target=gui, name="gui", daemon=True),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert all(not t.is_alive() for t in threads), "deadlocked"
        assert "sub" in result, "subscribe failed (lock-order inversion)"
        assert len(gui_reads) == 1, "scan storage read more than once per bind"
        first.close()
        result["sub"].close()

    def test_close_vs_dispatcher_callback_lock_order(self, data_api, mock_client):
        """ABBA guard: the client's callback handler holds its own lock while
        dispatching to our handlers (which take the DataAPI lock), so close()
        must never call into the handler while holding the DataAPI lock."""
        import threading

        original_callbacks = mock_client.callbacks
        handler = CallbackHandler()
        mock_client.callbacks = handler
        try:
            item = make_scan_item(mock_client, "scan_1")
            register_scan(mock_client, {"scan_1": item})
            mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
            plugin = data_api.paths[0]
            in_close = threading.Event()
            handler_lock_held = threading.Event()

            class Flag:
                def mark(self, *_args, **_kwargs):
                    handler_lock_held.set()

            flag = Flag()
            # Dispatch order: the flag first, then the data path's own handler
            # (which needs the DataAPI lock), both under the handler lock.
            handler.register("scan_segment", flag.mark)
            handler.register("scan_segment", plugin._on_scan_segment)
            sub = data_api.subscribe(
                sources=[("samx", "samx")],
                scan="live",
                callback=lambda u: None,
                min_emit_interval=0,
            )
            assert sub._callback_ids

            original_close = plugin.close

            def slow_close(request):  # runs under the DataAPI lock inside close()
                in_close.set()
                handler_lock_held.wait(1)
                original_close(request)

            plugin.close = slow_close

            def dispatcher():
                in_close.wait(2)
                handler.run("scan_segment", {"scan_id": "scan_1"}, {"scan_id": "scan_1"})

            threads = [
                threading.Thread(target=dispatcher, daemon=True),
                threading.Thread(target=sub.close, daemon=True),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            assert all(not thread.is_alive() for thread in threads), "deadlocked"
            assert all(i not in handler.callbacks for i in sub._callback_ids) and sub._closed
        finally:
            mock_client.callbacks = original_callbacks

    def test_set_min_emit_interval_live_change(self, data_api, mock_client):
        """A rate change applies to subsequent emissions without a rebuild."""
        sub, item, updates = self._subscribe_xy(data_api, mock_client)
        plugin = data_api.paths[0]
        assert sub.min_emit_interval == 0
        sub.set_min_emit_interval(3600.0)
        assert sub.min_emit_interval == 3600.0
        n_before = len(updates)
        # the initial backfill emission armed the limiter, so every live
        # emission within the huge interval is coalesced
        seed_xy(item, "scan_1", 0, 2)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        assert len(updates) == n_before
        sub.set_min_emit_interval(-5)  # clamped to 0 -> uncoalesced again
        assert sub.min_emit_interval == 0.0
        seed_xy(item, "scan_1", 2, 3)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        assert len(updates) == n_before + 1
        sub.close()

    def test_live_segments_extend_series(self, data_api, mock_client):
        sub, item, updates = self._subscribe_xy(data_api, mock_client)
        plugin = data_api.paths[0]
        seed_xy(item, "scan_1", 0, 2)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        assert list(updates[-1].aligned_ordinals) == [0, 1]
        assert updates[-1].reason == "live"
        sub.close()

    def test_async_join_by_ordinal_with_gap(self, data_api, mock_client):
        """Async ordinal 1 arriving late fills a hole instead of shifting the
        pairing — the core redesign guarantee (sub-signals of one AsyncMultiSignal)."""
        mock_client.device_manager.get_bec_signals.return_value = multi_signal_info(
            "det", "det_group", ("x", "y")
        )
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        keys = [("det", "det_group_x"), ("det", "det_group_y")]
        updates = []
        sub = data_api.subscribe(
            sources=keys, scan="live", callback=updates.append, min_emit_interval=0
        )
        assert sub.groups == {CorrelationGroup.multi("det", "det_group"): keys}
        plugin = data_api.paths[0]

        def multi_msg(x, y, ordinal):
            return messages.DeviceMessage(
                signals={
                    "det_group_x": {"value": x, "timestamp": 200.0 + ordinal},
                    "det_group_y": {"value": y, "timestamp": 200.0 + ordinal},
                },
                metadata={
                    "async_indices": {"det_group_x": ordinal, "det_group_y": ordinal},
                    "async_update": messages.DeviceAsyncUpdate(
                        type="add", max_shape=[None]
                    ).model_dump(),
                },
            )

        plugin._on_async_message({"data": multi_msg(0.0, 10.0, 0)}, "scan_1", "det")
        plugin._on_async_message({"data": multi_msg(2.0, 30.0, 2)}, "scan_1", "det")
        cols = updates[-1].aligned()
        assert list(updates[-1].aligned_ordinals) == [0, 2]
        assert list(cols[keys[0]]) == [0.0, 2.0]
        assert list(cols[keys[1]]) == [10.0, 30.0]
        assert not updates[-1].complete

        plugin._on_async_message({"data": multi_msg(1.0, 20.0, 1)}, "scan_1", "det")
        assert list(updates[-1].aligned_ordinals) == [0, 1, 2]
        assert list(updates[-1].aligned()[keys[1]]) == [10.0, 20.0, 30.0]
        assert updates[-1].complete
        sub.close()

    def test_async_multi_sub_signal_keys_by_obj_name(self, data_api, mock_client):
        """Multi-sub-signal async streams: the stream is keyed by the group's
        storage_name, but the DeviceMessage ``signals`` and ``async_indices``
        carry full obj_names (the producer normalizes bare sub-signal keys to
        ``f"{group}_{sub}"``) — lookups by ``spec.entry`` must therefore hit."""
        mock_client.device_manager.get_bec_signals.return_value = multi_signal_info(
            "det", "det_group", ("data1", "data2")
        )
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("det", "det_group_data1"), ("det", "det_group_data2")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
        )
        plugin = data_api.paths[0]
        msg = messages.DeviceMessage(
            signals={
                "det_group_data1": {"value": 1.0, "timestamp": 200.0},
                "det_group_data2": {"value": 2.0, "timestamp": 200.0},
            },
            metadata={
                "timestamp": 200.0,
                "async_indices": {"det_group_data1": 0, "det_group_data2": 0},
                "async_update": messages.DeviceAsyncUpdate(
                    type="add", max_shape=[None]
                ).model_dump(),
            },
        )
        plugin._on_async_message({"data": msg}, "scan_1", "det")
        cols = updates[-1].aligned()
        assert list(cols[("det", "det_group_data1")]) == [1.0]
        assert list(cols[("det", "det_group_data2")]) == [2.0]
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
        plugin = data_api.paths[0]
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
        assert list(source.ordinals) == [0, 1]
        assert [list(row) for row in source.values] == [[1, 2, 3], [4]]
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
        plugin = data_api.paths[0]
        plugin._on_async_message({"data": async_msg("wave", [1], 0, "replace")}, "scan_1", "det")
        plugin._on_async_message({"data": async_msg("wave", [1, 2], 1, "replace")}, "scan_1", "det")
        source = updates[-1].get("det", "wave")
        assert list(source.ordinals) == [0]
        assert list(source.values) == [[1, 2]]
        sub.close()

    def test_mixed_sources_partition_into_groups(self, data_api, mock_client):
        """Monitored + non-monitored async sources are served as separate
        correlation groups, each with its own aligned emissions."""
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
        plugin = data_api.paths[0]
        seed_xy(item, "scan_1", 0, 2)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        plugin._on_async_message({"data": async_msg("wave", 10.0, 0)}, "scan_1", "det")

        groups = {u.metadata.get("group") for u in updates}
        assert groups == {SCAN_GROUP, CorrelationGroup.standalone("det", "wave")}
        scan_updates = [u for u in updates if u.metadata.get("group") == SCAN_GROUP]
        async_updates = [
            u
            for u in updates
            if u.metadata.get("group") == CorrelationGroup.standalone("det", "wave")
        ]
        assert list(scan_updates[-1].aligned()[("samx", "samx")]) == [0.0, 1.0]
        assert set(scan_updates[-1].sources) == {("samx", "samx")}
        assert list(async_updates[-1].aligned()[("det", "wave")]) == [10.0]
        sub.close()

    def test_set_sources_atomic_rebind(self, data_api, mock_client):
        sub, item, updates = self._subscribe_xy(data_api, mock_client)
        seed_xy(item, "scan_1", 0, 2)
        data_api.paths[0]._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        sub.set_sources([("samx", "samx")])
        assert updates[-1].reason == "rebind"
        assert set(updates[-1].sources) == {("samx", "samx")}
        assert list(updates[-1].aligned()[("samx", "samx")]) == [0.0, 1.0]
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
        plugin = data_api.paths[0]
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
        data_api.paths[0]._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})

        sub._on_scan_status({"scan_id": "scan_2", "status": "open"}, {})
        assert sub.scan_id == "scan_2"
        assert updates[-1].reason == "rebind" and updates[-1].scan_id == "scan_2"
        seed_xy(item2, "scan_2", 0, 2)
        data_api.paths[0]._on_scan_segment({"scan_id": "scan_2"}, {"scan_id": "scan_2"})
        assert updates[-1].scan_id == "scan_2"
        assert list(updates[-1].aligned_ordinals) == [0, 1]

        n_before = len(updates)
        sub._on_scan_status({"scan_id": "scan_2", "status": "closed"}, {})
        assert len(updates) == n_before + 1  # final flush
        sub.close()

    def test_concrete_scan_retries_unavailable_sources_and_flushes_on_close(
        self, data_api, mock_client
    ):
        item = make_scan_item(mock_client, "scan_1", monitored=("samx",))
        other = make_scan_item(mock_client, "scan_2")
        register_scan(mock_client, {"scan_1": item, "scan_2": other})
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")],
            scan="scan_1",
            callback=updates.append,
            min_emit_interval=0,
        )
        assert sub.unbound_sources == [("samy", "samy")]
        sub._on_scan_status({"scan_id": "scan_2", "status": "open"}, {})  # not ours
        assert sub.scan_id == "scan_1" and len(updates) == 1
        item.status_message.readout_priority = {"monitored": ["samx", "samy"], "baseline": []}
        sub._on_scan_status({"scan_id": "scan_1", "status": "open"}, {})
        assert sub.unbound_sources == [] and updates[-1].reason == "rebind"
        seed_xy(item, "scan_1", 0, 1)
        data_api.paths[0]._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        n = len(updates)
        sub._on_scan_status({"scan_id": "scan_1", "status": "closed"}, {})
        assert len(updates) == n + 1 and updates[-1].reason == "live"  # terminal flush
        assert list(updates[-1].aligned_ordinals) == [0]
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

    def test_terminal_scan_classifies_closed(self, data_api, mock_client):
        """A terminal scan routes to CLOSED_SCAN (writer-latency window when
        no history entry exists yet), never to the live transports."""
        item = make_scan_item(mock_client, "scan_1", status="closed")
        register_scan(mock_client, {"scan_1": item})
        assert classify(mock_client, "scan_1", item, have_item=True) is Scenario.CLOSED_SCAN

    def test_scenario_classification(self, mock_client):
        """The level-1 decision: deterministic, no priorities, four outcomes."""
        open_item = make_scan_item(mock_client, "scan_open")
        register_scan(mock_client, {"scan_open": open_item})
        assert classify(mock_client, "scan_open", open_item, have_item=True) is Scenario.OPEN_SCAN
        assert classify(mock_client, "", None, have_item=True) is Scenario.NO_SCAN
        assert classify(mock_client, "gone", None, have_item=True) is Scenario.UNKNOWN
        # settling scan item (no status message yet) is not yet bindable
        unsettled = mock.MagicMock()
        unsettled.status_message = None
        assert classify(mock_client, "s", unsettled, have_item=True) is Scenario.UNKNOWN


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

    def test_register_path_replaces_a_path_by_data_source(self, data_api, mock_client):
        """A subclass of a core path (the extension recipe) can only be
        registered as the replacement of the path serving that data source."""

        class MyRedisDataPath(RedisDataPath):
            pass

        with pytest.raises(ValueError, match="already serves data source 'redis'"):
            data_api.register_path(MyRedisDataPath(mock_client))
        old = data_api.paths[0]
        data_api.register_path(MyRedisDataPath(mock_client), replace=True)
        assert old not in data_api.paths
        assert all(old not in table for table in data_api.data_paths.values())
        open_scan = [type(p).__name__ for p in data_api.data_paths[Scenario.OPEN_SCAN]]
        assert "MyRedisDataPath" in open_scan and "RedisDataPath" not in open_scan
        assert not old._callback_ids  # disconnected
        assert data_api.paths[0]._lock is data_api._lock

    def test_replacing_a_path_rebinds_its_subscriptions(self, data_api, mock_client):
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: mock.MagicMock()
        updates = []
        sub = data_api.subscribe(
            devices=["samx"], scan=None, callback=updates.append, min_emit_interval=0
        )
        old = data_api.paths[0]

        class MyRedisDataPath(RedisDataPath):
            pass

        data_api.register_path(MyRedisDataPath(mock_client), replace=True)
        new = next(p for p in data_api.data_paths[Scenario.NO_SCAN] if p.data_source == "redis")
        assert isinstance(new, MyRedisDataPath) and new._lock is data_api._lock
        assert sub._uses_path(new) and {s.path_name for s in sub._specs} == {"MyRedisDataPath"}
        assert updates[-1].reason == "rebind"
        assert not old._feeds and not old._stream_requests and new._feeds
        msg = messages.DeviceMessage(
            signals={"samx": {"value": 1.0, "timestamp": 1.0}}, metadata={}
        )
        new._on_stream_message({"data": msg}, ("stream", "readback", "samx", ""))
        assert updates[-1].reason == "live" and list(updates[-1].get("samx", "samx").values) == [
            1.0
        ]
        sub.close()
        assert not new._feeds

    def test_register_path_validates_declarations(self, data_api, mock_client):
        class NoScenarios(RedisDataPath):
            scenarios = frozenset()

        class BadScenarios(RedisDataPath):
            scenarios = frozenset({Scenario.UNKNOWN})

        class NoSource(RedisDataPath):
            data_source = None

        for cls in (NoScenarios, BadScenarios, NoSource):
            with pytest.raises(ValueError):
                data_api.register_path(cls(mock_client), replace=True)

    def test_register_path_rejects_duplicate_class(self, data_api, mock_client):
        """A second instance of an already-registered plugin class could never
        win routing (same resolve logic, consulted later) but its connect()
        side effects would run - refused before connect()."""
        registrations_before = mock_client.callbacks.register.call_count
        with pytest.raises(ValueError, match="already serves data source 'redis'"):
            data_api.register_path(RedisDataPath(mock_client))
        assert sum(type(p) is RedisDataPath for p in data_api.paths) == 1
        # Refused BEFORE connect(): no extra client callbacks were registered.
        assert mock_client.callbacks.register.call_count == registrations_before

    def test_close_removes_instance(self, mock_client):
        DataAPI.clear_instance()
        api = DataAPI(mock_client)
        api.close()
        fresh = DataAPI(mock_client)
        try:
            assert fresh is not api
            assert fresh.paths
        finally:
            DataAPI.clear_instance()

    def test_unbound_subscription_collectable_after_del(self, data_api, mock_client):
        """The client's callback handler references bound methods weakly and
        drops dead registrations itself, so an abandoned *unbound* live-follow
        subscription is collectable without any relay object in between."""
        mock_client.callbacks = CallbackHandler()
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="live", callback=lambda u: None)
        assert sub.scan_id is None
        callback_ids = list(sub._callback_ids)
        assert callback_ids and all(i in mock_client.callbacks.callbacks for i in callback_ids)
        ref = weakref.ref(sub)
        del sub
        gc.collect()
        assert ref() is None
        assert all(i not in mock_client.callbacks.callbacks for i in callback_ids)

    def test_bound_subscription_must_be_closed(self, data_api, mock_client):
        """A bound subscription is referenced by its data path's request
        tables (the request's sink), so it is not collectable: consumers
        close() it, and close() releases everything."""
        mock_client.callbacks = CallbackHandler()  # a MagicMock would pin the bound methods
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="live", callback=lambda u: None)
        assert sub.scan_id == "scan_1" and plugin._scan_requests
        ref = weakref.ref(sub)
        del sub
        gc.collect()
        assert ref() is not None and plugin._scan_requests
        ref().close()
        assert not plugin._scan_requests
        gc.collect()
        assert ref() is None

    def test_close_releases_plugin_request(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")], scan="live", callback=lambda u: None
        )
        plugin = data_api.paths[0]
        assert plugin._scan_requests
        sub.close()
        assert not plugin._scan_requests


class TestAxisModes:
    def test_async_message_without_update_metadata_is_dropped(self, data_api, mock_client):
        """An async message needs an explicit acquisition operation: a message
        lacking async_update metadata cannot be ordinal-mapped and is dropped."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("det", "wave")], scan="live", callback=updates.append, min_emit_interval=0
        )
        plugin = data_api.paths[0]
        msg = messages.DeviceMessage(
            signals={"wave": {"value": [1, 2], "timestamp": 100.0}}, metadata={}
        )
        plugin._on_async_message({"data": msg}, "scan_1", "det")
        source = updates[-1].get("det", "wave")
        assert source is None or len(source.ordinals) == 0
        assert all(spec.path_name == "RedisDataPath" for spec in sub._specs)
        assert all(spec.data_source == "redis" for spec in sub._specs)
        sub.close()

    @staticmethod
    def _async_add_message(i):
        return messages.DeviceMessage(
            signals={"wave": {"value": [i], "timestamp": 100.0 + i}},
            metadata={
                "async_indices": {"wave": i},
                "async_update": messages.DeviceAsyncUpdate(
                    type="add", max_shape=[None]
                ).model_dump(),
            },
        )

    def test_second_subscriber_catches_up_on_async_stream(self, data_api, mock_client, monkeypatch):
        """The from_start replay happens only on the first feed registration;
        a later subscriber to the same async source must be caught up from the
        stream or its backfill is empty."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]

        ups_a, ups_b = [], []
        sub_a = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_a.append, min_emit_interval=0
        )
        feed = next(iter(plugin._feeds.values()))
        msgs = [self._async_add_message(i) for i in range(2)]
        for msg in msgs:
            feed.callback({"data": msg})  # real feed path: counts deliveries
        assert feed.delivered == 2

        stream = [{"data": m} for m in msgs]
        monkeypatch.setattr(mock_client.connector, "xrange", lambda *a, **k: stream)
        sub_b = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_b.append, min_emit_interval=0
        )
        assert tuple(ups_a[-1].get("det", "wave").ordinals) == (0, 1)
        assert tuple(ups_b[-1].get("det", "wave").ordinals) == (0, 1)
        sub_a.close()
        sub_b.close()

    def test_catch_up_skips_inflight_stream_entries(self, data_api, mock_client, monkeypatch):
        """An entry already in the stream but not yet dispatched must not be
        replayed - it arrives through the connector callback afterwards, and
        replaying it too would double-ingest (fatal for add_slice rows)."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]

        ups_a, ups_b = [], []
        sub_a = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_a.append, min_emit_interval=0
        )
        feed = next(iter(plugin._feeds.values()))
        msgs = [self._async_add_message(i) for i in range(3)]
        for msg in msgs[:2]:
            feed.callback({"data": msg})
        # msg[2] is in the stream but has not been dispatched yet.
        stream = [{"data": m} for m in msgs]
        monkeypatch.setattr(mock_client.connector, "xrange", lambda *a, **k: stream)
        sub_b = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_b.append, min_emit_interval=0
        )
        assert tuple(ups_b[-1].get("det", "wave").ordinals) == (0, 1)
        feed.callback({"data": msgs[2]})  # in-flight entry arrives normally
        assert tuple(ups_b[-1].get("det", "wave").ordinals) == (0, 1, 2)
        sub_a.close()
        sub_b.close()

    def test_catch_up_does_not_depend_on_what_the_stream_still_holds(
        self, data_api, mock_client, monkeypatch
    ):
        """The device server trims async streams (MAXLEN). A later request of the same
        feed is caught up from the messages the feed delivered, so it gets the whole
        row, each fragment exactly once, and Redis is not read again."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]
        sub_a = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=lambda _: None, min_emit_interval=0
        )
        feed = next(iter(plugin._feeds.values()))
        msgs = [async_msg("wave", [i], i, "add_slice", slice_index=0) for i in range(5)]
        for msg in msgs[:4]:
            feed.callback({"data": msg})
        # The stream lost its two oldest entries; msgs[4] is in flight.
        xrange = mock.MagicMock(return_value=[{"data": m} for m in msgs[2:]])
        monkeypatch.setattr(mock_client.connector, "xrange", xrange)
        ups_b = []
        sub_b = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_b.append, min_emit_interval=0
        )
        feed.callback({"data": msgs[4]})
        source = ups_b[-1].get("det", "wave")
        assert list(source.values[0]) == [0, 1, 2, 3, 4]
        assert "stream_trimmed" not in source.metadata
        xrange.assert_not_called()
        sub_a.close()
        sub_b.close()

    def test_feed_attached_to_an_already_trimmed_stream_says_so(self, data_api, mock_client):
        """A first subscriber that joins a long scan late only gets what the stream still
        holds; the first message it sees is not message 0, and the source reports it."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]
        ups_a, ups_b = [], []
        sub_a = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_a.append, min_emit_interval=0
        )
        feed = next(iter(plugin._feeds.values()))
        for index in (7, 8):
            feed.callback({"data": self._async_add_message(index)})
        assert ups_a[-1].get("det", "wave").metadata["stream_trimmed"] is True
        sub_b = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_b.append, min_emit_interval=0
        )
        assert tuple(ups_b[-1].get("det", "wave").ordinals) == (7, 8)
        assert ups_b[-1].get("det", "wave").metadata["stream_trimmed"] is True
        sub_a.close()
        sub_b.close()

    def test_feed_log_is_bounded_while_every_request_is_capped(
        self, data_api, mock_client, monkeypatch
    ):
        """Requests with a retention cap must not make the feed keep a whole scan."""
        from bec_lib.data_api.paths import redis as redis_module

        monkeypatch.setattr(redis_module, "_FEED_LOG_CAP", 3)
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]
        sub_a = data_api.subscribe(
            sources=[("det", "wave")],
            scan="scan_1",
            callback=lambda _: None,
            min_emit_interval=0,
            max_points=2,
        )
        feed = next(iter(plugin._feeds.values()))
        for index in range(6):
            feed.callback({"data": self._async_add_message(index)})
        assert len(feed.log) == 3
        ups_b = []
        sub_b = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_b.append, min_emit_interval=0
        )
        source = ups_b[-1].get("det", "wave")
        assert tuple(source.ordinals) == (3, 4, 5)
        assert source.metadata["stream_trimmed"] is True
        sub_a.close()
        sub_b.close()

    def test_multi_signal_sub_signals_align_and_catch_up_once(
        self, data_api, mock_client, monkeypatch
    ):
        """Sub-signals of one AsyncMultiSignal share a stream and a correlation group:
        they arrive in one update, and a late subscriber replays the shared stream
        once (not once per sub-signal, which would repeat add_slice fragments)."""
        mock_client.device_manager.get_bec_signals.return_value = [
            (
                "gauss",
                f"data.{name}",
                {
                    "signal_class": "AsyncMultiSignal",
                    "obj_name": f"gauss_data_{name}",
                    "storage_name": "gauss_data",
                },
            )
            for name in ("x", "y")
        ]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]
        keys = [("gauss", "gauss_data_x"), ("gauss", "gauss_data_y")]

        def multi_msg(i):
            update = messages.DeviceAsyncUpdate(type="add_slice", index=0, max_shape=[None, None])
            return messages.DeviceMessage(
                signals={
                    "gauss_data_x": {"value": [i], "timestamp": 100.0 + i},
                    "gauss_data_y": {"value": [10 * i], "timestamp": 100.0 + i},
                },
                metadata={
                    "async_indices": {"gauss_data_x": i, "gauss_data_y": i},
                    "async_update": update.model_dump(),
                },
            )

        ups_a, ups_b = [], []
        sub_a = data_api.subscribe(
            sources=keys, scan="scan_1", callback=ups_a.append, min_emit_interval=0
        )
        assert sub_a.groups == {CorrelationGroup.multi("gauss", "gauss_data"): keys}
        (feed_key,) = plugin._feeds
        feed = plugin._feeds[feed_key]
        msgs = [multi_msg(i) for i in range(2)]
        for msg in msgs:
            feed.callback({"data": msg})
        stream = [{"data": m} for m in msgs]
        monkeypatch.setattr(mock_client.connector, "xrange", lambda *a, **k: stream)
        sub_b = data_api.subscribe(
            sources=keys, scan="scan_1", callback=ups_b.append, min_emit_interval=0
        )
        for update in (ups_a[-1], ups_b[-1]):
            aligned = update.aligned()
            assert list(aligned[keys[0]][0]) == [0, 1]
            assert list(aligned[keys[1]][0]) == [0, 10]
        sub_b.close()
        assert feed_key in plugin._feeds  # still held by sub_a
        sub_a.close()
        assert not plugin._feeds

    def test_open_failure_rolls_back_the_binding(self, data_api, mock_client, monkeypatch):
        """A path failing half-way through open() must not leave a half-bound
        subscription: opened feeds are released, no request stays registered,
        subscribe() closes the subscription and a live follow waits."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]
        real_register = mock_client.connector.register

        def failing_register(endpoint, *args, **kwargs):
            if "wave" in str(getattr(endpoint, "endpoint", endpoint)):
                raise RuntimeError("redis hiccup")
            return real_register(endpoint, *args, **kwargs)

        monkeypatch.setattr(mock_client.connector, "register", failing_register)
        for scan in ("scan_1", "live"):
            registered = mock_client.callbacks.register.call_count
            removed = mock_client.callbacks.remove.call_count
            with pytest.raises(RuntimeError, match="redis hiccup"):
                data_api.subscribe(
                    sources=[("samx", "samx"), ("det", "wave")], scan=scan, callback=lambda u: None
                )
            assert not plugin._scan_requests and not plugin._feeds
            # subscribe() closed the subscription: its client callbacks are gone.
            new_registrations = mock_client.callbacks.register.call_count - registered
            assert new_registrations == 2
            assert mock_client.callbacks.remove.call_count - removed == new_registrations

        # Dispatcher-side rebind: the failure is logged, the subscription is
        # left unbound and binds the next scan once the source works again.
        monkeypatch.setattr(mock_client.connector, "register", real_register)
        mock_client.queue.scan_storage.current_scan_id = []
        follow = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")], scan="live", callback=lambda u: None
        )
        assert follow.scan_id is None
        monkeypatch.setattr(mock_client.connector, "register", failing_register)
        follow._on_scan_status({"scan_id": "scan_1", "status": "open"}, {})
        assert follow.scan_id is None and not plugin._scan_requests and not plugin._feeds
        monkeypatch.setattr(mock_client.connector, "register", real_register)
        item2 = make_scan_item(mock_client, "scan_2")
        register_scan(mock_client, {"scan_1": item, "scan_2": item2})
        follow._on_scan_status({"scan_id": "scan_2", "status": "open"}, {})
        assert follow.scan_id == "scan_2" and follow.unbound_sources == []
        follow.close()
        assert not plugin._scan_requests and not plugin._feeds

    def test_live_subscribe_keeps_a_newer_scan_bound_by_the_dispatcher(
        self, data_api, mock_client, monkeypatch
    ):
        """If a scan opens between subscribe()'s lock-free prefetch and its
        bind, the dispatcher's newer binding wins over the stale prefetch."""
        item1 = make_scan_item(mock_client, "scan_1")
        item2 = make_scan_item(mock_client, "scan_2")
        register_scan(mock_client, {"scan_1": item1, "scan_2": item2})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        handlers = {}
        mock_client.callbacks.register.side_effect = (
            lambda event, handler: (handlers.setdefault(event, handler) and "id") or "id"
        )
        original = data_api._current_live_scan

        def racing_prefetch():
            target = original()
            handlers["scan_status"]({"scan_id": "scan_2", "status": "open"}, {})
            return target

        monkeypatch.setattr(data_api, "_current_live_scan", racing_prefetch)
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="live", callback=lambda u: None)
        assert sub.scan_id == "scan_2"
        sub.close()

    def test_only_changed_groups_emit(self, data_api, mock_client):
        """A scan segment emits the monitored group only; an async message
        emits the async source's group only; a message that inserts nothing
        emits nothing."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")],
            scan="scan_1",
            callback=updates.append,
            min_emit_interval=0,
        )
        assert {u.metadata["group"] for u in updates} == {
            SCAN_GROUP,
            CorrelationGroup.standalone("det", "wave"),
        }
        plugin = data_api.paths[0]
        n = len(updates)
        seed_xy(item, "scan_1", 0, 1)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        assert [u.metadata["group"] for u in updates[n:]] == [SCAN_GROUP]
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})  # nothing new
        assert len(updates) == n + 1
        plugin._on_async_message({"data": async_msg("wave", 10.0, 0)}, "scan_1", "det")
        assert [u.metadata["group"] for u in updates[n + 1 :]] == [
            CorrelationGroup.standalone("det", "wave")
        ]
        no_meta = messages.DeviceMessage(
            signals={"wave": {"value": [1], "timestamp": 1.0}}, metadata={}
        )
        plugin._on_async_message({"data": no_meta}, "scan_1", "det")
        assert len(updates) == n + 2
        sub.close()

    def test_inflight_message_reaches_a_new_subscriber_exactly_once(
        self, data_api, mock_client, monkeypatch
    ):
        """A message being delivered while a second subscriber opens must be
        applied to it once — by the catch-up replay or by the feed, never both
        (the delivered count and the ingestion are one critical section)."""
        import threading

        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        plugin = data_api.paths[0]

        def slice_message(i):
            return messages.DeviceMessage(
                signals={"wave": {"value": [i], "timestamp": 100.0 + i}},
                metadata={
                    "async_update": messages.DeviceAsyncUpdate(
                        type="add_slice", max_shape=[None, None], index=0
                    ).model_dump()
                },
            )

        ups_a, ups_b = [], []
        sub_a = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_a.append, min_emit_interval=0
        )
        feed = next(iter(plugin._feeds.values()))
        msgs = [slice_message(i) for i in range(2)]
        feed.callback({"data": msgs[0]})
        monkeypatch.setattr(
            mock_client.connector, "xrange", lambda *a, **k: [{"data": m} for m in msgs]
        )

        # Pause the feed callback for msgs[1] right before ingestion while the
        # second subscriber opens.
        paused, opened = threading.Event(), threading.Event()
        original = plugin._on_async_message

        def paused_on_async_message(msg, scan_id, device, **kwargs):
            paused.set()
            opened.wait(2)
            return original(msg, scan_id, device, **kwargs)

        monkeypatch.setattr(plugin, "_on_async_message", paused_on_async_message)
        inflight = threading.Thread(target=feed.callback, args=({"data": msgs[1]},))
        inflight.start()
        assert paused.wait(2)
        sub_b = data_api.subscribe(
            sources=[("det", "wave")], scan="scan_1", callback=ups_b.append, min_emit_interval=0
        )
        opened.set()
        inflight.join(2)
        assert [list(row) for row in ups_a[-1].get("det", "wave").values] == [[0, 1]]
        assert [list(row) for row in ups_b[-1].get("det", "wave").values] == [[0, 1]]
        assert feed.delivered == 2
        sub_a.close()
        sub_b.close()

    def test_unknown_async_update_type_is_dropped(self, data_api, mock_client, monkeypatch):
        """Forward-compat: when messages.py knows an async_update type before a
        reconstruction handler exists, the point is dropped with a warning
        instead of silently reconstructed as "add"."""
        mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("det", "wave")], scan="live", callback=updates.append, min_emit_interval=0
        )
        plugin = data_api.paths[0]
        # A future messages.py accepts "sparse_block"; this client's handler
        # table does not know it yet.
        future_update = SimpleNamespace(type="sparse_block", max_shape=[None], index=None)
        monkeypatch.setattr(
            "bec_lib.data_api.decoding.async_updates.DeviceAsyncUpdate",
            SimpleNamespace(model_validate=lambda _payload: future_update),
        )
        msg = messages.DeviceMessage.model_construct(
            signals={"wave": {"value": [1, 2], "timestamp": 100.0}},
            metadata={
                "async_indices": {"wave": 0},
                "async_update": {"type": "sparse_block", "max_shape": [None]},
            },
        )
        plugin._on_async_message({"data": msg}, "scan_1", "det")
        source = updates[-1].get("det", "wave")
        assert source is None or len(source.ordinals) == 0
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
        assert list(update.axis("index")) == [0, 1, 2]
        assert list(update.axis("device", ("samx", "samx"))) == [0.0, 1.0, 2.0]
        assert list(update.axis("timestamp", ("samx", "samx"))) == [100.0, 101.0, 102.0]
        sub.close()


class TestDeviceStreams:
    def _plugin(self, data_api):
        return data_api.paths[0]

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
            plugin._on_stream_message({"data": msg}, ("stream", "readback", "samx", ""))
        source = updates[-1].get("samx", "samx")
        # Retention cap keeps only the newest points.
        assert list(source.values) == [2.0, 3.0, 4.0]
        assert list(source.ordinals) == [2, 3, 4]
        assert source.kind == "unindexed"
        sub.close()
        assert not plugin._feeds

    def test_continuous_correlation_pairs_readbacks(self, data_api, mock_client):
        """``devices=[...]`` + ``correlation="continuous"``: independent
        readbacks are sample-and-hold paired into rows by the bundler, so a
        motor map no longer pairs positions itself."""
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            mock.MagicMock() if name in ("samx", "samy") else default
        )
        updates = []
        sub = data_api.subscribe(
            devices=["samx", "samy"],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
            correlation="continuous",
        )
        assert sub.sources == [("samx", "samx"), ("samy", "samy")]
        assert sub.correlation == "continuous"
        plugin = self._plugin(data_api)

        def readback(device, value, timestamp):
            msg = messages.DeviceMessage(
                signals={device: {"value": value, "timestamp": timestamp}}, metadata={}
            )
            plugin._on_stream_message({"data": msg}, ("stream", "readback", device, ""))

        readback("samx", 0.0, 100.0)
        assert list(updates[-1].aligned_ordinals) == []  # held until samy delivers
        readback("samy", 10.0, 101.0)
        readback("samx", 1.0, 102.0)
        readback("samy", 11.0, 103.0)
        update = updates[-1]
        assert update.metadata["group"] == CONTINUOUS_GROUP
        assert {k: list(v) for k, v in update.aligned().items()} == {
            ("samx", "samx"): [0.0, 1.0, 1.0],
            ("samy", "samy"): [10.0, 10.0, 11.0],
        }
        assert list(update.axis("timestamp")) == [101.0, 102.0, 103.0]
        sub.close()

    def test_empty_source_lists_are_rejected(self, data_api, mock_client):
        with pytest.raises(CorrelationGroupError):
            data_api.subscribe(sources=[], scan=None)
        with pytest.raises(CorrelationGroupError):
            data_api.subscribe(devices=[], scan=None)
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: mock.MagicMock()
        sub = data_api.subscribe(devices=["samx"], scan=None, callback=lambda u: None)
        with pytest.raises(CorrelationGroupError):
            sub.set_sources([])
        assert sub.sources == [("samx", "samx")] and sub.unbound_sources == []
        sub.close()

    def test_continuous_correlation_needs_scanless_and_one_source_form(self, data_api, mock_client):
        with pytest.raises(ValueError, match="scan-less"):
            data_api.subscribe(devices=["samx"], scan="live", correlation="continuous")
        with pytest.raises(ValueError, match="exactly one of"):
            data_api.subscribe(sources=[("samx", "samx")], devices=["samx"], scan=None)
        with pytest.raises(ValueError, match="exactly one of"):
            data_api.subscribe(scan=None)
        with pytest.raises(ValueError, match="Unknown correlation"):
            data_api.subscribe(devices=["samx"], scan=None, correlation="magic")

    def test_continuous_readbacks_are_seeded_from_the_cached_value(self, data_api, mock_client):
        """A parked motor publishes nothing after subscribe: its cached
        readback seeds a continuous group before either motor moves."""
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: mock.MagicMock()
        for device, value in (("samx", 1.5), ("samy", 2.5)):
            mock_client.connector.set_and_publish(
                MessageEndpoints.device_readback(device),
                messages.DeviceMessage(
                    signals={device: {"value": value, "timestamp": 7.0}}, metadata={}
                ),
            )
        updates = []
        sub = data_api.subscribe(
            devices=["samx", "samy"],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
            correlation="continuous",
        )
        assert updates[-1].reason == "backfill"
        assert {k: list(v) for k, v in updates[-1].aligned().items()} == {
            ("samx", "samx"): [1.5],
            ("samy", "samy"): [2.5],
        }
        # The seeded message arriving through the feed is not a new sample.
        plugin = self._plugin(data_api)
        seeded = mock_client.connector.get(MessageEndpoints.device_readback("samx"))
        plugin._on_stream_message({"data": seeded}, ("stream", "readback", "samx", ""))
        assert len(updates[-1].aligned_ordinals) == 1
        fresh = messages.DeviceMessage(
            signals={"samx": {"value": 1.6, "timestamp": 8.0}}, metadata={}
        )
        plugin._on_stream_message({"data": fresh}, ("stream", "readback", "samx", ""))
        assert list(updates[-1].aligned()[("samx", "samx")]) == [1.5, 1.6]
        sub.close()

    def test_cached_reading_with_array_metadata_is_not_counted_twice(self, data_api, mock_client):
        """The feed delivers the cached message again; comparing it must not depend on
        ``==`` having an answer for its metadata."""
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: mock.MagicMock()

        def reading():
            return messages.DeviceMessage(
                signals={"samx": {"value": 1.5, "timestamp": 7.0}}, metadata={"roi": np.arange(3)}
            )

        mock_client.connector.set_and_publish(MessageEndpoints.device_readback("samx"), reading())
        updates = []
        sub = data_api.subscribe(
            devices=["samx"], scan=None, callback=updates.append, min_emit_interval=0
        )
        assert list(updates[-1].get("samx", "samx").values) == [1.5]
        self._plugin(data_api)._on_stream_message(
            {"data": reading()}, ("stream", "readback", "samx", "")
        )
        assert list(updates[-1].get("samx", "samx").values) == [1.5]
        sub.close()

    def test_failing_cache_read_leaves_the_stream_subscribed(self, data_api, mock_client):
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: mock.MagicMock()
        updates = []
        with mock.patch.object(mock_client.connector, "get", side_effect=RuntimeError("down")):
            sub = data_api.subscribe(
                devices=["samx"], scan=None, callback=updates.append, min_emit_interval=0
            )
        assert len(updates[-1].get("samx", "samx").values) == 0
        fresh = messages.DeviceMessage(
            signals={"samx": {"value": 1.6, "timestamp": 8.0}}, metadata={}
        )
        self._plugin(data_api)._on_stream_message(
            {"data": fresh}, ("stream", "readback", "samx", "")
        )
        assert list(updates[-1].get("samx", "samx").values) == [1.6]
        sub.close()

    @pytest.mark.parametrize("correlation", ["ordinal", "continuous"])
    @pytest.mark.parametrize("stream", ["readback", "configuration"])
    def test_initial_reading_and_queued_publications(
        self, data_api, mock_client, correlation, stream
    ):
        device = SimpleNamespace(
            _info={
                "signals": {
                    "value": {
                        "obj_name": "det_value",
                        "kind_str": "config" if stream == "configuration" else "normal",
                    }
                }
            }
        )
        mock_client.device_manager.devices = {"det": device}
        endpoint = (
            MessageEndpoints.device_read_configuration("det")
            if stream == "configuration"
            else MessageEndpoints.device_readback("det")
        )

        def message(value, timestamp):
            return messages.DeviceMessage(
                signals={"det_value": {"value": np.array(value), "timestamp": timestamp}}
            )

        cached = message([1.0, 2.0], 2.0)
        mock_client.connector.set_and_publish(endpoint, cached)
        updates = []
        sub = data_api.subscribe(
            sources=[("det", "det_value")],
            scan=None,
            correlation=correlation,
            max_points=1,
            min_emit_interval=0,
            callback=updates.append,
        )
        assert updates[-1].reason == "backfill"
        initial = updates[-1].get("det", "det_value")
        assert list(initial.ordinals) == [0]
        np.testing.assert_array_equal(initial.values[0], [1.0, 2.0])
        assert initial.metadata["stream"] == stream
        plugin = self._plugin(data_api)
        feed = plugin._feeds[("stream", stream, "det", "")]
        assert feed.endpoint == endpoint
        count = len(updates)
        # Both an older queued publication and the cache message itself must
        # leave the initial snapshot unchanged, including array-valued readings.
        feed.callback(SimpleNamespace(value=message([0.0, 1.0], 1.0)))
        feed.callback(SimpleNamespace(value=cached))
        assert len(updates) == count
        feed.callback(SimpleNamespace(value=message([3.0, 4.0], 3.0)))
        current = updates[-1].get("det", "det_value")
        assert list(current.ordinals) == [1]
        np.testing.assert_array_equal(current.values[0], [3.0, 4.0])
        sub.close()
        assert not plugin._feeds

    def test_configuration_and_readback_feeds_are_independent(self, data_api, mock_client):
        mock_client.device_manager.devices = {
            "samx": SimpleNamespace(
                _info={
                    "signals": {
                        "readback": {"obj_name": "samx", "kind_str": "hinted"},
                        "velocity": {"obj_name": "samx_velocity", "kind_str": "config"},
                    }
                }
            )
        }
        readings, configurations = [], []
        readback = data_api.subscribe(
            sources=[("samx", "samx")], scan=None, callback=readings.append, min_emit_interval=0
        )
        config = data_api.subscribe(
            sources=[("samx", "samx_velocity")],
            scan=None,
            callback=configurations.append,
            min_emit_interval=0,
        )
        plugin = self._plugin(data_api)
        config_feed = plugin._feeds[("stream", "configuration", "samx", "")]
        config_feed.callback(
            SimpleNamespace(
                value=messages.DeviceMessage(
                    signals={"samx_velocity": {"value": 2.0, "timestamp": 4.0}}
                )
            )
        )
        assert list(configurations[-1].get("samx", "samx_velocity").values) == [2.0]
        assert list(readings[-1].get("samx", "samx").values) == []
        config.close()
        assert list(plugin._feeds) == [("stream", "readback", "samx", "")]
        readback.close()
        assert not plugin._feeds

    def test_continuous_group_holds_on_every_declared_member(self, data_api, mock_client):
        """An unavailable member never yields rows with a missing column: the
        group holds, reports the member as lagging, and re-resolves it on the
        next device update."""
        known = {"samx"}
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            mock.MagicMock() if name in known else default
        )
        updates = []
        sub = data_api.subscribe(
            devices=["samx", "samy"],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
            correlation="continuous",
        )
        assert sub.unbound_sources == [("samy", "samy")]
        plugin = self._plugin(data_api)
        msg = messages.DeviceMessage(
            signals={"samx": {"value": 1.0, "timestamp": 1.0}}, metadata={}
        )
        plugin._on_stream_message({"data": msg}, ("stream", "readback", "samx", ""))
        # A member's first (held) sample emits once, carrying the lagging report;
        # further held samples emit nothing.
        assert updates[-1].reason == "live" and list(updates[-1].aligned_ordinals) == []
        assert updates[-1].metadata["lagging_sources"] == [("samy", "samy")]
        n = len(updates)
        plugin._on_stream_message({"data": msg}, ("stream", "readback", "samx", ""))
        assert len(updates) == n

        known.add("samy")
        sub._on_device_update()
        assert sub.unbound_sources == [] and updates[-1].reason == "rebind"
        for device, value in (("samx", 1.0), ("samy", 2.0)):
            msg = messages.DeviceMessage(
                signals={device: {"value": value, "timestamp": 1.0}}, metadata={}
            )
            plugin._on_stream_message({"data": msg}, ("stream", "readback", device, ""))
        assert {k: list(v) for k, v in updates[-1].aligned().items()} == {
            ("samx", "samx"): [1.0],
            ("samy", "samy"): [2.0],
        }
        sub.close()

    def test_device_update_keeps_points_while_nothing_changed(self, data_api, mock_client):
        known = {"samx"}
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            mock.MagicMock() if name in known else default
        )
        updates = []
        sub = data_api.subscribe(
            devices=["samx", "ghost"], scan=None, callback=updates.append, min_emit_interval=0
        )
        plugin = self._plugin(data_api)
        for value in (1.0, 2.0):
            msg = messages.DeviceMessage(
                signals={"samx": {"value": value, "timestamp": 1.0}}, metadata={}
            )
            plugin._on_stream_message({"data": msg}, ("stream", "readback", "samx", ""))
        request = sub._requests[CorrelationGroup.standalone("samx", "samx")][0]
        sub._on_device_update()  # ghost is still unknown: nothing to gain from a rebind
        assert sub._requests[CorrelationGroup.standalone("samx", "samx")][0] is request
        assert list(updates[-1].get("samx", "samx").values) == [1.0, 2.0]
        known.add("ghost")
        sub._on_device_update()
        assert sub.unbound_sources == [] and updates[-1].reason == "rebind"
        sub.close()

    def test_shared_stream_feed_survives_until_the_last_subscriber(self, data_api, mock_client):
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: mock.MagicMock()
        plugin = self._plugin(data_api)
        sub_a = data_api.subscribe(devices=["samx"], scan=None, callback=lambda u: None)
        sub_b = data_api.subscribe(devices=["samx"], scan=None, callback=lambda u: None)
        (feed,) = plugin._feeds.values()
        assert feed.refcount == 2
        sub_a.close()
        assert feed.refcount == 1 and plugin._feeds
        sub_b.close()
        assert not plugin._feeds

    def test_legacy_monitor_entry_is_not_served(self, data_api, mock_client):
        # device_monitor_1d is no longer supported: the reserved entry resolves unavailable.
        sub = data_api.subscribe(
            sources=[("waveform", "monitor_1d")], scan=None, callback=lambda _: None
        )
        assert sub.unbound_sources == [("waveform", "monitor_1d")]
        sub.close()

    def test_preview_stream_carries_scan_metadata(self, data_api, mock_client):
        self._preview_device(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("eiger", "preview_img")],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
        )
        plugin = self._plugin(data_api)
        msg = messages.DevicePreviewMessage(
            device="eiger",
            signal="preview_img",
            data=np.array([1, 2, 3]),
            timestamp=7.0,
            metadata={"scan_id": "scan_7"},
        )
        plugin._on_stream_message({"data": msg}, ("stream", "preview", "eiger", "preview_img"))
        source = updates[-1].get("eiger", "preview_img")
        assert [list(value) for value in source.values] == [[1, 2, 3]]
        assert list(source.timestamps) == [7.0]
        assert source.metadata["scan_id"] == "scan_7"
        assert source.metadata["stream"] == "preview"
        sub.close()

    @staticmethod
    def _preview_device(mock_client):
        dev = mock.MagicMock()
        dev._info = {
            "signals": {"preview": {"obj_name": "preview_img", "signal_class": "PreviewSignal"}}
        }
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            dev if name == "eiger" else default
        )

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
        specs = plugin.resolve(
            [("eiger", "preview_img")], BindContext(scan_id="", scenario=Scenario.NO_SCAN)
        )
        assert specs[0].stream == "preview"

        updates = []
        sub = data_api.subscribe(
            sources=[("eiger", "preview_img")],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
        )
        msg = messages.DevicePreviewMessage(
            device="eiger", signal="preview_img", data=np.array([[1, 2], [3, 4]])
        )
        plugin._on_stream_message({"data": msg}, ("stream", "preview", "eiger", "preview_img"))
        (image,) = updates[-1].get("eiger", "preview_img").values
        assert image.tolist() == [[1, 2], [3, 4]]
        sub.close()

    def test_unknown_device_unavailable(self, data_api, mock_client):
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: default
        sub = data_api.subscribe(sources=[("ghost", "ghost")], scan=None, callback=lambda u: None)
        assert sub.unbound_sources == [("ghost", "ghost")]
        sub.close()


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
        assert list(updates[-1].get("samx", "samx").values) == [1.5, 2.5]
        sub.close()

    def test_preview_stream_payload_reaches_subscriber(self, data_api, mock_client):
        TestDeviceStreams._preview_device(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("eiger", "preview_img")],
            scan=None,
            callback=updates.append,
            min_emit_interval=0,
        )
        mock_client.connector.xadd(
            MessageEndpoints.device_preview("eiger", "preview_img"),
            {
                "data": messages.DevicePreviewMessage(
                    device="eiger", signal="preview_img", data=np.array([1, 2, 3])
                )
            },
        )
        assert self._pump(
            mock_client.connector,
            lambda: bool(updates)
            and updates[-1].get("eiger", "preview_img") is not None
            and len(updates[-1].get("eiger", "preview_img").values) > 0,
        ), "preview payload never reached the subscriber"
        source = updates[-1].get("eiger", "preview_img")
        assert list(source.values[-1]) == [1, 2, 3]
        sub.close()


class TestClientOwnershipAndVisibility:
    def test_close_removes_the_registry_entry(self, mock_client):
        DataAPI.clear_instance()
        api = DataAPI(mock_client)
        assert len(DataAPI._instances) == 1
        api.close()
        assert len(DataAPI._instances) == 0

    def test_registry_entry_dies_with_last_reference(self, mock_client):
        DataAPI.clear_instance()
        api = DataAPI(mock_client)
        assert len(DataAPI._instances) == 1
        del api  # no explicit close: the weak value dies with the last reference
        gc.collect()
        assert len(DataAPI._instances) == 0

    def test_device_update_invalidates_async_info_cache(self, data_api, mock_client):
        plugin = data_api.paths[0]
        plugin._async_info_cache[("det", "wave")] = {"obj_name": "wave"}
        # The plugin registered the device_update event at connect time.
        registered = {call.args[0] for call in mock_client.callbacks.register.call_args_list}
        assert "device_update" in registered
        plugin._on_device_update("update", {})
        assert plugin._async_info_cache == {}

    def test_lagging_sources_reported(self, data_api, mock_client):
        item = make_scan_item(mock_client, "scan_1")
        register_scan(mock_client, {"scan_1": item})
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("samy", "samy")],
            scan="live",
            callback=updates.append,
            min_emit_interval=0,
        )
        plugin = data_api.paths[0]
        # samx delivers 5 points; samy none — samy must be reported lagging.
        for i in range(5):
            msg = messages.ScanMessage(
                point_id=i,
                scan_id="scan_1",
                data={"samx": {"samx": {"value": float(i), "timestamp": 100.0 + i}}},
                metadata={"scan_id": "scan_1"},
            )
            item.live_data.set(i, msg)
        plugin._on_scan_segment({"scan_id": "scan_1"}, {"scan_id": "scan_1"})
        assert updates[-1].metadata.get("lagging_sources") == [("samy", "samy")]
        assert len(updates[-1].aligned_ordinals) == 0
        # samx data still reaches the consumer despite the stalled alignment.
        assert list(updates[-1].get("samx", "samx").values) == [0.0, 1.0, 2.0, 3.0, 4.0]
        sub.close()


def test_unbound_source_recovers_on_next_scan_status(data_api, mock_client):
    """A source that resolves unavailable at bind time (device info still
    settling) is re-resolved on the next status update of the same scan
    instead of staying lost for the whole scan (e2e flake regression)."""
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
    assert sub.unbound_sources == [("det", "wave")]

    # Device info settles: the async signal is now resolvable.
    mock_client.device_manager.get_bec_signals.return_value = [ASYNC_WAVE_INFO]
    data_api.paths[0]._async_info_cache.clear()
    sub._on_scan_status({"scan_id": "scan_1", "status": "open"}, {})

    assert sub.unbound_sources == []
    # The recovered source gets its own (standalone) group and receives data.
    assert sub.groups[CorrelationGroup.standalone("det", "wave")] == [("det", "wave")]
    plugin = data_api.paths[0]
    plugin._on_async_message({"data": async_msg("wave", 10.0, 0)}, "scan_1", "det")
    wave_updates = [u for u in updates if ("det", "wave") in u.sources]
    assert list(wave_updates[-1].get("det", "wave").values) == [10.0]
    sub.close()
