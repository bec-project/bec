"""Tests for the emission mechanism: sink-notified requests, one scheduler per client."""

import threading
import time
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.data_api import DataAPI
from bec_lib.data_api.data_bundler import DataBundler
from bec_lib.data_api.data_path import BindContext, SourceRequest, SourceSpec
from bec_lib.data_api.emission import EmissionScheduler
from bec_lib.data_api.models import SCAN_GROUP, CorrelationGroup
from bec_lib.data_api.scenarios import Scenario
from bec_lib.live_scan_data import LiveScanData
from bec_lib.scan_items import ScanItem

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name


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


def make_open_scan(mock_client, scan_id="scan_1", points=2):
    item = ScanItem(queue_id="q", scan_number=[1], scan_id=[scan_id], status="open")
    item.status_message = messages.ScanStatusMessage(scan_id=scan_id, status="open", info={})
    item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
    item.live_data = LiveScanData()
    seed(item, scan_id, 0, points)
    mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
        item if sid == scan_id else None
    )
    mock_client.queue.scan_storage.current_scan_id = [scan_id]
    return item


def seed(item, scan_id, start, stop):
    for i in range(start, stop):
        msg = messages.ScanMessage(
            point_id=i,
            scan_id=scan_id,
            data={"samx": {"samx": {"value": float(i), "timestamp": 100.0 + i}}},
            metadata={"scan_id": scan_id},
        )
        item.live_data.set(i, msg)


def segment(data_api, scan_id="scan_1"):
    data_api.paths[0]._on_scan_segment({"scan_id": scan_id}, {"scan_id": scan_id})


def wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def emit_threads():
    return [t for t in threading.enumerate() if t.name.startswith("data-api-emit-")]


class TestScheduler:
    def test_orders_by_deadline_and_isolates_exceptions(self):
        sched = EmissionScheduler("data-api-emit-test")
        seen = []
        owner = object()
        now = time.monotonic()

        def boom(_key):
            raise RuntimeError("boom")

        sched.schedule(owner, "late", now + 0.15, seen.append)
        sched.schedule(owner, "bad", now + 0.05, boom)
        sched.schedule(owner, "early", now + 0.1, seen.append)
        assert wait_for(lambda: seen == ["early", "late"])
        assert sched.thread.is_alive()  # the raising flush did not kill it
        assert sched.pending == 0
        sched.stop()
        assert not sched.thread.is_alive()

    def test_cancel_prunes_by_owner_and_key(self):
        sched = EmissionScheduler("data-api-emit-test")
        a, b = object(), object()
        far = time.monotonic() + 60
        sched.schedule(a, "x", far, lambda k: None)
        sched.schedule(a, "y", far, lambda k: None)
        sched.schedule(b, "x", far, lambda k: None)
        sched.cancel(a, "x")
        assert sched.pending == 2
        sched.cancel(a)
        assert sched.pending == 1
        sched.stop()
        assert sched.pending == 0

    def test_schedule_after_stop_is_refused(self):
        sched = EmissionScheduler("data-api-emit-test")
        sched.stop()
        assert sched.schedule(object(), "x", time.monotonic(), lambda k: None) is False
        assert sched.thread is None


class TestSinkContract:
    def test_request_notify_forwards_to_sink(self):
        sink = mock.MagicMock()
        request = SourceRequest(
            scan_id="s",
            group=SCAN_GROUP,
            specs=[SourceSpec(device="samx", entry="samx", kind="monitored")],
            bundle=DataBundler("s"),
            sink=sink,
            context=BindContext(scan_id="s", scenario=Scenario.OPEN_SCAN),
        )
        request.notify("live")
        sink.request_emission.assert_called_once_with(request, "live")

    def test_stale_request_notify_is_ignored(self, data_api, mock_client):
        make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=updates.append, min_emit_interval=0
        )
        old = sub._requests[SCAN_GROUP][0]
        sub.set_sources([("samx", "samx")])  # rebind: new request, same group label
        n = len(updates)
        old.notify("live")
        assert len(updates) == n
        sub._requests[SCAN_GROUP][0].notify("live")
        assert len(updates) == n + 1
        sub.close()


class TestCoalescing:
    def test_single_emit_thread_per_client_delivers_trailing_states(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        ups_a, ups_b = [], []
        sub_a = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=ups_a.append, min_emit_interval=0.4
        )
        sub_b = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=ups_b.append, min_emit_interval=0.4
        )
        assert [u.reason for u in ups_a] == ["backfill"] and [u.reason for u in ups_b] == [
            "backfill"
        ]
        with mock.patch("threading.Timer") as timer:
            for i in range(2, 6):
                seed(item, "scan_1", i, i + 1)
                segment(data_api)
            timer.assert_not_called()
        assert len(ups_a) == 1 and len(ups_b) == 1  # coalesced: backfill armed the window
        assert data_api._scheduler.pending == 2
        assert wait_for(lambda: len(ups_a) == 2 and len(ups_b) == 2)
        for ups in (ups_a, ups_b):
            assert ups[-1].reason == "live"
            assert list(ups[-1].aligned()[("samx", "samx")]) == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        assert len(emit_threads()) == 1
        assert data_api._scheduler.pending == 0
        sub_a.close()
        sub_b.close()

    def test_no_emit_thread_for_uncoalesced_subscription(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=updates.append, min_emit_interval=0
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)
        assert updates[-1].reason == "live" and len(updates[-1].aligned_ordinals) == 3
        assert data_api._scheduler.thread is None

    def test_groups_coalesce_independently(self, data_api, mock_client):
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: (
            mock.MagicMock() if name in ("samx", "samy") else default
        )
        updates = []
        sub = data_api.subscribe(
            devices=["samx", "samy"], scan=None, callback=updates.append, min_emit_interval=0.4
        )
        plugin = data_api.paths[0]
        for device in ("samx", "samy"):
            for value in (1.0, 2.0):
                msg = messages.DeviceMessage(
                    signals={device: {"value": value, "timestamp": 1.0}}, metadata={}
                )
                plugin._on_stream_message({"data": msg}, ("stream", "readback", device, ""))
        assert data_api._scheduler.pending == 2
        before = len(updates)
        assert wait_for(lambda: len(updates) == before + 2)
        assert {u.metadata["group"] for u in updates[-2:]} == {
            CorrelationGroup.standalone("samx", "samx"),
            CorrelationGroup.standalone("samy", "samy"),
        }
        sub.close()

    def test_terminal_flush_cancels_pending_entry(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="live", callback=updates.append, min_emit_interval=1.0
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)
        assert data_api._scheduler.pending == 1
        n = len(updates)
        sub._on_scan_status({"scan_id": "scan_1", "status": "closed"}, {})
        assert len(updates) == n + 1 and updates[-1].reason == "live"
        assert len(updates[-1].aligned_ordinals) == 3
        assert data_api._scheduler.pending == 0 and sub._slots[SCAN_GROUP].pending is None
        sub.close()

    def test_callback_exception_does_not_kill_emission(self, data_api, mock_client):
        item = make_open_scan(mock_client)

        def bad(update):
            if update.reason == "live":
                raise RuntimeError("consumer bug")

        good = []
        sub_bad = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=bad, min_emit_interval=0.1
        )
        sub_good = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=good.append, min_emit_interval=0.1
        )
        seed(item, "scan_1", 2, 3)
        with mock.patch("bec_lib.data_api.subscription.logger") as log:
            segment(data_api)
            assert wait_for(lambda: len(good) == 2)
            assert wait_for(lambda: log.exception.called)
        assert data_api._scheduler.thread.is_alive()
        sub_bad.close()
        sub_good.close()


class TestClose:
    def test_close_drops_pending_flush(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")],
            scan="scan_1",
            callback=updates.append,
            min_emit_interval=0.2,
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)
        assert data_api._scheduler.pending == 1
        n = len(updates)
        sub.close()
        assert data_api._scheduler.pending == 0
        time.sleep(0.3)
        assert len(updates) == n

    def test_close_waits_for_inflight_delivery(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        gate = threading.Event()
        seen = []

        def blocking(update):
            seen.append(update)
            if update.reason == "live":
                gate.wait(2)

        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=blocking, min_emit_interval=0
        )
        seed(item, "scan_1", 2, 3)
        dispatcher = threading.Thread(target=segment, args=(data_api,), name="dispatcher")
        dispatcher.start()
        assert wait_for(lambda: len(seen) == 2)
        closed = threading.Event()
        timing = {}

        def closer():
            t0 = time.monotonic()
            sub.close()
            timing["elapsed"] = time.monotonic() - t0
            closed.set()

        threading.Thread(target=closer, name="gui").start()
        time.sleep(0.15)
        assert not closed.is_set(), "close() returned while a callback was running"
        released = time.monotonic()
        gate.set()
        assert closed.wait(0.3), "close() did not return promptly once the callback finished"
        assert timing["elapsed"] >= 0.15 and time.monotonic() - released < 0.3
        dispatcher.join(1)
        segment(data_api)
        assert len(seen) == 2  # nothing after close

    def test_close_gives_up_on_a_stuck_callback(self, data_api, mock_client, monkeypatch):
        item = make_open_scan(mock_client)
        gate = threading.Event()

        def stuck(update):
            if update.reason == "live":
                gate.wait(5)

        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=stuck, min_emit_interval=0
        )
        seed(item, "scan_1", 2, 3)
        dispatcher = threading.Thread(target=segment, args=(data_api,), name="dispatcher")
        dispatcher.start()
        assert wait_for(lambda: sub._inflight)
        monkeypatch.setattr("bec_lib.data_api.subscription._INFLIGHT_TIMEOUT", 0.2)
        with mock.patch("bec_lib.data_api.subscription.logger") as log:
            t0 = time.monotonic()
            sub.close()
            assert 0.2 <= time.monotonic() - t0 < 1.0
            assert log.warning.called
        gate.set()
        dispatcher.join(1)

    def test_close_from_inside_scheduler_callback_does_not_deadlock(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        holder = {}
        done = threading.Event()

        def cb(update):
            if update.reason == "live":
                holder["sub"].close()
                done.set()

        holder["sub"] = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=cb, min_emit_interval=0.05
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)
        assert done.wait(1)
        assert holder["sub"]._closed

    def test_nested_emission_on_same_thread_then_close(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        holder = {}

        def cb(update):
            if update.reason == "live":
                holder["sub"].set_sources([("samx", "samx")])  # nested backfill emission
                t0 = time.monotonic()
                holder["sub"].close()
                holder["elapsed"] = time.monotonic() - t0

        holder["sub"] = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=cb, min_emit_interval=0
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)
        assert holder["sub"]._closed and holder["elapsed"] < 0.5

    def test_data_api_close_stops_scheduler(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")],
            scan="scan_1",
            callback=updates.append,
            min_emit_interval=0.5,
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)  # arms a trailing flush
        sched = data_api._scheduler
        request = sub._requests[SCAN_GROUP][0]
        assert sched.thread is not None and sub._slots[SCAN_GROUP].pending == "live"
        data_api.close()
        assert not sched.thread.is_alive() and sched.stopped
        assert sched.schedule(object(), "x", time.monotonic(), lambda k: None) is False
        # A late worker/feed notification must not revive a closed facade.
        n = len(updates)
        request.bundle.get_series("samx", "samx", "monitored").insert(3, 3.0, 1.0)
        request.notify("live")
        assert len(updates) == n
        assert sub._closed and not sub._requests and not sub._callback_ids
        with pytest.raises(RuntimeError, match="closed"):
            data_api.subscribe(devices=["samx"], scan=None)
        sub.close()

    def test_data_api_close_from_inside_a_trailing_flush(self, data_api, mock_client):
        item = make_open_scan(mock_client)
        done = threading.Event()

        def cb(update):
            if update.reason == "live":
                data_api.close()  # stop() must not join its own thread
                done.set()

        data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=cb, min_emit_interval=0.05
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)
        assert done.wait(1)
        assert data_api._scheduler.stopped


class TestDeliveryOrder:
    def test_failed_source_change_keeps_pending_delivery_valid(self, data_api, mock_client):
        make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(devices=["samx"], scan="scan_1", callback=updates.append)
        with sub._lock:
            delayed = sub._snapshot(
                sub._requests[SCAN_GROUP][0], sub._slots[SCAN_GROUP], "live", time.monotonic()
            )
        generation = sub.generation
        with mock.patch.object(data_api.paths[0], "resolve", return_value=None):
            with pytest.raises(ValueError, match="new sources"):
                sub.set_sources([("samy", "samy")])
        assert sub.generation == generation
        sub._deliver(delayed)
        # The delivered update is the delayed snapshot (aligned on delivery).
        assert updates[-1].sources is delayed.update.sources
        assert updates[-1].generation == generation
        sub.close()

    def test_old_group_delivery_is_dropped_after_sources_change(self, data_api, mock_client):
        make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(devices=["samx"], scan="scan_1", callback=updates.append)
        with sub._lock:
            delayed = sub._snapshot(
                sub._requests[SCAN_GROUP][0], sub._slots[SCAN_GROUP], "live", time.monotonic()
            )
        # The new set has no scan group, so per-group delivery ordering alone
        # cannot suppress a delayed delivery from the previous binding.
        sub.set_sources([("unknown", "unknown")])
        before = len(updates)
        sub._deliver(delayed)
        assert len(updates) == before
        assert not sub._inflight
        sub.close()

    def test_superseded_snapshot_is_dropped_and_first_reason_adopted(self, data_api, mock_client):
        """Deliveries of one group are ordered: an older snapshot delivered
        after a newer one is dropped, and the first delivered update of a
        binding carries the binding's first reason."""
        make_open_scan(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=updates.append, min_emit_interval=0
        )
        request, slot = sub._requests[SCAN_GROUP][0], sub._slots[SCAN_GROUP]
        with sub._lock:
            newer = sub._snapshot(request, slot, "live", time.monotonic())
            older_seq = newer.seq - 1
        sub._deliver(newer)
        stale = sub._snapshot(request, slot, "live", time.monotonic())
        stale.seq = older_seq
        sub._deliver(stale)
        assert [u.reason for u in updates] == ["backfill", "live"]

        # A fresh binding whose first snapshot ("rebind") is superseded by a
        # "live" one before delivery: the delivered update reads "rebind".
        with sub._lock:
            slot.seq, slot.first_reason = 0, None
            sub._generation += 1
            first = sub._snapshot(request, slot, "rebind", time.monotonic())
            second = sub._snapshot(request, slot, "live", time.monotonic())
        sub._deliver(second)
        sub._deliver(first)
        assert [u.reason for u in updates[2:]] == ["rebind"]
        assert not sub._inflight
        sub.close()

    def test_close_inside_the_first_groups_callback_leaks_nothing(self, data_api, mock_client):
        """A consumer closing the subscription from the first callback of a
        re-bind (two groups) must not leave the second group registered."""
        mock_client.device_manager.devices = mock.MagicMock()
        mock_client.device_manager.devices.get = lambda name, default=None: mock.MagicMock()
        plugin = data_api.paths[0]
        seen = []
        holder = {}

        def close_on_rebind(update):
            seen.append(update)
            if update.reason == "rebind":
                holder["sub"].close()

        holder["sub"] = data_api.subscribe(
            devices=["samx"], scan=None, callback=close_on_rebind, min_emit_interval=0
        )
        sub = holder["sub"]
        sub.set_sources([("samx", "samx"), ("samy", "samy")])  # two standalone groups
        assert sub._closed and [u.reason for u in seen] == ["backfill", "rebind"]
        assert not sub._requests and not plugin._stream_requests and not plugin._feeds
        assert not sub._inflight and not sub._deferred

    def test_consumer_exception_on_backfill_does_not_abort_subscribe(self, data_api, mock_client):
        item = make_open_scan(mock_client)

        def raising(update):
            raise RuntimeError("consumer bug")

        with mock.patch("bec_lib.data_api.subscription.logger") as log:
            sub = data_api.subscribe(
                sources=[("samx", "samx")], scan="scan_1", callback=raising, min_emit_interval=0
            )
            assert log.exception.called
        assert sub.scan_id == "scan_1" and sub._requests
        other = []
        sub2 = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_1", callback=other.append, min_emit_interval=0
        )
        seed(item, "scan_1", 2, 3)
        segment(data_api)  # the raising subscriber does not starve the other one
        assert other[-1].reason == "live" and len(other[-1].aligned_ordinals) == 3
        sub.close()
        sub2.close()
