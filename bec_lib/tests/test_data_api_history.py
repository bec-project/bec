"""Tests for the DataAPI history plugin and live→history routing."""

import time
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.data_api import DataAPI, DataPath, apply_size_gate, total_estimated_bytes
from bec_lib.data_api.data_bundler import DataBundler
from bec_lib.data_api.data_path import BindContext, SourceRequest, SourceSpec
from bec_lib.data_api.models import SCAN_GROUP, CorrelationGroup
from bec_lib.data_api.paths import FileDataPath
from bec_lib.data_api.scenarios import Scenario, classify
from bec_lib.live_scan_data import LiveScanData
from bec_lib.scan_data_container import SignalSlab
from bec_lib.scan_items import ScanItem

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name


class _FakeSignalRef:
    def __init__(self, values, timestamps):
        self._data = {"value": values, "timestamp": timestamps}

    def read(self):
        return dict(self._data)


class _SlabSignalRef:
    """A stored signal that is read in blocks of ``rows``, like SignalDataReference."""

    def __init__(self, values, timestamps, rows, gate=None):
        self.values = np.asarray(values)
        self.timestamps = np.asarray(timestamps)
        self.rows = rows
        self.gate = gate
        self.read_blocks = 0
        self.closed = False

    def read(self):
        raise AssertionError("A reference with read_slabs() is read block by block.")

    def read_slabs(self):
        total = len(self.values)
        try:
            for start in range(0, total, self.rows):
                if self.gate is not None:
                    self.gate(start)
                stop = min(start + self.rows, total)
                self.read_blocks += 1
                yield SignalSlab(
                    start,
                    stop,
                    total,
                    {"value": self.values[start:stop], "timestamp": self.timestamps[start:stop]},
                )
        finally:
            self.closed = True


class _FakeDeviceRef(dict):
    def get(self, key, default=None):
        return super().get(key, default)


def make_history(scan_id, stored, columns, num_points):
    """Build a fake client.history serving one scan (real message model, so
    the pydantic _StoredDataInfo value type is exercised)."""
    msg = messages.ScanHistoryMessage(
        scan_id=scan_id,
        scan_number=1,
        dataset_number=1,
        file_path=f"/data/{scan_id}_master.h5",
        exit_status="closed",
        start_time=1.0,
        end_time=2.0,
        scan_name="line_scan",
        num_points=num_points,
        num_monitored_readouts=num_points,
        stored_data_info=deepcopy(stored),
    )
    devices = {}
    for (device, entry), (values, timestamps) in columns.items():
        devices.setdefault(device, _FakeDeviceRef())[entry] = _FakeSignalRef(values, timestamps)
    container = SimpleNamespace(devices=SimpleNamespace(get=devices.get))
    history = mock.MagicMock()
    history.get_message.side_effect = {scan_id: msg}.get
    history.get_by_scan_id.return_value = container
    return history, msg


@pytest.fixture
def mock_client(connected_connector):
    client = mock.MagicMock(spec=BECClient)
    client.started = True
    client.connector = connected_connector
    client.callbacks = mock.MagicMock()
    client.queue = mock.MagicMock()
    client.queue.scan_storage.current_scan_id = []
    client.queue.scan_storage.find_scan_by_ID.return_value = None
    client.device_manager = mock.MagicMock()
    client.device_manager.get_bec_signals.return_value = []
    return client


@pytest.fixture
def data_api(mock_client):
    DataAPI.clear_instance()
    api = DataAPI(mock_client)
    yield api
    DataAPI.clear_instance()


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class TestHistoryPlugin:
    def test_history_respects_max_points(self, data_api, mock_client):
        stored = {"samx": {"samx": {"shape": (5,), "dtype": "float64"}}}
        columns = {("samx", "samx"): ([0.0, 1.0, 2.0, 3.0, 4.0], [100, 101, 102, 103, 104])}
        mock_client.history, _ = make_history("bounded-history", stored, columns, num_points=5)
        updates = []
        sub = data_api.subscribe(
            sources=list(columns), scan="bounded-history", callback=updates.append, max_points=2
        )
        assert wait_for(lambda: updates)
        source = updates[-1].get("samx", "samx")
        assert source is not None
        assert len(source.values) == 2
        np.testing.assert_array_equal(source.ordinals, [3, 4])
        np.testing.assert_array_equal(source.values, [3.0, 4.0])
        sub.close()

    def test_vanished_history_entry_marks_every_source_as_error(self, data_api, mock_client):
        stored = {
            "samx": {"samx": {"shape": (2,), "dtype": "float64"}},
            "samy": {"samy": {"shape": (2,), "dtype": "float64"}},
        }
        columns = {
            ("samx", "samx"): ([0.0, 1.0], [100.0, 101.0]),
            ("samy", "samy"): ([2.0, 3.0], [100.0, 101.0]),
        }
        mock_client.history, msg = make_history("vanished-history", stored, columns, num_points=2)
        updates = []
        with mock.patch.object(FileDataPath, "_history_message", side_effect=[msg, None]):
            sub = data_api.subscribe(
                sources=list(columns), scan="vanished-history", callback=updates.append
            )
        assert wait_for(lambda: updates)
        update = updates[-1]
        assert set(update.sources) == set(columns)
        assert not update.complete
        for source in update.sources.values():
            assert not source.complete
            assert source.metadata["error"] == "History entry disappeared before it could be read."
        sub.close()

    def test_terminal_scan_served_from_file(self, data_api, mock_client):
        stored = {
            "samx": {"samx": {"shape": (3,), "dtype": "float64"}},
            "det": {"wave": {"shape": (3, 100), "dtype": "float64"}},
        }
        columns = {
            ("samx", "samx"): ([0.0, 1.0, 2.0], [100.0, 101.0, 102.0]),
            ("det", "wave"): ([[1] * 3, [2] * 3, [3] * 3], [200.0, 201.0, 202.0]),
        }
        history, _ = make_history("scan_h", stored, columns, num_points=3)
        mock_client.history = history
        mock_client.device_manager.get_bec_signals.return_value = [
            ("det", None, {"obj_name": "wave", "storage_name": "wave"})
        ]

        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")], scan="scan_h", callback=updates.append
        )
        # The monitored signal and the async signal are separate correlation groups.
        assert sub.groups == {
            SCAN_GROUP: [("samx", "samx")],
            CorrelationGroup.standalone("det", "wave"): [("det", "wave")],
        }
        assert wait_for(lambda: len({u.metadata["group"] for u in updates}) == 2)
        by_group = {update.metadata["group"]: update for update in updates}
        scan_update = by_group[SCAN_GROUP]
        assert scan_update.reason == "history"
        assert scan_update.scan_id == "scan_h"
        assert list(scan_update.aligned_ordinals) == [0, 1, 2]
        assert scan_update.complete
        assert list(scan_update.aligned()[("samx", "samx")]) == [0.0, 1.0, 2.0]
        assert scan_update.get("samx", "samx").kind == "monitored"
        wave = by_group[CorrelationGroup.standalone("det", "wave")].get("det", "wave")
        assert np.array_equal(wave.as_array(), [[1] * 3, [2] * 3, [3] * 3])
        assert wave.kind == "async"
        assert wave.metadata["file_path"] == "/data/scan_h_master.h5"
        sub.close()

    def test_missing_signal_reported_unbound(self, data_api, mock_client):
        stored = {"samx": {"samx": {"shape": (2,), "dtype": "float64"}}}
        columns = {("samx", "samx"): ([0.0, 1.0], [100.0, 101.0])}
        history, _ = make_history("scan_h", stored, columns, num_points=2)
        mock_client.history = history

        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("ghost", "ghost")], scan="scan_h", callback=updates.append
        )
        assert sub.unbound_sources == [("ghost", "ghost")]
        assert wait_for(lambda: updates)
        assert list(updates[-1].aligned()[("samx", "samx")]) == [0.0, 1.0]
        sub.close()

    def test_writer_latency_window_uses_live_data(self, data_api, mock_client):
        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None
        item = ScanItem(queue_id="q", scan_number=[1], scan_id=["scan_t"], status="closed")
        item.status_message = messages.ScanStatusMessage(scan_id="scan_t", status="closed", info={})
        item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
        item.live_data = LiveScanData()
        for i in range(2):
            msg = messages.ScanMessage(
                point_id=i,
                scan_id="scan_t",
                data={"samx": {"samx": {"value": float(i), "timestamp": 100.0 + i}}},
                metadata={"scan_id": "scan_t"},
            )
            item.live_data.set(i, msg)
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
            item if sid == "scan_t" else None
        )

        updates = []
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_t", callback=updates.append)
        assert updates and updates[-1].reason == "history"
        assert list(updates[-1].aligned()[("samx", "samx")]) == [0.0, 1.0]
        assert {(s.path_name, s.data_source) for s in sub._specs} == {("RedisDataPath", "redis")}
        sub.close()

    def test_published_file_wins_over_scan_memory(self, data_api, mock_client):
        """The Redis path is consulted first for CLOSED_SCAN but declines as
        soon as the history entry exists: the file is authoritative."""
        item = ScanItem(queue_id="q", scan_number=[1], scan_id=["scan_t"], status="closed")
        item.status_message = messages.ScanStatusMessage(scan_id="scan_t", status="closed", info={})
        item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
        item.live_data = LiveScanData()
        item.live_data.set(
            0,
            messages.ScanMessage(
                point_id=0,
                scan_id="scan_t",
                data={"samx": {"samx": {"value": -1.0, "timestamp": 100.0}}},
                metadata={"scan_id": "scan_t"},
            ),
        )
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
            item if sid == "scan_t" else None
        )
        stored = {"samx": {"samx": {"shape": (2,), "dtype": "float64"}}}
        columns = {("samx", "samx"): ([5.0, 6.0], [100.0, 101.0])}
        mock_client.history, _ = make_history("scan_t", stored, columns, num_points=2)

        updates = []
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_t", callback=updates.append)
        assert {(s.path_name, s.data_source) for s in sub._specs} == {("FileDataPath", "file")}
        assert wait_for(lambda: updates)
        assert list(updates[-1].aligned()[("samx", "samx")]) == [5.0, 6.0]
        sub.close()

    def test_force_source_pins_the_data_path(self, data_api, mock_client):
        """``force_source`` restricts routing to one data source for the
        life of the subscription; a concrete scan that source cannot serve
        raises instead of falling through."""
        item = ScanItem(queue_id="q", scan_number=[1], scan_id=["scan_t"], status="closed")
        item.status_message = messages.ScanStatusMessage(scan_id="scan_t", status="closed", info={})
        item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
        item.live_data = LiveScanData()
        item.live_data.set(
            0,
            messages.ScanMessage(
                point_id=0,
                scan_id="scan_t",
                data={"samx": {"samx": {"value": -1.0, "timestamp": 100.0}}},
                metadata={"scan_id": "scan_t"},
            ),
        )
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
            item if sid == "scan_t" else None
        )
        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None

        # Writer-latency window: only Redis (scan memory) can serve it.
        with pytest.raises(ValueError, match="'file' cannot serve scan 'scan_t'"):
            data_api.subscribe(sources=[("samx", "samx")], scan="scan_t", force_source="file")
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_t", force_source="redis")
        assert sub.force_source == "redis"
        assert {s.data_source for s in sub._specs} == {"redis"}
        sub.close()

        # Published file: forcing Redis refuses (Redis declines closed scans
        # with a history entry), forcing the file serves the file.
        stored = {"samx": {"samx": {"shape": (2,), "dtype": "float64"}}}
        columns = {("samx", "samx"): ([5.0, 6.0], [100.0, 101.0])}
        mock_client.history, _ = make_history("scan_t", stored, columns, num_points=2)
        with pytest.raises(ValueError, match="'redis' cannot serve scan 'scan_t'"):
            data_api.subscribe(sources=[("samx", "samx")], scan="scan_t", force_source="redis")
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_t", callback=updates.append, force_source="file"
        )
        assert {s.data_source for s in sub._specs} == {"file"}
        assert wait_for(lambda: updates)
        assert list(updates[-1].aligned()[("samx", "samx")]) == [5.0, 6.0]
        sub.close()

        assert data_api.estimate_bytes([("samx", "samx")], "scan_t", force_source="file") == 16
        assert data_api.estimate_bytes([("samx", "samx")], "scan_t", force_source="redis") is None
        with pytest.raises(ValueError, match="Unknown data source 'rest'"):
            data_api.subscribe(sources=[("samx", "samx")], scan="scan_t", force_source="rest")

    def test_concrete_scan_reroutes_from_scan_memory_to_the_file(self, data_api, mock_client):
        """A concrete-scan subscription bound in the writer-latency window is
        re-routed to the authoritative file when it is published (async
        sources become available); a file binding is not re-read on later
        history updates."""
        mock_client.device_manager.get_bec_signals.return_value = [
            ("det", None, {"obj_name": "wave", "storage_name": "wave"})
        ]
        item = ScanItem(queue_id="q", scan_number=[1], scan_id=["scan_t"], status="closed")
        item.status_message = messages.ScanStatusMessage(scan_id="scan_t", status="closed", info={})
        item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
        item.live_data = LiveScanData()
        item.live_data.set(
            0,
            messages.ScanMessage(
                point_id=0,
                scan_id="scan_t",
                data={"samx": {"samx": {"value": -1.0, "timestamp": 100.0}}},
                metadata={"scan_id": "scan_t"},
            ),
        )
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
            item if sid == "scan_t" else None
        )
        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")], scan="scan_t", callback=updates.append
        )
        assert {s.path_name for s in sub._specs} == {"RedisDataPath"}
        assert sub.unbound_sources == [("det", "wave")]

        stored = {
            "samx": {"samx": {"shape": (2,), "dtype": "float64"}},
            "det": {"wave": {"shape": (2, 3), "dtype": "float64"}},
        }
        columns = {
            ("samx", "samx"): ([5.0, 6.0], [1.0, 2.0]),
            ("det", "wave"): (np.zeros((2, 3)), [1.0, 2.0]),
        }
        mock_client.history, msg = make_history("scan_t", stored, columns, 2)
        sub._on_scan_history_update(history_msg=msg)
        assert {s.path_name for s in sub._specs} == {"FileDataPath"}
        assert sub.unbound_sources == []

        # Two groups (scan, standalone wave) each emit their own history
        # update from their own worker; the order between them is arbitrary,
        # so wait for both before sampling the read count.
        def both_arrived():
            history = [u for u in updates if u.reason == "history"]
            return len({u.metadata["group"] for u in history}) == 2 and any(
                u.get("det", "wave") and len(u.get("det", "wave").ordinals) for u in history
            )

        assert wait_for(both_arrived)
        reads = mock_client.history.get_by_scan_id.call_count
        sub._on_scan_history_update(history_msg=msg)  # already served by the file
        assert mock_client.history.get_by_scan_id.call_count == reads
        sub.close()

    def test_open_scan_only_path_hands_a_concrete_scan_to_the_file(self, data_api, mock_client):
        """A path that never declares CLOSED_SCAN cannot keep a scan once its
        history entry exists, whatever its resolve() says."""
        calls = []

        class StickyOpenPath(DataPath):
            data_source = "sticky"
            scenarios = frozenset({Scenario.OPEN_SCAN})
            priority = 1

            def resolve(self, sources, context):
                calls.append(context.scenario)
                return [SourceSpec(device=d, entry=e, kind="monitored") for d, e in sources]

            def _open(self, request):
                request.notify("backfill")

            def close(self, request):
                pass

        data_api.register_path(StickyOpenPath(mock_client))
        item = ScanItem(queue_id="q", scan_number=[1], scan_id=["scan_t"], status="open")
        item.status_message = messages.ScanStatusMessage(scan_id="scan_t", status="open", info={})
        item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
        item.live_data = LiveScanData()
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
            item if sid == "scan_t" else None
        )
        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None
        updates = []
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_t", callback=updates.append)
        assert {s.path_name for s in sub._specs} == {"StickyOpenPath"}
        item.status = "closed"
        stored = {"samx": {"samx": {"shape": (2,), "dtype": "float64"}}}
        mock_client.history, msg = make_history(
            "scan_t", stored, {("samx", "samx"): ([5.0, 6.0], [1.0, 2.0])}, 2
        )
        sub._on_scan_history_update(history_msg=msg)
        assert {s.path_name for s in sub._specs} == {"FileDataPath"}
        assert Scenario.CLOSED_SCAN not in calls  # never asked about a scenario it does not serve
        assert wait_for(lambda: updates and updates[-1].reason == "history")
        sub.close()

    def test_set_sources_on_a_file_binding_emits_rebind(self, data_api, mock_client):
        stored = {
            "samx": {"samx": {"shape": (2,), "dtype": "float64"}},
            "samy": {"samy": {"shape": (2,), "dtype": "float64"}},
        }
        columns = {
            ("samx", "samx"): ([5.0, 6.0], [1.0, 2.0]),
            ("samy", "samy"): ([7.0, 8.0], [1.0, 2.0]),
        }
        mock_client.history, _ = make_history("scan_h", stored, columns, 2)
        updates = []
        sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_h", callback=updates.append)
        assert wait_for(lambda: updates and updates[-1].reason == "history")
        sub.set_sources([("samx", "samx"), ("samy", "samy")])
        assert wait_for(lambda: updates[-1].reason == "rebind" and updates[-1].get("samy", "samy"))
        assert list(updates[-1].aligned()[("samy", "samy")]) == [7.0, 8.0]
        sub.close()

    def test_set_sources_that_no_path_can_serve(self, data_api, mock_client):
        """A failed re-bind must not leave the subscription silently serving the
        old sources: a concrete scan raises and keeps its binding, a live
        follow drops the stale binding and waits."""
        item = ScanItem(queue_id="q", scan_number=[1], scan_id=["scan_t"], status="closed")
        item.status_message = messages.ScanStatusMessage(scan_id="scan_t", status="closed", info={})
        item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
        item.live_data = LiveScanData()
        item.live_data.set(
            0,
            messages.ScanMessage(
                point_id=0,
                scan_id="scan_t",
                data={"samx": {"samx": {"value": 1.0, "timestamp": 100.0}}},
                metadata={"scan_id": "scan_t"},
            ),
        )
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
            item if sid == "scan_t" else None
        )
        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_t", callback=updates.append, force_source="redis"
        )
        # The file gets published: Redis now declines the scan.
        stored = {"samx": {"samx": {"shape": (2,), "dtype": "float64"}}}
        columns = {("samx", "samx"): ([5.0, 6.0], [1.0, 2.0])}
        mock_client.history, _ = make_history("scan_t", stored, columns, 2)
        with pytest.raises(ValueError, match="cannot serve scan 'scan_t' with the new sources"):
            sub.set_sources([("samx", "samx"), ("samy", "samy")])
        assert sub.sources == [("samx", "samx")] and sub.unbound_sources == []
        assert sub._requests  # previous binding intact
        sub.close()

        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None
        item.status = "open"
        mock_client.queue.scan_storage.current_scan_id = ["scan_t"]
        follow = data_api.subscribe(
            sources=[("samx", "samx")], scan="live", callback=updates.append
        )
        assert follow.scan_id == "scan_t"
        request = follow._requests[SCAN_GROUP][0]
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: None  # vanished
        with pytest.raises(ValueError, match="No data source can serve scan 'scan_t'"):
            follow.set_sources([("samy", "samy")])
        # The live follow keeps its previous set and binding.
        assert follow.scan_id == "scan_t" and follow.sources == [("samx", "samx")]
        assert follow._requests[SCAN_GROUP][0] is request and follow.unbound_sources == []
        follow.close()

    def test_live_follow_reroutes_to_history_on_publication(self, data_api, mock_client):
        """A live-follow subscription hands over to the authoritative file
        once the scan-history entry appears."""
        item = ScanItem(queue_id="q", scan_number=[1], scan_id=["scan_1"], status="open")
        item.status_message = messages.ScanStatusMessage(scan_id="scan_1", status="open", info={})
        item.status_message.readout_priority = {"monitored": ["samx"], "baseline": []}
        item.live_data = LiveScanData()
        mock_client.queue.scan_storage.find_scan_by_ID.side_effect = lambda sid: (
            item if sid == "scan_1" else None
        )
        mock_client.queue.scan_storage.current_scan_id = ["scan_1"]
        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None

        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="live", callback=updates.append, min_emit_interval=0
        )
        assert sub.scan_id == "scan_1"

        # Scan ends; file gets published with corrected values.
        item.status = "closed"
        stored = {"samx": {"samx": {"shape": (2,), "dtype": "float64"}}}
        columns = {("samx", "samx"): ([5.0, 6.0], [100.0, 101.0])}
        history, msg = make_history("scan_1", stored, columns, num_points=2)
        mock_client.history = history

        sub._on_scan_history_update(history_msg=msg)
        assert wait_for(lambda: updates and updates[-1].reason == "history")
        assert list(updates[-1].aligned()[("samx", "samx")]) == [5.0, 6.0]
        sub.close()

    def test_unknown_scan_still_raises(self, data_api, mock_client):
        mock_client.history = mock.MagicMock()
        mock_client.history.get_message.return_value = None
        with pytest.raises(ValueError):
            data_api.subscribe(sources=[("samx", "samx")], scan="nope", callback=lambda u: None)


class TestSizeGuard:
    def _history(self, mock_client, num_points=1000, entry_shape=(1000, 500)):
        stored = {
            "samx": {"samx": {"shape": (num_points,), "dtype": "float64"}},
            "det": {"wave": {"shape": entry_shape, "dtype": "float64"}},
        }
        columns = {
            ("samx", "samx"): ([0.0] * 3, [1.0] * 3),
            ("det", "wave"): ([[0.0]] * 3, [1.0] * 3),
        }
        history, msg = make_history("scan_big", stored, columns, num_points=num_points)
        mock_client.history = history
        return msg

    def test_estimate_matches_shapes_and_dtype(self, data_api, mock_client):
        self._history(mock_client)
        # 1000*8 + 1000*500*8 bytes
        expected = 1000 * 8 + 1000 * 500 * 8
        assert data_api.estimate_bytes([("samx", "samx"), ("det", "wave")], "scan_big") == expected

    def test_estimate_needs_no_file_access(self, data_api, mock_client):
        self._history(mock_client)
        data_api.estimate_bytes([("det", "wave")], "scan_big")
        mock_client.history.get_by_scan_id.assert_not_called()

    def test_oversized_subscription_is_withheld_until_confirmed(self, data_api, mock_client):
        self._history(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")],
            scan="scan_big",
            callback=updates.append,
            size_limit_bytes=1024,
        )
        assert sub.size_gated is True
        assert sub.estimated_bytes == 1000 * 8 + 1000 * 500 * 8
        # Nothing was read and nothing was delivered.
        assert updates == []
        mock_client.history.get_by_scan_id.assert_not_called()

        sub.confirm_size()
        assert sub.size_gated is False
        assert wait_for(lambda: updates)
        assert updates[-1].reason == "history"
        sub.close()

    def test_within_limit_loads_immediately(self, data_api, mock_client):
        self._history(mock_client, num_points=3, entry_shape=(3,))
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")],
            scan="scan_big",
            callback=updates.append,
            size_limit_bytes=10_000_000,
        )
        assert sub.size_gated is False
        assert wait_for(lambda: updates)
        sub.close()

    def test_no_limit_never_gates(self, data_api, mock_client):
        self._history(mock_client)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")], scan="scan_big", callback=updates.append
        )
        assert sub.size_gated is False
        assert wait_for(lambda: updates)
        sub.close()

    def test_history_read_runs_off_the_caller_thread(self, data_api, mock_client):
        """The file read must not block the calling (GUI) thread."""
        import threading

        self._history(mock_client, num_points=3, entry_shape=(3,))
        caller_thread = threading.current_thread().name
        threads = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")],
            scan="scan_big",
            callback=lambda u: threads.append(threading.current_thread().name),
        )
        assert wait_for(lambda: threads)
        assert threads[0] != caller_thread
        sub.close()


def test_async_source_resolved_by_storage_name(data_api, mock_client):
    """The file writer keys async datasets by storage_name, not obj_name: a
    subscription using the obj_name must still resolve, read and emit under
    the obj_name key (e2e handover regression)."""
    stored = {
        "samx": {"samx": {"shape": (3,), "dtype": "float64"}},
        "waveform": {"waveform_0d": {"shape": (3,), "dtype": "float64"}},
    }
    columns = {
        ("samx", "samx"): ([0.0, 1.0, 2.0], [100.0, 101.0, 102.0]),
        ("waveform", "waveform_0d"): ([10.0, 20.0, 30.0], [100.0, 101.0, 102.0]),
    }
    history, _ = make_history("scan_h", stored, columns, num_points=3)
    mock_client.history = history
    mock_client.device_manager.get_bec_signals.return_value = [
        ("waveform", None, {"obj_name": "waveform_waveform_0d", "storage_name": "waveform_0d"})
    ]

    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("waveform", "waveform_waveform_0d")],
        scan="scan_h",
        callback=updates.append,
    )
    assert sub.unbound_sources == []
    key = ("waveform", "waveform_waveform_0d")
    assert wait_for(lambda: any(key in u.sources and len(u.get(*key).values) == 3 for u in updates))
    update = next(u for u in reversed(updates) if key in u.sources)
    # Emitted under the subscription's obj_name key, read from the storage key.
    assert set(update.sources) == {key}
    assert list(update.get(*key).values) == [10.0, 20.0, 30.0]
    assert update.get(*key).kind == "async"
    # The estimate finds the dataset through the same translation.
    assert data_api.estimate_bytes(sub.sources, "scan_h") == 3 * 8 + 3 * 8
    sub.close()


def test_async_storage_key_derived_without_device_info(data_api, mock_client):
    """Even with no device info at all (get_bec_signals empty — e.g. config
    reload in flight at handover time), the storage key is derived from the
    obj_name convention f"{device}_{storage_name}" (e2e flake regression)."""
    stored = {"waveform": {"waveform_0d": {"shape": (2,), "dtype": "float64"}}}
    columns = {("waveform", "waveform_0d"): ([1.0, 2.0], [100.0, 101.0])}
    history, _ = make_history("scan_h", stored, columns, num_points=2)
    mock_client.history = history
    mock_client.device_manager.get_bec_signals.return_value = []

    updates = []
    sub = data_api.subscribe(
        sources=[("waveform", "waveform_waveform_0d")], scan="scan_h", callback=updates.append
    )
    assert sub.unbound_sources == []
    assert wait_for(lambda: updates and list(updates[-1].aligned_ordinals) == [0, 1])
    assert list(updates[-1].aligned()[("waveform", "waveform_waveform_0d")]) == [1.0, 2.0]
    sub.close()


def test_history_without_stored_data_info_still_serves(data_api, mock_client):
    """Older ScanHistoryMessages carry no stored_data_info; the scan must
    still be served (classification degrades to the device declaration and
    the file read decides)."""
    columns = {("samx", "samx"): ([0.0, 1.0], [100.0, 101.0])}
    history, msg = make_history("scan_h", {"samx": {"samx": {"shape": (2,)}}}, columns, 2)
    object.__setattr__(msg, "stored_data_info", None)
    mock_client.history = history

    updates = []
    sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_h", callback=updates.append)
    assert sub.unbound_sources == []
    assert wait_for(lambda: updates and list(updates[-1].aligned_ordinals) == [0, 1])
    assert list(updates[-1].aligned()[("samx", "samx")]) == [0.0, 1.0]
    sub.close()


def test_history_without_stored_data_info_leaves_evidenced_sources_to_other_paths(
    data_api, mock_client
):
    """Without stored_data_info the file path cannot tell what the file holds. A path
    that positively serves a source therefore comes first; the file keeps the rest."""
    samx, outside = ("samx", "samx"), ("archive", "ring_current")
    columns = {samx: ([0.0, 1.0], [100.0, 101.0])}
    history, msg = make_history("scan_h", {"samx": {"samx": {"shape": (2,)}}}, columns, 2)
    object.__setattr__(msg, "stored_data_info", None)
    mock_client.history = history

    class Outside(DataPath):
        data_source = "outside"
        scenarios = frozenset({Scenario.CLOSED_SCAN})
        priority = 100  # consulted after the file path

        def resolve(self, sources, context):
            return [SourceSpec(*key, kind="unindexed") for key in sources if key == outside]

        def _open(self, request):
            request.series(outside).extend([5.0, 6.0], [1.0, 2.0])
            request.notify("history")

        def close(self, request):
            """Nothing to release."""

    data_api.register_path(Outside(mock_client))
    updates = []
    sub = data_api.subscribe(sources=[samx, outside], scan="scan_h", callback=updates.append)
    assert sub.routes == {samx: "file", outside: "outside"}
    assert wait_for(
        lambda: any(
            list(u.sources[outside].values) == [5.0, 6.0] for u in updates if outside in u.sources
        )
    )
    assert wait_for(
        lambda: any(
            list(u.sources[samx].values) == [0.0, 1.0] for u in updates if samx in u.sources
        )
    )
    sub.close()


def test_close_joins_history_worker(data_api, mock_client):
    """Closing a history subscription must not leak the worker thread."""
    import threading

    columns = {("samx", "samx"): ([0.0], [100.0])}
    history, _ = make_history("scan_h", {"samx": {"samx": {"shape": (1,)}}}, columns, 1)
    mock_client.history = history
    sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_h", callback=lambda u: None)
    sub.close()
    assert not any(t.name == "data-api-history" and t.is_alive() for t in threading.enumerate())


def test_file_read_delivers_numpy_columns(data_api, mock_client):
    """The history worker must hand the file's numpy columns through unchanged;
    tuples of Python floats forced an O(n) rebuild on the GUI thread."""
    file_values = np.arange(4.0)
    file_timestamps = np.arange(4.0) + 100.0
    stored = {"samx": {"samx": {"shape": (4,), "dtype": "float64"}}}
    columns = {("samx", "samx"): (file_values, file_timestamps)}
    history, _ = make_history("scan_np", stored, columns, num_points=4)
    mock_client.history = history

    updates = []
    sub = data_api.subscribe(sources=[("samx", "samx")], scan="scan_np", callback=updates.append)
    assert wait_for(lambda: updates)
    source = updates[-1].sources[("samx", "samx")]
    assert isinstance(source.values, np.ndarray)
    assert np.shares_memory(source.values, file_values)
    assert isinstance(updates[-1].aligned_ordinals, np.ndarray)
    sub.close()


def test_subclass_serves_stored_data_through_load_container(data_api, mock_client):
    """A path that fetches stored data elsewhere overrides only ``_load_container``
    and keeps the worker, decoding and delivery of FileDataPath."""
    stored = {"samx": {"samx": {"shape": (3,), "dtype": "float64"}}}
    columns = {("samx", "samx"): (np.arange(3.0), np.arange(3.0) + 100.0)}
    history, _ = make_history("scan_archive", stored, columns, num_points=3)
    archive = history.get_by_scan_id.return_value
    history.get_by_scan_id.return_value = None  # the scan file itself is gone
    mock_client.history = history
    loads = []

    class ArchivePath(FileDataPath):
        def _load_container(self, request, msg):
            loads.append((request.scan_id, msg.file_path))
            return archive

    data_api.register_path(ArchivePath(mock_client), replace=True)
    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx")], scan="scan_archive", callback=updates.append
    )
    assert wait_for(lambda: updates)
    assert list(updates[-1].sources[("samx", "samx")].values) == [0.0, 1.0, 2.0]
    assert loads == [("scan_archive", "/data/scan_archive_master.h5")]
    history.get_by_scan_id.assert_not_called()
    sub.close()


def test_history_read_invokes_progress_callback(data_api, mock_client):
    """subscribe(progress_callback=...) reaches the file read and ends at 1.0."""
    stored = {"samx": {"samx": {"shape": (3,), "dtype": "float64"}}}
    columns = {("samx", "samx"): ([0.0, 1.0, 2.0], [100.0, 101.0, 102.0])}
    history, _ = make_history("scan_p", stored, columns, num_points=3)
    mock_client.history = history

    updates, fractions = [], []
    sub = data_api.subscribe(
        sources=[("samx", "samx")],
        scan="scan_p",
        callback=updates.append,
        progress_callback=fractions.append,
    )
    assert wait_for(lambda: updates)
    assert fractions and fractions[-1] == 1.0
    sub.close()


class TestSizeGateInThePath:
    """The gate is decided by the path at resolve time, enforced by
    ``DataPath.open`` and released by ``DataPath.confirm_size``."""

    STORED = {
        "samx": {"samx": {"shape": (4,), "dtype": "float64"}},
        "det": {"wave": {"shape": (4, 100), "dtype": "float64"}},
    }
    COLUMNS = {
        ("samx", "samx"): ([0.0, 1.0, 2.0, 3.0], [1.0, 2.0, 3.0, 4.0]),
        ("det", "wave"): (np.zeros((4, 100)), [1.0, 2.0, 3.0, 4.0]),
    }

    def test_apply_size_gate_and_total_estimated_bytes(self):
        specs = [
            SourceSpec(device="a", entry="a", kind="monitored", estimated_bytes=100),
            SourceSpec(device="b", entry="b", kind="async", estimated_bytes=50, available=False),
            SourceSpec(device="c", entry="c", kind="monitored"),
        ]
        assert total_estimated_bytes(specs) == 100  # unavailable specs are ignored
        assert total_estimated_bytes([specs[2]]) is None
        assert apply_size_gate(specs, 80) is True
        # The gate is a request-level decision: every available spec is marked,
        # never an unavailable one (it could not be released by confirm_size).
        assert [s.size_gated for s in specs] == [True, False, True]
        assert apply_size_gate(specs, None) is False  # re-application resets stale marks
        assert not any(s.size_gated for s in specs)
        assert apply_size_gate([specs[2]], 0) is False  # nothing estimable: never gated

    def test_resolve_marks_available_specs_size_gated(self, data_api, mock_client):
        mock_client.history, _ = make_history("scan_h", self.STORED, self.COLUMNS, num_points=4)
        file_path = next(p for p in data_api.paths if p.data_source == "file")
        sources = [("samx", "samx"), ("det", "wave"), ("ghost", "ghost")]
        gated = file_path.resolve(
            sources,
            BindContext("scan_h", Scenario.CLOSED_SCAN, have_item=True, size_limit_bytes=1024),
        )
        assert [s.size_gated for s in gated] == [True, True, False]
        assert gated[2].available is False
        free = file_path.resolve(
            sources, BindContext("scan_h", Scenario.CLOSED_SCAN, have_item=True)
        )
        assert not any(s.size_gated for s in free)
        # estimate_bytes never gates (its context carries no limit)
        assert data_api.estimate_bytes(sources, "scan_h") == 4 * 8 + 4 * 100 * 8

    def test_open_template_withholds_and_confirm_size_reopens(self, data_api, mock_client):
        mock_client.history, _ = make_history("scan_h", self.STORED, self.COLUMNS, num_points=4)
        file_path = next(p for p in data_api.paths if p.data_source == "file")
        context = BindContext("scan_h", Scenario.CLOSED_SCAN, have_item=True, size_limit_bytes=16)
        specs = file_path.resolve([("samx", "samx")], context)
        sink = mock.MagicMock()
        bundle = DataBundler("scan_h")
        bundle.get_series("samx", "samx", "monitored")
        request = SourceRequest(
            scan_id="scan_h",
            group=SCAN_GROUP,
            specs=specs,
            bundle=bundle,
            sink=sink,
            context=context,
        )
        assert request.size_gated and request.estimated_bytes == 32
        with data_api._lock:
            file_path.open(request)
        assert "worker" not in request.state
        mock_client.history.get_by_scan_id.assert_not_called()
        with data_api._lock:
            file_path.confirm_size(request)
        assert not request.size_gated
        assert wait_for(lambda: sink.request_emission.called)
        sink.request_emission.assert_called_once_with(request, "history")
        assert list(bundle.build_update("history").aligned()[("samx", "samx")]) == [
            0.0,
            1.0,
            2.0,
            3.0,
        ]
        file_path.join(request, timeout=5)

    def test_size_gate_clears_after_confirm_with_unavailable_source(self, data_api, mock_client):
        mock_client.history, _ = make_history("scan_h", self.STORED, self.COLUMNS, num_points=4)
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave"), ("ghost", "ghost")],
            scan="scan_h",
            callback=updates.append,
            size_limit_bytes=1024,
        )
        assert sub.size_gated and sub.estimated_bytes == 4 * 8 + 4 * 100 * 8
        assert sub.unbound_sources == [("ghost", "ghost")]
        assert updates == []
        sub.confirm_size()
        assert not sub.size_gated
        assert wait_for(lambda: updates)
        assert updates[-1].reason == "history"
        sub.close()

    def test_custom_path_can_override_confirm_size_and_join(self, data_api, mock_client):
        mock_client.history, _ = make_history("scan_h", self.STORED, self.COLUMNS, num_points=4)
        calls = []

        class GatingPath(DataPath):
            data_source = "custom"
            scenarios = frozenset({Scenario.CLOSED_SCAN})
            priority = 1

            def __init__(self, client, lock=None):
                super().__init__(client, lock)

            def resolve(self, sources, context):
                specs = [
                    SourceSpec(device=d, entry=e, kind="monitored", estimated_bytes=500)
                    for d, e in sources
                ]
                apply_size_gate(specs, context.size_limit_bytes)
                return specs

            def _open(self, request):
                calls.append(("open", request.group, request.context.have_item))
                request.notify("history")

            def confirm_size(self, request):
                calls.append(("confirm", request.group))
                super().confirm_size(request)

            def close(self, request):
                calls.append(("close", request.group))

            def join(self, request, timeout):
                calls.append(("join", request.group, timeout))

        data_api.register_path(GatingPath(mock_client))
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")], scan="scan_h", callback=updates.append, size_limit_bytes=100
        )
        assert sub.size_gated and calls == []
        assert {s.path_name for s in sub._specs} == {"GatingPath"}
        # A binding made on a dispatcher thread carries no scan item: the
        # facade prefetches before handing the request back to the path.
        request = sub._requests[SCAN_GROUP][0]
        request.context = replace(request.context, scan_item=None, have_item=False)
        sub.confirm_size()
        assert calls == [("confirm", SCAN_GROUP), ("open", SCAN_GROUP, True)]
        assert updates and updates[-1].reason == "history"
        sub.confirm_size()  # idempotent: nothing gated any more
        assert calls == [("confirm", SCAN_GROUP), ("open", SCAN_GROUP, True)]
        sub.close()
        assert calls[-2] == ("close", SCAN_GROUP)
        # close() joins within one shared deadline: the path gets what is left of it.
        # It is short: a worker still inside a blocking read is left to the reaper.
        assert calls[-1][:2] == ("join", SCAN_GROUP) and 0 < calls[-1][2] <= 0.25


def test_stored_datasets_with_one_row_per_readout_pair_by_row(data_api, mock_client):
    """The scan file does not say how a dataset was acquired: datasets with one
    row per monitored readout pair row by row (x against y of the same scan);
    a dataset of another length stays standalone."""
    stored = {
        "samx": {"samx": {"shape": (2,), "dtype": "float64"}},
        "det": {"wave": {"shape": (2, 3), "dtype": "float64"}},
        "waveform": {"data": {"shape": (5,), "dtype": "float64"}},
    }
    columns = {
        ("samx", "samx"): ([1, 2], [10, 20]),
        ("det", "wave"): ([[1, 2, 3], [4, 5, 6]], [10, 20]),
        ("waveform", "data"): ([1, 2, 3, 4, 5], [10, 11, 12, 13, 14]),
    }
    mock_client.history, _ = make_history("stored-rows", stored, columns, 2)
    updates = []
    sub = data_api.subscribe(sources=list(columns), scan="stored-rows", callback=updates.append)
    assert sub.groups == {
        SCAN_GROUP: [("samx", "samx"), ("det", "wave")],
        CorrelationGroup.standalone("waveform", "data"): [("waveform", "data")],
    }
    assert wait_for(lambda: {u.metadata["group"] for u in updates} == set(sub.groups))
    by_group = {update.metadata["group"]: update for update in updates}
    scan = by_group[SCAN_GROUP]
    assert list(scan.aligned_ordinals) == [0, 1]
    assert list(scan.aligned()[("samx", "samx")]) == [1, 2]
    for key in (("samx", "samx"), ("det", "wave")):
        assert scan.get(*key).kind == "monitored"
    other = by_group[CorrelationGroup.standalone("waveform", "data")].get("waveform", "data")
    assert other.kind == "unindexed"
    assert len(other.values) == 5
    sub.close()


def test_stopped_scan_pairs_its_monitored_signals_by_row(data_api, mock_client):
    """An aborted scan has fewer rows than planned readouts; x and y still pair point by
    point, and a baseline signal with its single row stays out of the scan group."""
    stored = {
        "samx": {"samx": {"shape": (7,), "dtype": "float64"}},
        "bpm4i": {"bpm4i": {"shape": (7,), "dtype": "float64"}},
        "samy": {"samy": {"shape": (1,), "dtype": "float64"}},
    }
    columns = {
        ("samx", "samx"): (np.arange(7.0), np.arange(7.0) + 100),
        ("bpm4i", "bpm4i"): (np.arange(7.0) * 10, np.arange(7.0) + 100),
        ("samy", "samy"): ([3.0], [100.0]),
    }
    mock_client.history, _ = make_history("stopped", stored, columns, num_points=10)
    updates = []
    sub = data_api.subscribe(sources=list(columns), scan="stopped", callback=updates.append)
    assert sub.groups == {
        SCAN_GROUP: [("samx", "samx"), ("bpm4i", "bpm4i")],
        CorrelationGroup.standalone("samy", "samy"): [("samy", "samy")],
    }
    assert wait_for(lambda: any(u.metadata["group"] == SCAN_GROUP for u in updates))
    scan = next(u for u in reversed(updates) if u.metadata["group"] == SCAN_GROUP)
    assert list(scan.aligned_ordinals) == list(range(7))
    aligned = scan.aligned()
    np.testing.assert_array_equal(aligned[("bpm4i", "bpm4i")], 10 * aligned[("samx", "samx")])
    sub.close()


def test_stored_sub_signals_of_one_multi_signal_pair_by_row(data_api, mock_client):
    """The device manager declares the async signals; the sub-signals of one
    AsyncMultiSignal form one group in a stored scan as they do in a running one, and
    their stored rows pair one by one. A declared async signal stays async even when it
    has one row per monitored readout."""
    stored = {
        "samx": {"samx": {"shape": (2,), "dtype": "float64"}},
        "gauss": {
            "gauss_data_x": {"shape": (6,), "dtype": "float64"},
            "gauss_data_y": {"shape": (6,), "dtype": "float64"},
            "trace": {"shape": (2, 3), "dtype": "float64"},
        },
    }
    columns = {
        ("samx", "samx"): ([1.0, 2.0], [10.0, 20.0]),
        ("gauss", "gauss_data_x"): (np.arange(6.0), [10.0, 20.0]),
        ("gauss", "gauss_data_y"): (np.arange(6.0) * 2, [10.0, 20.0]),
        ("gauss", "trace"): (np.ones((2, 3)), [10.0, 20.0]),
    }
    mock_client.history, _ = make_history("stored-multi", stored, columns, 2)
    multi = {"signal_class": "AsyncMultiSignal", "storage_name": "gauss_data"}
    mock_client.device_manager.get_bec_signals.return_value = [
        ("gauss", None, {**multi, "obj_name": "gauss_data_x"}),
        ("gauss", None, {**multi, "obj_name": "gauss_data_y"}),
        (
            "gauss",
            None,
            {"signal_class": "AsyncSignal", "obj_name": "gauss_trace", "storage_name": "trace"},
        ),
    ]
    x, y, trace = ("gauss", "gauss_data_x"), ("gauss", "gauss_data_y"), ("gauss", "gauss_trace")
    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx"), x, y, trace], scan="stored-multi", callback=updates.append
    )
    assert sub.unbound_sources == []
    assert sub.groups == {
        SCAN_GROUP: [("samx", "samx")],
        CorrelationGroup.multi("gauss", "gauss_data"): [x, y],
        CorrelationGroup.standalone("gauss", "gauss_trace"): [trace],
    }
    assert wait_for(lambda: {u.metadata["group"] for u in updates} == set(sub.groups))
    by_group = {update.metadata["group"]: update for update in updates}
    assert by_group[SCAN_GROUP].get("samx", "samx").kind == "monitored"
    pair = by_group[CorrelationGroup.multi("gauss", "gauss_data")]
    assert list(pair.aligned_ordinals) == [0, 1, 2, 3, 4, 5]
    aligned = pair.aligned()
    np.testing.assert_array_equal(aligned[y], 2 * np.asarray(aligned[x]))
    assert pair.get(*x).kind == "async"
    # The stored column of time stamps is per update, not per row: it is not delivered.
    assert list(pair.get(*x).timestamps) == [None] * 6
    # One row per monitored readout, but declared async: not a scan-point signal.
    standalone = by_group[CorrelationGroup.standalone("gauss", "gauss_trace")].get(*trace)
    assert standalone.kind == "async" and standalone.as_array().shape == (2, 3)
    sub.close()


def test_corrupt_history_emits_error_instead_of_complete_empty_data(data_api, mock_client):
    stored = {"samx": {"samx": {"shape": (2,), "dtype": "float64"}}}
    columns = {("samx", "samx"): ([1, 2], [10, 20])}
    mock_client.history, _ = make_history("corrupt", stored, columns, 2)
    container = mock_client.history.get_by_scan_id.return_value
    del container.devices.get("samx")["samx"]._data["value"]
    updates = []
    sub = data_api.subscribe(sources=list(columns), scan="corrupt", callback=updates.append)
    assert wait_for(lambda: updates)
    assert not updates[-1].complete
    source = updates[-1].get("samx", "samx")
    assert not source.complete
    assert source.metadata["error"] == "Stored signal is missing."
    sub.close()


def test_file_reader_type_error_is_reported_without_retry(data_api, mock_client):
    stored = {"samx": {"samx": {"shape": (1,), "dtype": "float64"}}}
    mock_client.history, _ = make_history(
        "read-error", stored, {("samx", "samx"): ([1.0], [0.0])}, 1
    )
    reference = mock_client.history.get_by_scan_id.return_value.devices.get("samx")["samx"]
    with mock.patch.object(reference, "read", side_effect=TypeError("invalid dataset")) as read:
        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx")],
            scan="read-error",
            callback=updates.append,
            progress_callback=lambda fraction: None,
        )
        assert wait_for(lambda: updates)
        read.assert_called_once()
        assert updates[-1].get("samx", "samx").metadata["error"] == "invalid dataset"
        assert not updates[-1].complete
        sub.close()


def test_close_does_not_wait_for_a_blocking_history_read(data_api, mock_client):
    """A widget closing during a large read must not freeze: the worker is cancelled,
    publishes nothing, and is joined by the reaper instead of the caller."""
    import threading
    import time

    release = threading.Event()

    class SlowReference:
        def read(self):
            release.wait(5)
            return {"value": [1.0, 2.0], "timestamp": [1.0, 2.0]}

    columns = {("samx", "samx"): ([1.0, 2.0], [1.0, 2.0])}
    history, _ = make_history("hist", {"samx": {"samx": {"shape": (2,)}}}, columns, 2)
    history.get_by_scan_id.return_value = SimpleNamespace(
        devices=SimpleNamespace(get={"samx": {"samx": SlowReference()}}.get)
    )
    mock_client.history = history
    updates = []
    sub = data_api.subscribe(sources=[("samx", "samx")], scan="hist", callback=updates.append)
    (worker,) = sub._all_requests()[0].workers
    started = time.monotonic()
    sub.close()
    assert time.monotonic() - started < 1.0
    assert worker.is_alive()
    release.set()
    worker.join(2)
    assert not worker.is_alive() and updates == []


def _serve_slabs(mock_client, scan_id, references, rows):
    """Serve ``references`` ({(device, entry): reference}) as the stored data of one scan."""
    stored = {}
    for (device, entry), reference in references.items():
        stored.setdefault(device, {})[entry] = {
            "shape": reference.values.shape,
            "dtype": str(reference.values.dtype),
        }
    history, _ = make_history(scan_id, stored, {}, rows)
    devices = {}
    for (device, entry), reference in references.items():
        devices.setdefault(device, {})[entry] = reference
    history.get_by_scan_id.return_value = SimpleNamespace(devices=SimpleNamespace(get=devices.get))
    mock_client.history = history


def test_stored_scan_is_delivered_block_by_block(data_api, mock_client):
    """A source that is read in blocks grows update by update; the last update is complete."""
    import threading

    steps = [threading.Event() for _ in range(3)]
    values = np.arange(6, dtype=np.float64)
    samx = _SlabSignalRef(
        values, values + 100, rows=2, gate=lambda start: steps[start // 2].wait(5)
    )
    _serve_slabs(mock_client, "blocks", {("samx", "samx"): samx}, rows=6)

    updates, fractions = [], []
    sub = data_api.subscribe(
        sources=[("samx", "samx")],
        scan="blocks",
        callback=updates.append,
        progress_callback=fractions.append,
        min_emit_interval=0,
    )
    seen = []
    for step in steps:
        step.set()
        assert wait_for(lambda: len(updates) > len(seen))
        seen = list(updates)

    assert [list(update.get("samx", "samx").values) for update in updates] == [
        [0.0, 1.0],
        [0.0, 1.0, 2.0, 3.0],
        [0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
    ]
    assert [update.reason for update in updates] == ["history"] * 3
    assert [update.complete for update in updates] == [False, False, True]
    assert list(updates[-1].get("samx", "samx").timestamps) == list(values + 100)
    assert list(updates[-1].get("samx", "samx").ordinals) == [0, 1, 2, 3, 4, 5]
    # an update that was delivered does not change when the next block arrives
    assert list(updates[0].get("samx", "samx").values) == [0.0, 1.0]
    assert fractions == sorted(fractions) and fractions[-1] == 1.0 and len(fractions) == 3
    assert wait_for(lambda: samx.closed)
    sub.close()


def test_scan_file_is_loaded_through_the_slab_reader(data_api, mock_client, tmp_path, monkeypatch):
    """The real reference of a scan file hands out blocks, and the file path delivers them."""
    import h5py

    from bec_lib.scan_data_container import SignalDataReference

    file_path = tmp_path / "scan.h5"
    frames = np.arange(40, dtype=np.int32).reshape(10, 4)
    with h5py.File(file_path, "w") as f:
        group = f.create_group("entry/collection/devices/cam/cam_frames")
        group.create_dataset("value", data=frames, chunks=(2, 4), maxshape=(None, 4))
        group.create_dataset("timestamp", data=np.arange(10, dtype=float))
    monkeypatch.setattr(SignalDataReference, "_SLAB_ELEMENTS", 4 * 4)  # 4 rows per block
    reference = SignalDataReference(str(file_path), "entry/collection/devices/cam/cam_frames")
    stored = {"cam": {"cam_frames": {"shape": frames.shape, "dtype": "int32"}}}
    history, _ = make_history("file", stored, {}, 10)
    history.get_by_scan_id.return_value = SimpleNamespace(
        devices=SimpleNamespace(get={"cam": {"cam_frames": reference}}.get)
    )
    mock_client.history = history

    updates = []
    sub = data_api.subscribe(
        sources=[("cam", "cam_frames")], scan="file", callback=updates.append, min_emit_interval=0
    )
    assert wait_for(lambda: updates and updates[-1].complete)
    assert [len(update.get("cam", "cam_frames").values) for update in updates] == [4, 8, 10]
    last = updates[-1].get("cam", "cam_frames")
    np.testing.assert_array_equal(last.values, frames)
    assert last.values.dtype == np.int32
    assert list(last.timestamps) == list(np.arange(10, dtype=float))
    sub.close()


def test_stored_blocks_are_shown_without_copying_them_again(data_api, mock_client):
    """Every update is a view of the one column the load fills."""
    values = np.arange(6, dtype=np.float64)
    samx = _SlabSignalRef(values, values, rows=2)
    _serve_slabs(mock_client, "views", {("samx", "samx"): samx}, rows=6)

    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx")], scan="views", callback=updates.append, min_emit_interval=0
    )
    assert wait_for(lambda: updates and updates[-1].complete)
    columns = [update.get("samx", "samx").values for update in updates]
    assert all(np.shares_memory(columns[0], column) for column in columns)
    assert columns[-1].dtype == np.float64 and not columns[-1].flags.writeable
    sub.close()


def test_stored_sources_of_one_group_grow_together(data_api, mock_client):
    """One block of every source is read per round, so paired sources stay close."""
    import threading

    go = threading.Event()
    values = np.arange(4, dtype=np.float64)
    samx = _SlabSignalRef(values, values, rows=2)
    bpm = _SlabSignalRef(values * 10, values, rows=2, gate=lambda start: start == 0 or go.wait(5))
    _serve_slabs(mock_client, "pair", {("samx", "samx"): samx, ("bpm4i", "bpm4i"): bpm}, rows=4)

    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("bpm4i", "bpm4i")],
        scan="pair",
        callback=updates.append,
        min_emit_interval=0,
    )
    assert wait_for(lambda: updates)
    first = updates[0].aligned()
    assert list(first[("samx", "samx")]) == [0.0, 1.0]
    assert list(first[("bpm4i", "bpm4i")]) == [0.0, 10.0]
    assert not updates[0].complete
    go.set()
    assert wait_for(lambda: updates[-1].complete)
    assert list(updates[-1].aligned()[("bpm4i", "bpm4i")]) == [0.0, 10.0, 20.0, 30.0]
    sub.close()


def test_block_wise_read_stops_once_the_request_is_cancelled(data_api, mock_client):
    """Closing the subscription ends the read after the block in flight and lets go of the file."""
    import threading

    first_block_read, release = threading.Event(), threading.Event()

    def gate(start):
        if start > 0:
            first_block_read.set()
            release.wait(5)

    values = np.arange(100, dtype=np.float64)
    samx = _SlabSignalRef(values, values, rows=10, gate=gate)
    _serve_slabs(mock_client, "cancel", {("samx", "samx"): samx}, rows=100)

    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx")], scan="cancel", callback=updates.append, min_emit_interval=0
    )
    (worker,) = sub._all_requests()[0].workers
    assert first_block_read.wait(5)
    sub.close()
    release.set()
    worker.join(2)
    assert not worker.is_alive()
    assert samx.read_blocks <= 2 and samx.closed
    assert all(len(update.get("samx", "samx").values) <= 10 for update in updates)


def test_failing_block_reports_the_source_and_keeps_the_others(data_api, mock_client):
    values = np.arange(4, dtype=np.float64)

    def broken(start):
        if start > 0:
            raise OSError("file went away")

    samx = _SlabSignalRef(values, values, rows=2)
    bpm = _SlabSignalRef(values, values, rows=2, gate=broken)
    _serve_slabs(mock_client, "broken", {("samx", "samx"): samx, ("bpm4i", "bpm4i"): bpm}, rows=4)

    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx"), ("bpm4i", "bpm4i")],
        scan="broken",
        callback=updates.append,
        min_emit_interval=0,
    )
    assert wait_for(lambda: updates and len(updates[-1].get("samx", "samx").values) == 4)
    last = updates[-1]
    assert last.get("bpm4i", "bpm4i").metadata["error"] == "file went away"
    assert len(last.get("bpm4i", "bpm4i").values) == 0
    assert not last.complete
    assert wait_for(lambda: bpm.closed and samx.closed)
    sub.close()


def test_partial_history_updates_are_coalesced(data_api, mock_client):
    """With an emit interval the blocks in between are merged; the last state always arrives."""
    values = np.arange(200, dtype=np.float64)
    samx = _SlabSignalRef(values, values, rows=1)
    _serve_slabs(mock_client, "many", {("samx", "samx"): samx}, rows=200)

    updates = []
    sub = data_api.subscribe(
        sources=[("samx", "samx")], scan="many", callback=updates.append, min_emit_interval=30.0
    )
    assert wait_for(lambda: updates and updates[-1].complete)
    assert samx.read_blocks == 200
    assert len(updates) <= 3
    assert len(updates[-1].get("samx", "samx").values) == 200
    sub.close()


def test_force_source_must_serve_open_scans_to_follow_live(data_api):
    with pytest.raises(ValueError, match="cannot follow live scans"):
        data_api.subscribe(devices=["samx"], scan="live", force_source="file")
    with pytest.raises(ValueError, match=r"registered: \[.*'file', 'redis'.*\]"):
        data_api.subscribe(devices=["samx"], scan="live", force_source="rest")


def test_device_scope_routes_only_to_no_scan_paths(data_api, mock_client):
    """The classifier routes the device scope to NO_SCAN, and only the
    paths declaring NO_SCAN are in that routing table."""
    assert classify(mock_client, "", None, have_item=True) is Scenario.NO_SCAN
    no_scan = [type(t).__name__ for t in data_api.data_paths[Scenario.NO_SCAN]]
    assert "RedisDataPath" in no_scan and "FileDataPath" not in no_scan
    closed = [type(t).__name__ for t in data_api.data_paths[Scenario.CLOSED_SCAN]]
    assert closed.index("RedisDataPath") < closed.index("FileDataPath")
