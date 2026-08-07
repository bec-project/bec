"""Tests for the DataAPI history plugin and live→history routing."""

import time
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.data_api import DataAPI
from bec_lib.live_scan_data import LiveScanData
from bec_lib.scan_items import ScanItem

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name


class _FakeSignalRef:
    def __init__(self, values, timestamps):
        self._data = {"value": values, "timestamp": timestamps}

    def read(self):
        return dict(self._data)


class _FakeDeviceRef(dict):
    def get(self, key, default=None):
        return super().get(key, default)


def make_history(scan_id, stored, columns, num_points):
    """Build a fake client.history serving one scan."""
    msg = SimpleNamespace(
        scan_id=scan_id,
        file_path=f"/data/{scan_id}_master.h5",
        stored_data_info=stored,
        num_points=num_points,
        num_monitored_readouts=num_points,
    )
    devices = {}
    for (device, entry), (values, timestamps) in columns.items():
        devices.setdefault(device, _FakeDeviceRef())[entry] = _FakeSignalRef(values, timestamps)
    container = SimpleNamespace(devices=SimpleNamespace(get=devices.get))
    history = mock.MagicMock()
    history._scan_data = {scan_id: msg}
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
            (
                "det",
                None,
                {
                    "obj_name": "wave",
                    "storage_name": "wave",
                    "describe": {"signal_info": {"acquisition_group": "monitored"}},
                },
            )
        ]

        updates = []
        sub = data_api.subscribe(
            sources=[("samx", "samx"), ("det", "wave")], scan="scan_h", callback=updates.append
        )
        assert wait_for(lambda: updates)
        update = updates[-1]
        assert update.reason == "history"
        assert update.scan_id == "scan_h"
        assert update.aligned_ordinals == (0, 1, 2)
        assert update.complete
        cols = update.aligned()
        assert cols[("samx", "samx")] == (0.0, 1.0, 2.0)
        assert cols[("det", "wave")] == ([1] * 3, [2] * 3, [3] * 3)
        assert update.get("samx", "samx").kind == "monitored"
        assert update.get("det", "wave").kind == "async"
        assert update.get("det", "wave").metadata["file_path"] == "/data/scan_h_master.h5"
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
        assert updates[-1].aligned()[("samx", "samx")] == (0.0, 1.0)
        sub.close()

    def test_writer_latency_window_uses_live_data(self, data_api, mock_client):
        mock_client.history = mock.MagicMock()
        mock_client.history._scan_data = {}
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
        assert updates[-1].aligned()[("samx", "samx")] == (0.0, 1.0)
        sub.close()

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
        mock_client.history._scan_data = {}

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
        assert updates[-1].aligned()[("samx", "samx")] == (5.0, 6.0)
        sub.close()

    def test_unknown_scan_still_raises(self, data_api, mock_client):
        mock_client.history = mock.MagicMock()
        mock_client.history._scan_data = {}
        with pytest.raises(ValueError):
            data_api.subscribe(sources=[("samx", "samx")], scan="nope", callback=lambda u: None)
