"""Live followers while no scan runs: they show the most recent scan until the next one opens."""

from types import SimpleNamespace
from unittest import mock

from bec_lib import messages
from bec_lib.data_api import client_views
from bec_lib.data_api.testing import wait_for

from .test_data_api import (  # noqa: F401
    data_api,
    make_scan_item,
    mock_client,
    register_scan,
    seed_xy,
)

# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access

SOURCES = [("samx", "samx"), ("samy", "samy")]


def _finished_scan(mock_client, scan_id, number, points=3):
    item = make_scan_item(mock_client, scan_id, status="closed")
    item.scan_id, item.scan_number = scan_id, number
    seed_xy(item, scan_id, 0, points)
    return item


def _history_message(scan_id, number):
    return messages.ScanHistoryMessage(
        scan_id=scan_id,
        scan_number=number,
        dataset_number=1,
        exit_status="closed",
        file_path=f"/tmp/{scan_id}.h5",
        start_time=0.0,
        end_time=1.0,
        scan_name="line_scan",
        num_points=3,
    )


def _set_history(mock_client, message):
    mock_client.history = mock.MagicMock()
    mock_client.history.latest_message.return_value = message
    mock_client.history.get_message.side_effect = lambda sid: (
        message if message is not None and sid == message.scan_id else None
    )


def test_latest_scan_prefers_the_newer_of_scan_storage_and_history(mock_client):
    older, newer = _finished_scan(mock_client, "s6", 6), _finished_scan(mock_client, "s7", 7)
    mock_client.queue.scan_storage.storage = [newer, older]
    _set_history(mock_client, _history_message("h5", 5))
    assert client_views.latest_scan(mock_client) == ("s7", newer)

    _set_history(mock_client, _history_message("h9", 9))
    assert client_views.latest_scan(mock_client) == ("h9", None)

    mock_client.queue.scan_storage.storage = []
    _set_history(mock_client, None)
    assert client_views.latest_scan(mock_client) is None


def test_a_client_that_has_not_started_knows_no_scan_and_no_device():
    """Before ``start()`` the client's queue, history and device manager are ``None``."""
    client = SimpleNamespace(queue=None, history=None, device_manager=None)
    assert client_views.scan_storage(client) is None
    assert client_views.scan_item(client, "scan_1") is None
    assert client_views.history_message(client, "scan_1") is None
    assert client_views.current_live_scan(client) is None
    assert client_views.latest_scan(client) is None
    assert client_views.device(client, "samx") is None
    assert client_views.device_info(client, "samx") is None


def test_idle_live_follower_shows_the_most_recent_scan(data_api, mock_client):
    item = _finished_scan(mock_client, "s7", 7)
    mock_client.queue.scan_storage.storage = [item]
    register_scan(mock_client, {"s7": item})
    _set_history(mock_client, None)
    updates = []

    sub = data_api.subscribe(sources=SOURCES, scan="live", callback=updates.append)

    assert sub.scan_id == "s7"
    wait_for(lambda: updates and len(updates[-1].sources[("samx", "samx")].values) == 3)
    assert list(updates[-1].sources[("samx", "samx")].values) == [0.0, 1.0, 2.0]
    sub.close()


def test_idle_live_follower_moves_to_the_next_scan_when_it_opens(data_api, mock_client):
    finished = _finished_scan(mock_client, "s7", 7)
    running = make_scan_item(mock_client, "s8", status="open")
    running.scan_id, running.scan_number = "s8", 8
    seed_xy(running, "s8", 0, 1)
    mock_client.queue.scan_storage.storage = [finished]
    register_scan(mock_client, {"s7": finished, "s8": running})
    _set_history(mock_client, None)
    updates = []
    sub = data_api.subscribe(sources=SOURCES, scan="live", callback=updates.append)
    assert sub.scan_id == "s7"

    sub._on_scan_status({"scan_id": "s8", "status": "open"}, {})

    assert sub.scan_id == "s8"
    wait_for(lambda: updates and updates[-1].scan_id == "s8")
    sub.close()


def test_live_follower_without_any_scan_stays_unbound(data_api, mock_client):
    mock_client.queue.scan_storage.storage = []
    _set_history(mock_client, None)
    sub = data_api.subscribe(sources=SOURCES, scan="live", callback=lambda _: None)
    assert sub.scan_id is None
    sub.close()
