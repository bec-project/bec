"""Live followers while no scan runs: they show the most recent scan until the next one opens."""

import threading
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


def _history_message(scan_id, number, start_time=0.0):
    return messages.ScanHistoryMessage(
        scan_id=scan_id,
        scan_number=number,
        dataset_number=1,
        exit_status="closed",
        file_path=f"/tmp/{scan_id}.h5",
        start_time=start_time,
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


def _started(item, when):
    item.start_time = when
    return item


def test_latest_scan_is_the_one_that_started_last(mock_client):
    """
    Neither the scan number nor the place in the storage says which scan ran last: a new
    experiment sets the number back, and the storage is in the order the scans were queued.
    """
    first = _started(_finished_scan(mock_client, "first", 500), 100.0)
    second = _started(_finished_scan(mock_client, "second", 2), 300.0)
    third = _started(_finished_scan(mock_client, "third", 1), 200.0)
    mock_client.queue.scan_storage.storage = [first, second, third]  # queued second, ran third
    _set_history(mock_client, _history_message("first", 500, start_time=100.0))
    assert client_views.latest_scan(mock_client) == ("second", second)

    queued = make_scan_item(mock_client, "queued")  # no status yet: it has not started
    queued.status_message = None
    mock_client.queue.scan_storage.storage = [first, second, third, queued]
    assert client_views.latest_scan(mock_client) == ("second", second)

    # A scan the client missed is only in the history: it still counts when it ran later.
    _set_history(mock_client, _history_message("missed", 3, start_time=400.0))
    assert client_views.latest_scan(mock_client) == ("missed", None)

    mock_client.queue.scan_storage.storage = []  # a client that started after the scans
    assert client_views.latest_scan(mock_client) == ("missed", None)

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


def test_idle_live_follower_finds_the_last_scan_when_the_history_has_loaded(data_api, mock_client):
    """
    The scan history loads in the background. A follower that was created before it was
    there is unbound; the event that ends the load binds it to the most recent scan.
    """
    mock_client.queue.scan_storage.storage = []
    _set_history(mock_client, None)
    updates = []
    sub = data_api.subscribe(sources=SOURCES, scan="live", callback=updates.append)
    assert sub.scan_id is None

    item = _finished_scan(mock_client, "s7", 7)
    mock_client.queue.scan_storage.storage = [item]
    register_scan(mock_client, {"s7": item})
    sub._on_scan_history_loaded(history_msgs=[])

    assert wait_for(lambda: sub.scan_id == "s7")
    wait_for(lambda: updates and len(updates[-1].sources[("samx", "samx")].values) == 3)
    sub.close()


def test_history_loaded_event_does_not_wait_for_the_scan_storage(
    data_api, mock_client, monkeypatch
):
    """
    The event comes on the thread of the scan history, with the lock of the callback
    handler held. The dispatcher holds the lock of the scan storage when it takes that
    one, so the follower must not read the scan storage on the history thread.
    """
    mock_client.queue.scan_storage.storage = []
    _set_history(mock_client, None)
    sub = data_api.subscribe(sources=SOURCES, scan="live", callback=lambda _: None)
    assert sub.scan_id is None
    item = _finished_scan(mock_client, "s7", 7)
    mock_client.queue.scan_storage.storage = [item]
    register_scan(mock_client, {"s7": item})

    storage_free = threading.Event()  # the dispatcher holds the scan storage until then
    current_live_scan = client_views.current_live_scan

    def waits_for_the_storage(client):
        storage_free.wait(5)
        return current_live_scan(client)

    monkeypatch.setattr(client_views, "current_live_scan", waits_for_the_storage)
    history_thread = threading.Thread(target=sub._on_scan_history_loaded)
    history_thread.start()
    history_thread.join(1)
    returned = not history_thread.is_alive()
    storage_free.set()
    history_thread.join(5)
    assert returned
    assert wait_for(lambda: sub.scan_id == "s7")
    sub.close()


def test_live_follower_without_any_scan_stays_unbound(data_api, mock_client):
    mock_client.queue.scan_storage.storage = []
    _set_history(mock_client, None)
    sub = data_api.subscribe(sources=SOURCES, scan="live", callback=lambda _: None)
    assert sub.scan_id is None
    sub.close()
