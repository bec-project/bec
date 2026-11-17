import os
import threading
import time
from threading import Event
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.callback_handler import CallbackHandler, EventType
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_lib.scan_history import ScanHistory

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring


@pytest.fixture
def scan_history_without_thread(connected_connector, file_history_messages):
    with mock.patch("bec_lib.scan_history.os.access") as access:
        from bec_lib.scan_history import ScanHistory

        for msg in file_history_messages:
            connected_connector.xadd(MessageEndpoints.scan_history(), {"data": msg})
        client = BECClient(connector_cls=RedisConnector)
        client.connector = connected_connector
        yield ScanHistory(client=client, load_threaded=False), access


def test_scan_history_loads_messages(scan_history_without_thread, file_history_messages):
    scan_history_without_thread, _ = scan_history_without_thread
    container = scan_history_without_thread.get_by_scan_number(1)
    assert container._msg == file_history_messages[0]

    container = scan_history_without_thread.get_by_scan_number(2)
    assert container._msg == file_history_messages[1]

    container = scan_history_without_thread.get_by_scan_number(3)
    assert container._msg == file_history_messages[2]

    container = scan_history_without_thread.get_by_scan_number(4)
    assert container is None

    container = scan_history_without_thread.get_by_scan_id("scan_id_1")
    assert container._msg == file_history_messages[0]

    container = scan_history_without_thread.get_by_scan_id("scan_id_2")
    assert container._msg == file_history_messages[1]

    container = scan_history_without_thread.get_by_dataset_number(2)
    assert container[0]._msg == file_history_messages[1]
    assert container[1]._msg == file_history_messages[2]

    assert scan_history_without_thread._scan_numbers == [1, 2, 3]
    assert [
        scan_history_without_thread._scan_data[sid].scan_number
        for sid in scan_history_without_thread._scan_ids
    ] == scan_history_without_thread._scan_numbers


class _ScanHistoryUpdateRecorder:
    def __init__(self):
        self.cbs_run = 0
        self.event = Event()

    def on_update(self, history_msg):
        self.cbs_run += 1
        if self.cbs_run >= 2:
            self.event.set()


# @pytest.mark.timeout(20)
def test_scan_history_get_message_reads_the_registry_without_the_lock(
    scan_history_without_thread, file_history_messages
):
    history, _ = scan_history_without_thread
    assert history.get_message("scan_id_2") == file_history_messages[1]
    assert history.get_message("unknown") is None
    # Must not wait for the registry lock: the update callback holds it while dispatching.
    with history._scan_data_lock:
        result = []
        reader = threading.Thread(target=lambda: result.append(history.get_message("scan_id_1")))
        reader.start()
        reader.join(timeout=1)
        assert result == [file_history_messages[0]]


def test_scan_history_latest_message(scan_history_without_thread, file_history_messages):
    history, _ = scan_history_without_thread
    assert history.latest_message() == file_history_messages[-1]
    history._scan_ids.clear()
    assert history.latest_message() is None


def test_scan_history_removes_oldest_scan(scan_history_without_thread, file_history_messages):
    scan_history, _ = scan_history_without_thread
    recorder = _ScanHistoryUpdateRecorder()

    scan_history._client.callbacks.register(EventType.SCAN_HISTORY_UPDATE, recorder.on_update)
    msg = [
        messages.ScanHistoryMessage(
            scan_id="scan_id_4",
            scan_number=4,
            dataset_number=4,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
        messages.ScanHistoryMessage(
            scan_id="scan_id_5",
            scan_number=5,
            dataset_number=5,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
    ]
    with mock.patch("bec_lib.scan_history.os.access", return_value=True):
        scan_history._max_scans = 2
        for m in msg:
            scan_history._connector.xadd(MessageEndpoints.scan_history(), {"data": m})

        while len(scan_history._scan_ids) > 2:
            time.sleep(0.1)

    if not recorder.event.wait(timeout=1):
        raise TimeoutError()

    with scan_history._scan_data_lock:

        assert scan_history.get_by_scan_number(1) is None
        assert scan_history.get_by_scan_number(4)._msg == msg[0]

        assert len(scan_history._scan_numbers) == len(scan_history._scan_ids) == 2
        assert scan_history._scan_numbers == [4, 5]
        assert [
            scan_history._scan_data[sid].scan_number for sid in scan_history._scan_ids
        ] == scan_history._scan_numbers


def test_scan_history_slices(scan_history_without_thread, file_history_messages):
    scan_history_without_thread, _ = scan_history_without_thread
    assert [scan._msg for scan in scan_history_without_thread[0:2]] == file_history_messages[:2]
    assert [scan._msg for scan in scan_history_without_thread[1:]] == file_history_messages[1:]
    assert [scan._msg for scan in scan_history_without_thread[-2:]] == file_history_messages[-2:]
    assert scan_history_without_thread[-1]._msg == file_history_messages[-1]


@pytest.mark.timeout(10)
def test_scan_history_filters_readable_files(
    scan_history_without_thread, file_history_messages, tmp_path
):
    scan_history_without_thread, access = scan_history_without_thread
    access.side_effect = lambda arg, *_: "unreadable" not in arg
    # Create a temporary file that is not readable
    unreadable_file = tmp_path / "unreadable_file.txt"
    unreadable_file.write_text("This file is not readable")
    unreadable_file.chmod(0o000)  # Make it unreadable

    readable_file = tmp_path / "readable_file.txt"
    readable_file.write_text("This file is readable")
    readable_file.chmod(0o644)  # Make it readable

    # Add a message with the unreadable file path
    unreadable_msg = messages.ScanHistoryMessage(
        scan_id="scan_id_unreadable",
        scan_number=5,
        dataset_number=5,
        file_path=str(unreadable_file),
        exit_status="closed",
        start_time=time.time(),
        end_time=time.time(),
        scan_name="line_scan",
        num_points=10,
    )
    readable_msg = messages.ScanHistoryMessage(
        scan_id="scan_id_readable",
        scan_number=6,
        dataset_number=6,
        file_path=str(tmp_path / "readable_file.txt"),
        exit_status="closed",
        start_time=time.time(),
        end_time=time.time(),
        scan_name="line_scan",
        num_points=10,
    )
    scan_history_without_thread._connector.xadd(
        MessageEndpoints.scan_history(), {"data": unreadable_msg}
    )
    scan_history_without_thread._connector.xadd(
        MessageEndpoints.scan_history(), {"data": readable_msg}
    )

    while scan_history_without_thread.get_by_scan_id("scan_id_readable") is None:
        time.sleep(0.1)
    # Verify that the unreadable file is not included in the history
    other = scan_history_without_thread.get_by_scan_id("scan_id_unreadable")
    assert other is None
    assert scan_history_without_thread.get_by_scan_id("scan_id_readable")._msg == readable_msg

    # New: ensure unreadable scan_number wasn't appended, readable was
    nums = scan_history_without_thread._scan_numbers
    assert 5 not in nums
    assert nums[-1] == 6


@pytest.mark.timeout(10)
def test_scan_history_update_callback(scan_history_without_thread, file_history_messages):
    """Test the scan history update callbacks."""
    scan_history_without_thread, _ = scan_history_without_thread

    with mock.patch.object(
        scan_history_without_thread._client.callbacks, "run"
    ) as mock_callback_run:
        for ii, msg in enumerate(file_history_messages):
            scan_history_without_thread._connector.xadd(
                MessageEndpoints.scan_history(), {"data": msg}
            )
            # Sleep is needed to ensure the message is processed
            time.sleep(0.1)
            while (
                scan_history_without_thread.get_by_scan_id(msg.scan_id) is None
            ):  # Wait for the message to be added
                time.sleep(0.1)
            mock_callback_run.assert_called_with(
                event_type=EventType.SCAN_HISTORY_UPDATE, history_msg=msg
            )


@pytest.mark.timeout(20)
def test_scan_history_multiple_scan_numbers(scan_history_without_thread, file_history_messages):
    """Test the scan history update callbacks."""
    scan_history_without_thread, _ = scan_history_without_thread

    msgs = [
        messages.ScanHistoryMessage(
            scan_id="scan_id_1_a",
            scan_number=1,
            dataset_number=4,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
        messages.ScanHistoryMessage(
            scan_id="scan_id_1_b",
            scan_number=1,
            dataset_number=5,
            file_path="file_path",
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
    ]
    with mock.patch("bec_lib.scan_history.os.access", return_value=True):
        for m in msgs:
            scan_history_without_thread._connector.xadd(
                MessageEndpoints.scan_history(), {"data": m}
            )

        while len(scan_history_without_thread._scan_ids) < len(file_history_messages) + len(msgs):
            time.sleep(0.1)

    containers = scan_history_without_thread.get_by_scan_number(1)
    assert isinstance(containers, list)
    assert len(containers) == 3  # 1 from fixture + 2 new
    assert containers[0]._msg == file_history_messages[0]
    assert containers[1]._msg == msgs[0]
    assert containers[2]._msg == msgs[1]


def test_history_update_event_fires_after_registry_store(connected_connector):
    """Consumers of SCAN_HISTORY_UPDATE may read the registry from the
    callback; the message must be stored before the event fires."""
    client = mock.MagicMock()
    client.connector = connected_connector
    client.callbacks = CallbackHandler()
    history = ScanHistory(client=client, load_threaded=False)

    class _Recorder:
        # The callback handler accepts bound methods only (weakly referenced).
        def __init__(self):
            self.seen_in_registry = []

        def on_update(self, history_msg=None, **_kwargs):
            self.seen_in_registry.append(history.get_by_scan_id(history_msg.scan_id) is not None)

    recorder = _Recorder()
    seen_in_registry = recorder.seen_in_registry
    client.callbacks.register(EventType.SCAN_HISTORY_UPDATE, recorder.on_update)
    msg = messages.ScanHistoryMessage(
        scan_id="hist-1",
        scan_number=1,
        dataset_number=1,
        file_path=os.devnull,
        exit_status="closed",
        start_time=1.0,
        end_time=2.0,
        scan_name="line_scan",
        num_points=1,
    )
    history._on_scan_history_update({"data": msg})
    client.callbacks.poll()
    assert seen_in_registry == [True]


@pytest.mark.parametrize("unreadable", [0, 65])
def test_initial_history_reads_bounded_reverse_batches(
    connected_connector, file_history_messages, unreadable
):
    """Only decode enough bounded batches to find the latest 50 readable scans."""
    endpoint = MessageEndpoints.scan_history()
    for index in range(150):
        msg = file_history_messages[0].model_copy(
            update={
                "scan_id": f"scan-{index}",
                "scan_number": index,
                "file_path": "missing" if index >= 150 - unreadable else os.devnull,
            }
        )
        connected_connector.xadd(endpoint, {"data": msg})
    client = mock.MagicMock(connector=connected_connector)
    with (
        mock.patch("bec_lib.scan_history.os.access", side_effect=lambda path, _: path != "missing"),
        mock.patch.object(
            connected_connector, "xrevrange", wraps=connected_connector.xrevrange
        ) as read,
        mock.patch.object(connected_connector, "xread") as unbounded_read,
    ):
        history = ScanHistory(client, load_threaded=False)
    assert history._scan_numbers == list(range(100 - unreadable, 150 - unreadable))
    assert history.latest_message().scan_number == 149 - unreadable
    assert history._scan_history_loaded_event.is_set()
    assert read.call_count == (1 if unreadable == 0 else 3)
    assert all(call.kwargs["count"] == 50 for call in read.call_args_list)
    unbounded_read.assert_not_called()
    client.callbacks.run.assert_called_once_with(
        event_type=EventType.SCAN_HISTORY_LOADED,
        history_msgs=[history.get_message(scan_id) for scan_id in history._scan_ids],
    )
    history._connector.unregister(endpoint, cb=history._on_scan_history_update)


def test_initial_history_empty_stream(connected_connector):
    history = ScanHistory(mock.MagicMock(connector=connected_connector), load_threaded=False)
    assert len(history) == 0
    assert history._scan_history_loaded_event.is_set()
    history._connector.unregister(
        MessageEndpoints.scan_history(), cb=history._on_scan_history_update
    )


@pytest.mark.parametrize("overlap", [False, True])
def test_initial_history_preserves_updates_during_loading(file_history_messages, overlap):
    """A concurrent update survives merging and overlapping entries appear only once."""
    snapshot_ready = Event()
    resume_loading = Event()
    client = mock.MagicMock()
    snapshot = [
        file_history_messages[0].model_copy(
            update={"scan_id": f"scan-{index}", "scan_number": index, "file_path": os.devnull}
        )
        for index in range(50)
    ]

    def read_batch(*_args, **_kwargs):
        snapshot_ready.set()
        assert resume_loading.wait(timeout=5)
        return [(f"{index}-0", {"data": snapshot[index]}) for index in reversed(range(50))]

    client.connector.xrevrange.side_effect = read_batch
    with mock.patch.object(ScanHistory, "_start_retrieval"):
        history = ScanHistory(client, load_threaded=True)
    loader = threading.Thread(target=history._load_data)
    loader.start()
    try:
        assert snapshot_ready.wait(timeout=5)
        if overlap:
            history._on_scan_history_update({"data": snapshot[-1]})
        newest = snapshot[-1].model_copy(update={"scan_id": "scan-50", "scan_number": 50})
        history._on_scan_history_update({"data": newest})
    finally:
        resume_loading.set()
        loader.join(timeout=5)
    assert not loader.is_alive()
    assert history._scan_history_loaded_event.is_set()
    assert history._scan_numbers == list(range(1, 51))
    assert len(history._scan_ids) == len(history._scan_data) == 50
    assert history.latest_message() == newest
    assert history.get_message("scan-50") == newest
    assert history.get_message("scan-0") is None
    client.callbacks.run.assert_called_with(
        event_type=EventType.SCAN_HISTORY_LOADED,
        history_msgs=[history.get_message(scan_id) for scan_id in history._scan_ids],
    )


def test_initial_history_keeps_older_overlap_in_snapshot_order(file_history_messages):
    """A delayed callback must not move an older snapshot entry to the end."""
    client = mock.MagicMock()
    snapshot = [
        file_history_messages[0].model_copy(
            update={"scan_id": f"scan-{index}", "scan_number": index, "file_path": os.devnull}
        )
        for index in range(50)
    ]
    updated = snapshot[48].model_copy(update={"num_points": 123})
    with mock.patch.object(ScanHistory, "_start_retrieval"):
        history = ScanHistory(client, load_threaded=False)

    def read_batch(*_args, **_kwargs):
        history._on_scan_history_update({"data": updated})
        return [(f"{index}-0", {"data": snapshot[index]}) for index in reversed(range(50))]

    client.connector.xrevrange.side_effect = read_batch
    history._load_data()
    assert history._scan_numbers == list(range(50))
    assert history.latest_message() == snapshot[49]
    assert history.get_message("scan-48") == updated
    assert len(history._scan_ids) == len(history._scan_data) == 50
    client.callbacks.run.assert_called_with(
        event_type=EventType.SCAN_HISTORY_LOADED,
        history_msgs=[*snapshot[:48], updated, snapshot[49]],
    )


def test_initial_history_empty_mock_stream_stops():
    """Unconfigured connector mocks must not spin forever during client startup."""
    client = mock.MagicMock()
    history = ScanHistory(client, load_threaded=False)
    assert len(history) == 0
    assert history._scan_history_loaded_event.is_set()
    client.connector.xrevrange.assert_called_once()
