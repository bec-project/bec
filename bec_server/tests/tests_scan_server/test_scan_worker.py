# pylint: skip-file
from types import SimpleNamespace
from unittest import mock

import pytest

from bec_lib.tests.fixtures import dm_with_devices
from bec_server.scan_server.errors import ScanAbortion
from bec_server.scan_server.scan_worker import ScanWorker


@pytest.fixture
def scan_worker_mock(dm_with_devices) -> ScanWorker:
    parent = SimpleNamespace(
        device_manager=dm_with_devices,
        connector=mock.MagicMock(),
        queue_manager=SimpleNamespace(queues={}),
    )
    scan_worker = ScanWorker(parent=parent)
    yield scan_worker


def test_run_executes_direct_scan(scan_worker_mock):
    queue = mock.MagicMock()
    queue.stopped = False

    delegated_worker = mock.MagicMock()

    def _process(_queue):
        scan_worker_mock.signal_event.set()

    delegated_worker.process_instructions.side_effect = _process

    with mock.patch(
        "bec_server.scan_server.scan_worker.DirectScanWorker", return_value=delegated_worker
    ) as worker_cls:
        scan_worker_mock.parent.queue_manager.queues[scan_worker_mock.queue_name] = [queue]

        scan_worker_mock.run()

    worker_cls.assert_called_once_with(worker=scan_worker_mock)
    delegated_worker.process_instructions.assert_called_once_with(queue)
    queue.append_to_queue_history.assert_called_once()


def test_run_handles_direct_scan_abortion(scan_worker_mock):
    queue = mock.MagicMock()
    delegated_worker = mock.MagicMock()
    delegated_worker.process_instructions.side_effect = ScanAbortion()

    def _handle(_queue, _exc):
        scan_worker_mock.signal_event.set()

    delegated_worker._handle_scan_abortion.side_effect = _handle

    with mock.patch(
        "bec_server.scan_server.scan_worker.DirectScanWorker", return_value=delegated_worker
    ):
        scan_worker_mock.parent.queue_manager.queues[scan_worker_mock.queue_name] = [queue]

        scan_worker_mock.run()

    delegated_worker._handle_scan_abortion.assert_called_once()


def test_run_reports_queue_iteration_error_before_first_scan(scan_worker_mock):
    queue = mock.MagicMock()
    queue.__iter__.side_effect = RuntimeError("Queue unavailable")
    scan_worker_mock.parent.queue_manager.queues[scan_worker_mock.queue_name] = queue

    scan_worker_mock.run()

    scan_worker_mock.connector.raise_alarm.assert_called_once()
    error = scan_worker_mock.connector.raise_alarm.call_args.kwargs["info"]
    assert error.exception_type == "RuntimeError"
    assert "Queue unavailable" in error.error_message
    assert scan_worker_mock.connector.raise_alarm.call_args.kwargs["metadata"] == {}
    queue.abort.assert_called_once_with()


def test_shutdown(scan_worker_mock):
    with mock.patch.object(scan_worker_mock.signal_event, "set") as set_mock:
        scan_worker_mock._started = mock.MagicMock()
        scan_worker_mock._started.is_set.return_value = True
        with mock.patch.object(scan_worker_mock, "join") as join_mock:
            scan_worker_mock.shutdown()
            set_mock.assert_called_once()
            join_mock.assert_called_once()
