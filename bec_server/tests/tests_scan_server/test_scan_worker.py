"""Execution-channel tests independent of Redis and queue internals."""

from types import SimpleNamespace
from unittest import mock

import pytest

from bec_server.scan_server.queue_channels import Channel, ChannelClosed, ExecutionControl
from bec_server.scan_server.scan_worker import ScanWorker


@pytest.fixture
def worker():
    parent = SimpleNamespace(device_manager=mock.Mock(), connector=mock.Mock())
    parent.connector.raise_alarm.side_effect = RuntimeError("Redis is down")
    worker = ScanWorker(parent=parent, queue_name="primary", work=Channel(), manager=mock.Mock())
    yield worker
    worker.shutdown(timeout=2)


def test_receive_blocks_until_assignment_then_reports_exact_token(worker):
    assignment = SimpleNamespace(control=ExecutionControl(), token="token")
    report = mock.Mock()
    with mock.patch("bec_server.scan_server.scan_worker.DirectScanWorker") as executor:
        executor.return_value.run.return_value = report
        worker._work.send(assignment)
        worker._work.close()
        worker.start()
        worker.join(2)
    executor.return_value.run.assert_called_once_with(assignment)
    worker._manager.worker_report.assert_called_once_with(report)
    assert not worker.is_alive()


def test_failure_acknowledgement_does_not_depend_on_alarm_delivery(worker):
    assignment = SimpleNamespace(control=ExecutionControl(), token="token")
    with mock.patch("bec_server.scan_server.scan_worker.DirectScanWorker") as executor:
        executor.return_value.run.side_effect = RuntimeError("cannot describe scan")
        worker._work.send(assignment)
        worker._work.close()
        worker.start()
        worker.join(2)
    worker._manager.worker_failed.assert_called_once_with("token", "cannot describe scan")
    assert not worker.is_alive()


def test_shutdown_wakes_empty_receiver(worker):
    worker.start()
    worker.shutdown(timeout=1)
    assert not worker.is_alive()
    with pytest.raises(ChannelClosed):
        worker._work.send(mock.Mock())


def test_shutdown_signals_assignment_already_accepted(worker):
    assignment = SimpleNamespace(control=ExecutionControl(), token="token")
    worker._work.send(assignment)
    worker.request_shutdown()
    with mock.patch("bec_server.scan_server.scan_worker.DirectScanWorker") as executor:
        worker.start()
        worker.join(2)
    assert assignment.control.shutdown_event.is_set()
    executor.return_value.run.assert_called_once_with(assignment)
