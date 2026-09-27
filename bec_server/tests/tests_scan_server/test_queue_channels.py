"""Close/drain semantics and the narrow execution cancellation capability."""

import threading
from queue import Empty

import pytest

from bec_server.scan_server.errors import UserScanInterruption
from bec_server.scan_server.queue_channels import (
    Channel,
    ChannelClosed,
    ExecutionControl,
    LaneJob,
    SerialLane,
)


def test_channel_close_drains_accepted_values_and_rejects_send():
    channel = Channel()
    channel.send(1)
    channel.send(2)
    channel.close()
    assert channel.receive() == 1
    assert channel.receive() == 2
    with pytest.raises(ChannelClosed):
        channel.receive()
    with pytest.raises(ChannelClosed):
        channel.send(3)
    channel.close()


def test_channel_close_wakes_receiver():
    channel = Channel()
    closed = threading.Event()

    def receive():
        with pytest.raises(ChannelClosed):
            channel.receive()
        closed.set()

    receiver = threading.Thread(target=receive)
    receiver.start()
    channel.close()
    receiver.join(1)
    assert closed.is_set()


def test_channel_timeout_does_not_close():
    channel = Channel()
    with pytest.raises(Empty):
        channel.receive(timeout=0.01)
    channel.send(1)
    assert channel.receive() == 1


def test_finish_waits_for_stop_receipt_and_rejects_late_stop():
    control = ExecutionControl()
    receipt = control.stop(("aborted", "user"))
    finished = threading.Event()
    thread = threading.Thread(target=lambda: (control.finish(), finished.set()))
    thread.start()
    try:
        assert not finished.wait(0.02)
        assert control.stop(("halted", "user")) is None
    finally:
        receipt.set()
        thread.join(1)
    assert finished.is_set()


def test_repeated_stop_before_cleanup_prevents_it():
    control = ExecutionControl()
    control.stop(("aborted", "user")).set()
    control.stop(("halted", "user"), cleanup=False).set()
    assert not control.begin_cleanup()
    assert control.exit_info == ("aborted", "user")


def test_shutdown_during_cleanup_cannot_be_cleared():
    control = ExecutionControl()
    control.stop(("aborted", "user")).set()
    assert control.begin_cleanup()
    assert control.execution_event.is_set() and not control.cleanup_event.is_set()
    control.shutdown()
    with pytest.raises(UserScanInterruption):
        control.checkpoint()
    assert not control.begin_cleanup()


def test_lane_does_not_retain_transferred_payload_while_idle():
    released = threading.Event()

    class Payload:
        def __del__(self):
            released.set()

    payload = Payload()
    lane = SerialLane("test-preparation")
    lane.start()
    try:
        lane.jobs.send(LaneJob(lambda value=payload: value, lambda result: None))
        del payload
        assert released.wait(1)
    finally:
        lane.jobs.close()
        lane.join(1)
