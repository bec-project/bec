"""Regression tests for broadcasting the latest scan hook or instruction update."""

from __future__ import annotations

import threading
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.scan_server.scan_hook_broadcaster import ScanHookBroadcaster


def _event(hook: str = "stage", *, device: str | None = None) -> messages.ScanHookMessage:
    instructions = (
        []
        if device is None
        else [messages.DeviceInstructionMessage(device=device, action="stage", parameter={})]
    )
    return messages.ScanHookMessage(
        scan_id="scan-id",
        hook=hook,
        instructions=instructions,
        metadata={"scan_id": "scan-id", "RID": "rid"},
    )


def test_only_latest_update_in_window_is_sent():
    connector = mock.Mock()
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.start()
    with mock.patch("bec_server.scan_server.scan_hook_broadcaster.threading.Timer") as timer:
        events = [_event(), _event(device="samx"), _event(device="samy"), _event("pre_scan")]
        for event in events:
            broadcaster.add(event)
        timer.assert_called_once_with(0.1, broadcaster._flush_scheduled)
        timer.return_value.start.assert_called_once()
        connector.send.assert_not_called()
        timer.call_args.args[1]()
        connector.send.assert_called_once_with(
            MessageEndpoints.scan_hook(), events[-1], timeout_s=broadcaster.PUBLISH_TIMEOUT_S
        )
        broadcaster.flush()
        assert connector.send.call_count == 1
        broadcaster.add(_event("close_scan"))
        assert timer.call_count == 2
        broadcaster.stop()
    assert connector.send.call_count == 2


def test_stop_flushes_latest_update_and_joins_timer():
    connector = mock.Mock()
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.start()
    broadcaster.add(_event())
    timer = broadcaster._timer
    broadcaster.stop()
    assert not timer.is_alive()
    connector.send.assert_called_once()
    broadcaster.add(_event("close_scan"))
    broadcaster.flush()
    assert connector.send.call_count == 1


def test_events_before_scan_execution_are_ignored():
    connector = mock.Mock()
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.add(_event())
    broadcaster.flush()
    connector.send.assert_not_called()


def test_failed_flush_retains_latest_update_for_retry():
    connector = mock.Mock()
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.start()
    with mock.patch("bec_server.scan_server.scan_hook_broadcaster.threading.Timer"):
        event = _event()
        broadcaster.add(event)
        connector.send.side_effect = RuntimeError("Redis unavailable")
        with pytest.raises(RuntimeError):
            broadcaster.flush()
        connector.send.side_effect = None
        broadcaster.stop()
    assert connector.send.call_args.args[1] == event


def test_timer_flushes_without_further_scan_activity():
    connector = mock.Mock()
    sent = threading.Event()
    connector.send.side_effect = lambda *args, **kwargs: sent.set()
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.start()
    broadcaster.add(_event())
    try:
        assert sent.wait(2)
    finally:
        broadcaster.stop()
    connector.send.assert_called_once()


def test_new_update_replaces_failed_publication():
    connector = mock.Mock()
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.start()
    with mock.patch("bec_server.scan_server.scan_hook_broadcaster.threading.Timer"):
        broadcaster.add(_event("stage"))
        connector.send.side_effect = RuntimeError("Redis unavailable")
        with pytest.raises(RuntimeError):
            broadcaster.flush()
        latest = _event("pre_scan", device="samx")
        broadcaster.add(latest)
        connector.send.side_effect = None
        broadcaster.stop()
    assert connector.send.call_args.args[1] == latest


@pytest.mark.parametrize("fail_first", [False, True])
def test_slow_publication_does_not_block_or_overwrite_new_updates(fail_first):
    connector = mock.Mock()
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.start()
    publishing = threading.Event()
    release = threading.Event()
    added = threading.Event()
    original = _event("stage")
    latest = _event("pre_scan")

    def send(*args, **kwargs):
        publishing.set()
        assert release.wait(5)
        if fail_first:
            raise RuntimeError("Redis unavailable")

    connector.send.side_effect = send

    def add_latest():
        broadcaster.add(latest)
        added.set()

    with mock.patch("bec_server.scan_server.scan_hook_broadcaster.threading.Timer"):
        broadcaster.add(original)
        publisher = threading.Thread(target=broadcaster._flush_scheduled)
        producer = threading.Thread(target=add_latest)
        publisher.start()
        try:
            assert publishing.wait(2)
            producer.start()
            assert added.wait(2), "Adding an update blocked on monitoring publication"
        finally:
            release.set()
            publisher.join(5)
            if producer.ident is not None:
                producer.join(5)
        assert not publisher.is_alive()
        assert not producer.is_alive()
        connector.send.side_effect = None
        broadcaster.stop()
    assert [call.args[1] for call in connector.send.call_args_list] == [original, latest]


def test_stop_logs_publication_failure_and_joins_timer():
    connector = mock.Mock()
    connector.send.side_effect = RuntimeError("Redis unavailable")
    broadcaster = ScanHookBroadcaster(connector)
    broadcaster.start()
    broadcaster.add(_event())
    timer = broadcaster._timer
    with mock.patch("bec_server.scan_server.scan_hook_broadcaster.logger.exception") as log:
        broadcaster.stop()
    assert not timer.is_alive()
    log.assert_called_once()
    assert not broadcaster._active
