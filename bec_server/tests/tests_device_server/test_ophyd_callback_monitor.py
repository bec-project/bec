"""Tests for the standalone ophyd callback queue monitor."""

from types import SimpleNamespace
from unittest import mock

import pytest

from bec_server.device_server.ophyd_callback_monitor import OphydCallbackMonitor

# pylint: disable=protected-access


@pytest.mark.parametrize("queue_size", [0, 999, 1000, 1001])
def test_sample_warns_only_above_threshold(queue_size):
    callback_monitor = OphydCallbackMonitor()
    monitor_thread = SimpleNamespace(
        queue=mock.Mock(qsize=mock.Mock(return_value=queue_size)),
        ident=42,
        current_callback="slow_callback",
    )
    frame = object()

    with (
        mock.patch("bec_server.device_server.ophyd_callback_monitor.ophyd.get_cl") as get_cl,
        mock.patch(
            "bec_server.device_server.ophyd_callback_monitor.sys._current_frames",
            return_value={42: frame},
        ) as frames,
        mock.patch(
            "bec_server.device_server.ophyd_callback_monitor.traceback.format_stack",
            return_value=["callback stack"],
        ) as stack,
        mock.patch("bec_server.device_server.ophyd_callback_monitor.logger.warning") as warning,
    ):
        get_cl.return_value.get_dispatcher.return_value = SimpleNamespace(
            threads={"monitor": monitor_thread}
        )

        callback_monitor._sample()

    if queue_size > 1000:
        warning.assert_called_once_with(
            "Ophyd callback monitor queue exceeds 1000: "
            "queued=1001, last_started=slow_callback\ncallback stack"
        )
        frames.assert_called_once_with()
        stack.assert_called_once_with(frame, limit=20)
    else:
        warning.assert_not_called()
        frames.assert_not_called()
        stack.assert_not_called()


def test_sample_repeats_warning_while_over_custom_threshold_and_recovers():
    callback_monitor = OphydCallbackMonitor(queue_threshold=2)
    monitor_thread = SimpleNamespace(
        queue=mock.Mock(qsize=mock.Mock(side_effect=[3, 4, 2])), ident=None, current_callback=None
    )

    with (
        mock.patch("bec_server.device_server.ophyd_callback_monitor.ophyd.get_cl") as get_cl,
        mock.patch(
            "bec_server.device_server.ophyd_callback_monitor.sys._current_frames", return_value={}
        ),
        mock.patch("bec_server.device_server.ophyd_callback_monitor.logger.warning") as warning,
    ):
        get_cl.return_value.get_dispatcher.return_value = SimpleNamespace(
            threads={"monitor": monitor_thread}
        )

        for _ in range(3):
            callback_monitor._sample()

    assert warning.call_args_list == [
        mock.call(
            "Ophyd callback monitor queue exceeds 2: "
            f"queued={size}, last_started=None\n<no stack>"
        )
        for size in (3, 4)
    ]


@pytest.mark.parametrize("dispatcher", [None, SimpleNamespace(threads={})])
def test_sample_without_monitor_thread_does_not_warn(dispatcher):
    callback_monitor = OphydCallbackMonitor()

    with (
        mock.patch("bec_server.device_server.ophyd_callback_monitor.ophyd.get_cl") as get_cl,
        mock.patch("bec_server.device_server.ophyd_callback_monitor.logger.warning") as warning,
    ):
        get_cl.return_value.get_dispatcher.return_value = dispatcher

        callback_monitor._sample()

    warning.assert_not_called()


def test_run_samples_at_configured_interval_until_stopped():
    callback_monitor = OphydCallbackMonitor(sample_interval=0.25)

    with (
        mock.patch.object(callback_monitor._stop_event, "wait", side_effect=[False, True]) as wait,
        mock.patch.object(callback_monitor, "_sample") as sample,
    ):
        callback_monitor._run()

    assert wait.call_args_list == [mock.call(0.25), mock.call(0.25)]
    sample.assert_called_once_with()


def test_start_is_idempotent_and_monitor_can_restart_after_join():
    callback_monitor = OphydCallbackMonitor(sample_interval=60)

    try:
        callback_monitor.start()
        first_thread = callback_monitor._thread
        assert first_thread is not None
        assert first_thread.is_alive()

        callback_monitor.start()
        assert callback_monitor._thread is first_thread

        callback_monitor.request_stop()
        callback_monitor.join(timeout=1)
        assert not first_thread.is_alive()
        callback_monitor.start()
        assert callback_monitor._thread is first_thread

        callback_monitor.join()

        callback_monitor.start()
        second_thread = callback_monitor._thread
        assert second_thread is not None
        assert second_thread is not first_thread
        assert second_thread.is_alive()
    finally:
        callback_monitor.request_stop()
        callback_monitor.join()


def test_rejects_negative_queue_threshold():
    with pytest.raises(ValueError, match="queue_threshold must be non-negative"):
        OphydCallbackMonitor(queue_threshold=-1)
