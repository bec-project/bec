from unittest import mock

import pytest

from bec_messages import messages
from bec_lib.channel_monitor import channel_callback, channel_monitor_launch, log_callback
from bec_lib.redis_connector import MessageObject


def test_channel_monitor_callback():
    with mock.patch("builtins.print") as mock_print:
        msg = messages.DeviceMessage(
            signals={"x": {"value": 1}, "y": {"value": 2}}, metadata={"name": "test"}
        )
        msg_obj = {"data": msg}
        channel_callback(msg_obj)
        mock_print.assert_called_once()


def test_channel_monitor_start_register():
    with mock.patch("bec_lib.channel_monitor.argparse") as mock_argparse:
        with mock.patch("bec_lib.channel_monitor.RedisConnector") as mock_connector:
            with mock.patch("bec_lib.channel_monitor.threading") as mock_threading:
                clargs = mock.MagicMock()
                mock_argparse.ArgumentParser().parse_args.return_value = clargs
                clargs.config = "test_config"
                clargs.channel = "test_channel"
                mock_threading.Event().wait.return_value = True
                mock_connector.return_value = mock.MagicMock()
                channel_monitor_launch()
                mock_connector().register.assert_called_once()
                mock_threading.Event().wait.assert_called_once()


def test_log_monitor_callback_without_filter():
    with mock.patch("builtins.print") as mock_print:
        msg = messages.LogMessage(log_type="info", log_msg={"text": "test"})
        msg_obj = {"data": msg}
        log_callback(msg_obj)
        mock_print.assert_called_once_with("test")


@pytest.mark.parametrize(
    "text, log_filter, printed",
    [
        ("test", "te", True),
        ("test", "no", False),
        ("test", None, True),
        ("test", ".*", True),
        ("test", ".*no.*", False),
    ],
)
def test_log_monitor_callback_filter(text, log_filter, printed):
    with mock.patch("builtins.print") as mock_print:
        msg = messages.LogMessage(log_type="info", log_msg={"text": text})
        msg_obj = {"data": msg}
        log_callback(msg_obj, log_filter=log_filter)
        if printed:
            mock_print.assert_called_once_with("test")
        else:
            mock_print.assert_not_called()
