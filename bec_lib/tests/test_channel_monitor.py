from unittest import mock

from bec_lib import messages
from bec_lib.channel_monitor import channel_callback, channel_monitor_launch
from bec_lib.redis_connector import MessageObject


def test_channel_monitor_callback():
    with mock.patch("builtins.print") as mock_print:
        msg = messages.DeviceMessage(
            signals={"x": 1, "y": 2, "z": 3}, metadata={"name": "test"}
        ).dumps()
        msg_obj = MessageObject(msg, "test")
        channel_callback(msg_obj)
        mock_print.assert_called_once()


def test_channel_monitor_start_consumer():
    with mock.patch("bec_lib.channel_monitor.argparse") as mock_argparse:
        with mock.patch("bec_lib.channel_monitor.ServiceConfig") as mock_config:
            with mock.patch("bec_lib.channel_monitor.RedisConnector") as mock_connector:
                with mock.patch("bec_lib.channel_monitor.threading") as mock_threading:
                    clargs = mock.MagicMock()
                    mock_argparse.ArgumentParser().parse_args.return_value = clargs
                    clargs.config = "test_config"
                    clargs.channel = "test_channel"
                    mock_threading.Event().wait.return_value = True
                    mock_config.return_value = mock.MagicMock()
                    mock_connector.return_value = mock.MagicMock()
                    channel_monitor_launch()
                    mock_connector().consumer.assert_called_once()
                    mock_connector().consumer.return_value.start.assert_called_once()
                    mock_threading.Event().wait.assert_called_once()