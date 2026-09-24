from unittest import mock

import pytest

from bec_server.data_processing.cli.launch import main


def test_main():
    with mock.patch(
        "bec_server.data_processing.cli.launch.parse_cmdline_args", return_value=(None, None, None)
    ) as mock_parser:
        with mock.patch("bec_server.data_processing.dap_server.DAPServer") as mock_data_processing:
            with mock.patch("bec_server.data_processing.cli.launch.threading.Event") as mock_event:
                main()
                mock_parser.assert_called_once()
                mock_data_processing.assert_called_once()
                mock_event.assert_called_once()


@pytest.mark.parametrize("interrupt_during_start", [False, True])
def test_main_shutdown(interrupt_during_start):
    with mock.patch(
        "bec_server.data_processing.cli.launch.parse_cmdline_args", return_value=(None, None, None)
    ) as mock_parser:
        with mock.patch("bec_server.data_processing.dap_server.DAPServer") as mock_data_processing:
            with mock.patch("bec_server.data_processing.cli.launch.threading.Event") as mock_event:
                if interrupt_during_start:
                    mock_data_processing.return_value.start.side_effect = KeyboardInterrupt()
                else:
                    mock_event.return_value.wait.side_effect = KeyboardInterrupt()
                main()
                mock_parser.assert_called_once()
                mock_data_processing.assert_called_once()
                mock_event.assert_called_once()
                mock_data_processing.return_value.shutdown.assert_called_once()
