from unittest import mock

from data_processing.cli.launch import main


def test_main():
    with mock.patch("data_processing.cli.launch.argparse.ArgumentParser") as mock_parser:
        with mock.patch("data_processing.cli.launch.ServiceConfig") as mock_config:
            mock_config.return_value.redis = "dummy:6379"
            with mock.patch("data_processing.dap_server.DAPServer") as mock_data_processing:
                with mock.patch("data_processing.cli.launch.threading.Event") as mock_event:
                    main()
                    mock_parser.assert_called_once()
                    mock_config.assert_called_once()
                    mock_data_processing.assert_called_once()
                    mock_event.assert_called_once()


def test_main_shutdown():
    with mock.patch("data_processing.cli.launch.argparse.ArgumentParser") as mock_parser:
        with mock.patch("data_processing.cli.launch.ServiceConfig") as mock_config:
            mock_config.return_value.redis = "dummy:6379"
            with mock.patch("data_processing.dap_server.DAPServer") as mock_data_processing:
                with mock.patch("data_processing.cli.launch.threading.Event") as mock_event:
                    mock_event.return_value.wait.side_effect = KeyboardInterrupt()
                    try:
                        main()
                    except KeyboardInterrupt:
                        pass
                    mock_parser.assert_called_once()
                    mock_config.assert_called_once()
                    mock_data_processing.assert_called_once()
                    mock_event.assert_called_once()
                    mock_data_processing.return_value.shutdown.assert_called_once()
