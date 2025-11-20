from unittest import mock

from bec_server.bec_server_utils.launch import main


def test_main_start():
    with mock.patch("bec_server.bec_server_utils.launch.ServiceHandler") as mock_service_handler:
        with mock.patch("bec_server.bec_server_utils.launch.argparse") as mock_argparse:
            mock_argparse.ArgumentParser().parse_args.return_value = mock.MagicMock(
                command="start", config=None, interface="tmux", service=None
            )
            main()
            mock_service_handler.assert_called_once_with(
                bec_path=mock.ANY,
                config_path=None,
                interface="tmux",
                start_redis=False,
                no_persistence=False,
            )
            mock_service_handler().start.assert_called_once()


def test_main_stop():
    with mock.patch("bec_server.bec_server_utils.launch.ServiceHandler") as mock_service_handler:
        with mock.patch("bec_server.bec_server_utils.launch.argparse") as mock_argparse:
            mock_argparse.ArgumentParser().parse_args.return_value = mock.MagicMock(
                command="stop", config=None, interface="tmux", service=None
            )
            main()
            mock_service_handler.assert_called_once_with(
                bec_path=mock.ANY,
                config_path=None,
                interface="tmux",
                start_redis=False,
                no_persistence=False,
            )
            mock_service_handler().stop.assert_called_once()


def test_main_restart():
    with mock.patch("bec_server.bec_server_utils.launch.ServiceHandler") as mock_service_handler:
        with mock.patch("bec_server.bec_server_utils.launch.argparse") as mock_argparse:
            mock_argparse.ArgumentParser().parse_args.return_value = mock.MagicMock(
                command="restart", config=None, interface="tmux", service=None
            )
            main()
            mock_service_handler.assert_called_once_with(
                bec_path=mock.ANY,
                config_path=None,
                interface="tmux",
                start_redis=False,
                no_persistence=False,
            )
            mock_service_handler().restart.assert_called_once()


def test_main_start_with_config():
    with mock.patch("bec_server.bec_server_utils.launch.ServiceHandler") as mock_service_handler:
        with mock.patch("bec_server.bec_server_utils.launch.argparse") as mock_argparse:
            mock_argparse.ArgumentParser().parse_args.return_value = mock.MagicMock(
                command="start", config="/path/to/config", interface="tmux", service=None
            )
            main()
            mock_service_handler.assert_called_once_with(
                bec_path=mock.ANY,
                config_path="/path/to/config",
                interface="tmux",
                start_redis=False,
                no_persistence=False,
            )
            mock_service_handler().start.assert_called_once()


def test_main_restart_with_config():
    with mock.patch("bec_server.bec_server_utils.launch.ServiceHandler") as mock_service_handler:
        with mock.patch("bec_server.bec_server_utils.launch.argparse") as mock_argparse:
            mock_argparse.ArgumentParser().parse_args.return_value = mock.MagicMock(
                command="restart", config="/path/to/config", interface="tmux", service=None
            )
            main()
            mock_service_handler.assert_called_once_with(
                bec_path=mock.ANY,
                config_path="/path/to/config",
                interface="tmux",
                start_redis=False,
                no_persistence=False,
            )
            mock_service_handler().restart.assert_called_once()


def test_main_restart_single_service():
    with mock.patch("bec_server.bec_server_utils.launch.ServiceHandler") as mock_service_handler:
        with mock.patch("bec_server.bec_server_utils.launch.argparse") as mock_argparse:
            mock_argparse.ArgumentParser().parse_args.return_value = mock.MagicMock(
                command="restart", config=None, interface="tmux", service="scan_server"
            )
            main()
            mock_service_handler.assert_called_once_with(
                bec_path=mock.ANY,
                config_path=None,
                interface="tmux",
                start_redis=False,
                no_persistence=False,
            )
            mock_service_handler().restart_service.assert_called_once_with("scan_server")
