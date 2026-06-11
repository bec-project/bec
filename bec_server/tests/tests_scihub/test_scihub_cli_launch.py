from unittest import mock

from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.scihub.cli.launch import main
from bec_server.scihub.scihub import SciHub


def test_main():
    with mock.patch(
        "bec_server.scihub.cli.launch.parse_cmdline_args", return_value=(None, None, None)
    ) as mock_parser:
        with mock.patch("bec_server.scihub.SciHub") as mock_scihub:
            with mock.patch("bec_server.scihub.cli.launch.threading.Event") as mock_event:
                main()
                mock_parser.assert_called_once()
                mock_scihub.assert_called_once()
                mock_event.assert_called_once()


def test_main_shutdown():
    with mock.patch(
        "bec_server.scihub.cli.launch.parse_cmdline_args", return_value=(None, None, None)
    ) as mock_parser:
        with mock.patch("bec_server.scihub.SciHub") as mock_scihub:
            with mock.patch("bec_server.scihub.cli.launch.threading.Event") as mock_event:
                mock_event.return_value.wait.side_effect = KeyboardInterrupt()
                main()
                mock_parser.assert_called_once()
                mock_scihub.assert_called_once()
                mock_event.assert_called_once()
                mock_scihub.return_value.shutdown.assert_called_once()


def test_scihub_starts_and_stops_messaging_manager():
    config = ServiceConfig(
        redis={"host": "dummy", "port": 6379},
        service_config={
            "file_writer": {"plugin": "default_NeXus_format", "base_path": "./"},
            "log_writer": {"base_path": "./"},
        },
    )

    with (
        mock.patch.object(SciHub, "_start_metrics_emitter"),
        mock.patch.object(SciHub, "wait_for_service"),
        mock.patch("bec_server.scihub.scihub.AtlasConnector"),
        mock.patch("bec_server.scihub.scihub.ServiceHandler"),
        mock.patch("bec_server.scihub.scihub.MessagingManager") as mock_messaging_manager,
    ):
        scihub = SciHub(config, ConnectorMock)
        try:
            mock_messaging_manager.assert_called_once_with(scihub.connector)
        finally:
            scihub.shutdown()

    mock_messaging_manager.return_value.shutdown.assert_called_once()
