import copy
from unittest import mock

from bec_server.bec_server_utils.service_handler import ServiceHandler


def test_service_handler():
    bec_path = "/path/to/bec"
    config_path = "/path/to/config"

    with mock.patch("bec_server.bec_server_utils.service_handler.sys") as mock_sys:
        mock_sys.platform = "linux"
        service_handler = ServiceHandler(bec_path, config_path)
        assert service_handler.interface == "tmux"


def test_service_handler_start():
    bec_path = "/path/to/bec"

    with mock.patch("bec_server.bec_server_utils.service_handler.sys") as mock_sys:
        mock_sys.platform = "linux"
        service_handler = ServiceHandler(bec_path)

        with mock.patch(
            "bec_server.bec_server_utils.service_handler.tmux_start"
        ) as mock_tmux_start:
            service_handler.start()

            mock_tmux_start.assert_called_once_with(bec_path, service_handler.SERVICES)


def test_service_handler_stop():
    with mock.patch("bec_server.bec_server_utils.service_handler.tmux_stop") as mock_tmux_stop:
        service_handler = ServiceHandler("/path/to/bec")
        service_handler.stop()
        mock_tmux_stop.assert_called()


def test_service_handler_restart():
    bec_path = "/path/to/bec"
    config_path = "/path/to/config"

    with mock.patch("bec_server.bec_server_utils.service_handler.sys") as mock_sys:
        mock_sys.platform = "linux"
        service_handler = ServiceHandler(bec_path, config_path)
        services = copy.deepcopy(service_handler.SERVICES)
        for service_name, service_desc in services.items():
            service_desc.command += f" --config {config_path}"

        with mock.patch("bec_server.bec_server_utils.service_handler.tmux_stop") as mock_tmux_stop:
            with mock.patch(
                "bec_server.bec_server_utils.service_handler.tmux_start"
            ) as mock_tmux_start:
                service_handler.restart()
                mock_tmux_stop.assert_called()
                mock_tmux_start.assert_called_once_with(bec_path, services)


def test_service_handler_services():
    service_handler = ServiceHandler("/path/to/bec", "/path/to/config")
    assert (
        service_handler.SERVICES["scan_server"].path.substitute(base_path="/path/to/bec")
        == "/path/to/bec/scan_server"
    )

    assert service_handler.SERVICES["scan_server"].command == "bec-scan-server"
