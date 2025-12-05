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


def test_service_handler_restart_service_success():
    """Test successfully restarting a single service"""
    bec_path = "/path/to/bec"
    config_path = "/path/to/config"

    with mock.patch("bec_server.bec_server_utils.service_handler.sys") as mock_sys:
        mock_sys.platform = "linux"
        service_handler = ServiceHandler(bec_path, config_path)

        with mock.patch(
            "bec_server.bec_server_utils.service_handler.tmux_restart_service"
        ) as mock_tmux_restart:
            mock_tmux_restart.return_value = True

            result = service_handler.restart_service("scan_server")

            assert result is True
            mock_tmux_restart.assert_called_once()
            # Verify the service config includes the config path
            call_args = mock_tmux_restart.call_args
            assert call_args[0][0] == bec_path
            assert call_args[0][1] == "scan_server"
            assert f"--config {config_path}" in call_args[0][2].command


def test_service_handler_restart_service_unknown_service():
    """Test restarting an unknown service"""
    service_handler = ServiceHandler("/path/to/bec")

    result = service_handler.restart_service("unknown_service")

    assert result is False


def test_service_handler_restart_service_not_found():
    """Test restarting a service that is not running"""
    bec_path = "/path/to/bec"

    with mock.patch("bec_server.bec_server_utils.service_handler.sys") as mock_sys:
        mock_sys.platform = "linux"
        service_handler = ServiceHandler(bec_path)

        with mock.patch(
            "bec_server.bec_server_utils.service_handler.tmux_restart_service"
        ) as mock_tmux_restart:
            mock_tmux_restart.return_value = False

            result = service_handler.restart_service("scan_server")

            assert result is False


def test_service_handler_restart_service_unsupported_interface():
    """Test restarting a service with systemctl interface"""
    bec_path = "/path/to/bec"

    service_handler = ServiceHandler(bec_path, interface="systemctl")

    result = service_handler.restart_service("scan_server")

    assert result is False
