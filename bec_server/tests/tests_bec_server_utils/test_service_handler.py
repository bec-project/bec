import copy
from unittest import mock

from bec_server.bec_server_utils.service_handler import ServiceHandler
from bec_server.scihub.service_handler.service_handler import ServiceHandler as SciHubServiceHandler


def test_scihub_restart_starts_new_session():
    handler = SciHubServiceHandler(mock.MagicMock())

    with mock.patch("bec_server.scihub.service_handler.service_handler.subprocess.Popen") as popen:
        handler.on_restart()

    popen.assert_called_once()
    assert popen.call_args.kwargs["start_new_session"] is True
    assert "preexec_fn" not in popen.call_args.kwargs


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

            mock_tmux_start.assert_called_once_with(
                bec_path, {name: desc for name, (desc, _) in service_handler.SERVICES.items()}
            )


def test_service_handler_stop():
    with mock.patch("bec_server.bec_server_utils.service_handler.tmux_stop") as mock_tmux_stop:
        service_handler = ServiceHandler("/path/to/bec")
        service_handler.stop()
        mock_tmux_stop.assert_called()


def test_service_handler_stop_subprocess_forwards_timeout():
    with mock.patch(
        "bec_server.bec_server_utils.service_handler.subprocess_stop"
    ) as mock_subprocess_stop:
        service_handler = ServiceHandler("/path/to/bec", interface="subprocess")
        processes = [object()]

        service_handler.stop(processes, timeout_s=12)

        mock_subprocess_stop.assert_called_once_with(processes, timeout_s=12)


def test_service_handler_restart():
    bec_path = "/path/to/bec"
    config_path = "/path/to/config"

    with mock.patch("bec_server.bec_server_utils.service_handler.sys") as mock_sys:
        mock_sys.platform = "linux"
        service_handler = ServiceHandler(bec_path, config_path)
        services = {name: desc for name, (desc, _) in service_handler.SERVICES.items()}
        expected_services = copy.deepcopy(services)
        for service_desc in expected_services.values():
            service_desc.command += f" --config {config_path}"

        with (
            mock.patch("bec_server.bec_server_utils.service_handler.tmux_stop") as mock_tmux_stop,
            mock.patch("bec_server.bec_server_utils.service_handler.tmux_start") as mock_tmux_start,
        ):
            service_handler.restart()
            mock_tmux_stop.assert_called()
            mock_tmux_start.assert_called_once_with(bec_path, expected_services)


def test_service_handler_services():
    service_handler = ServiceHandler("/path/to/bec", "/path/to/config")
    assert (
        service_handler.SERVICES["scan_server"][0].path.substitute(base_path="/path/to/bec")
        == "/path/to/bec/scan_server"
    )

    assert service_handler.SERVICES["scan_server"][0].command == "bec-scan-server"
