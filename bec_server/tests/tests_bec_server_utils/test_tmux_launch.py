from string import Template
from unittest import mock

from bec_server.bec_server_utils.service_handler import ServiceDesc
from bec_server.bec_server_utils.tmux_launch import (
    activate_venv,
    tmux_restart_service,
    tmux_start,
    tmux_start_service,
    tmux_stop,
    tmux_stop_service,
)


def test_tmux_start():
    with mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux_server:
        tmux_start(
            "/path/to/bec",
            {
                "scan_server": ServiceDesc(Template("$base_path/scan_server"), "bec-scan-server"),
                "scan_bundler": ServiceDesc(
                    Template("$base_path/scan_bundler"), "bec-scan-bundler"
                ),
            },
        )
        mock_libtmux_server.Server().new_session.assert_called_once_with(
            "bec", window_name="BEC server. Use `ctrl+b d` to detach.", kill_session=True
        )
        assert (
            mock_libtmux_server.Server().new_session().attached_window.select_layout.call_count == 1
        )

        assert mock_libtmux_server.Server().new_session().set_option.call_count == 1
        assert mock_libtmux_server.Server().new_session().set_option.call_args[0][0] == "mouse"
        assert mock_libtmux_server.Server().new_session().set_option.call_args[0][1] == "on"


def test_tmux_stop_without_sessions():
    with mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux_server:
        mock_libtmux_server.Server().sessions.filter.return_value = []
        tmux_stop()
        mock_libtmux_server.Server().kill_server.assert_not_called()


def test_tmux_stop_with_sessions():
    session = mock.MagicMock()
    with mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux_server:
        mock_libtmux_server.Server().sessions.filter.return_value = [session]
        tmux_stop()
        session.kill_session.assert_called_once()


def test_tmux_restart_service_success():
    """Test successfully restarting a service"""
    service_config = ServiceDesc(Template("$base_path/scan_server"), "bec-scan-server")

    with (
        mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux,
        mock.patch("bec_server.bec_server_utils.tmux_launch.psutil") as mock_psutil,
        mock.patch("bec_server.bec_server_utils.tmux_launch.activate_venv") as mock_activate_venv,
    ):

        # Setup mock session and pane
        mock_session = mock.MagicMock()
        mock_pane = mock.MagicMock()
        mock_pane.pane_pid = "12345"
        # Mock display_message to return the service name as pane title
        mock_pane.display_message.return_value = ["scan_server"]
        mock_session.panes = [mock_pane]
        mock_libtmux.Server().sessions.filter.return_value = [mock_session]

        # Setup mock process
        mock_process = mock.MagicMock()
        mock_child = mock.MagicMock()
        mock_child.is_running.return_value = False
        mock_process.children.return_value = [mock_child]
        mock_psutil.Process.return_value = mock_process

        # Call the function
        result = tmux_restart_service("/path/to/bec", "scan_server", service_config)

        # Assertions
        assert result is True
        mock_pane.display_message.assert_called_with("#{pane_title}", get_text=True)
        mock_pane.send_keys.assert_any_call("^C")
        mock_pane.send_keys.assert_any_call("bec-scan-server")
        mock_activate_venv.assert_called_once()


def test_tmux_restart_service_not_found():
    """Test restarting a service that is not running"""
    service_config = ServiceDesc(Template("$base_path/scan_server"), "bec-scan-server")

    with mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux:

        # Setup mock session with pane running different service
        mock_session = mock.MagicMock()
        mock_pane = mock.MagicMock()
        # Mock display_message to return a different service name
        mock_pane.display_message.return_value = ["device_server"]
        mock_session.panes = [mock_pane]
        mock_libtmux.Server().sessions.filter.return_value = [mock_session]

        # Call the function
        result = tmux_restart_service("/path/to/bec", "scan_server", service_config)

        # Assertions
        assert result is False


def test_tmux_restart_service_no_session():
    """Test restarting a service when no session exists"""
    service_config = ServiceDesc(Template("$base_path/scan_server"), "bec-scan-server")

    with mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux:
        mock_libtmux.Server().sessions.filter.return_value = []

        result = tmux_restart_service("/path/to/bec", "scan_server", service_config)

        assert result is False


def test_tmux_stop_service_success():
    """Test successfully stopping a single service"""
    with (
        mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux,
        mock.patch("bec_server.bec_server_utils.tmux_launch.psutil") as mock_psutil,
    ):
        # Setup mock session and pane
        mock_session = mock.MagicMock()
        mock_pane = mock.MagicMock()
        mock_pane.pane_pid = "12345"
        mock_pane.display_message.return_value = ["scan_server"]
        mock_session.panes = [mock_pane]
        mock_libtmux.Server().sessions.filter.return_value = [mock_session]

        # Setup mock process
        mock_process = mock.MagicMock()
        mock_child = mock.MagicMock()
        mock_child.is_running.return_value = False
        mock_process.children.return_value = [mock_child]
        mock_psutil.Process.return_value = mock_process

        # Call the function
        result = tmux_stop_service("scan_server")

        # Assertions
        assert result is True
        mock_pane.send_keys.assert_called_with("^C")


def test_tmux_start_service_success():
    """Test successfully starting a single service"""
    service_config = ServiceDesc(Template("$base_path/scan_server"), "bec-scan-server")

    with (
        mock.patch("bec_server.bec_server_utils.tmux_launch.libtmux") as mock_libtmux,
        mock.patch("bec_server.bec_server_utils.tmux_launch.activate_venv") as mock_activate_venv,
    ):
        # Setup mock session and pane
        mock_session = mock.MagicMock()
        mock_pane = mock.MagicMock()
        mock_pane.display_message.return_value = ["scan_server"]
        mock_session.panes = [mock_pane]
        mock_libtmux.Server().sessions.filter.return_value = [mock_session]

        # Call the function
        result = tmux_start_service("/path/to/bec", "scan_server", service_config)

        # Assertions
        assert result is True
        mock_activate_venv.assert_called_once_with(
            mock_pane, service_name="scan_server", service_path="/path/to/bec/scan_server"
        )
        mock_pane.send_keys.assert_called_with("bec-scan-server")


def test_activate_venv_with_service_venv():
    """Test activate_venv when service-specific venv exists"""
    mock_pane = mock.MagicMock()

    with (
        mock.patch("bec_server.bec_server_utils.tmux_launch.os.path.exists") as mock_exists,
        mock.patch("bec_server.bec_server_utils.tmux_launch.os.getenv") as mock_getenv,
    ):
        # Mock that service_venv exists
        mock_exists.side_effect = lambda path: "scan_server_venv" in path
        mock_getenv.return_value = None

        activate_venv(mock_pane, "scan_server", "/path/to/bec/scan_server")

        mock_pane.send_keys.assert_called_once_with(
            "source /path/to/bec/scan_server/scan_server_venv/bin/activate"
        )


def test_activate_venv_with_conda():
    """Test activate_venv when running in Conda environment"""
    mock_pane = mock.MagicMock()

    with (
        mock.patch("bec_server.bec_server_utils.tmux_launch.os.path.exists") as mock_exists,
        mock.patch("bec_server.bec_server_utils.tmux_launch.os.getenv") as mock_getenv,
        mock.patch(
            "bec_server.bec_server_utils.tmux_launch.os.environ",
            {"CONDA_PREFIX": "/opt/conda/envs/bec-env"},
        ),
    ):
        # No venv exists
        mock_exists.return_value = False
        # CONDA_PREFIX is set
        mock_getenv.return_value = "/opt/conda/envs/bec-env"

        activate_venv(mock_pane, "scan_server", "/path/to/bec/scan_server")

        mock_pane.send_keys.assert_called_once_with("conda activate bec-env")
