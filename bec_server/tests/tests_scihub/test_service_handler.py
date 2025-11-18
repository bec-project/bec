from unittest import mock

from bec_lib import messages
from bec_server.scihub.service_handler.service_handler import ServiceHandler


def test_service_handler_initialization():
    """Test ServiceHandler initialization"""
    mock_connector = mock.MagicMock()
    handler = ServiceHandler(mock_connector)

    assert handler.connector == mock_connector
    assert "python" in handler.command
    assert "bec_server.bec_server_utils.launch" in handler.command


def test_service_handler_start():
    """Test ServiceHandler registers message handler"""
    mock_connector = mock.MagicMock()
    handler = ServiceHandler(mock_connector)

    handler.start()

    mock_connector.register.assert_called_once()
    call_args = mock_connector.register.call_args
    assert call_args[1]["cb"] == handler.handle_service_request
    assert call_args[1]["parent"] == handler


def test_handle_service_request_restart_all():
    """Test handling restart request for all services"""
    mock_connector = mock.MagicMock()
    handler = ServiceHandler(mock_connector)

    # Create mock message for restarting all services
    mock_msg = mock.MagicMock()
    mock_msg.value = messages.ServiceRequestMessage(action="restart")

    with mock.patch.object(handler, "on_restart") as mock_on_restart:
        ServiceHandler.handle_service_request(mock_msg, handler)
        mock_on_restart.assert_called_once_with(service_name=None)


def test_handle_service_request_restart_single_service():
    """Test handling restart request for a single service"""
    mock_connector = mock.MagicMock()
    handler = ServiceHandler(mock_connector)

    # Create mock message for restarting a specific service
    mock_msg = mock.MagicMock()
    mock_msg.value = messages.ServiceRequestMessage(action="restart", service_name="scan_server")

    with mock.patch.object(handler, "on_restart") as mock_on_restart:
        ServiceHandler.handle_service_request(mock_msg, handler)
        mock_on_restart.assert_called_once_with(service_name="scan_server")


def test_on_restart_all_services():
    """Test restarting all services"""
    mock_connector = mock.MagicMock()
    handler = ServiceHandler(mock_connector)

    with mock.patch(
        "bec_server.scihub.service_handler.service_handler.subprocess.Popen"
    ) as mock_popen:
        handler.on_restart(service_name=None)

        mock_popen.assert_called_once()
        call_args = mock_popen.call_args
        command = call_args[0][0]

        assert "restart" in command
        assert "--service" not in command


def test_on_restart_single_service():
    """Test restarting a single service"""
    mock_connector = mock.MagicMock()
    handler = ServiceHandler(mock_connector)

    with mock.patch(
        "bec_server.scihub.service_handler.service_handler.subprocess.Popen"
    ) as mock_popen:
        handler.on_restart(service_name="scan_server")

        mock_popen.assert_called_once()
        call_args = mock_popen.call_args
        command = call_args[0][0]

        assert "restart" in command
        assert "--service scan_server" in command


def test_on_restart_subprocess_detached():
    """Test that subprocess is launched detached"""
    mock_connector = mock.MagicMock()
    handler = ServiceHandler(mock_connector)

    with mock.patch(
        "bec_server.scihub.service_handler.service_handler.subprocess.Popen"
    ) as mock_popen:
        handler.on_restart(service_name="device_server")

        call_kwargs = mock_popen.call_args[1]
        assert call_kwargs["shell"] is True
        assert call_kwargs["stdout"] == mock.ANY
        assert call_kwargs["stderr"] == mock.ANY
        assert call_kwargs["stdin"] == mock.ANY
        assert "preexec_fn" in call_kwargs
