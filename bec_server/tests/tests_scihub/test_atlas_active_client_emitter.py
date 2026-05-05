from unittest import mock

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints


def create_status_message(
    name: str,
    *,
    status: messages.BECStatus = messages.BECStatus.RUNNING,
    hostname: str = "localhost",
):
    return messages.StatusMessage(
        name=name, status=status, info=messages.ServiceInfo(user="test-user", hostname=hostname)
    )


def test_get_active_client_metrics_filters_internal_services(atlas_connector):
    emitter = atlas_connector.active_client_emitter
    service_status = {
        "BECIPythonClient/client-1": create_status_message(
            "BECIPythonClient/client-1", hostname="client-host-1"
        ),
        "BECClient/client-2": create_status_message("BECClient/client-2", hostname="client-host-2"),
        "SciHub": create_status_message("SciHub", hostname="internal-host"),
        "DAPServer/image-analysis": create_status_message(
            "DAPServer/image-analysis", hostname="internal-dap-host"
        ),
        "BECIPythonClient/not-running": create_status_message(
            "BECIPythonClient/not-running", status=messages.BECStatus.IDLE, hostname="idle-host"
        ),
    }

    with mock.patch.object(
        type(atlas_connector.scihub), "service_status", new_callable=mock.PropertyMock
    ) as mock_service_status:
        mock_service_status.return_value = service_status
        assert emitter._get_active_client_metrics() == {
            "BECIPythonClient/client-1": "client-host-1",
            "BECClient/client-2": "client-host-2",
        }


def test_handle_service_status_emits_active_client_metric(atlas_connector):
    emitter = atlas_connector.active_client_emitter
    service_status = {
        "BECIPythonClient/client-1": create_status_message(
            "BECIPythonClient/client-1", hostname="client-host-1"
        ),
        "ScanServer": create_status_message("ScanServer", hostname="server-host"),
    }

    with (
        mock.patch.object(
            type(atlas_connector.scihub), "service_status", new_callable=mock.PropertyMock
        ) as mock_service_status,
        mock.patch.object(atlas_connector.connector, "publish_metrics") as mock_publish_metrics,
    ):
        mock_service_status.return_value = service_status
        emitter._handle_service_status(None)
    mock_publish_metrics.assert_called_once_with(
        "active_clients", {"BECIPythonClient/client-1": "client-host-1"}
    )


def test_handle_service_status_does_not_emit_unchanged_metric(atlas_connector):
    emitter = atlas_connector.active_client_emitter
    service_status = {
        "BECIPythonClient/client-1": create_status_message(
            "BECIPythonClient/client-1", hostname="client-host-1"
        )
    }

    with mock.patch.object(
        type(atlas_connector.scihub), "service_status", new_callable=mock.PropertyMock
    ) as mock_service_status:
        mock_service_status.return_value = service_status
        emitter._emit_active_client_metrics()
        with mock.patch.object(atlas_connector.connector, "publish_metrics") as mock_publish:
            emitter._handle_service_status(None)

    mock_publish.assert_not_called()
