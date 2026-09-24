from unittest import mock

import pytest

from bec_lib.client import BECClient
from bec_lib.messages import BECStatus
from bec_lib.service_config import ServiceConfig
from bec_server.data_processing.dap_server import DAPServer
from bec_server.data_processing.dap_service import DAPServiceBase


@pytest.fixture
def dap_server():
    config = ServiceConfig()
    server = DAPServer(
        config=config, connector_cls=mock.MagicMock(), provided_services=DAPServiceBase, forced=True
    )
    yield server
    server.shutdown()
    server._reset_singleton()


def test_dap_server(dap_server):
    assert dap_server._service_id == "DAPServiceBase"


def test_dap_server_waits_before_starting_client_services(dap_server):
    startup = mock.Mock()
    with (
        mock.patch.object(dap_server, "wait_for_service") as wait,
        mock.patch.object(
            dap_server, "_start_services", side_effect=RuntimeError("stop before client services")
        ) as start_services,
        mock.patch.object(dap_server, "load_high_level_interface"),
        mock.patch("builtins.bec", create=True),
    ):
        startup.attach_mock(wait, "wait")
        startup.attach_mock(start_services, "start_services")
        with pytest.raises(RuntimeError, match="stop before client services"):
            dap_server.start()

    assert startup.mock_calls == [
        mock.call.wait("ScanServer", BECStatus.RUNNING),
        mock.call.wait("ScanBundler", BECStatus.RUNNING),
        mock.call.wait("DeviceServer", BECStatus.RUNNING),
        mock.call.wait("SciHub", BECStatus.RUNNING),
        mock.call.start_services(),
    ]


@pytest.mark.parametrize("publish_fails", [False, True])
def test_dap_plugins_published_before_running(dap_server, publish_fails):
    events = []

    def publish_plugins():
        events.append("plugins")
        assert dap_server.status == BECStatus.BUSY
        if publish_fails:
            raise RuntimeError("plugin publication failed")

    def publish_status():
        if dap_server.status == BECStatus.RUNNING:
            events.append("running")

    dap_server.device_manager = mock.Mock()
    with (
        mock.patch.object(dap_server, "wait_for_service"),
        mock.patch.object(BECClient, "_start_services"),
        mock.patch.object(dap_server, "load_high_level_interface"),
        mock.patch.object(dap_server, "_send_service_status", side_effect=publish_status),
        mock.patch.object(
            dap_server._dap_service_manager,
            "publish_available_services",
            side_effect=publish_plugins,
        ),
        mock.patch("builtins.bec", create=True),
    ):
        if publish_fails:
            with pytest.raises(RuntimeError, match="plugin publication failed"):
                dap_server.start()
            assert events == ["plugins"]
        else:
            dap_server.start()
            assert events == ["plugins", "running"]
