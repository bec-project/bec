from unittest import mock

from bec_lib import messages
from bec_server.scihub.atlas.atlas_connector import AtlasConnector


def test_atlas_connector_load_env(SciHubMock, connected_atlas_connector):
    atlas_connector = AtlasConnector(SciHubMock, SciHubMock.connector, connected_atlas_connector)

    with mock.patch("os.path.exists", side_effect=[False, True]):
        with mock.patch(
            "bec_server.scihub.atlas.atlas_connector.dotenv_values"
        ) as mock_dotenv_values:
            with mock.patch.object(atlas_connector, "_update_config") as mock_update_config:
                mock_dotenv_values.return_value = {
                    "ATLAS_HOST": "dummy_host",
                    "ATLAS_DEPLOYMENT": "dummy_deployment",
                    "ATLAS_KEY": "dummy_key",
                }
                atlas_connector._load_environment()
                mock_dotenv_values.assert_called_once()
                mock_update_config.assert_called_once_with(
                    ATLAS_HOST="dummy_host",
                    ATLAS_DEPLOYMENT="dummy_deployment",
                    ATLAS_KEY="dummy_key",
                )


def test_atlas_connector_update_config(SciHubMock, connected_atlas_connector):
    atlas_connector = AtlasConnector(SciHubMock, SciHubMock.connector, connected_atlas_connector)

    atlas_connector._update_config(
        ATLAS_HOST="dummy_host", ATLAS_DEPLOYMENT="dummy_deployment", ATLAS_KEY="dummy_key"
    )
    assert atlas_connector.host == "dummy_host"
    assert atlas_connector.deployment_name == "dummy_deployment"
    assert atlas_connector.atlas_key == "dummy_key"
    assert atlas_connector._env_configured == True


def test_atlas_connector_ingest_data(atlas_connector):

    with mock.patch.object(atlas_connector.redis_atlas, "xadd") as mock_xadd:
        atlas_connector.ingest_data({"data": "dummy_data"})
        mock_xadd.assert_called_once_with(
            "internal/deployment/test-deployment/ingest", {"data": "dummy_data"}, max_size=1000
        )


def test_atlas_connector_update_available_endpoints(atlas_connector):
    with mock.patch.object(atlas_connector.connector, "set") as mock_set:
        atlas_connector.update_available_endpoints()
        mock_set.assert_called_once()
        msg = mock_set.mock_calls[0][1][1]
        assert isinstance(msg, messages.AvailableResourceMessage)
        assert "device_readback" in msg.resource
