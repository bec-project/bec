from unittest import mock

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.service_config import ServiceConfigModel
from bec_server.scihub.scilog import SciLogConnector


def test_scilog_connector(SciHubMock):
    scilog = SciLogConnector(SciHubMock, SciHubMock.connector)
    scilog.shutdown()


def test_scilog_connector_with_env(SciHubMock):
    with mock.patch("bec_server.scihub.scilog.scilog.dotenv_values") as mock_dotenv_values:
        mock_dotenv_values.return_value = {
            "SCILOG_DEFAULT_HOST": "https://dummy_host",
            "SCILOG_USER": "dummy_user",
            "SCILOG_USER_SECRET": "dummy_user_secret",
        }
        config = ServiceConfigModel(scilog={"env_file": "dummy_env_file"}).model_dump()
        SciHubMock.config.config = config
        with mock.patch("bec_server.scihub.scilog.scilog.requests") as mock_requests:
            mock_requests.post.return_value.ok = True
            mock_requests.post.return_value.json.return_value = {"token": "dummy_token"}
            scilog = SciLogConnector(SciHubMock, mock.MagicMock())
            assert scilog.host == "https://dummy_host"
            assert scilog.user == "dummy_user"
            assert scilog.user_secret == "dummy_user_secret"
            scilog.connector.set.assert_called_once_with(
                MessageEndpoints.logbook(),
                messages.CredentialsMessage(
                    credentials={
                        "url": "https://dummy_host",
                        "token": "Bearer dummy_token",
                        "user": "dummy_user",
                    }
                ),
            )
            scilog.shutdown()
            mock_dotenv_values.assert_called_once()
