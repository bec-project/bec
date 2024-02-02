from unittest import mock

from test_scibec_connector import SciHubMock

from scihub.scilog import SciLogConnector


def test_scilog_connector(SciHubMock):
    scilog = SciLogConnector(SciHubMock, SciHubMock.connector)
    scilog.shutdown()


def test_scilog_connector_with_env(SciHubMock):
    with mock.patch("scihub.scilog.scilog.dotenv_values") as mock_dotenv_values:
        mock_dotenv_values.return_value = {
            "SCILOG_DEFAULT_HOST": "https://dummy_host",
            "SCILOG_USER": "dummy_user",
            "SCILOG_USER_SECRET": "dummy_user_secret",
        }
        SciHubMock.config.service_config = {"scilog": {"env_file": "dummy_env_file"}}
        with mock.patch("scihub.scilog.scilog.requests") as mock_requests:
            mock_requests.post.return_value.ok = True
            mock_requests.post.return_value.json.return_value = {"token": "dummy_token"}
            scilog = SciLogConnector(SciHubMock, SciHubMock.connector)
            assert scilog.host == "https://dummy_host"
            assert scilog.user == "dummy_user"
            assert scilog.user_secret == "dummy_user_secret"
            scilog.shutdown()
            mock_dotenv_values.assert_called_once()