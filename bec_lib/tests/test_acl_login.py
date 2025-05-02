from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.acl_login import BECAccess, BECAuthenticationError
from bec_lib.utils.user_acls_test import BECAccessDemo

# pylint: disable=protected-access


@pytest.fixture
def bec_access(connected_connector):
    return BECAccess(connected_connector)


def _login_info(accounts: list[str] | None = None):
    return messages.LoginInfoMessage(
        host="", deployment="", atlas_login=False, available_accounts=accounts if accounts else []
    )


@pytest.fixture
def access_control(bec_access):
    # patch for fakeredis; remove once v2.28 is released
    bec_access.connector._redis_conn.acl_setuser(
        "default",
        enabled=True,
        nopass=True,
        categories=["+@all"],
        keys=["*"],
        channels=["*"],
        reset_channels=True,
        reset_keys=True,
    )
    bec_access.connector._redis_conn.acl_setuser(
        "admin",
        enabled=True,
        passwords=["+admin"],
        categories=["+@all"],
        keys=["*"],
        channels=["*"],
        reset_channels=True,
        reset_keys=True,
    )
    handler = BECAccessDemo(bec_access.connector)
    handler.add_account("default", "null", "admin")
    return handler


def test_login(bec_access, access_control):
    bec_access._info = _login_info()
    access_control.add_account("admin", "admin", "admin")

    with mock.patch.object(bec_access, "_local_login", return_value="admin"):
        bec_access.login("admin")
        conn = bec_access.connector._redis_conn.connection_pool.connection_kwargs
        assert conn["username"] == "admin"
        assert conn["password"] == "admin"


def test_login_raises_with_no_login_info(bec_access):
    with pytest.raises(BECAuthenticationError):
        bec_access.login("bec")


def test_login_prompts_user_for_account(bec_access, access_control):
    bec_access._info = _login_info()
    access_control.add_account("admin", "admin", "admin")

    with mock.patch.object(bec_access, "_ask_user_for_account", return_value="admin"):
        with mock.patch.object(bec_access, "_local_login", return_value="admin"):
            bec_access.login()
            conn = bec_access.connector._redis_conn.connection_pool.connection_kwargs
            assert conn["username"] == "admin"
            assert conn["password"] == "admin"


def test_login_psi_login(bec_access, access_control):
    bec_access._info = _login_info()
    bec_access._atlas_login = True
    access_control.add_account("admin", "admin", "admin")

    with mock.patch.object(bec_access, "_local_login") as local_login:
        with mock.patch.object(bec_access, "_psi_login", return_value="admin") as psi_login:

            bec_access.login("admin")
            conn = bec_access.connector._redis_conn.connection_pool.connection_kwargs
            assert conn["username"] == "admin"
            assert conn["password"] == "admin"
            psi_login.assert_called_once()
            local_login.assert_not_called()


def test_bec_service_login_default(bec_access):
    bec_access._info = _login_info()

    bec_access._bec_service_login()
    conn = bec_access.connector._redis_conn.connection_pool.connection_kwargs
    assert conn["username"] is None
    assert conn["password"] is None


@mock.patch("bec_lib.acl_login.input")
@mock.patch("bec_lib.acl_login.Console")
def test_ask_user_for_account_number_input(mock_console, mock_input, bec_access):
    """Test _ask_user_for_account with numeric input."""
    console_instance = mock.MagicMock()
    mock_console.return_value = console_instance
    mock_input.return_value = "2"

    bec_access._info = _login_info(["user1", "user2", "admin"])

    result = bec_access._ask_user_for_account(console_instance)

    assert result == "user2"
    assert mock_input.call_count == 1
    # Verify console output was called with expected arguments
    assert any(
        "You selected" in str(call) and "user2" in str(call)
        for call in console_instance.print.call_args_list
    )


@mock.patch("bec_lib.acl_login.input")
@mock.patch("bec_lib.acl_login.Console")
def test_ask_user_for_account_name_input(mock_console, mock_input, bec_access):
    """Test _ask_user_for_account with name input."""
    console_instance = mock.MagicMock()
    mock_console.return_value = console_instance
    mock_input.return_value = "admin"

    bec_access._info = _login_info(["user1", "user2", "admin"])

    result = bec_access._ask_user_for_account(console_instance)

    assert result == "admin"
    assert mock_input.call_count == 1


@mock.patch("bec_lib.acl_login.input")
@mock.patch("bec_lib.acl_login.Console")
def test_ask_user_for_account_invalid_input(mock_console, mock_input, bec_access):
    """Test _ask_user_for_account with invalid input followed by valid input."""
    console_instance = mock.MagicMock()
    mock_console.return_value = console_instance
    mock_input.side_effect = ["invalid", "1"]

    bec_access._info = _login_info(["user1", "user2", "admin"])

    result = bec_access._ask_user_for_account(console_instance)

    assert result == "user1"
    assert mock_input.call_count == 2
    # Verify error message was printed
    assert any("Invalid selection" in str(call) for call in console_instance.print.call_args_list)


@mock.patch("bec_lib.acl_login.getpass")
def test_local_login(mock_getpass, bec_access):
    """Test _local_login method."""
    mock_getpass.return_value = "test_password"

    result = bec_access._local_login("test_user")

    assert result == "test_password"
    mock_getpass.assert_called_once_with("Enter the token for test_user (hidden): ")


@mock.patch("bec_lib.acl_login.input")
@mock.patch("bec_lib.acl_login.getpass")
@mock.patch("bec_lib.acl_login.requests.post")
@mock.patch("bec_lib.acl_login.requests.get")
def test_psi_login_success(mock_get, mock_post, mock_getpass, mock_input, bec_access):
    """Test successful _psi_login."""
    mock_input.return_value = "psi_user"
    mock_getpass.return_value = "psi_password"

    # Mock post response
    post_response = mock.MagicMock()
    post_response.json.return_value = "jwt_token"
    post_response.status_code = 200
    mock_post.return_value = post_response

    # Mock get response
    get_response = mock.MagicMock()
    get_response.status_code = 200
    get_response.json.return_value = "access_token"
    mock_get.return_value = get_response

    # Set up info attributes
    bec_access._info = mock.MagicMock()
    bec_access._info.host = "https://api.example.com"
    bec_access._info.deployment = "test_deployment"

    result = bec_access._psi_login("test_account")

    assert result == "access_token"
    mock_post.assert_called_once_with(
        "https://api.example.com/api/v1/user/login",
        json={"username": "psi_user", "password": "psi_password"},
        timeout=15,
    )
    mock_get.assert_called_once_with(
        "https://api.example.com/api/v1/bec_access",
        params={"deployment_id": "test_deployment", "user": "test_account"},
        headers={"Authorization": "Bearer jwt_token"},
        timeout=15,
    )


@mock.patch("bec_lib.acl_login.input")
@mock.patch("bec_lib.acl_login.getpass")
@mock.patch("bec_lib.acl_login.requests.post")
@mock.patch("bec_lib.acl_login.requests.get")
def test_psi_login_error_response(mock_get, mock_post, mock_getpass, mock_input, bec_access):
    """Test _psi_login with error response."""
    mock_input.return_value = "psi_user"
    mock_getpass.return_value = "psi_password"

    # Mock post response
    post_response = mock.MagicMock()
    post_response.json.return_value = "jwt_token"
    mock_post.return_value = post_response

    # Mock get response with error
    get_response = mock.MagicMock()
    get_response.status_code = 403
    get_response.raise_for_status = mock.MagicMock(side_effect=Exception("Access denied"))
    mock_get.return_value = get_response

    # Set up info attributes
    bec_access._info = mock.MagicMock()
    bec_access._info.host = "https://api.example.com"
    bec_access._info.deployment = "test_deployment"

    with pytest.raises(BECAuthenticationError) as exc:
        bec_access._psi_login("test_account")

    assert exc.value.args[0].startswith("An error occurred while logging in. Status code") is True


@mock.patch("os.path.exists")
@mock.patch("bec_lib.acl_login.dotenv_values")
def test_config_login_successful_with_env_file(mock_dotenv, mock_exists, bec_access):
    """Test _config_login_successful with environment file."""
    mock_exists.return_value = True
    mock_dotenv.return_value = {"REDIS_USER": "env_user", "REDIS_PASSWORD": "env_pass"}

    with mock.patch.object(bec_access, "_check_redis_auth", return_value=True) as mock_check:
        result = bec_access._config_login_successful(False, "/path/to/.env")

        assert result is True
        mock_check.assert_called_once_with("env_user", "env_pass")


@mock.patch("os.path.exists")
def test_config_login_successful_with_env_file_failure(mock_exists, bec_access):
    """Test _config_login_successful with environment file but auth fails."""
    mock_exists.return_value = True

    with mock.patch(
        "bec_lib.acl_login.dotenv_values",
        return_value={"REDIS_USER": "env_user", "REDIS_PASSWORD": "env_pass"},
    ):
        with mock.patch.object(bec_access, "_check_redis_auth", return_value=False) as mock_check:
            result = bec_access._config_login_successful(False, "/path/to/.env")

            assert result is False
            mock_check.assert_called_once_with("env_user", "env_pass")


def test_config_login_successful_with_dict(bec_access):
    """Test _config_login_successful with dictionary."""
    acl_config = {"username": "dict_user", "password": "dict_pass"}

    with mock.patch.object(bec_access, "_check_redis_auth", return_value=True) as mock_check:
        result = bec_access._config_login_successful(False, acl_config)

        assert result is True
        mock_check.assert_called_once_with("dict_user", "dict_pass")


def test_config_login_successful_with_dict_no_password(bec_access):
    """Test _config_login_successful with dictionary but no password."""
    acl_config = {"username": "dict_user"}

    with mock.patch.object(bec_access, "_check_redis_auth", return_value=False) as mock_check:
        with mock.patch.object(bec_access, "_user_service_login") as mock_user_login:
            result = bec_access._config_login_successful(True, acl_config)

            assert result is True
            mock_check.assert_called_once_with("dict_user", None)
            mock_user_login.assert_called_once_with(username="dict_user")


def test_config_login_successful_invalid_type(bec_access):
    """Test _config_login_successful with invalid acl_config type."""
    with pytest.raises(ValueError, match="Invalid value for 'acl' in the service config"):
        bec_access._config_login_successful(False, 123)


def test_default_user_login_successful_false_access_error(bec_access):
    """Test _default_user_login_successful with connection but access error."""
    with mock.patch.object(bec_access, "_check_redis_auth", return_value=True) as mock_check:
        with mock.patch.object(bec_access.connector, "get", side_effect=Exception("Access denied")):
            result = bec_access._default_user_login_successful(True)
            assert result is False
            mock_check.assert_called_once_with(None, None)


def test_default_user_login_successful_false_no_connection(bec_access):
    """Test _default_user_login_successful with no connection."""
    with mock.patch.object(bec_access, "_check_redis_auth", return_value=False) as mock_check:
        result = bec_access._default_user_login_successful(True)
        assert result is False
        mock_check.assert_called_once_with(None, None)


def test_user_service_login(bec_access):
    """Test _user_service_login."""

    with mock.patch.object(bec_access.connector, "get", return_value=_login_info()):
        with mock.patch.object(bec_access, "login") as mock_login:
            bec_access._user_service_login("test_user")

            mock_login.assert_called_once_with("test_user")
