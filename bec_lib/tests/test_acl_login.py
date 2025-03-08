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


def _add_user_bec(connector):
    demo = BECAccessDemo(connector)
    demo.add_bec()


def test_login(bec_access):
    bec_access._info = _login_info()
    _add_user_bec(bec_access.connector)

    with mock.patch.object(bec_access, "_local_login", return_value="bec"):
        bec_access.login("bec")
        conn = bec_access.connector._redis_conn.connection_pool.connection_kwargs
        assert conn["username"] == "bec"
        assert conn["password"] == "bec"


def test_login_raises_with_no_login_info(bec_access):
    with pytest.raises(BECAuthenticationError):
        bec_access.login("bec")


def test_login_prompts_user_for_account(bec_access):
    bec_access._info = _login_info()
    _add_user_bec(bec_access.connector)

    with mock.patch.object(bec_access, "_ask_user_for_account", return_value="bec"):
        with mock.patch.object(bec_access, "_local_login", return_value="bec"):
            bec_access.login()
            conn = bec_access.connector._redis_conn.connection_pool.connection_kwargs
            assert conn["username"] == "bec"
            assert conn["password"] == "bec"


def test_login_psi_login(bec_access):
    bec_access._info = _login_info()
    bec_access._atlas_login = True
    _add_user_bec(bec_access.connector)

    with mock.patch.object(bec_access, "_local_login") as local_login:
        with mock.patch.object(bec_access, "_psi_login", return_value="bec") as psi_login:

            bec_access.login("bec")
            conn = bec_access.connector._redis_conn.connection_pool.connection_kwargs
            assert conn["username"] == "bec"
            assert conn["password"] == "bec"
            psi_login.assert_called_once()
            local_login.assert_not_called()
