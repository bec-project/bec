from __future__ import annotations

import os
from functools import wraps
from getpass import getpass
from typing import TYPE_CHECKING, cast

import requests
from dotenv import dotenv_values
from rich.console import Console
from rich.table import Table

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_lib.utils.import_utils import lazy_import_from

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib.messages import LoginInfoMessage
else:
    LoginInfoMessage = lazy_import_from("bec_lib.messages", "LoginInfoMessage")


logger = bec_logger.logger


class BECAuthenticationError(Exception):
    """
    Exception raised when the authentication process fails.
    """


def login_info_available(func):
    """
    Decorator to check if login information is available before calling the function.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # pylint: disable=protected-access
        if self._info is None:
            raise BECAuthenticationError(
                "Unable to login: Missing login information.\n"
                "The login information is not available, likely because the system is not set up for ACLs. "
                "Try to restart the server. If the error persists, contact the BEC team.\n"
            )
        return func(self, *args, **kwargs)

    return wrapper


class BECAccess:
    """
    This class provides a way to authenticate with the BEC instance using the ACL system
    """

    def __init__(self, connector: RedisConnector):
        self.connector = connector
        self._info: LoginInfoMessage | None = None
        self._atlas_login = False

    @login_info_available
    def login(self, name: str | None = None):
        """
        Start the login process for the BEC instance.

        Args:
            name: The name of the account to login with. If not provided, the user will be prompted to select an account.
        """
        console = Console()

        if name is None:
            selected_account = self._ask_user_for_account(console)
        else:
            selected_account = name

        if self._atlas_login:
            token = self._psi_login(selected_account)
        else:
            token = self._local_login(selected_account)

        self.connector.authenticate(username=selected_account, password=token)

    def login_with_token(self, *, username: str, token: str | None):
        """
        Login with a username and token.
        """
        self.connector.authenticate(username=username, password=token)

    def _bec_service_login(
        self, prompt_for_acl: bool = False, acl_config: dict | str | None = None
    ) -> None:
        """
        Login to Redis using the ACL system. This is the main entry point for the login process, started
        by the BECService.

        Args:
            prompt_for_acl (bool): If True, prompt the user to login using ACL. This is typically only used
                for user-facing services. Default is False.
            acl_config (dict or str): The ACL configuration. If a string is provided, it will be treated as a
                path to the configuration file. If a dictionary is provided, it should contain the username and
                password for the account.
        """

        if not self.connector.redis_server_is_running():
            logger.warning("Redis server is not running.")
            return

        # Check if we have the login specified in the configuration file
        if acl_config and self._config_login_successful(prompt_for_acl, acl_config):
            return

        # Check if we can simply use the default user account
        if self._default_user_login_successful(full_access=True):
            return

        if prompt_for_acl:
            # If the user is launching the service, prompt them to login
            self._user_service_login()
            return

        raise BECAuthenticationError("Could not connect to Redis.")

    @login_info_available
    def _ask_user_for_account(self, console: Console) -> str:
        """
        Ask the user to select an account to login with.

        Args:
            console (Console): The console object to use for printing

        Returns:
            str: The account name
        """
        # Note: The decorator will check if the login information is available and raise an error if not.
        self._info = cast(LoginInfoMessage, self._info)
        accounts = self._info.available_accounts

        console.print(
            "\n\n[blue]The BEC instance you are trying to connect to enforces access control. \nPlease follow the instructions below to gain access for a particular user or user group:[/blue]\n\n"
        )
        table = Table(title="Available Accounts")
        table.add_column("Number", justify="center", style="cyan", no_wrap=True)
        table.add_column("Account Name", justify="left", style="magenta")

        for i, account in enumerate(accounts, 1):
            table.add_row(str(i), account)

        console.print(table)

        selected_account = None
        while selected_account is None:
            user_input = input("Select an account (enter the number or full name): ").strip()
            if user_input.isdigit() and 1 <= int(user_input) <= len(accounts):
                selected_account = accounts[int(user_input) - 1]
            elif user_input in accounts:
                selected_account = user_input
            else:
                console.print("[red]Invalid selection. Please try again.[/red]")

        console.print(f"[green]You selected:[/green] {selected_account}\n")
        return selected_account

    @login_info_available
    def _psi_login(self, selected_account: str) -> str:
        """
        Login using the Atlas system.

        Args:
            selected_account (str): The account to login with.
        Returns:
            str: The token for the account
        """

        # Note: The decorator will check if the login information is available and raise an error if not.
        self._info = cast(LoginInfoMessage, self._info)

        if not self._info.host.startswith("https://") and not self._info.host.startswith("http://"):
            raise BECAuthenticationError(
                f"The host is not a valid URL. Please check the configuration. Host: {self._info.host}"
            )

        username = input("Enter your PSI username: ").strip()
        password = getpass("Enter your PSI password (hidden): ")

        try:
            out = requests.post(
                self._info.host + "/api/v1/user/login",
                json={"username": username, "password": password},
                timeout=15,
            )
        except requests.exceptions.Timeout as exc:
            raise BECAuthenticationError(
                f"Timeout error while trying to connect to the host: {self._info.host}. Please check your connection. Error: {exc}"
            ) from exc
        except Exception as exc:
            raise BECAuthenticationError(
                f"An unexpected error occurred while trying to connect to the host: {self._info.host}. Please check your connection. Error: {exc}"
            ) from exc

        if out.status_code != 200:
            match out.status_code:
                case 401:
                    raise BECAuthenticationError(
                        "Invalid username or password. Please check your credentials."
                    )
                case 404:
                    raise BECAuthenticationError(
                        f"The host is not a valid URL. Please check the configuration. Host: {self._info.host}"
                    )
                case _:
                    raise BECAuthenticationError(
                        f"An error occurred while logging in. Status code: {out.status_code}"
                    )

        jwt_token = out.json()
        out = requests.get(
            self._info.host + "/api/v1/bec_access",
            params={"deployment_id": self._info.deployment, "user": selected_account},
            headers={"Authorization": f"Bearer {jwt_token}"},
            timeout=15,
        )
        if out.status_code != 200:
            match out.status_code:
                case 404:
                    raise BECAuthenticationError(
                        "The selected account does not exist or is not available to the specified user."
                    )
                case _:
                    raise BECAuthenticationError(
                        f"An error occurred while logging in. Status code: {out.status_code}"
                    )

        token = out.json()
        return token

    def _local_login(self, selected_account: str) -> str:
        """
        Login using the local system, i.e. without the Atlas system.
        """
        password = getpass(f"Enter the token for {selected_account} (hidden): ")
        return password

    def _config_login_successful(self, prompt_for_acl: bool, acl_config: dict | str) -> bool:
        """
        Login to Redis using the configuration file.

        Args:
            prompt_for_acl (bool): If True, prompt the user to login using ACL. Default is False.
            acl_config (dict or str): The ACL configuration. If a string is provided, it will be treated as a path to the configuration file.

        Returns:
            bool: True if the login was successful, False otherwise.
        """

        if isinstance(acl_config, str):
            if os.path.exists(acl_config) and not prompt_for_acl:
                # Load the account information from the .env file
                # This is relevant for the BEC services that are not launched by the user
                # but are auto-deployed.
                account = dotenv_values(acl_config)
                user = account.get("REDIS_USER")
                password = account.get("REDIS_PASSWORD")
                if self._check_redis_auth(user, password):
                    return True
        elif isinstance(acl_config, dict):
            username = acl_config.get("username")
            password = acl_config.get("password")
            if self._check_redis_auth(username, password):
                return True
            if not password and prompt_for_acl:
                self._user_service_login(username=username)
                return True
        else:
            raise ValueError(
                "Invalid value for 'acl' in the service config. Must be a dict or a path to a .env file."
            )
        return False

    def _default_user_login_successful(self, full_access: bool) -> bool:
        """
        Login using the default user account.

        Args:
            full_access (bool): If True, the user has to have full access to the Redis

        Returns:
            bool: True if the login was successful, False otherwise.
        """
        if self._check_redis_auth(None, None):
            if not full_access:
                return True

            try:
                self.connector.get(MessageEndpoints.acl_accounts())
                # ACLs are enabled but we have full access. No need to login.
                return True
            # pylint: disable=broad-except
            except Exception:
                pass
        return False

    def _update_login_info(self) -> None:
        """
        Update the login information.
        """
        self._info = self.connector.get(MessageEndpoints.login_info())

    def _user_service_login(self, username: str | None = None) -> None:
        """
        Login using the specified username.
        If no username is provided, the user will be prompted to enter their username.

        Args:
            username (str): The username to login with. If not provided, the user will be prompted to enter their username.
        """
        self._update_login_info()
        if not self._info:
            raise BECAuthenticationError("Login information not available. Unable to login.")
        self._atlas_login = self._info.atlas_login
        if not username and not self._info.atlas_login:
            return self.login_with_token(username="user", token=None)
        return self.login(username)

    def _check_redis_auth(self, user: str | None, password: str | None) -> bool:
        """
        Check if the user and password are correct.

        Args:
            user (str): The username
            password (str): The password

        Returns:
            bool: True if the user and password are correct, False otherwise
        """
        try:
            if user:
                self.connector.authenticate(username=user, password=password)
            return self.connector.can_connect()
        # pylint: disable=broad-except
        except Exception:
            return False
