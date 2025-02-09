from __future__ import annotations

from getpass import getpass
from typing import TYPE_CHECKING

import requests
from rich.console import Console
from rich.table import Table

from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector

if TYPE_CHECKING:  # pragma: no cover
    from bec_lib import messages


class BECAuthenticationError(Exception):
    """
    Exception raised when the authentication process fails.
    """


class BECAccess:
    """
    This class provides a way to authenticate with the BEC instance using the ACL system
    """

    def __init__(self, connector: RedisConnector):
        self.connector = connector
        self._info = None
        self._atlas_login = False

    def initialize(self):
        """
        Initialize the access control system.
        """
        self.connector.authenticate("bec", "bec")
        self._info: messages.LoginInfoMessage | None = self.connector.get(
            MessageEndpoints.login_info()
        )
        self._atlas_login = self._info.atlas_login

    def login(self, name: str | None = None):
        """
        Start the login process for the BEC instance.

        Args:
            name: The name of the account to login with. If not provided, the user will be prompted to select an account.
        """
        if self._info is None:
            raise BECAuthenticationError("Login information not available. Unable to login.")

        console = Console()

        if name is None:
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
        else:
            selected_account = name

        if self._atlas_login:
            token = self._psi_login(selected_account)
        else:
            token = self._default_login(selected_account)

        self.connector.authenticate(token, selected_account)

    def _psi_login(self, selected_account: str) -> str:
        """
        Login using the Atlas system.
        """
        username = input("Enter your PSI username: ").strip()
        password = getpass("Enter your PSI password (hidden): ")

        out = requests.post(
            self._info.host + "/api/v1/login",
            json={"username": username, "password": password},
            timeout=5,
        )
        out.raise_for_status()

        jwt_token = out.json()
        out = requests.get(
            self._info.host + "/api/v1/bec_access",
            params={"deployment_id": self._info.deployment, "user": selected_account},
            headers={"Authorization": f"Bearer {jwt_token}"},
            timeout=5,
        )

        if out.status_code != 200:
            out.raise_for_status()
        token = out.json()
        return token

    def _default_login(self, selected_account: str) -> str:
        """
        Login using the default login system.
        """
        password = getpass(f"Enter the token for {selected_account} (hidden): ")
        return password

    def login_with_token(self, username: str, token: str | None):
        """
        Login with a username and token.
        """
        self.connector.authenticate(token, username)
