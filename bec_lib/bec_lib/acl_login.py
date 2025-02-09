from getpass import getpass

import requests
from rich.console import Console
from rich.table import Table

from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector


class BECAuthenticationError(Exception):
    pass


class BECAccess:
    def __init__(self, connector: RedisConnector):
        self.connector = connector

    def _get_available_accounts(self):
        # return self.connector.get(MessageEndpoints.available_accounts())
        return ["wakonig_k", "p12345", "account3"]

    def login(self):
        console = Console()

        accounts = self._get_available_accounts()

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

        username = input("Enter your PSI username: ").strip()
        password = getpass("Enter your PSI password (hidden): ")

        out = requests.post(
            "http://localhost/api/v1/user/login",
            json={"username": username, "password": password},
            timeout=5,
        )
        if out.status_code != 200:
            out.raise_for_status()
            return
        jwt_token = out.json()
        out = requests.get(
            "http://localhost/api/v1/bec_access",
            params={"deployment_id": "678aa8d4875568640bd92176", "user": selected_account},
            headers={"Authorization": f"Bearer {jwt_token}"},
            timeout=5,
        )
        console.print(
            f"[blue]Login request for account:[/blue] {selected_account} with user: {username}"
        )
        if out.status_code != 200:
            out.raise_for_status()
            return
        console.print(out.json())


if __name__ == "__main__":
    access = BECAccess()
    access.login()
