import sys
from typing import Literal

from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector


# pylint: disable=protected-access
class BECAccessDemo:  # pragma: no cover
    def __init__(self, connector: RedisConnector | None = None):
        if connector:
            self.connector = connector
        else:
            self.connector = RedisConnector("localhost:6379")
        self.connector.authenticate(**self._find_admin_account())
        self.username = "user"
        self.admin_username = "admin"
        self.deployment_id = "test_deployment"

    def _find_admin_account(self) -> dict[str, str]:
        for user, token in [("default", "null"), ("admin", "admin")]:
            try:
                self.connector.authenticate(username=user, password=token)
                return {"username": user, "password": token}
            except Exception:
                pass
        raise RuntimeError("No admin account found. Please restart the Redis server.")

    def add_user(self):
        available_user = self.connector._redis_conn.acl_list()
        if f"user {self.username}" in " ".join(available_user):
            self.connector._redis_conn.acl_deluser(self.username)
        self.connector._redis_conn.acl_setuser(
            self.username,
            enabled=True,
            nopass=True,
            categories=["+@all", "-@dangerous"],
            keys=[
                "%R~public/*",  # Read-only access
                "%R~info/*",  # Read-only access
                f"%RW~personal/{self.username}/*",  # Read/Write access
                "%RW~user/*",  # Read/Write access
            ],
            channels=[
                "public/*",
                "info/*",
                f"personal/{self.username}/*",
                "user/*",
                MessageEndpoints.public_file("*", "*").endpoint,
                # MessageEndpoints.device_read("*").endpoint, # probably not even needed
            ],
            commands=["+keys"],
            reset_channels=True,
            reset_keys=True,
        )

    def add_account(self, name: str, password: str | None, level: Literal["admin", "user"]):
        available_user = self.connector._redis_conn.acl_list()
        if f"user {name}" in " ".join(available_user):
            self.connector._redis_conn.acl_deluser(name)
        if level == "admin":
            config = {
                "enabled": True,
                "categories": ["+@all"],
                "keys": ["*"],
                "channels": ["*"],
                "reset_channels": True,
                "reset_keys": True,
            }
        else:
            config = {
                "enabled": True,
                "nopass": True,
                "categories": ["+@all", "-@dangerous"],
                "keys": [
                    "%R~public/*",  # Read-only access
                    "%R~info/*",  # Read-only access
                    f"%RW~personal/{name}/*",  # Read/Write access
                    "%RW~user/*",  # Read/Write access
                ],
                "channels": [
                    "public/*",
                    "info/*",
                    f"personal/{name}/*",
                    "user/*",
                    MessageEndpoints.public_file("*", "*").endpoint,
                    # MessageEndpoints.device_read("*").endpoint, # probably not even needed
                ],
                "commands": ["+keys"],
                "reset_channels": True,
                "reset_keys": True,
            }
        if password:
            config["passwords"] = [f"+{password}"]
        else:
            config["nopass"] = True

        self.connector._redis_conn.acl_setuser(name, **config)

    def add_admin(self):
        available_user = self.connector._redis_conn.acl_list()
        if f"user {self.admin_username}" in " ".join(available_user):
            self.connector._redis_conn.acl_deluser(self.admin_username)
        self.connector._redis_conn.acl_setuser(
            self.admin_username,
            enabled=True,
            passwords=["+admin"],
            categories=["+@all"],
            keys=["*"],
            channels=["*"],
            reset_channels=True,
            reset_keys=True,
        )

    def reset(self):
        try:
            self.connector.authenticate(username="admin", password="admin")
        # pylint: disable=broad-except
        except Exception:
            pass

        self.set_default_limited(False)
        self.connector._redis_conn.reset()
        available_user = self.connector._redis_conn.acl_list()
        for user in ["user", "admin", "bec"]:
            if f"user {user}" in " ".join(available_user):
                self.connector._redis_conn.acl_deluser(user)

    def set_default_limited(self, limited: bool):
        if limited:
            self.connector._redis_conn.acl_setuser(
                "default",
                enabled=True,
                nopass=True,
                categories=["+read"],
                keys=["public/*"],
                reset_channels=True,
                reset_keys=True,
            )
        else:
            self.connector._redis_conn.acl_setuser(
                "default",
                enabled=True,
                nopass=True,
                categories=["+@all"],
                keys=["*"],
                channels=["*"],
                reset_channels=True,
                reset_keys=True,
            )


def _main(
    mode: str, connector: RedisConnector | None = None, shutdown: bool = True
):  # pragma: no cover
    demo = BECAccessDemo(connector=connector)

    match mode:
        case "default":
            demo.reset()
            print("Enabled mode default. Please restart the bec server.")
        case "admin":
            demo.reset()
            demo.add_user()
            demo.add_admin()
            demo.set_default_limited(True)
            print(
                "Enabled mode admin. Please make sure to place the .bec_acl.env file in the root directory and restart the bec server."
            )
        case _:
            raise ValueError(f"Invalid mode: {mode}")

    if shutdown:
        demo.connector.shutdown()
        sys.exit(0)


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reset the user ACLs")
    parser.add_argument("--mode", type=str, help="Mode to run the script")

    args = parser.parse_args()

    if args.mode is None:
        args.mode = "default"

    _main("default" if args.reset else args.mode)
