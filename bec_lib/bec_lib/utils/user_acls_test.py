import sys

from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector


# pylint: disable=protected-access
class BECAccessDemo:  # pragma: no cover
    def __init__(self, connector: RedisConnector | None = None):
        if connector:
            self.connector = connector
        else:
            self.connector = RedisConnector("localhost:6379")
        self.connector.authenticate(*self._find_admin_account())
        self.username = "user"
        self.admin_username = "admin"
        self.deployment_id = "test_deployment"

    def _find_admin_account(self):
        for user, token in [("default", None), ("bec", "bec"), ("admin", "admin")]:
            try:
                self.connector.authenticate(token, user)
                return token, user
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

    def add_bec_limited(self):
        available_user = self.connector._redis_conn.acl_list()
        if "user bec" in " ".join(available_user):
            self.connector._redis_conn.acl_deluser("bec")
        self.connector._redis_conn.acl_setuser(
            "bec",
            enabled=True,
            passwords=["+bec"],
            categories=["+read"],
            keys=["public/*"],
            channels=[""],
            reset_channels=True,
            reset_keys=True,
        )

    def add_bec(self):
        available_user = self.connector._redis_conn.acl_list()
        if "user bec" in " ".join(available_user):
            self.connector._redis_conn.acl_deluser("bec")
        self.connector._redis_conn.acl_setuser(
            "bec",
            enabled=True,
            passwords=["+bec"],
            categories=["+@all"],
            keys=["*"],
            channels=["*"],
            reset_channels=True,
            reset_keys=True,
        )

    def reset(self):
        try:
            self.connector.authenticate("admin", "admin")
        # pylint: disable=broad-except
        except Exception:
            pass

        self.enable_default(True)
        self.connector._redis_conn.reset()
        available_user = self.connector._redis_conn.acl_list()
        for user in ["user", "admin", "bec"]:
            if f"user {user}" in " ".join(available_user):
                self.connector._redis_conn.acl_deluser(user)

    def enable_default(self, enable: bool):
        self.connector._redis_conn.acl_setuser(
            "default",
            enabled=enable,
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
        case "bec":
            demo.reset()
            demo.add_bec()
            demo.enable_default(False)
            print("Enabled mode bec. Please restart the bec server.")
        case "admin":
            demo.reset()
            demo.add_user()
            demo.add_admin()
            demo.add_bec_limited()
            demo.enable_default(False)
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
