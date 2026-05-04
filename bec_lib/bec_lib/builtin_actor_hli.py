from typing import TYPE_CHECKING

from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import BuiltinActorStateChangeMessage

if TYPE_CHECKING:
    from bec_lib.client import BECClient

VAR_PREFIX = "_BuiltinActors"


def builtin_actor_enabled_var(actor_name: str):
    return f"{VAR_PREFIX}/enabled/{actor_name}"


class BuiltinActorHli:
    def __init__(self, client: "BECClient") -> None:
        self._client = client

    def _notify(self, actor_name):
        self._client.connector.send(
            MessageEndpoints.builtin_actor_notification(),
            BuiltinActorStateChangeMessage(actor_name=actor_name),
        )

    def check_enabled(self, actor_name: str):
        return bool(self._client.get_global_var(builtin_actor_enabled_var(actor_name)))

    def set_enabled(self, actor_name: str):
        self._client.set_global_var(builtin_actor_enabled_var(actor_name), True)
        self._notify(actor_name)

    def set_disabled(self, actor_name: str):
        self._client.set_global_var(builtin_actor_enabled_var(actor_name), False)
        self._notify(actor_name)
