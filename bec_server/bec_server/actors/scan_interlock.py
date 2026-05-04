from bec_lib.client import BECClient
from bec_lib.endpoints import EndpointInfo
from bec_server.actors.actor import SubscriptionActor


def convert_to_endpoint(signal_name: str) -> EndpointInfo: ...


class ScanInterlockActor(SubscriptionActor):
    def __init__(self, client: BECClient, name: str, exec_id: str):
        self._signal_cache = {}
        self.action_table = {self._check_state: self._enable_interlock}
        super().__init__(client, name, exec_id)

    def _check_state(self, *_, **__):
        return True

    def _enable_interlock(self, client: BECClient): ...

    def _update_cache(self, *args, **kwargs): ...

    def add_interlock_signal(self, signal_name, enable_value, disable_value):
        ep = convert_to_endpoint(signal_name)
        self._endpoints.add(ep)
