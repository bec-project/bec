from uuid import uuid4

from bec_lib.client import BECClient
from bec_server.actors.actor import BlStateActor


class ScanInterlockActor(BlStateActor):
    """Sets a scan lock on the primary queue if any of the the states in the state_table don't match
    the required value. Removes the lock if all of them match."""

    state_table = {"samx_within_limits": "valid"}

    def __init__(self, client: BECClient, name: str, exec_id: str):
        super().__init__(client, name, exec_id)
        self.lock_id: str | None = None

    def some_mismatch_action(self, client: BECClient):
        if self.client.queue is None or self.lock_id is not None:
            return
        self.lock_id = str(uuid4())
        self.client.queue.add_queue_lock(
            queue="primary", reason="ScanInterlockActor", lock_id=self.lock_id
        )

    def all_match_action(self, client: BECClient):
        self._unlock()

    def _unlock(self):
        if self.client.queue is None or self.lock_id is None:
            return
        try:
            self.client.queue.remove_queue_lock(queue="primary", lock_id=self.lock_id)
        finally:
            self.lock_id = None

    def run(self):
        super().run()
        self._unlock()
