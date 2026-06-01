from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import (
    BuiltinActorStateUpdatedNotification,
    ScanInterlockModifyStateTableMessage,
    ScanInterlockStateTableContent,
)
from bec_lib.messaging_hooks import MessagingEvent
from bec_lib.messaging_services import NotificationMessageObject
from bec_server.actors.actor import BlStateActor

logger = bec_logger.logger


class ScanInterlockActor(BlStateActor):
    """Sets a scan lock on the primary queue if any of the the states in the state_table don't match
    the required value. Removes the lock if all of them match."""

    def __init__(self, client: BECClient, name: str, exec_id: str):
        self._LOCK_ID = "ScanInterlockActor"
        states_msg: ScanInterlockStateTableContent | None = client.connector.get(
            MessageEndpoints.scan_interlock_states()
        )
        if states_msg is not None:
            self.state_table = states_msg.states_watched
        else:
            self.state_table = {}

        super().__init__(client, name, exec_id)

    def _ping_clients(self):
        logger.debug(f"{self.name} pinging clients that it was updated")
        self.client.connector.send(
            MessageEndpoints.builtin_actor_update_notif(self.name),
            BuiltinActorStateUpdatedNotification(actor_name=self.name),
        )

    def _update_watched_states_in_redis(self):
        self.client.connector.set(
            MessageEndpoints.scan_interlock_states(),
            ScanInterlockStateTableContent(states_watched=self.state_table),
        )
        self._ping_clients()

    def _on_state_modification(self, msg: ScanInterlockModifyStateTableMessage):
        with self.state_table_lock:
            if msg.action == "add":
                logger.info(f"Adding {msg.state_name} to the scan interlock actor")
                if self.client.beamline_states.get_status_by_name(msg.state_name) is None:
                    logger.warning(f"Beamline state {msg.state_name} doesn't exist - not adding.")
                    return
                if msg.state_name not in self.state_table:
                    self.client.connector.register(
                        MessageEndpoints.beamline_state(msg.state_name), cb=self.evaluate
                    )
                self.state_table[msg.state_name] = msg.status  # type: ignore # msg is validated
            elif msg.action == "remove_all":
                for state in self.state_table:
                    self.client.connector.unregister(MessageEndpoints.beamline_state(state))
                self.state_table = {}
                self.state_cache = {}
            else:
                logger.info(f"Removing {msg.state_name} from the scan interlock actor")
                if msg.state_name in self.state_table:
                    self.client.connector.unregister(
                        MessageEndpoints.beamline_state(msg.state_name)
                    )
                    del self.state_table[msg.state_name]
                    del self.state_cache[msg.state_name]
            self._update_cache()
            self._update_watched_states_in_redis()
        super(BlStateActor, self).evaluate()

    @property
    def mismatched_states(self):
        """A list of all the states which are out of spec"""
        with self.state_table_lock:
            return [
                state_name
                for state_name, expected_state in self.state_table.items()
                if (current_state := self.state_cache.get(state_name)) is not None
                and current_state != expected_state
            ]

    def some_mismatch_action(self, client: BECClient):
        notification = NotificationMessageObject()
        notification.add_text(
            f"Scan interlock triggered for beamline states: {self.mismatched_states}", color="red"
        )
        notification.add_tags("scan_interlock")
        self.client.connector.notify(MessagingEvent.SCAN_INTERLOCK, notification)

        if self.client.queue is None:
            return
        logger.info(
            f"{self.name} placing queue lock due to mismatched states: "
            f"{self.mismatched_states}; cache={self.state_cache}; table={self.state_table}"
        )
        self.client.queue.add_queue_lock(
            queue="primary",
            reason=f"Interlock for beamline states: {self.mismatched_states}",
            lock_id=self._LOCK_ID,
        )
        self.client.queue.request_scan_restart()

    def all_match_action(self, client: BECClient):
        self._unlock()

    def _unlock(self):
        if self.client.queue is None:
            return
        if (q := self.client.queue) is not None:
            if (curr_q := q.queue_storage.current_scan_queue) is not None:
                if (primary := curr_q.get("primary")) is not None and primary.locks != []:
                    logger.info(
                        f"{self.name} removing queue lock if present; "
                        f"queue_locks={primary.locks}; cache={self.state_cache}; table={self.state_table}"
                    )
                    notification = NotificationMessageObject()
                    notification.add_text("Scan interlock cleared", color="green")
                    notification.add_tags("scan_interlock")
                    self.client.connector.notify(MessagingEvent.SCAN_INTERLOCK, notification)
                    self.client.queue.remove_queue_lock(queue="primary", lock_id=self._LOCK_ID)

    def run(self):
        super().run()
        self._unlock()

    def stop(self, *_):
        self._unlock()
        super().stop()
