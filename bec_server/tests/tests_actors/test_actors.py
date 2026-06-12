from threading import Thread
from time import sleep
from unittest.mock import MagicMock, patch

import pytest
from fakeredis import TcpFakeServer

from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ActorStartRequestMessage, ProcedureWorkerStatus, RawMessage
from bec_lib.redis_connector import MessageObject, RedisConnector
from bec_server.actors.actor import ActorBase, BlStateActor
from bec_server.actors.manager import ActorManager
from bec_server.actors.worker import actor_procedure
from bec_server.procedures.constants import BecClientType
from bec_server.procedures.oop_worker_base import _create_client
from bec_server.test.actor_test_utils import PollingActor, SubscriptionTestActor, ep, sub_ep
from bec_server.test.helpers import wait_until


@pytest.fixture
def fakeredis_config():
    redis_config = "localhost", 44556
    server = TcpFakeServer(redis_config)
    t = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
    t.start()
    yield redis_config
    server.shutdown()
    server.server_close()
    t.join()


@pytest.fixture
def actor_manager_and_conn(fakeredis_config):
    host, port = fakeredis_config
    redis = f"{host}:{port}"
    manager = ActorManager(redis)
    conn = RedisConnector([redis])
    yield manager, conn
    manager.shutdown()
    conn.shutdown()


def test_validate_and_spawn_called_on_request(
    actor_manager_and_conn: tuple[ActorManager, RedisConnector],
):
    manager, conn = actor_manager_and_conn
    with (
        patch.object(manager, "_validate_request", side_effect=lambda x: x),
        patch.object(manager, "spawn"),
    ):
        conn.send(
            MessageEndpoints.actor_start_request(),
            ActorStartRequestMessage(actor_module="test", actor_class_name="Test"),
        )
        wait_until(lambda: manager._validate_request.call_count == 1)
        wait_until(lambda: manager.spawn.call_count == 1)


def test_polling_actor(actor_manager_and_conn: tuple[ActorManager, RedisConnector]):
    manager, conn = actor_manager_and_conn
    action_triggered = False

    def action_callback(msg: MessageObject):
        nonlocal action_triggered
        if msg.value.data == {"test": "result"}:
            action_triggered = True

    actor_mod = "bec_server.test.actor_test_utils"
    actor_cls = "PollingTestActor"
    conn.register(ep, cb=action_callback)
    manager._process_queue_request(
        MessageObject(
            topic="",
            value=ActorStartRequestMessage(actor_module=actor_mod, actor_class_name=actor_cls),
        )
    )
    wait_until(lambda: manager._active_workers != {})
    wait_until(
        lambda: manager.worker_status(f"{actor_mod}.{actor_cls}") == ProcedureWorkerStatus.RUNNING,
        timeout_s=5,
    )
    wait_until(lambda: action_triggered, timeout_s=2)


def test_subscription_actor(actor_manager_and_conn: tuple[ActorManager, RedisConnector]):
    manager, conn = actor_manager_and_conn
    action_triggered = False

    def action_callback(msg: MessageObject):
        nonlocal action_triggered
        if msg.value.data == {"test": "result"}:
            action_triggered = True

    actor_mod = "bec_server.test.actor_test_utils"
    actor_cls = "SubscriptionTestActor"
    conn.register(ep, cb=action_callback)
    manager._process_queue_request(
        MessageObject(
            topic="",
            value=ActorStartRequestMessage(actor_module=actor_mod, actor_class_name=actor_cls),
        )
    )
    wait_until(lambda: manager._active_workers != {})
    wait_until(
        lambda: manager.worker_status(f"{actor_mod}.{actor_cls}") == ProcedureWorkerStatus.RUNNING,
        timeout_s=5,
    )
    sleep(0.1)
    conn.set_and_publish(sub_ep, RawMessage(data=None))
    wait_until(lambda: action_triggered, timeout_s=1)


def test_subscription_actor_unsubs():
    actor = SubscriptionTestActor(client=MagicMock(), name="test", exec_id="test")
    actor.client.connector.register.assert_called()
    actor.client.connector.unregister.assert_not_called()
    actor.stop()
    actor.client.connector.unregister.assert_called()


def test_polling_actor_inline(fakeredis_config):
    host, port = fakeredis_config
    redis = {"host": host, "port": port}
    client: BECClient = _create_client(BecClientType.BECClient, redis=redis)  # type: ignore
    client.start()
    redis = f"{host}:{port}"
    test_action = MagicMock()

    class PollTestActor(PollingActor):
        action_table = {(lambda *_, **__: True): test_action}

    actor = PollTestActor(client, name="SubActorTest", exec_id="SubActorTest")

    actor_thread = Thread(target=actor.run)
    actor_thread.start()
    try:
        wait_until(lambda: test_action.call_count > 0, timeout_s=10)
    finally:
        actor.stop_event.set()
        actor_thread.join()
        client.shutdown()


def test_actor_procedure_happy_path():
    recorder = MagicMock()

    class DummyActor(ActorBase):
        def __init__(self, *args, **kwargs):
            pass

        def run(self):
            recorder()

    with patch("bec_server.test.actor_test_utils.PollingTestActor", DummyActor):
        actor_procedure("bec_server.test.actor_test_utils", "PollingTestActor", "test", MagicMock())
        recorder.assert_called_once()


def test_actor_procedure_logs_error_missing_class():
    with patch("bec_server.actors.worker.logger") as logger:
        actor_procedure("bec_server.test.actor_test_utils", "DoesntExist", "test", MagicMock())
        assert "does not contain DoesntExist" in logger.error.call_args.args[0]


def test_actor_procedure_logs_error_missing_module():
    with patch("bec_server.actors.worker.logger") as logger:
        actor_procedure("bec_server.test.doesnt_exist", "DoesntExist", "test", MagicMock())
        assert "Module 'bec_server.test.doesnt_exist' not found!" in logger.error.call_args.args[0]


def test_actor_procedure_logs_error_not_actor():
    with patch("bec_server.actors.worker.logger") as logger:
        actor_procedure("bec_server.test.actor_test_utils", "EndpointInfo", "test", MagicMock())
        assert "is not a valid Actor!" in logger.error.call_args.args[0]


class BlStateTestActor(BlStateActor):
    state_table = {"test_state": ["valid"], "test_state_2": ["valid"]}


def test_blstateactor_init_table_and_cache():
    mock_client = MagicMock()

    def get_status_by_name(name: str):
        if name == "test_state":
            return "valid"

    mock_client.beamline_states.get_status_by_name.side_effect = get_status_by_name
    actor = BlStateTestActor(mock_client, "Test", "Test")
    actor.stop_event.set()
    actor.run()

    assert actor.state_table == {"test_state": ["valid"]}
    assert actor.state_cache == {"test_state": "valid"}


def test_bl_state_actor_waits_for_states():
    mock_client = MagicMock()

    mock_client.beamline_states.ready = False
    actor = BlStateTestActor(mock_client, "Test", "Test")
    actor.evaluate = MagicMock()
    with patch("bec_server.actors.actor.logger") as mock_logger:
        t = Thread(target=actor.run)
        t.start()
        sleep(0.1)
        mock_logger.warning.assert_called()
        actor.evaluate.assert_not_called()
        mock_client.beamline_states.ready = True
        sleep(0.2)
        actor.stop_event.set()
        t.join()
    actor.evaluate.assert_called()
