import threading
from threading import Thread
from unittest.mock import patch

import pytest
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ActorStartRequestMessage, RawMessage
from bec_lib.redis_connector import MessageObject, RedisConnector
from fakeredis import TcpFakeServer

from bec_server.actors.manager import ActorManager
from bec_server.test.actor_test_utils import ep, sub_ep
from bec_server.test.helpers import wait_until


@pytest.fixture(autouse=True)
def threads_check(fakeredis_config):
    threads_at_start = set(th for th in threading.enumerate() if th is not threading.main_thread())
    yield
    threads_after = set(
        th
        for th in threading.enumerate()
        # ignore the server threads that fakeredis makes, but don't ignore worker/manager threads
        # there is a bug in fakeredis where daemon threads are not actually cleaned up until the
        # end of the process.
        if th is not threading.main_thread() and "process_request_thread" not in th.name
    )
    additional_threads = threads_after - threads_at_start
    assert len(additional_threads) == 0, (
        f"Test creates {len(additional_threads)} threads that are not cleaned: {additional_threads}"
    )


@pytest.fixture
def fakeredis_config():
    redis_config = "localhost", 44556
    server = TcpFakeServer(redis_config, server_type="redis")
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
        patch.object(manager, "_validate_request", side_effect=lambda x: x["request"]),
        patch.object(manager, "spawn"),
    ):
        conn.xadd(
            MessageEndpoints.actor_start_request(),
            {"request": ActorStartRequestMessage(actor_module="test", actor_class_name="Test")},
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

    conn.register(ep, cb=action_callback)
    manager._process_queue_request(
        msg=ActorStartRequestMessage(
            actor_module="bec_server.test.actor_test_utils", actor_class_name="PollingTestActor"
        )
    )
    wait_until(lambda: manager._active_workers != {})
    wait_until(lambda: action_triggered, timeout_s=10)


def test_subscription_actor(actor_manager_and_conn: tuple[ActorManager, RedisConnector]):
    manager, conn = actor_manager_and_conn
    action_triggered = False

    def action_callback(msg: MessageObject):
        nonlocal action_triggered
        if msg.value.data == {"test": "result"}:
            action_triggered = True

    conn.register(ep, cb=action_callback)
    manager._process_queue_request(
        msg=ActorStartRequestMessage(
            actor_module="bec_server.test.actor_test_utils",
            actor_class_name="SubscriptionTestActor",
        )
    )
    wait_until(lambda: manager._active_workers != {})
    conn.set_and_publish(sub_ep, RawMessage(data=None))
    wait_until(lambda: action_triggered, timeout_s=5)
