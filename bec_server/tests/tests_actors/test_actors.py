import time
from itertools import count
from threading import Thread
from time import sleep
from unittest.mock import MagicMock, patch

import pytest
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ActorStartRequestMessage, ProcedureWorkerStatus, RawMessage
from bec_lib.redis_connector import MessageObject, RedisConnector
from fakeredis import FakeConnection
from fakeredis import TcpFakeServer as _TcpFakeServer
from fakeredis._server import FakeServer
from fakeredis._tcp_server import TCPFakeRequestHandler as _TCPFakeRequestHandler

from bec_server.actors.manager import ActorManager
from bec_server.procedures.constants import BecClientType
from bec_server.procedures.oop_worker_base import _create_client
from bec_server.test.actor_test_utils import PollingActor, SubscriptionActor, ep, sub_ep
from bec_server.test.helpers import wait_until


class TCPFakeRequestHandler(_TCPFakeRequestHandler):
    server: "TcpFakeServer"  # type: ignore

    def handle(self) -> None:
        while True:
            try:
                if self.server._shutdown_requested:
                    break
                if self.current_client.can_read():
                    response = self.current_client.read_response()
                    self.writer.dump(response)
                    continue

                data = self.rfile.readline()
                if data == b"":
                    time.sleep(0)
                else:
                    self.current_client.get_socket().sendall(data)

            except Exception as e:
                self.writer.dump(e)
                break


class TcpFakeServer(_TcpFakeServer):
    def __init__(self, server_address: tuple[str, int]):
        self.allow_reuse_address = True
        self._shutdown_requested = False
        super(_TcpFakeServer, self).__init__(server_address, TCPFakeRequestHandler, True)
        self.fake_server = FakeServer(server_type="redis", version=(7, 4))
        self.client_ids = count(0)
        self.daemon_threads = False
        self.block_on_close = True
        self.clients: dict[int, FakeConnection] = {}

    def shutdown(self) -> None:
        self._shutdown_requested = True
        return super().shutdown()


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

    actor_mod = "bec_server.test.actor_test_utils"
    actor_cls = "PollingTestActor"
    conn.register(ep, cb=action_callback)
    manager._process_queue_request(
        msg=ActorStartRequestMessage(actor_module=actor_mod, actor_class_name=actor_cls)
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
        msg=ActorStartRequestMessage(actor_module=actor_mod, actor_class_name=actor_cls)
    )
    wait_until(lambda: manager._active_workers != {})
    wait_until(
        lambda: manager.worker_status(f"{actor_mod}.{actor_cls}") == ProcedureWorkerStatus.RUNNING,
        timeout_s=5,
    )
    sleep(0.1)
    conn.set_and_publish(sub_ep, RawMessage(data=None))
    wait_until(lambda: action_triggered, timeout_s=1)


def test_subscription_actor_inline(fakeredis_config):
    host, port = fakeredis_config
    redis = {"host": host, "port": port}
    client: BECClient = _create_client(BecClientType.BECClient, redis=redis)  # type: ignore
    client.start()
    redis = f"{host}:{port}"
    conn = RedisConnector([redis])
    test_action = MagicMock()

    class SubTestActor(SubscriptionActor):
        action_table = {(lambda *_, **__: True): test_action}

        def default_monitor_endpoints(self):
            return {sub_ep}

    try:
        actor = SubTestActor(client, name="SubActorTest", exec_id="SubActorTest")
        wait_until(lambda: sub_ep.endpoint in actor.client.connector._topics_cb, timeout_s=0.5)
        for _ in range(20):
            conn.set_and_publish(sub_ep, RawMessage(data=None))
            if test_action.call_count > 0:
                break
            sleep(1)
        assert test_action.call_count > 0
    finally:
        client.shutdown()
        conn.shutdown()


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
