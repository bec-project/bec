from typing import Callable, Generator

import fakeredis
import pytest

from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.scan_queue import QueueManager

# overwrite threads_check fixture from bec_lib,
# to have it in autouse


@pytest.fixture(autouse=True)
def threads_check(threads_check):
    yield
    bec_logger.logger.remove()


def fake_redis_server(host, port, **kwargs):
    redis = fakeredis.FakeRedis()
    return redis


@pytest.fixture
def connected_connector():
    connector = RedisConnector("localhost:1", redis_cls=fake_redis_server)
    connector._redis_conn.flushall()
    try:
        yield connector
    finally:
        connector.shutdown()


@pytest.fixture
def queuemanager_mock(
    scan_server_mock,
) -> Generator[Callable[[None | str | list[str]], QueueManager], None, None]:
    def _get_queuemanager(queues=None):
        scan_server = scan_server_mock
        if queues is None:
            queues = ["primary"]
        if isinstance(queues, str):
            queues = [queues]
        for queue in queues:
            scan_server.queue_manager.add_queue(queue)
        return scan_server.queue_manager

    yield _get_queuemanager

    scan_server_mock.queue_manager.shutdown()
