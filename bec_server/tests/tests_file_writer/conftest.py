import fakeredis
import pytest

from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector

# overwrite threads_check fixture from bec_lib,
# to have it in autouse


def fake_redis_server(host, port):
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


@pytest.fixture(autouse=True)
def threads_check(threads_check):
    yield
    bec_logger.logger.remove()
