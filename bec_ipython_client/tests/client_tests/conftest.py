import fakeredis
import pytest

from bec_lib.redis_connector import RedisConnector


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
