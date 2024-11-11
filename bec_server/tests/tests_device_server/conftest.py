import fakeredis
import pytest

from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector

### THE NEXT FIXTURE HAS TO BE RE-ACTIVATED ONCE
### OPHYD "STATUS CALLBACKS" THREADS ARE CLEANED
### (NEXT OPHYD RELEASE)
# overwrite threads_check fixture from bec_lib,
# to have it in autouse

# @pytest.fixture(autouse=True)
# def threads_check(threads_check):
#    yield
#    bec_logger.logger.remove()
###
### MEANWHILE, THIS FIXTURE WILL JUST CLEAN LOGGER
### THREADS, AND THERE WILL BE NO CHECK FOR DANGLING
### THREADS FOR DEVICE SERVER TESTS (LIKE BEFORE...)


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
def threads_check():
    yield
    # try:
    #     # if ophyd is installed, stop the dispatcher
    #     from ophyd._pyepics_shim import _dispatcher

    #     _dispatcher.stop()
    # except Exception:
    #     pass
    bec_logger.logger.remove()
