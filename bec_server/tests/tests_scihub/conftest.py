from unittest import mock

import fakeredis
import pytest

from bec_lib.logger import bec_logger
from bec_lib.messages import BECStatus
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.utils import ConnectorMock
from bec_server.scihub import SciHub
from bec_server.scihub.atlas.atlas_connector import AtlasConnector

# overwrite threads_check fixture from bec_lib,
# to have it in autouse


@pytest.fixture(autouse=True)
def threads_check(threads_check):
    yield
    bec_logger.logger.remove()


class SciHubMocked(SciHub):
    def _start_metrics_emitter(self):
        pass

    def wait_for_service(self, name, status=BECStatus.RUNNING):
        pass

    def _start_atlas_connector(self):
        pass

    def _start_scilog_connector(self):
        pass


@pytest.fixture()
def SciHubMock():
    config = ServiceConfig(
        redis={"host": "dummy", "port": 6379},
        service_config={
            "file_writer": {"plugin": "default_NeXus_format", "base_path": "./"},
            "log_writer": {"base_path": "./"},
        },
    )
    scihub_mocked = SciHubMocked(config, ConnectorMock)
    yield scihub_mocked
    scihub_mocked.shutdown()


def fake_redis_server(host, port, **kwargs):
    redis = fakeredis.FakeRedis()
    return redis


@pytest.fixture
def connected_atlas_connector():
    connector = RedisConnector("localhost:1", redis_cls=fake_redis_server)
    connector._redis_conn.flushall()
    try:
        yield connector
    finally:
        connector.shutdown()


@pytest.fixture()
def atlas_connector(SciHubMock, connected_atlas_connector):
    atlas_connector = AtlasConnector(SciHubMock, SciHubMock.connector, connected_atlas_connector)
    with mock.patch.object(atlas_connector, "_load_environment"):
        with mock.patch.object(atlas_connector, "_env_configured", True):
            atlas_connector.host = "test-host"
            atlas_connector.deployment_name = "test-deployment"
            atlas_connector.atlas_key = "test-key"
            atlas_connector.start()
            yield atlas_connector
    atlas_connector.shutdown()


@pytest.fixture()
def config_handler(atlas_connector):
    with mock.patch.object(atlas_connector, "_start_config_request_handler"):
        yield atlas_connector.config_handler
