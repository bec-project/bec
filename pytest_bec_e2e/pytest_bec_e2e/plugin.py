"""Fixtures for end-to-end tests"""

# pylint: skip-file
import os
import pathlib
import platform
import shutil
import tempfile

import pytest
from pytest_redis import factories as pytest_redis_factories
from redis import Redis

from bec_ipython_client import BECIPythonClient
from bec_lib.client import BECClient
from bec_lib.config_helper import ConfigHelper
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig, ServiceConfigModel
from bec_lib.tests.utils import wait_for_empty_queue


class LogTestTool:
    def __init__(self, client: BECIPythonClient):
        self._conn: RedisConnector = client.connector
        self._logs = None

    def fetch(self, count: int | None = None) -> None:
        """Fetch logs from the server and store them for interrogation, get all by default or get
        the last `count` logs (latter will not fetch read logs again if they have been seen!)"""
        log_data = self._conn.xread(MessageEndpoints.log(), from_start=(count is None), count=count)
        if log_data is None:
            self._logs = None
            return
        self._logs = list(item["data"].log_msg["text"] for item in log_data)

    def is_present_in_any_message(self, needle: str) -> bool:
        """Assert that the provided string is in at least one log message"""
        if self._logs is None:
            raise RuntimeError("No logs fetched")
        for log in self._logs:
            if needle in log:
                return True
        return False

    def are_present_in_order(self, needles: list[str]) -> tuple[bool, str]:
        if self._logs is None:
            raise RuntimeError("No logs fetched")
        _needles = list(reversed(needles.copy()))
        for log in self._logs:
            if _needles[-1] in log:
                _needles.pop()
            if len(_needles) == 0:
                return True, ""
        return False, _needles[0]


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption("--start-servers", action="store_true", default=False)
    parser.addoption("--bec-redis-host", action="store", default="localhost")
    parser.addoption("--bec-redis-cmd", action="store", default=None)
    parser.addoption("--flush-redis", action="store_true", default=False)
    parser.addoption("--files-path", action="store", default=None)


redis_server_fixture = None
bec_redis_fixture = None
_start_servers = False
bec_servers_scope = (
    lambda fixture_name, config: config.getoption("--flush-redis") and "function" or "session"
)


def _check_path(file_path):
    if os.path.exists(file_path):
        return pathlib.Path(file_path)
    else:
        raise RuntimeError(
            f"end2end tests: --files-path directory {repr(file_path)} does not exist"
        )


def _get_tmp_dir():
    # on MacOS, gettempdir() returns path like /var/folders/nj/269977hs0_96bttwj2gs_jhhp48z54/T[...],
    # and if building a Unix socket file (like pytest-redis does to connect to redis) it can
    # exceed the 109 characters limit, so make a special case for MacOS
    return pathlib.Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir())


@pytest.hookimpl
def pytest_configure(config):
    global redis_server_fixture
    global bec_redis_fixture
    global _start_servers
    global _bec_servers_scope

    if config.getoption("--start-servers"):
        # configure 'datadir' == where redis Unix socket will go, and .rdb file (if any)
        # try to use specified files path (hope it does not exceed 109 chars) or
        # just use the normal tmp file directory except on MacOS where it must be enforced
        # to /tmp
        user_tmp_path = config.getoption("--files-path")
        if user_tmp_path is not None:
            datadir = _check_path(user_tmp_path)
        else:
            datadir = _get_tmp_dir()
        # session-scoped fixture that starts redis using provided cmd
        redis_server_fixture = pytest_redis_factories.proc.redis_proc(
            executable=config.getoption("--bec-redis-cmd"), datadir=datadir
        )

        if config.getoption("--flush-redis"):
            bec_redis_fixture = pytest_redis_factories.client.redisdb("redis_server_fixture")
            _bec_servers_scope = "function"  # have to restart servers at each test
        else:
            bec_redis_fixture = redis_server_fixture
    else:
        # do not automatically start redis - bec_redis_fixture will use existing
        # process, will wait for 3 seconds max (must be running already);
        # there is no point checking if we want to flush redis
        # since it would remove available scans which are only populated
        # when scan server starts
        redis_server_fixture = pytest_redis_factories.noproc.redis_noproc(
            host=config.getoption("--bec-redis-host"), startup_timeout=3
        )
        bec_redis_fixture = redis_server_fixture

    _start_servers = config.getoption("--start-servers")


@pytest.fixture(scope=bec_servers_scope)
def bec_files_path(request):
    user_tmp_path = request.config.getoption("--files-path")
    if user_tmp_path is not None:
        yield _check_path(user_tmp_path)
    else:
        if request.config.getoption("--flush-redis"):
            request.fixturenames.append("tmp_path")
            yield request.getfixturevalue("tmp_path")
        else:
            request.fixturenames.append("tmp_path_factory")
            yield request.getfixturevalue("tmp_path_factory").mktemp("bec_files")


@pytest.fixture(scope=bec_servers_scope)
def bec_services_config_file_path(bec_files_path):
    return pathlib.Path(bec_files_path / "services_config.yaml")


@pytest.fixture(scope=bec_servers_scope)
def bec_test_config_file_path(bec_files_path):
    return pathlib.Path(bec_files_path / "test_config.yaml")


@pytest.fixture(scope=bec_servers_scope)
def bec_redis_host_port(request, bec_redis_fixture):
    if isinstance(bec_redis_fixture, Redis):
        server_fixture = request.getfixturevalue("redis_server_fixture")
        return server_fixture.host, server_fixture.port
    else:
        return bec_redis_fixture.host, bec_redis_fixture.port


@pytest.fixture(scope=bec_servers_scope)
def bec_servers(
    test_config_yaml_file_path,
    bec_services_config_file_path,
    bec_test_config_file_path,
    bec_files_path,
    bec_redis_host_port,
):
    redis_host, redis_port = bec_redis_host_port
    # ensure configuration files are written where appropriate for tests,
    # i.e. either in /tmp/pytest/... directory, or following user "--files-path"
    # 1) test config (devices...)
    shutil.copyfile(test_config_yaml_file_path, bec_test_config_file_path)
    # 2) path where files are saved
    file_writer_path = bec_services_config_file_path.parent  # / "writer_output"
    # file_writer_path.mkdir(exist_ok=True)
    # 3) services config
    with open(bec_services_config_file_path, "w") as services_config_file:
        service_config = ServiceConfigModel(
            redis={"host": redis_host, "port": redis_port},
            file_writer={"base_path": str(file_writer_path)},
        )

        services_config_file.write(service_config.model_dump_json(indent=4))

    if _start_servers:
        from bec_server.bec_server_utils.service_handler import ServiceHandler

        # Start all BEC servers, kill them at the end
        # when interface='subprocess', 'bec_path' indicate the cwd
        # for the process (working directory), i.e. where log files will go
        service_handler = ServiceHandler(
            bec_path=bec_files_path,
            config_path=bec_services_config_file_path,
            interface="subprocess",
        )
        processes = service_handler.start()
        try:
            yield
        finally:
            service_handler.stop(processes)
    else:
        # Nothing to do here: servers are supposed to be started externally.
        yield


@pytest.fixture
def bec_ipython_client_with_demo_config(
    bec_redis_fixture, bec_services_config_file_path, bec_servers
):
    config = ServiceConfig(bec_services_config_file_path)
    bec = BECIPythonClient(config, RedisConnector, forced=True)
    bec.start()
    bec.config.load_demo_config()
    try:
        yield bec
    finally:
        bec.shutdown()
        bec._client._reset_singleton()


@pytest.fixture
def bec_client_lib_with_demo_config(bec_redis_fixture, bec_services_config_file_path, bec_servers):
    config = ServiceConfig(bec_services_config_file_path)
    bec = BECClient(config, RedisConnector, forced=True, wait_for_server=True)
    bec.start()
    bec.config.load_demo_config()
    try:
        yield bec
    finally:
        bec.shutdown()
        bec._client._reset_singleton()


@pytest.fixture
def bec_ipython_client_fixture(bec_ipython_client_with_demo_config):
    bec = bec_ipython_client_with_demo_config
    bec.queue.request_queue_reset()
    bec.queue.request_scan_continuation()
    wait_for_empty_queue(bec)
    yield bec


@pytest.fixture
def bec_client_lib(bec_client_lib_with_demo_config):
    bec = bec_client_lib_with_demo_config
    bec.queue.request_queue_reset()
    bec.queue.request_scan_continuation()
    wait_for_empty_queue(bec)
    yield bec


@pytest.fixture
def bec_ipython_client_fixture_with_logtool(bec_ipython_client_fixture):
    bec: BECIPythonClient = bec_ipython_client_fixture
    yield bec, LogTestTool(bec)
