from __future__ import annotations

import os
import time
from dataclasses import dataclass
from importlib.metadata import version
from typing import TYPE_CHECKING, Callable, Generator
from unittest.mock import patch
from uuid import uuid4

import pytest
from pytest_redis import factories as redis_factories

from bec_ipython_client.main import BECIPythonClient
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.procedures.helper import ProcedureState
from bec_server.procedures.constants import _CONTAINER, _WORKER, PROCEDURE
from bec_server.procedures.container_utils import (
    _run_and_capture_error,
    get_backend,
    podman_available,
)
from bec_server.procedures.container_worker import ContainerProcedureWorker
from bec_server.procedures.manager import ProcedureManager
from bec_server.procedures.subprocess_worker import SubProcessWorker

if TYPE_CHECKING:
    from pytest_bec_e2e.plugin import LogTestTool

logger = bec_logger.logger

# pylint: disable=protected-access

# Random order disabled for this module so that the test for building the worker container runs first
# and we can use lower timeouts for the remaining tests
pytestmark = pytest.mark.random_order(disabled=True)


@dataclass(frozen=True)
class PATCHED_CONSTANTS:
    WORKER = _WORKER()
    CONTAINER = _CONTAINER()
    MANAGER_SHUTDOWN_TIMEOUT_S = 2
    BEC_VERSION = version("bec_lib")
    REDIS_HOST = "localhost"


@pytest.fixture
def client_logtool_and_manager(
    bec_ipython_client_fixture_with_logtool: tuple[BECIPythonClient, "LogTestTool"],
) -> Generator[tuple[BECIPythonClient, "LogTestTool", ProcedureManager], None, None]:
    client, logtool = bec_ipython_client_fixture_with_logtool
    manager = ProcedureManager(f"{client.connector.host}:{client.connector.port}", SubProcessWorker)
    try:
        yield client, logtool, manager
    finally:
        manager.shutdown()


def _wait_while(cond: Callable[[], bool], timeout_s):
    start = time.monotonic()
    while cond():
        if (time.monotonic() - start) > timeout_s:
            raise TimeoutError()
        time.sleep(0.01)


@pytest.mark.timeout(100)
def test_building_worker_image():
    podman_utils = get_backend()
    build = podman_utils.build_worker_image()
    assert len(build._command_output.splitlines()[-1]) == 64  # type: ignore
    assert podman_utils.image_exists(f"bec_procedure_worker:v{version('bec_lib')}")


@pytest.mark.timeout(100)
@patch("bec_server.procedures.manager.procedure_registry.is_registered", lambda _: True)
@patch("bec_server.procedures.oop_worker_base.PROCEDURE", PATCHED_CONSTANTS())
@patch("bec_server.procedures.container_worker.PROCEDURE", PATCHED_CONSTANTS())
def test_procedure_runner_spawns_worker(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager],
):
    client, _, manager = client_logtool_and_manager
    assert manager._active_workers == {}
    endpoint = MessageEndpoints.procedure_request()
    msg = messages.ProcedureRequestMessage(
        identifier="sleep", args_kwargs=((), {"time_s": 0.1}), queue="test"
    )

    logs = []

    def cb(worker: SubProcessWorker):
        nonlocal logs
        logs = worker.logs()

    manager.add_callback("test", cb)
    client.connector.send(endpoint, msg)

    _wait_while(lambda: manager._active_workers == {}, 5)
    _wait_while(lambda: manager._active_workers != {}, 90)


@pytest.mark.timeout(100)
@patch("bec_server.procedures.manager.procedure_registry.is_registered", lambda _: True)
@patch("bec_server.procedures.oop_worker_base.PROCEDURE", PATCHED_CONSTANTS())
@patch("bec_server.procedures.container_worker.PROCEDURE", PATCHED_CONSTANTS())
def test_happy_path_container_procedure_runner(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager],
):
    test_args = (1, 2, 3)
    test_kwargs = {"a": "b", "c": "d"}
    client, logtool, manager = client_logtool_and_manager
    assert manager._active_workers == {}
    conn = client.connector
    endpoint = MessageEndpoints.procedure_request()
    msg = messages.ProcedureRequestMessage(
        identifier="_log_msg_args", args_kwargs=(test_args, test_kwargs)
    )
    conn.send(endpoint, msg)

    _wait_while(lambda: manager._active_workers == {}, 5)
    _wait_while(lambda: manager._active_workers != {}, 90)

    logtool.fetch()
    assert logtool.is_present_in_any_message("procedure accepted: True, message:")
    assert logtool.is_present_in_any_message(
        "Procedure worker started container for queue primary"
    ), f"Log content relating to procedures: {manager._logs}"

    res, msg = logtool.are_present_in_order(
        [
            "Procedure worker 'primary' status update: IDLE",
            "Procedure worker 'primary' status update: RUNNING",
            "Procedure worker 'primary' status update: IDLE",
            "Procedure worker 'primary' status update: FINISHED",
        ]
    )
    assert res, f"failed on {msg}"

    assert logtool.is_present_in_any_message(
        f"Builtin procedure log_message_args_kwargs called with args: {test_args} and kwargs: {test_kwargs}"
    )


# A separate Redis prevents the scan server's manager from also consuming this request.
proc_redis = redis_factories.redis_proc()


@pytest.mark.timeout(120)
def test_container_procedure_execution(proc_redis, monkeypatch):
    """Execute a real procedure in Podman against the configured test Redis server.

    Requires the worker image and a host-networked ``local_bec`` pod. For a remote
    Podman VM, BEC_TEST_CONTAINER_REDIS_HOST can supply a container-reachable host.
    """
    if not podman_available():
        pytest.skip("Podman is not available")
    host, port = proc_redis.host, proc_redis.port
    queue = f"container_test_{uuid4().hex}"
    if container_host := os.environ.get("BEC_TEST_CONTAINER_REDIS_HOST"):
        worker_environment = ContainerProcedureWorker._worker_environment
        monkeypatch.setattr(
            ContainerProcedureWorker,
            "_worker_environment",
            lambda worker: worker_environment(worker)
            | {"redis_server": f"{container_host}:{port}"},
        )
    manager = ProcedureManager(f"{host}:{port}", ContainerProcedureWorker)
    try:
        status = manager._helper.request.procedure(
            "_log_msg_args", args_kwargs=(("podman regression",), {}), queue=queue
        )
        status.wait(timeout_s=90, raise_on_failure=True)
        assert status.done, "Container procedure remained scheduled"
        assert status.state == ProcedureState.SUCCESS
        _wait_while(lambda: bool(manager.active_workers()), 20)
        assert any("podman regression" in line for line in manager._logs)
    finally:
        try:
            manager.shutdown()
        finally:
            container_name = f"bec_procedure_{PROCEDURE.BEC_VERSION}_{queue}"
            _run_and_capture_error("podman", "rm", "--force", "--ignore", container_name)
