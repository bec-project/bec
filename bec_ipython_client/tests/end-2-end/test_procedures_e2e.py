from __future__ import annotations

import time
from dataclasses import dataclass
from importlib.metadata import version
from typing import TYPE_CHECKING, Callable, Generator
from unittest.mock import patch
from uuid import uuid4

import pytest

from bec_ipython_client.main import BECIPythonClient
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.procedures.constants import _CONTAINER, _WORKER
from bec_server.procedures.container_utils import get_backend
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
    MANAGER_SHUTDOWN_TIMEOUT_S = 15
    BEC_VERSION = version("bec_lib")
    REDIS_HOST = "localhost"


@pytest.fixture
def client_logtool_and_manager(
    bec_ipython_client_fixture_with_logtool: tuple[BECIPythonClient, "LogTestTool"], threads_check
) -> Generator[tuple[BECIPythonClient, "LogTestTool", ProcedureManager], None, None]:
    client, logtool = bec_ipython_client_fixture_with_logtool
    manager = ProcedureManager(
        f"{client.connector.host}:{client.connector.port}", ContainerProcedureWorker
    )
    yield client, logtool, manager
    manager.shutdown()
    time.sleep(1)


def _wait_while(cond: Callable[[], bool], timeout_s):
    start = time.monotonic()
    while cond():
        if (time.monotonic() - start) > timeout_s:
            raise TimeoutError()
        time.sleep(0.01)


def _wait_while_with_cleanup(cond: Callable[[], bool], timeout_s, manager):
    try:
        _wait_while(cond, timeout_s)
    except Exception as e:
        with manager.lock:
            for worker_entry in manager._active_workers.values():
                worker = worker_entry.get("worker")
                if worker is not None:
                    worker.abort()
                    raise Exception(worker.logs()) from e  # print the logs if there is an error


def test_building_worker_image():
    podman_utils = get_backend()
    build = podman_utils.build_worker_image()
    assert len(build._command_output.splitlines()[-1]) == 64  # type: ignore
    assert podman_utils.image_exists(f"bec_procedure_worker:v{version('bec_lib')}")


@patch("bec_server.procedures.manager.procedure_registry.is_registered", lambda _: True)
@patch("bec_server.procedures.oop_worker_base.PROCEDURE", PATCHED_CONSTANTS())
@patch("bec_server.procedures.container_worker.PROCEDURE", PATCHED_CONSTANTS())
def test_procedure_runner_spawns_worker(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager],
):
    client, _, manager = client_logtool_and_manager
    assert manager._active_workers == {}
    queue = str(uuid4())
    endpoint = MessageEndpoints.procedure_request()
    msg = messages.ProcedureRequestMessage(
        identifier="sleep", args_kwargs=((), {"time_s": 0.1}), queue=queue
    )

    logs = []

    def cb(worker: SubProcessWorker):
        nonlocal logs
        logs = worker.logs()

    manager.add_callback(queue, cb)
    client.connector.send(endpoint, msg)

    _wait_while(lambda: manager._active_workers == {}, 5)
    _wait_while_with_cleanup(lambda: manager._active_workers != {}, 120, manager)
    _wait_while(lambda: logs == [], 20)


@patch("bec_server.procedures.manager.procedure_registry.is_registered", lambda _: True)
@patch("bec_server.procedures.oop_worker_base.PROCEDURE", PATCHED_CONSTANTS())
@patch("bec_server.procedures.container_worker.PROCEDURE", PATCHED_CONSTANTS())
def test_happy_path_container_procedure_runner(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager],
):
    test_args = (1, 2, 3)
    test_kwargs = {"a": "b", "c": "d"}
    queue = str(uuid4())
    client, logtool, manager = client_logtool_and_manager
    assert manager._active_workers == {}
    conn = client.connector
    endpoint = MessageEndpoints.procedure_request()
    msg = messages.ProcedureRequestMessage(
        identifier="_log_msg_args", args_kwargs=(test_args, test_kwargs), queue=queue
    )
    conn.send(endpoint, msg)

    _wait_while(lambda: manager._active_workers == {}, 10)
    _wait_while_with_cleanup(lambda: manager._active_workers != {}, 180, manager)

    def _check_for_logs():
        time.sleep(5)
        logtool.fetch()
        return all(
            (
                logtool.is_present_in_any_message("procedure accepted: True, message:"),
                logtool.is_present_in_any_message(
                    "Procedure worker started container for queue primary"
                ),
                logtool.are_present_in_order(
                    [
                        "Procedure worker 'primary' status update: IDLE",
                        "Procedure worker 'primary' status update: RUNNING",
                        "Procedure worker 'primary' status update: IDLE",
                        "Procedure worker 'primary' status update: FINISHED",
                    ]
                ),
                logtool.is_present_in_any_message(
                    f"Builtin procedure log_message_args_kwargs called with args: {test_args} and kwargs: {test_kwargs}"
                ),
            )
        )

    _wait_while_with_cleanup(lambda: not _check_for_logs(), 30, manager)
