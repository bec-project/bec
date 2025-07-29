from __future__ import annotations

import time
from importlib.metadata import version
from typing import TYPE_CHECKING, Callable, Generator
from unittest.mock import MagicMock, patch

import pytest

from bec_ipython_client.main import BECIPythonClient
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.scan_server.procedures.constants import PROCEDURE
from bec_server.scan_server.procedures.container_utils import get_backend
from bec_server.scan_server.procedures.container_worker import ContainerProcedureWorker
from bec_server.scan_server.procedures.manager import ProcedureManager

if TYPE_CHECKING:
    from pytest_bec_e2e.plugin import LogTestTool

logger = bec_logger.logger

# pylint: disable=protected-access

# Random order disabled for this module so that the test for building the worker container runs first
# and we can use lower timeouts for the remaining tests
pytestmark = pytest.mark.random_order(disabled=True)


@pytest.fixture
def client_logtool_and_manager(
    bec_ipython_client_fixture_with_logtool: tuple[BECIPythonClient, "LogTestTool"],
) -> Generator[tuple[BECIPythonClient, "LogTestTool", ProcedureManager], None, None]:
    client, logtool = bec_ipython_client_fixture_with_logtool
    server = MagicMock()
    server.bootstrap_server = f"{client.connector.host}:{client.connector.port}"
    manager = ProcedureManager(server, ContainerProcedureWorker)
    yield client, logtool, manager
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
    assert len(build._command_output.splitlines()[-1]) == 64
    assert podman_utils.image_exists(f"bec_procedure_worker:v{version('bec_lib')}")


@pytest.mark.timeout(100)
@patch("bec_server.scan_server.procedures.manager.procedure_registry.is_registered", lambda _: True)
def test_procedure_runner_spawns_worker(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager],
):
    client, _, manager = client_logtool_and_manager
    assert manager.active_workers == {}
    endpoint = MessageEndpoints.procedure_request()
    msg = messages.ProcedureRequestMessage(
        identifier="sleep", args_kwargs=((), {"time_s": 2}), queue="test"
    )

    logs = []

    def cb(worker: ContainerProcedureWorker):
        nonlocal logs
        logs = worker._backend.logs(worker._container_id)

    manager.add_callback("test", cb)
    client.connector.xadd(topic=endpoint, msg_dict=msg.model_dump())

    _wait_while(lambda: manager.active_workers == {}, 5)
    _wait_while(lambda: manager.active_workers != {}, 20)

    assert logs != []


@pytest.mark.timeout(100)
@patch("bec_server.scan_server.procedures.manager.procedure_registry.is_registered", lambda _: True)
def test_happy_path_container_procedure_runner(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager],
):
    test_args = (1, 2, 3)
    test_kwargs = {"a": "b", "c": "d"}
    client, logtool, manager = client_logtool_and_manager
    assert manager.active_workers == {}
    conn = client.connector
    endpoint = MessageEndpoints.procedure_request()
    msg = messages.ProcedureRequestMessage(
        identifier="log execution message args", args_kwargs=(test_args, test_kwargs)
    )
    conn.xadd(topic=endpoint, msg_dict=msg.model_dump())

    _wait_while(lambda: manager.active_workers == {}, 5)
    _wait_while(lambda: manager.active_workers != {}, 20)

    logtool.fetch()
    assert logtool.is_present_in_any_message("procedure accepted: True, message:")
    assert logtool.is_present_in_any_message("ContainerWorker started container for queue primary")
    res, msg = logtool.are_present_in_order(
        [
            "Container worker 'primary' status update: IDLE",
            "Container worker 'primary' status update: RUNNING",
            "Container worker 'primary' status update: IDLE",
            "Container worker 'primary' status update: FINISHED",
        ]
    )
    assert res, f"failed on {msg}"
    res, msg = logtool.are_present_in_order(
        [
            "Container worker 'primary' status update: IDLE",
            f"Builtin procedure log_message_args_kwargs called with args: {test_args} and kwargs: {test_kwargs}",
            "Container worker 'primary' status update: IDLE",
            "Container worker 'primary' status update: FINISHED",
        ]
    )
    assert res, f"failed on {msg}"
