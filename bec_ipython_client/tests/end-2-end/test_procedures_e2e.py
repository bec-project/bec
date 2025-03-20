from __future__ import annotations

import time
from typing import TYPE_CHECKING, Generator, cast
from unittest.mock import MagicMock, patch

import podman
import pytest

from bec_ipython_client.main import BECIPythonClient
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.scan_server.procedures.constants import PROCEDURE
from bec_server.scan_server.procedures.container_worker import ContainerProcedureWorker
from bec_server.scan_server.procedures.manager import ProcedureManager

if TYPE_CHECKING:
    from pytest_bec_e2e.plugin import LogTestTool

logger = bec_logger.logger

# pylint: disable=protected-access

try:
    with podman.PodmanClient(base_url=PROCEDURE.CONTAINER.PODMAN_URI) as client:
        client.info()
except Exception:
    pytest.skip(reason="podman not available in this environment!", allow_module_level=True)


@pytest.fixture
def client_logtool_and_manager(
    bec_ipython_client_fixture_with_logtool: tuple[BECIPythonClient, "LogTestTool"]
) -> Generator[tuple[BECIPythonClient, "LogTestTool", ProcedureManager], None, None]:
    client, logtool = bec_ipython_client_fixture_with_logtool
    server = MagicMock()
    server.bootstrap_server = f"{client.connector.host}:{client.connector.port}"
    manager = ProcedureManager(server, ContainerProcedureWorker)
    client._client.connector._redis_conn.flushall()
    yield client, logtool, manager
    manager.shutdown()


@pytest.mark.timeout(100)
@patch("bec_server.scan_server.procedures.manager.procedure_registry.is_registered", lambda _: True)
def test_happy_path_container_procedure_runner(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager]
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

    for _ in range(1000):
        time.sleep(0.1)
        if manager.active_workers == {}:
            break

    logtool.fetch()
    assert logtool.is_present_in_any_message(f"procedure accepted: True, message:")
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
