from __future__ import annotations

import time
from typing import TYPE_CHECKING, Generator, cast
from unittest.mock import MagicMock, patch

import pytest

import bec_server
import bec_server.scan_server
import bec_server.scan_server.procedures
import bec_server.scan_server.procedures.manager
from bec_ipython_client.main import BECIPythonClient
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_server.scan_server.procedures import manager as manager_module
from bec_server.scan_server.procedures.container_worker import ContainerProcedureWorker
from bec_server.scan_server.procedures.manager import ProcedureManager

if TYPE_CHECKING:
    from pytest_bec_e2e.plugin import LogTestTool

logger = bec_logger.logger

# pylint: disable=protected-access


@pytest.fixture
def client_logtool_and_manager(
    bec_ipython_client_fixture_with_logtool: tuple[BECIPythonClient, "LogTestTool"]
) -> Generator[tuple[BECIPythonClient, "LogTestTool", ProcedureManager], None, None]:
    client, logtool = bec_ipython_client_fixture_with_logtool
    server = MagicMock()
    server.bootstrap_server = f"{client.connector.host}:{client.connector.port}"
    manager_module.QUEUE_TIMEOUT_S = 1
    manager = ProcedureManager(server, ContainerProcedureWorker)
    client._client.connector._redis_conn.flushall()
    yield client, logtool, manager
    manager.shutdown()


@pytest.mark.timeout(100)
def test_happy_path_container_procedure_runner(
    client_logtool_and_manager: tuple[BECIPythonClient, "LogTestTool", ProcedureManager]
):
    with patch.dict(
        bec_server.scan_server.procedures.manager.PROCEDURE_REGISTRY,
        {"test procedure identifier": MagicMock()},
    ):
        client, logtool, manager = client_logtool_and_manager
        assert manager.active_workers == {}
        conn = client.connector
        endpoint = MessageEndpoints.procedure_request()
        msg = messages.ProcedureRequestMessage(
            identifier="test procedure identifier", args_kwargs=((), {})
        )
        conn.xadd(topic=endpoint, msg_dict=msg.model_dump())

        for _ in range(1000):
            time.sleep(0.1)
            if manager.active_workers == {}:
                break
        logtool.fetch()

        assert logtool.is_present_in_any_message(f"procedure accepted: True, message:")
        assert logtool.is_present_in_any_message(
            "ContainerWorker started container for queue primary"
        )
        assert logtool.are_present_in_order(
            [
                "Container worker 'primary' status update: IDLE",
                "Container worker 'primary' status update: RUNNING",
                "Container worker 'primary' status update: IDLE",
                "Container worker 'primary' status update: FINISHED",
            ]
        )

        [
            print(f)
            for f in filter(
                lambda s: "devicemanager" not in s and "scan_bundler" not in s,
                cast(list, logtool._logs),
            )
        ]


# is shutter open??
# is beam avail??
