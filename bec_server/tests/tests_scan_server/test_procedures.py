import builtins
import threading
import time
from functools import partial
from typing import Any, Iterable
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from bec_lib.acl_login import BECAccess
from bec_lib.client import BECClient, RedisConnector
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import (
    ProcedureExecutionMessage,
    ProcedureRequestMessage,
    ProcedureWorkerStatus,
    RawMessage,
    RequestResponseMessage,
)
from bec_lib.procedures.helper import FrontendProcedureHelper, ProcedureState
from bec_lib.serialization import MsgpackSerialization
from bec_lib.service_config import ServiceConfig
from bec_server.procedures.builtin_procedures import run_macro
from bec_server.procedures.constants import PROCEDURE, BecProcedure
from bec_server.procedures.manager import ProcedureManager
from bec_server.procedures.oop_worker_base import main as oop_worker_main
from bec_server.procedures.procedure_registry import (
    _BUILTIN_PROCEDURES,
    ProcedureRegistryError,
    callable_from_execution_message,
    register,
)
from bec_server.procedures.subprocess_worker import SubProcessWorker
from bec_server.test.helpers import wait_until

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=redefined-outer-name


LOG_MSG_PROC_NAME = "_log_msg_args"
FAKEREDIS_HOST = "127.0.0.1"
FAKEREDIS_PORT = 6380


def _eq_except_id(
    a: ProcedureExecutionMessage | ProcedureRequestMessage,
    b: ProcedureExecutionMessage | ProcedureRequestMessage,
):
    return a.identifier == b.identifier and a.queue == b.queue and a.args_kwargs == b.args_kwargs


@pytest.fixture(autouse=True)
def shutdown_client():
    with patch("bec_lib.bec_service.BECAccess", lambda *_, **__: MagicMock(spec=BECAccess)):
        bec_client = BECClient(
            config=ServiceConfig(
                config={"redis": {"host": FAKEREDIS_HOST, "port": FAKEREDIS_PORT}}
            ),
            connector_cls=partial(RedisConnector, redis_cls=fakeredis.FakeRedis),  # type: ignore
        )
        bec_client.start()
        yield bec_client
        bec_client.shutdown()
        fakeredis.FakeRedis().flushall()


def mock_redis_conn(tasks: Iterable[ProcedureExecutionMessage] = []):
    def do(bootstrap: str | list[str], **kwargs):
        if isinstance(bootstrap, list):
            bootstrap = bootstrap[0]
        conn_mock = MagicMock()
        conn_mock.host, conn_mock.port = bootstrap.split(":")
        conn_mock.blocking_list_pop_to_set_add.side_effect = tasks
        return conn_mock

    return do


class InlineWorker(SubProcessWorker):
    def __init__(
        self,
        server: str,
        queue: str,
        lifetime_s: float | None = None,
        env: dict[str, str] | None = None,
    ):
        self._worker_id = None
        self._control_credentials = None
        self._queue = queue
        self.key = MessageEndpoints.procedure_execution(queue)
        self._active_procs_endpoint = MessageEndpoints.active_procedure_executions()
        self.status = ProcedureWorkerStatus.IDLE
        self._redis_server = server
        self._conn = MagicMock(name="InlineWorker RedisConnector")
        self._conn.blocking_list_pop = MagicMock(side_effect=lambda *_, **__: time.sleep(0.2))
        self._helper = MagicMock()
        self._lifetime_s = lifetime_s or PROCEDURE.WORKER.QUEUE_TIMEOUT_S
        self.client_id = 0
        self._current_execution_id: str | None = None
        self._aborted = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._process = MagicMock()
        self._output_logs = []
        self._output_lock = threading.Lock()
        self._process.poll = MagicMock(return_value=None)
        self._ending: bool = False

    def _setup_execution_environment(self):
        self._ending = False

    def _kill_process(self):
        self._ending = True

    def run_tasks(self, tasks: Iterable[ProcedureExecutionMessage] = []):
        """Put task on the queue"""
        with (
            patch("bec_server.procedures.oop_worker_base.BECIPythonClient") as client,
            patch(
                "bec_server.procedures.oop_worker_base.BackendProcedureHelper",
                lambda _: MagicMock(),
            ),
            patch("bec_server.procedures.oop_worker_base.bec_logger", MagicMock()),
            patch("bec_server.procedures.oop_worker_base.logger", MagicMock()),
            patch(
                "bec_server.procedures.oop_worker_base.get_env",
                lambda: {
                    "redis_server": self._redis_server,
                    "timeout_s": self._lifetime_s,
                    "queue": self._queue,
                    "client_class": "BECIPythonClient",
                },
            ),
            patch("bec_server.procedures.oop_worker_base.RedisConnector", mock_redis_conn(tasks)),
        ):
            oop_worker_main()
            return client


@pytest.fixture
def procedure_manager():
    with (
        patch(
            "bec_server.procedures.manager.RedisConnector",
            partial(RedisConnector, redis_cls=fakeredis.FakeRedis),  # type: ignore
        ),
        patch(
            "bec_server.procedures.worker_base.RedisConnector",
            partial(RedisConnector, redis_cls=fakeredis.FakeRedis),  # type: ignore
        ),
    ):
        manager = ProcedureManager(f"{FAKEREDIS_HOST}:{FAKEREDIS_PORT}", InlineWorker)  # type: ignore
        yield manager
    manager.shutdown()


def test_helper_log_streams(procedure_manager):
    conn = procedure_manager._conn
    helper = FrontendProcedureHelper(conn)
    conn.xadd(MessageEndpoints.procedure_logs("queue1"), {"data": RawMessage(data=str("data"))})
    conn.xadd(MessageEndpoints.procedure_logs("queue2"), {"data": RawMessage(data=str("data"))})
    assert helper.get.log_queue_names() == ["queue1", "queue2"]


def test_procedure_manager_authenticates_with_forwarded_credentials():
    conn = MagicMock()
    with (
        patch("bec_server.procedures.manager.RedisConnector", return_value=conn),
        patch(
            "bec_server.procedures.queue_manager.BackendProcedureHelper", return_value=MagicMock()
        ),
    ):
        manager = ProcedureManager(
            f"{FAKEREDIS_HOST}:{FAKEREDIS_PORT}",
            InlineWorker,
            redis_credentials={"username": "admin", "token": "admin"},
        )

    conn.authenticate.assert_called_once_with(username="admin", password="admin")
    manager.shutdown()


@pytest.mark.parametrize(["accepted", "msg"], zip([True, False], ["test true", "test false"]))
def test_ack(procedure_manager: ProcedureManager, accepted: bool, msg: str):
    ps = procedure_manager._conn._managed_connection._redis_conn.pubsub()
    ps.subscribe(procedure_manager._reply_ep.endpoint)
    ps.get_message()
    procedure_manager._ack(accepted, msg, "1234")
    message = ps.get_message()
    assert message is not None
    data = MsgpackSerialization.loads(message["data"])
    assert isinstance(data, RequestResponseMessage)
    assert data.accepted == accepted
    assert data.message == {"execution_id": "1234", "message": msg}


VALIDATION_TEST_CASES: list[tuple[dict[str, Any], ProcedureRequestMessage | None]] = [
    ({"identifier": LOG_MSG_PROC_NAME}, ProcedureRequestMessage(identifier=LOG_MSG_PROC_NAME)),
    (
        {"identifier": LOG_MSG_PROC_NAME, "queue": "queue2"},
        ProcedureRequestMessage(identifier=LOG_MSG_PROC_NAME, queue="queue2"),
    ),
    ({"identifier": "doesn't exist"}, None),
]


@pytest.mark.parametrize(["message", "result"], VALIDATION_TEST_CASES)
def test_validate(procedure_manager: ProcedureManager, message, result):
    procedure_manager._ack = MagicMock()
    validated = procedure_manager._validate_request(ProcedureRequestMessage.model_validate(message))
    if validated is None:
        assert result is None
    else:
        assert _eq_except_id(validated, result)


@patch("bec_server.procedures.builtin_procedures.logger")
def test_builtin_procedure_log_args(procedure_logger: MagicMock):
    test_string = "test string for logging as an arg"
    with InlineWorker("localhost:1", "primary", 1) as worker:
        worker.run_tasks(
            [
                ProcedureExecutionMessage(
                    identifier="_log_msg_args",
                    queue="primary",
                    args_kwargs=((test_string,), {"kwarg": "test"}),
                    execution_id="1234",
                )
            ]
        )
    log_call_arg_0 = procedure_logger.success.call_args.args[0]
    assert test_string in log_call_arg_0
    assert "'kwarg': 'test'" in log_call_arg_0


def test_builtin_procedure_scan_execution(shutdown_client):
    args = ("samx", -10, 10)
    kwargs = {"steps": 5, "relative": False}
    with InlineWorker(f"{FAKEREDIS_HOST}:{FAKEREDIS_PORT}", "primary", 1) as worker:
        client = worker.run_tasks(
            [
                ProcedureExecutionMessage(
                    identifier="run scan",
                    queue="primary",
                    args_kwargs=(("line_scan",), {"args": args, "parameters": kwargs}),
                    execution_id="1234",
                )
            ]
        )
    client().scans.line_scan.assert_called_with(*args, **kwargs)


def test_builtin_procedures_are_bec_procedures():
    for proc in _BUILTIN_PROCEDURES.values():
        assert isinstance(proc, BecProcedure)


def test_callable_from_message():
    with pytest.raises(ProcedureRegistryError) as e:
        callable_from_execution_message(
            ProcedureExecutionMessage(
                identifier="doesn't exist", queue="primary", execution_id="1234"
            )
        )
    assert e.match("No registered procedure")


def test_register_rejects_wrong_type():
    with pytest.raises(ProcedureRegistryError) as e:
        register("test", "test")  # type: ignore
    assert e.match("not a valid procedure")


def test_register_rejects_already_registered():
    with pytest.raises(ProcedureRegistryError) as e:
        register("run scan", lambda *_, **__: None)
    assert e.match("already registered")


@patch("bec_server.procedures.oop_worker_base.BECIPythonClient", MagicMock)
def test_procedure_status_rejected(procedure_manager):
    status = procedure_manager._helper.request.procedure("doesn't exist")
    assert status.state == ProcedureState.REQUESTED
    wait_until(lambda: status.state == ProcedureState.REJECTED, timeout_s=10)
    assert status.done


@patch("bec_server.procedures.oop_worker_base.BECIPythonClient", MagicMock)
def test_procedure_status_rejected_not_cancellable(procedure_manager):
    status = procedure_manager._helper.request.procedure("doesn't exist")
    wait_until(lambda: status.state == ProcedureState.REJECTED, timeout_s=10)

    with pytest.raises(ValueError) as e:
        status.cancel()

    assert e.match("A procedure which is already")


def test_builtin_proc_run_macro_found(shutdown_client):
    recorder = MagicMock()
    with patch.dict(
        builtins.__dict__, _user_macros={"test_macro": {"cls": lambda a, b: recorder(a + b)}}
    ):
        run_macro("test_macro", ((5, 6), {}), bec=shutdown_client)
    recorder.assert_called_with(11)


def test_builtin_proc_run_macro_not_found(shutdown_client):
    with pytest.raises(ValueError) as e:
        run_macro("not found", bec=shutdown_client)
    assert e.match("not found in the client namespace")
