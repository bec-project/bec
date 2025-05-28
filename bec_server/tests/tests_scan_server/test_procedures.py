import threading
import time
from functools import partial
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from bec_lib.client import BECClient, RedisConnector
from bec_lib.messages import (
    ProcedureExecutionMessage,
    ProcedureRequestMessage,
    RequestResponseMessage,
)
from bec_lib.serialization import MsgpackSerialization
from bec_server.scan_server.procedures import (
    InProcessProcedureWorker,
    ProcedureManager,
    ProcedureWorker,
    WorkerAlreadyExists,
)
from bec_server.scan_server.procedures.manager import DEFAULT_QUEUE, ProcedureWorker
from bec_server.scan_server.procedures.worker_base import ProcedureWorkerStatus

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=redefined-outer-name


LOG_MSG_PROC_NAME = "log execution message args"


@pytest.fixture(autouse=True)
def shutdown_client():
    bec_client = BECClient()
    bec_client.start()
    yield
    bec_client.shutdown()


@pytest.fixture
def procedure_manager():
    server = MagicMock()
    server.bootstrap_server = "localhost:1"
    with patch(
        "bec_server.scan_server.procedures.manager.RedisConnector",
        partial(RedisConnector, redis_cls=fakeredis.FakeRedis),  # type: ignore
    ):
        manager = ProcedureManager(server, InProcessProcedureWorker)
        yield manager
    manager.shutdown()


@pytest.mark.parametrize(["accepted", "msg"], zip([True, False], ["test true", "test false"]))
def test_ack(procedure_manager: ProcedureManager, accepted: bool, msg: str):
    ps = procedure_manager._conn._redis_conn.pubsub()
    ps.subscribe(procedure_manager._reply_endpoint.endpoint)
    ps.get_message()
    procedure_manager._ack(accepted, msg)
    message = ps.get_message()
    assert message is not None
    data = MsgpackSerialization.loads(message["data"])
    assert isinstance(data, RequestResponseMessage)
    assert data.accepted == accepted
    assert data.message == msg


VALIDATION_TEST_CASES: list[tuple[dict[str, Any], ProcedureRequestMessage | None]] = [
    ({"identifier": LOG_MSG_PROC_NAME}, ProcedureRequestMessage(identifier=LOG_MSG_PROC_NAME)),
    (
        {"identifier": LOG_MSG_PROC_NAME, "queue": "queue2"},
        ProcedureRequestMessage(identifier=LOG_MSG_PROC_NAME, queue="queue2"),
    ),
    ({"identifier": "doesn't exist"}, None),
    ({"incorrect": "arguments"}, None),
]


@pytest.mark.parametrize(["message", "result"], VALIDATION_TEST_CASES)
def test_validate(procedure_manager: ProcedureManager, message, result):
    procedure_manager._ack = MagicMock()
    assert procedure_manager._validate_request(message) == result


PROCESS_REQUEST_TEST_CASES = [
    ProcedureRequestMessage(identifier=LOG_MSG_PROC_NAME),
    ProcedureRequestMessage(identifier=LOG_MSG_PROC_NAME, queue="queue2"),
    ProcedureRequestMessage(identifier="test other procedure", queue="queue2"),
]


@pytest.fixture
def process_request_manager(procedure_manager: ProcedureManager):
    procedure_manager._validate_request = MagicMock(side_effect=lambda msg: msg)
    procedure_manager._ack = MagicMock()
    procedure_manager._conn.rpush = MagicMock()
    procedure_manager.spawn = MagicMock()
    yield procedure_manager


@pytest.mark.parametrize("message", PROCESS_REQUEST_TEST_CASES)
def test_process_request_happy_paths(process_request_manager, message: ProcedureRequestMessage):
    process_request_manager.process_queue_request(message)
    process_request_manager._ack.assert_called_with(True, f"Running procedure {message.identifier}")
    process_request_manager._conn.rpush.assert_called()
    endpoint, execution_msg = process_request_manager._conn.rpush.call_args.args
    queue = message.queue or DEFAULT_QUEUE
    assert queue in endpoint.endpoint
    assert execution_msg.identifier == message.identifier
    process_request_manager.spawn.assert_called()
    assert queue in process_request_manager.active_workers.keys()


def test_process_request_failure(process_request_manager):
    process_request_manager.process_queue_request(None)
    process_request_manager._ack.assert_not_called()
    process_request_manager._conn.rpush.assert_not_called()
    process_request_manager.spawn.assert_not_called()
    assert process_request_manager.active_workers == {}


class UnlockableWorker(ProcedureWorker):
    TEST_TIMEOUT = 10

    def __init__(self, server: str, queue: str):
        super().__init__(server, queue)
        self.event = threading.Event()

    def _setup_execution_environment(self): ...
    def _kill_process(self): ...
    def _run_task(self, item):
        self.event.wait(self.TEST_TIMEOUT)


def _wait_until(predicate: Callable[[], bool], timeout_s: float = 0.1):
    # Yes I know this is actually more like retries than a timeout,
    # it's just to make sure the threads have plenty of chances to switch in the test
    elapsed, step = 0.0, timeout_s / 10
    while not predicate():
        time.sleep(step)
        elapsed += step
        if elapsed > timeout_s:
            raise TimeoutError()


@patch("bec_server.scan_server.procedures.manager.queue_TIMEOUT_S", 1)
@patch("bec_server.scan_server.procedures.worker_base.RedisConnector")
@patch("bec_server.scan_server.procedures.manager.RedisConnector", MagicMock())
def test_spawn(redis_connector, procedure_manager: ProcedureManager):
    procedure_manager._worker_cls = UnlockableWorker
    message = PROCESS_REQUEST_TEST_CASES[0]
    # popping from the list queue should give the execution message
    redis_connector().blocking_list_pop_to_set_add.side_effect = [message, None]
    queue = message.queue or DEFAULT_QUEUE
    procedure_manager._validate_request = MagicMock(side_effect=lambda msg: msg)
    # trigger the running of the test message
    procedure_manager.process_queue_request(message)  # type: ignore
    assert queue in procedure_manager.active_workers.keys()

    # spawn method should be added as a future
    _wait_until(procedure_manager.active_workers[queue]["future"].running)
    # and then create the worker
    _wait_until(lambda: procedure_manager.active_workers[queue].get("worker") is not None)
    worker = procedure_manager.active_workers[queue]["worker"]
    assert isinstance(worker, UnlockableWorker)
    _wait_until(lambda: worker.status == ProcedureWorkerStatus.RUNNING)

    # check that you can't instantiate the same worker twice - call spawn directly to
    # raise the exception in this thread
    with pytest.raises(WorkerAlreadyExists):
        procedure_manager.spawn(queue)

    # queue "timed out" and brpop returns None, so work() will return on the next iteration
    with procedure_manager.lock:
        worker.event.set()  # let the task end and return to ProcedureWorker.work()
        # queue deletion at the end of spawn needs the lock so we can catch it in IDLE
        _wait_until(lambda: worker.status == ProcedureWorkerStatus.IDLE)
    # spawn deletes the worker queue
    _wait_until(lambda: len(procedure_manager.active_workers) == 0)


@patch("bec_server.scan_server.procedures.worker_base.RedisConnector", MagicMock())
@patch("bec_server.scan_server.procedures.in_process_worker.BECClient", MagicMock())
@patch("bec_server.scan_server.procedures.in_process_worker.callable_from_execution_message")
def test_in_process_worker(procedure_function):
    queue = "primary"
    with InProcessProcedureWorker("localhost:1", queue, 1) as worker:
        worker._run_task("wrong type")  # type: ignore
        procedure_function().assert_not_called()
        worker._run_task(ProcedureExecutionMessage(identifier="not builtin", queue=queue))
        procedure_function().assert_not_called()
        worker._run_task(ProcedureExecutionMessage(identifier=LOG_MSG_PROC_NAME, queue=queue))
        procedure_function().assert_called()
        worker._run_task(
            ProcedureExecutionMessage(
                identifier=LOG_MSG_PROC_NAME, args_kwargs=((1, 2, 3), {"foo": "bar"}), queue=queue
            )
        )
        procedure_function().assert_called_with(1, 2, 3, foo="bar")


@patch("bec_server.scan_server.procedures.builtin_procedures.logger")
@patch("bec_server.scan_server.procedures.worker_base.RedisConnector")
def test_builtin_procedure_log_args(_, procedure_logger: MagicMock):
    test_string = "test string for logging as an arg"
    with InProcessProcedureWorker("localhost:1", "primary", 1) as worker:
        worker._run_task(
            ProcedureExecutionMessage(
                identifier="log execution message args",
                queue="primary",
                args_kwargs=((test_string,), {"kwarg": "test"}),
            )
        )
    log_call_arg_0 = procedure_logger.info.call_args.args[0]
    assert test_string in log_call_arg_0
    assert "'kwarg': 'test'" in log_call_arg_0


@patch("bec_server.scan_server.procedures.in_process_worker.BECClient")
@patch("bec_server.scan_server.procedures.worker_base.RedisConnector")
def test_builtin_procedure_scan_execution(_, Client):
    from bec_server.scan_server.procedures.builtin_procedures import run_scan

    run_scan.__annotations__["bec"] = Client
    args = ("samx", -10, 10)
    kwargs = {"steps": 5, "relative": False}
    with InProcessProcedureWorker("localhost:1", "primary", 1) as worker:
        worker._run_task(
            ProcedureExecutionMessage(
                identifier="run scan",
                queue="primary",
                args_kwargs=(("line_scan",), {"args": args, "parameters": kwargs}),
            )
        )
    Client().scans.line_scan.assert_called_with(*args, **kwargs)
