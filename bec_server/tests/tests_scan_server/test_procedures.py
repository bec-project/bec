import threading
import time
from functools import partial
from itertools import starmap
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

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
from bec_server.procedures.constants import PROCEDURE, BecProcedure, WorkerAlreadyExists
from bec_server.procedures.in_process_worker import InProcessProcedureWorker
from bec_server.procedures.manager import ProcedureManager
from bec_server.procedures.procedure_registry import (
    _BUILTIN_PROCEDURES,
    ProcedureRegistryError,
    callable_from_execution_message,
    register,
)
from bec_server.procedures.worker_base import ProcedureWorker

# pylint: disable=protected-access
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=redefined-outer-name


LOG_MSG_PROC_NAME = "log execution message args"
FAKEREDIS_HOST = "127.0.0.1"
FAKEREDIS_PORT = 6380


def _eq_except_id(
    a: ProcedureExecutionMessage | ProcedureRequestMessage,
    b: ProcedureExecutionMessage | ProcedureRequestMessage,
):
    return a.identifier == b.identifier and a.queue == b.queue and a.args_kwargs == b.args_kwargs


@pytest.fixture(autouse=True)
@patch("bec_lib.bec_service.BECAccess", MagicMock)
def shutdown_client():
    bec_client = BECClient(
        config=ServiceConfig(config={"redis": {"host": FAKEREDIS_HOST, "port": FAKEREDIS_PORT}}),
        connector_cls=partial(RedisConnector, redis_cls=fakeredis.FakeRedis),  # type: ignore
    )
    bec_client.start()
    yield bec_client
    bec_client.shutdown()


class ShortLifetimeWorker(InProcessProcedureWorker):
    def __init__(self, server: str, queue: str, lifetime_s: float | None = None):
        super().__init__(server, queue, lifetime_s)
        self._lifetime_s = 0.1

    def _kill_process(self):
        self.bec_client.shutdown()
        super()._kill_process()


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
        manager = ProcedureManager(f"{FAKEREDIS_HOST}:{FAKEREDIS_PORT}", ShortLifetimeWorker)  # type: ignore
        yield manager
    manager.shutdown()


def test_helper_log_streams(procedure_manager):
    conn = procedure_manager._conn
    helper = FrontendProcedureHelper(conn)
    conn.xadd(MessageEndpoints.procedure_logs("queue1"), {"data": RawMessage(data=str("data"))})
    conn.xadd(MessageEndpoints.procedure_logs("queue2"), {"data": RawMessage(data=str("data"))})
    assert helper.get.log_queue_names() == ["queue1", "queue2"]


@pytest.mark.parametrize(["accepted", "msg"], zip([True, False], ["test true", "test false"]))
def test_ack(procedure_manager: ProcedureManager, accepted: bool, msg: str):
    ps = procedure_manager._conn._redis_conn.pubsub()
    ps.subscribe(procedure_manager._reply_endpoint.endpoint)
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
    ({"incorrect": "arguments"}, None),
]


@pytest.mark.parametrize(["message", "result"], VALIDATION_TEST_CASES)
def test_validate(procedure_manager: ProcedureManager, message, result):
    procedure_manager._ack = MagicMock()
    validated = procedure_manager._validate_request(message)
    if validated is None:
        assert result is None
    else:
        assert _eq_except_id(validated, result)


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
    process_request_manager._process_queue_request(message)
    process_request_manager._ack.assert_called_with(
        True, f"Running procedure {message.identifier}", message.execution_id
    )
    process_request_manager._conn.rpush.assert_called()
    endpoint, execution_msg = process_request_manager._conn.rpush.call_args.args
    queue = message.queue or PROCEDURE.WORKER.DEFAULT_QUEUE
    assert queue in endpoint.endpoint
    assert execution_msg.identifier == message.identifier
    process_request_manager.spawn.assert_called()
    assert queue in process_request_manager._active_workers.keys()


def test_process_request_failure(process_request_manager):
    process_request_manager._process_queue_request(None)
    process_request_manager._ack.assert_not_called()
    process_request_manager._conn.rpush.assert_not_called()
    process_request_manager.spawn.assert_not_called()
    assert process_request_manager._active_workers == {}


class UnlockableWorker(ProcedureWorker):
    TEST_TIMEOUT = 10

    def __init__(self, server: str, queue: str, lifetime_s: int | None = None, execution_id="test"):
        super().__init__(server, queue, lifetime_s)
        self.event_1 = threading.Event()
        self.event_2 = threading.Event()
        self.execution_id = execution_id

    def abort_execution(self, execution_id: str): ...
    def _setup_execution_environment(self): ...
    def _kill_process(self): ...
    def _run_task(self, item):
        self.status = ProcedureWorkerStatus.RUNNING
        self._helper.status_update(self.execution_id, "Started")
        self.event_1.wait(self.TEST_TIMEOUT)
        self.status = ProcedureWorkerStatus.IDLE
        self._helper.status_update(self.execution_id, "Finished")
        self.event_2.wait(self.TEST_TIMEOUT)


def _wait_until(predicate: Callable[[], bool], timeout_s: float = 0.1):
    # Yes I know this is actually more like retries than a timeout,
    # it's just to make sure the threads have plenty of chances to switch in the test
    elapsed, step = 0.0, timeout_s / 10
    while not predicate():
        time.sleep(step)
        elapsed += step
        if elapsed > timeout_s:
            raise TimeoutError()


@patch("bec_server.procedures.worker_base.RedisConnector")
@patch("bec_server.procedures.manager.RedisConnector", MagicMock())
def test_spawn(redis_connector, procedure_manager: ProcedureManager):
    procedure_manager._worker_cls = UnlockableWorker
    message = PROCESS_REQUEST_TEST_CASES[0]
    # popping from the list queue should give the execution message
    redis_connector().blocking_list_pop_to_set_add.side_effect = [message, None]
    queue = message.queue or PROCEDURE.WORKER.DEFAULT_QUEUE
    procedure_manager._validate_request = MagicMock(side_effect=lambda msg: msg)
    # trigger the running of the test message
    procedure_manager._process_queue_request(message)  # type: ignore
    assert queue in procedure_manager._active_workers.keys()

    # spawn method should be added as a future
    _wait_until(procedure_manager._active_workers[queue]["future"].running)
    # and then create the worker
    _wait_until(lambda: procedure_manager._active_workers[queue].get("worker") is not None)
    worker = procedure_manager._active_workers[queue]["worker"]
    assert isinstance(worker, UnlockableWorker)
    _wait_until(lambda: worker.status == ProcedureWorkerStatus.RUNNING)

    # check that you can't instantiate the same worker twice - call spawn directly to
    # raise the exception in this thread
    with pytest.raises(WorkerAlreadyExists):
        procedure_manager.spawn(queue)

    # queue "timed out" and brpop returns None, so work() will return on the next iteration
    with procedure_manager.lock:
        worker.event_1.set()  # let the task end and return to ProcedureWorker.work()
        # queue deletion callback needs the lock so we can catch it in FINISHED
        _wait_until(lambda: worker.status == ProcedureWorkerStatus.IDLE)
        worker.event_2.set()
        _wait_until(lambda: worker.status == ProcedureWorkerStatus.FINISHED)
    # spawn deletes the worker queue
    _wait_until(lambda: len(procedure_manager._active_workers) == 0)


@patch("bec_server.procedures.worker_base.RedisConnector", MagicMock())
@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock())
@patch("bec_server.procedures.in_process_worker.callable_from_execution_message")
def test_in_process_worker(procedure_function):
    queue = "primary"
    kwargs = {"queue": queue, "execution_id": "test", "metadata": {}}
    with ShortLifetimeWorker("localhost:1", queue, 1) as worker:
        worker._run_task("wrong type")  # type: ignore
        procedure_function().assert_not_called()
        worker._run_task(ProcedureExecutionMessage(identifier="not builtin", **kwargs))
        procedure_function().assert_not_called()
        worker._run_task(ProcedureExecutionMessage(identifier=LOG_MSG_PROC_NAME, **kwargs))
        procedure_function().assert_called()
        worker._run_task(
            ProcedureExecutionMessage(
                identifier=LOG_MSG_PROC_NAME, args_kwargs=((1, 2, 3), {"foo": "bar"}), **kwargs
            )
        )
        procedure_function().assert_called_with(1, 2, 3, foo="bar")


@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock)
@patch("bec_server.procedures.builtin_procedures.logger")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_builtin_procedure_log_args(_, procedure_logger: MagicMock):
    test_string = "test string for logging as an arg"
    with ShortLifetimeWorker("localhost:1", "primary", 1) as worker:
        worker._run_task(
            ProcedureExecutionMessage(
                identifier="log execution message args",
                queue="primary",
                args_kwargs=((test_string,), {"kwarg": "test"}),
                execution_id="1234",
            )
        )
    log_call_arg_0 = procedure_logger.success.call_args.args[0]
    assert test_string in log_call_arg_0
    assert "'kwarg': 'test'" in log_call_arg_0


@patch("bec_server.procedures.in_process_worker.BECIPythonClient")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_builtin_procedure_scan_execution(_, Client):
    from bec_server.procedures.builtin_procedures import run_scan

    run_scan.__annotations__["bec"] = Client
    args = ("samx", -10, 10)
    kwargs = {"steps": 5, "relative": False}
    with InProcessProcedureWorker("localhost:1", "primary", 1) as worker:
        worker._run_task(
            ProcedureExecutionMessage(
                identifier="run scan",
                queue="primary",
                args_kwargs=(("line_scan",), {"args": args, "parameters": kwargs}),
                execution_id="1234",
            )
        )
    Client().scans.line_scan.assert_called_with(*args, **kwargs)


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


def _yield_once():
    yield "value"
    while True:
        yield None


@patch(
    "bec_server.procedures.worker_base.RedisConnector",
    side_effect=lambda *_: MagicMock(
        blocking_list_pop_to_set_add=MagicMock(side_effect=_yield_once())
    ),
)
def test_manager_status_api(_conn, procedure_manager):
    procedure_manager._worker_cls = UnlockableWorker
    for message in PROCESS_REQUEST_TEST_CASES:
        procedure_manager._process_queue_request(message)
    _wait_until(lambda: procedure_manager.active_workers() == ["primary", "queue2"])
    _wait_until(
        lambda: procedure_manager.worker_statuses()
        == {"primary": ProcedureWorkerStatus.RUNNING, "queue2": ProcedureWorkerStatus.RUNNING}
    )
    for w in procedure_manager._active_workers.values():
        w["worker"].event_1.set()
    _wait_until(
        lambda: procedure_manager.worker_statuses()
        == {"primary": ProcedureWorkerStatus.IDLE, "queue2": ProcedureWorkerStatus.IDLE}
    )
    for w in procedure_manager._active_workers.values():
        w["worker"].event_2.set()
    _wait_until(lambda: procedure_manager.active_workers() == [])


_ManagerWithMsgs = tuple[ProcedureManager, list[ProcedureExecutionMessage]]


@pytest.fixture
def manager_with_test_msgs(procedure_manager: ProcedureManager):
    procedure_manager._worker_cls = MagicMock
    procedure_manager._conn._redis_conn.flushdb()
    contents = [
        ("test_identifier_1", "queue1", ((), {})),
        ("test_identifier_2", "queue1", ((), {})),
        ("test_identifier_1", "queue2", ((), {})),
        ("test_identifier_2", "queue2", ((), {})),
    ]
    msgs = iter(
        ProcedureRequestMessage(identifier=c[0], queue=c[1], args_kwargs=c[2]) for c in contents
    )
    procedure_manager._validate_request = lambda msg: next(msgs)
    for _ in range(len(contents)):
        procedure_manager._process_queue_request({})
    yield (
        procedure_manager,
        [
            ProcedureExecutionMessage(
                metadata={}, identifier=c[0], queue=c[1], args_kwargs=c[2], execution_id=str(i)
            )
            for i, c in enumerate(contents)
        ],
    )


def _all_eq_except_id(a: list[ProcedureExecutionMessage], b: list[ProcedureExecutionMessage]):
    if len(a) != len(b):
        return False
    return all(starmap(_eq_except_id, zip(a, b)))


@pytest.mark.parametrize("queue", ["queue1", "queue2"])
@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock())
def test_startup(manager_with_test_msgs: _ManagerWithMsgs, queue: str):
    procedure_manager, expected = manager_with_test_msgs
    procedure_manager._worker_cls = MagicMock
    queue_expected = list(filter(lambda msg: msg.queue == queue, expected))

    execution_list = procedure_manager._helper.get.exec_queue(queue)
    assert _all_eq_except_id(execution_list, queue_expected)

    procedure_manager._startup()

    # on startup, the manager should move active queues to unhandled queues
    execution_list = procedure_manager._helper.get.exec_queue(queue)
    unhandled_execution_list = procedure_manager._conn.lrange(
        MessageEndpoints.unhandled_procedure_execution(queue), 0, -1
    )
    assert execution_list == []
    assert _all_eq_except_id(unhandled_execution_list, queue_expected)


@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock())
def test_abort_queue(manager_with_test_msgs: _ManagerWithMsgs):
    procedure_manager, expected = manager_with_test_msgs
    remaining_expected = list(filter(lambda msg: msg.queue == "queue2", expected))
    aborted_expected = list(filter(lambda msg: msg.queue == "queue1", expected))

    q1_execution_list = procedure_manager._helper.get.exec_queue("queue1")
    assert _all_eq_except_id(q1_execution_list, aborted_expected)
    q2_execution_list = procedure_manager._helper.get.exec_queue("queue2")
    assert _all_eq_except_id(q2_execution_list, remaining_expected)

    procedure_manager._process_abort({"queue": "queue1"})

    # on abort, the manager should move active queues to unhandled queues
    # this should happen for q1 and not q2
    q2_execution_list = procedure_manager._helper.get.exec_queue("queue2")
    unhandled_execution_list = procedure_manager._conn.lrange(
        MessageEndpoints.unhandled_procedure_execution("queue1"), 0, -1
    )
    assert _all_eq_except_id(q2_execution_list, remaining_expected)
    assert _all_eq_except_id(unhandled_execution_list, aborted_expected)


@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock())
def test_abort_individual(manager_with_test_msgs: _ManagerWithMsgs):
    procedure_manager, expected = manager_with_test_msgs
    q1_expected = list(filter(lambda msg: msg.queue == "queue1", expected))
    q2_expected = list(filter(lambda msg: msg.queue == "queue2", expected))

    q1_execution_list = procedure_manager._helper.get.exec_queue("queue1")
    assert _all_eq_except_id(
        q1_execution_list, list(filter(lambda msg: msg.queue == "queue1", q1_expected))
    )
    q2_execution_list = procedure_manager._helper.get.exec_queue("queue2")
    assert _all_eq_except_id(q2_execution_list, q2_expected)

    procedure_manager._process_abort({"execution_id": q2_execution_list[1].execution_id})

    q1_execution_list = procedure_manager._helper.get.exec_queue("queue1")
    q2_execution_list = procedure_manager._helper.get.exec_queue("queue2")
    assert _all_eq_except_id(q1_execution_list, q1_expected)
    assert _all_eq_except_id(q2_execution_list, [q2_expected[0]])


@patch("bec_server.procedures.in_process_worker.callable_from_execution_message", lambda *_: True)
@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock)
def test_abort_all(manager_with_test_msgs: _ManagerWithMsgs):
    procedure_manager, expected = manager_with_test_msgs
    q1_expected = list(filter(lambda msg: msg.queue == "queue1", expected))
    q2_expected = list(filter(lambda msg: msg.queue == "queue2", expected))

    q1_execution_list = procedure_manager._helper.get.exec_queue("queue1")
    assert _all_eq_except_id(
        q1_execution_list, list(filter(lambda msg: msg.queue == "queue1", q1_expected))
    )
    q2_execution_list = procedure_manager._helper.get.exec_queue("queue2")
    assert _all_eq_except_id(q2_execution_list, q2_expected)

    procedure_manager._process_abort({"abort_all": True})

    q1_execution_list = procedure_manager._helper.get.exec_queue("queue1")
    q2_execution_list = procedure_manager._helper.get.exec_queue("queue2")
    q1_unhandled_list = procedure_manager._helper.get.unhandled_queue("queue1")
    q2_unhandled_list = procedure_manager._helper.get.unhandled_queue("queue2")

    assert q1_execution_list == []
    assert q2_execution_list == []
    assert _all_eq_except_id(q1_unhandled_list, q1_expected)
    assert _all_eq_except_id(q2_unhandled_list, q2_expected)


@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock)
def test_procedure_status_rejected(procedure_manager):
    status = procedure_manager._helper.request.procedure("doesn't exist")
    assert status.state == ProcedureState.REQUESTED
    _wait_until(lambda: status.state == ProcedureState.REJECTED)
    assert status.done


@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock)
def test_procedure_status_accepted(procedure_manager):
    procedure_manager._worker_cls = UnlockableWorker
    msg = ProcedureRequestMessage(
        identifier="sleep", args_kwargs=((), {"time_s": 0.5}), execution_id="test"
    )
    status = procedure_manager._helper.request._procedure(msg)
    assert status.state == ProcedureState.REQUESTED
    _wait_until(lambda: procedure_manager._active_workers.get("primary") is not None, timeout_s=1)
    worker = procedure_manager._active_workers["primary"]["worker"]
    assert isinstance(worker, UnlockableWorker)
    worker.event_1.set()
    _wait_until(lambda: status.state == ProcedureState.RUNNING, timeout_s=2)
    worker.event_2.set()
    _wait_until(lambda: status.state == ProcedureState.SUCCESS, timeout_s=2)


def _mock_error_procedure(*args, **kwargs):
    raise RuntimeError("Encountered error in procedure")


@patch("bec_server.procedures.in_process_worker.BECIPythonClient", MagicMock)
def test_procedure_status_error(procedure_manager):
    register("error", _mock_error_procedure)
    msg = ProcedureRequestMessage(identifier="error", execution_id="test")
    status = procedure_manager._helper.request._procedure(msg)
    assert status.state == ProcedureState.REQUESTED
    _wait_until(lambda: procedure_manager._active_workers.get("primary") is not None, timeout_s=1)
    worker = procedure_manager._active_workers["primary"]["worker"]
    assert isinstance(worker, ShortLifetimeWorker)
    with pytest.raises(RuntimeError) as e:
        status.wait(timeout_s=2, raise_on_failure=True)

    e.match("error in procedure")
    assert status.error is not None
