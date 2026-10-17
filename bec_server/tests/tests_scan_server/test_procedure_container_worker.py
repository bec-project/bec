import os
import time
from itertools import repeat
from threading import Event, Thread
from time import sleep
from unittest.mock import MagicMock, call, patch

import pytest

from bec_lib.messages import ProcedureWorkerStatus, ProcedureWorkerStatusMessage
from bec_server.procedures import procedure_registry
from bec_server.procedures.constants import PodmanContainerStates
from bec_server.procedures.container_worker import ContainerProcedureWorker
from bec_server.procedures.oop_worker_base import main as container_worker_main
from bec_server.procedures.protocol import ContainerCommandBackend


@patch(
    "bec_server.procedures.container_worker.get_backend",
    lambda: MagicMock(spec=ContainerCommandBackend),
)
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_container_worker_init(redis_mock):
    redis_mock().host = "server"
    redis_mock().port = "port"
    worker = ContainerProcedureWorker(server="server:port", queue="test_queue", lifetime_s=1)
    assert worker._worker_environment() == {
        "redis_server": "server:port",
        "queue": "test_queue",
        "timeout_s": "1",
    }


@patch(
    "bec_server.procedures.container_worker.get_backend",
    MagicMock(return_value=MagicMock(spec=ContainerCommandBackend)),
)
@patch("bec_server.procedures.oop_worker_base.logger")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_container_worker_work(redis_mock, logger_mock):
    redis_mock().host = "server"
    redis_mock().port = "port"

    msgs = [
        ProcedureWorkerStatusMessage(
            worker_queue="test_queue",
            status=ProcedureWorkerStatus.RUNNING,
            current_execution_id="test",
        ),
        ProcedureWorkerStatusMessage(
            worker_queue="test_queue", status=ProcedureWorkerStatus.FINISHED
        ),
    ]

    def _mock_pop():
        yield from msgs
        while True:
            yield from repeat(None)

    mock_pop = _mock_pop()

    redis_mock().blocking_list_pop.side_effect = lambda *_, **__: next(mock_pop)
    worker = ContainerProcedureWorker(server="server:port", queue="test_queue", lifetime_s=1)

    t = Thread(target=worker.work)

    def cleanup():
        worker._backend.state.return_value = PodmanContainerStates.EXITED
        t.join()

    t.start()
    start = time.monotonic()
    while time.monotonic() < start + 1000:
        try:
            assert (
                logger_mock.info.call_args_list[0].args[0]
                == "Container worker 'test_queue' status update: RUNNING"
            )
            assert (
                logger_mock.info.call_args_list[1].args[0]
                == "Container worker 'test_queue' status update: FINISHED"
            )
            break
        except IndexError:  # we never want to hang here forever
            ...
        except AssertionError:
            cleanup()
            break
        sleep(0.1)
    else:
        cleanup()
        raise TimeoutError(f"Received log calls: {logger_mock.info.call_args_list}")

    cleanup()


@patch("bec_server.procedures.oop_worker_base.logger")
def test_main_exits_without_env_variables(logger_mock):
    with patch.dict(os.environ, clear=True), pytest.raises(SystemExit):
        container_worker_main()
    assert "Missing environment variable " in logger_mock.error.call_args.args[0]


@patch("bec_server.procedures.oop_worker_base.logger")
def test_main_continues_with_env_variables(logger_mock):
    with pytest.raises(ValueError) as e:
        with patch.dict(
            os.environ,
            values={"redis_server": "str", "queue": "str", "timeout_s": "int"},
            clear=True,
        ):
            container_worker_main()
    # should get stuck trying to open a redis connection to "str"
    assert e.match("not enough values to unpack")
    logger_mock.error.assert_not_called()
    logger_mock.info.assert_called()


class MockItem:
    def __init__(self, name: str, args: tuple = (), kwargs: dict = {}):
        self.identifier = name
        self.args = args
        self.kwargs = kwargs
        self.execution_id = f"test_{name}"

    def __repr__(self) -> str:
        return self.identifier

    @property
    def args_kwargs(self):
        return (self.args, self.kwargs)


@patch(
    "bec_server.procedures.container_worker.get_backend",
    MagicMock(return_value=MagicMock(spec=ContainerCommandBackend)),
)
@patch("bec_server.procedures.oop_worker_base.BECIPythonClient", MagicMock())
@patch("bec_server.procedures.oop_worker_base.logger")
@patch("bec_server.procedures.oop_worker_base.RedisConnector")
@patch("bec_server.procedures.oop_worker_base.procedure_registry")
def test_main_running(registry_mock, redis_mock, logger_mock):
    redis_mock().blocking_list_pop_to_set_add.side_effect = [MockItem("1"), MockItem("2"), None]
    function_recorder = MagicMock()
    registry_mock.callable_from_execution_message.return_value = lambda *args, **kwargs: (
        function_recorder(*args, **kwargs)
    )
    with patch.dict(
        os.environ,
        values={"redis_server": "host:1111", "queue": "str", "timeout_s": "10"},
        clear=True,
    ):
        container_worker_main()
    logger_mock.debug.assert_has_calls(
        [call("running task 1"), call("running task 2")], any_order=True
    )
    logger_mock.success.assert_called_with("Procedure runner shutting down")


@patch(
    "bec_server.procedures.container_worker.get_backend",
    MagicMock(return_value=MagicMock(spec=ContainerCommandBackend)),
)
@patch("bec_server.procedures.oop_worker_base.BECIPythonClient", MagicMock())
@patch("bec_server.procedures.oop_worker_base.logger")
@patch("bec_server.procedures.oop_worker_base.RedisConnector")
def test_main_running_newly_registered_proc(redis_mock, logger_mock):
    PROC_NAME = "new_test_procedure"

    redis_mock().blocking_list_pop_to_set_add.side_effect = [
        MockItem(PROC_NAME, ("a",), {"b": "c"}),
        None,
    ]
    function_recorder = MagicMock()
    procedure_registry.register(
        PROC_NAME, lambda *args, **kwargs: function_recorder(*args, **kwargs)
    )

    with patch.dict(
        os.environ,
        values={"redis_server": "host:1111", "queue": "str", "timeout_s": "10"},
        clear=True,
    ):
        container_worker_main()
    logger_mock.debug.assert_has_calls([call(f"running task {PROC_NAME}")], any_order=True)
    function_recorder.assert_called_once_with("a", b="c")
    logger_mock.success.assert_called_with("Procedure runner shutting down")


@patch("bec_server.procedures.container_worker.get_backend")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_container_worker_uses_supplied_environment(redis_mock, backend_mock):
    worker = ContainerProcedureWorker(
        server="127.0.0.1:20772",
        queue="test",
        env={"redis_server": "host.containers.internal:20772"},
    )
    assert backend_mock().run.call_args.args[1]["redis_server"] == "host.containers.internal:20772"
    assert worker._worker_environment()["redis_server"] == "127.0.0.1:20772"


@patch("bec_server.procedures.container_worker.get_backend")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_container_worker_setup_failure_closes_connector(redis_mock, backend_mock):
    backend_mock().run.side_effect = RuntimeError("missing pod")
    with pytest.raises(RuntimeError, match="missing pod"):
        ContainerProcedureWorker(server="localhost:6379", queue="test")
    redis_mock().shutdown.assert_called_once()


@patch("bec_server.procedures.container_worker.get_backend")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_container_worker_early_exit_reports_logs(redis_mock, backend_mock):
    from bec_server.procedures.constants import ProcedureWorkerError

    redis_mock().blocking_list_pop.return_value = None
    backend_mock().state.return_value = PodmanContainerStates.EXITED
    backend_mock().logs.return_value = ["Cannot connect to Redis"]
    worker = ContainerProcedureWorker(server="localhost:6379", queue="test")
    with pytest.raises(ProcedureWorkerError, match="Cannot connect to Redis"):
        worker.work()
    assert worker.status == ProcedureWorkerStatus.DEAD


@patch("bec_server.procedures.container_worker.get_backend")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_container_worker_drains_final_status_before_exit(redis_mock, backend_mock):
    redis_mock().blocking_list_pop.side_effect = [
        ProcedureWorkerStatusMessage(worker_queue="test", status=ProcedureWorkerStatus.FINISHED),
        None,
    ]
    backend_mock().state.return_value = PodmanContainerStates.EXITED
    worker = ContainerProcedureWorker(server="localhost:6379", queue="test")
    worker.work()
    assert worker.status == ProcedureWorkerStatus.FINISHED


@patch("bec_server.procedures.container_worker.get_backend")
@patch("bec_server.procedures.worker_base.RedisConnector")
def test_abort_and_context_exit_serialize_connector_shutdown(redis_mock, backend_mock):
    worker = ContainerProcedureWorker(server="localhost:6379", queue="test")
    closing = Event()
    release = Event()
    exit_started = Event()
    concurrent_close = Event()
    errors = []

    def close_connector():
        if closing.is_set():
            concurrent_close.set()
            raise RuntimeError("concurrent Redis connector shutdown")
        closing.set()
        try:
            assert release.wait(2)
        finally:
            closing.clear()

    def abort():
        try:
            worker.abort()
        except BaseException as exc:
            errors.append(exc)

    def exit_context():
        exit_started.set()
        try:
            worker.__exit__(None, None, None)
        except BaseException as exc:
            errors.append(exc)

    redis_mock().shutdown.side_effect = close_connector
    worker._kill_process = MagicMock()
    abort_thread = Thread(target=abort)
    exit_thread = Thread(target=exit_context)
    abort_thread.start()
    try:
        assert closing.wait(2)
        exit_thread.start()
        assert exit_started.wait(2)
        assert not concurrent_close.wait(0.1)
    finally:
        release.set()
        abort_thread.join(2)
        if exit_thread.ident is not None:
            exit_thread.join(2)
    assert not abort_thread.is_alive()
    assert not exit_thread.is_alive()
    assert errors == []
    worker._kill_process.assert_called()


@patch("bec_server.procedures.container_worker.get_backend")
@patch("bec_server.procedures.worker_base.RedisConnector")
@pytest.mark.parametrize("abort", [True, False])
def test_connector_shutdown_failure_still_stops_worker(redis_mock, backend_mock, abort):
    worker = ContainerProcedureWorker(server="localhost:6379", queue="test")
    redis_mock().shutdown.side_effect = RuntimeError("failed to close Redis")
    worker._kill_process = MagicMock()
    with pytest.raises(RuntimeError, match="failed to close Redis"):
        if abort:
            worker.abort()
        else:
            worker.__exit__(None, None, None)
    worker._kill_process.assert_called_once()


def test_cancel_before_attached_start_removes_created_container():
    worker = ContainerProcedureWorker.__new__(ContainerProcedureWorker)
    worker._worker_id = "worker"
    worker.container_name = "pending-worker"
    worker._backend = MagicMock()
    worker._backend.state.return_value = PodmanContainerStates.CREATED
    worker._process = MagicMock()
    exists = {"container": True}

    def remove(name):
        assert name == "pending-worker"
        exists["container"] = False

    def delayed_start(timeout):
        assert not exists["container"], "The pending start could still launch the container"
        return 125

    worker._backend.remove.side_effect = remove
    worker._process.wait.side_effect = delayed_start
    worker._kill_process()
    worker._backend.remove.assert_called_once_with("pending-worker")
    worker._process.stdin.close.assert_called_once()
