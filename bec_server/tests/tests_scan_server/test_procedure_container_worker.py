import os
import time
from itertools import repeat
from threading import Thread
from time import sleep
from unittest.mock import MagicMock, call, patch

import pytest

from bec_lib.messages import ProcedureWorkerStatus, ProcedureWorkerStatusMessage
from bec_server.scan_server.procedures import procedure_registry
from bec_server.scan_server.procedures.constants import PodmanContainerStates
from bec_server.scan_server.procedures.container_worker import (
    ContainerProcedureWorker,
    ContainerWorkerEnv,
)
from bec_server.scan_server.procedures.container_worker import main as container_worker_main
from bec_server.scan_server.procedures.protocol import ContainerCommandBackend
from bec_server.scan_server.procedures.worker_base import ProcedureWorker


@patch(
    "bec_server.scan_server.procedures.container_worker.get_backend",
    lambda: MagicMock(spec=ContainerCommandBackend),
)
@patch("bec_server.scan_server.procedures.worker_base.RedisConnector")
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
    "bec_server.scan_server.procedures.container_worker.get_backend",
    MagicMock(return_value=MagicMock(spec=ContainerCommandBackend)),
)
@patch("bec_server.scan_server.procedures.container_worker.logger")
@patch("bec_server.scan_server.procedures.worker_base.RedisConnector")
def test_container_worker_work(redis_mock, logger_mock):
    redis_mock().host = "server"
    redis_mock().port = "port"

    def _mock_pop():
        msgs = [
            ProcedureWorkerStatusMessage(
                worker_queue="test_queue", status=ProcedureWorkerStatus.RUNNING
            ),
            ProcedureWorkerStatusMessage(
                worker_queue="test_queue", status=ProcedureWorkerStatus.FINISHED
            ),
        ]
        for msg in msgs:
            sleep(0.05)
            yield msg
        while True:
            sleep(0.05)
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
    while time.monotonic() < start + 5:
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


@patch("bec_server.scan_server.procedures.container_worker.logger")
def test_main_exits_without_env_variables(logger_mock):
    with patch.dict(os.environ, clear=True):
        container_worker_main()
    assert "Missing environment variable " in logger_mock.error.call_args.args[0]


@patch("bec_server.scan_server.procedures.container_worker.logger")
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

    def __repr__(self) -> str:
        return self.identifier

    @property
    def args_kwargs(self):
        return (self.args, self.kwargs)


@patch(
    "bec_server.scan_server.procedures.container_worker.get_backend",
    MagicMock(return_value=MagicMock(spec=ContainerCommandBackend)),
)
@patch("bec_server.scan_server.procedures.container_worker.BECClient", MagicMock())
@patch("bec_server.scan_server.procedures.container_worker.logger")
@patch("bec_server.scan_server.procedures.container_worker.RedisConnector")
@patch("bec_server.scan_server.procedures.container_worker.procedure_registry")
def test_main_running(registry_mock, redis_mock, logger_mock):
    redis_mock().blocking_list_pop_to_set_add.side_effect = [MockItem("1"), MockItem("2"), None]
    function_recorder = MagicMock()
    registry_mock.callable_from_execution_message.return_value = (
        lambda *args, **kwargs: function_recorder(*args, **kwargs)
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
    logger_mock.success.assert_called_with("Container runner shutting down")


@patch(
    "bec_server.scan_server.procedures.container_worker.get_backend",
    MagicMock(return_value=MagicMock(spec=ContainerCommandBackend)),
)
@patch("bec_server.scan_server.procedures.container_worker.BECClient", MagicMock())
@patch("bec_server.scan_server.procedures.container_worker.logger")
@patch("bec_server.scan_server.procedures.container_worker.RedisConnector")
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
    logger_mock.success.assert_called_with("Container runner shutting down")
