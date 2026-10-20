"""Exercise subprocess output with real child processes and isolated Redis."""

from unittest.mock import patch

import fakeredis
import pytest

from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import RedisConnector
from bec_server.procedures.subprocess_worker import SubProcessWorker
from bec_server.test.helpers import wait_until


@pytest.fixture
def subprocess_worker(tmp_path, monkeypatch):
    server = fakeredis.FakeServer()
    connections = []

    def connect(*args, **kwargs):
        conn = RedisConnector(
            "localhost:6379", redis_cls=lambda **kw: fakeredis.FakeRedis(server=server, **kw)
        )
        connections.append(conn)
        return conn

    worker_file = tmp_path / "worker.py"
    monkeypatch.setattr(SubProcessWorker, "WORKER_FILE", str(worker_file))
    monkeypatch.setattr("bec_server.procedures.worker_base.RedisConnector", connect)
    monkeypatch.setattr("bec_server.procedures.subprocess_worker.RedisConnector", connect)
    workers = []

    def create(script, **kwargs):
        worker_file.write_text(script, encoding="utf-8")
        worker = SubProcessWorker("localhost:6379", "output", **kwargs)
        workers.append(worker)
        return worker

    yield create
    for worker in workers:
        worker.abort()
        assert not worker._output_thread.is_alive()
    for conn in connections:
        conn.shutdown()


def redis_output(worker):
    entries = worker._conn.xrange(MessageEndpoints.procedure_logs("output"), "-", "+") or []
    return "".join(entry["data"].data for entry in entries)


def test_streams_both_outputs_before_exit(subprocess_worker):
    worker = subprocess_worker(
        "import sys, time\n"
        "sys.stdout.write('stdout without newline')\n"
        "sys.stderr.write('stderr without newline')\n"
        "time.sleep(60)\n"
    )
    wait_until(lambda: "stderr without newline" in redis_output(worker), timeout_s=5)
    assert worker._process.poll() is None
    assert redis_output(worker) == "stdout without newlinestderr without newline"
    assert worker.logs() == ["stdout without newlinestderr without newline"]


@pytest.mark.parametrize("output_encoding", ["utf-8", "ascii"])
def test_verbose_output_does_not_block(subprocess_worker, monkeypatch, output_encoding):
    monkeypatch.setenv("PYTHONIOENCODING", output_encoding)
    worker = subprocess_worker(
        "import sys\n"
        "for _ in range(100):\n"
        "    sys.stdout.write('a' * 4096)\n"
        "    sys.stderr.write('b' * 4096)\n"
        "sys.stdout.write(' Unicode: λ完\\n')\n"
    )
    return_code = worker._process.wait(timeout=10)
    captured = "\n".join(worker.logs())
    assert return_code == 0, captured[-4000:]
    assert captured.count("a") == 100 * 4096
    assert captured.count("b") == 100 * 4096
    assert captured.endswith(" Unicode: λ完")
    assert redis_output(worker).rstrip("\n") == captured
    assert worker.logs() == [captured]


def test_redis_failure_still_drains_output(subprocess_worker):
    with patch(
        "bec_lib.redis_connector.RedisConnector.xadd", side_effect=RuntimeError("Redis unavailable")
    ):
        worker = subprocess_worker("import sys\nsys.stderr.write('x' * 300000)\n")
        assert worker._process.wait(timeout=10) == 0
        assert worker.logs() == ["x" * 300000]


def test_restart_preserves_output_and_stops_previous_reader(subprocess_worker):
    worker = subprocess_worker("import time\nprint('started')\ntime.sleep(60)\n")
    wait_until(lambda: "started" in redis_output(worker), timeout_s=5)
    old_reader = worker._output_thread
    worker._current_execution_id = "running"
    worker.abort_execution("running")
    wait_until(lambda: redis_output(worker).count("started") == 2, timeout_s=5)
    assert not old_reader.is_alive()
    assert worker._output_thread.is_alive()
    assert "\n".join(worker.logs()).count("started") == 2


def test_split_utf8_output_is_preserved(subprocess_worker):
    worker = subprocess_worker(
        "import os, time\n"
        "os.write(1, b'\\xe2')\n"
        "time.sleep(0.1)\n"
        "os.write(2, b'\\x82\\xac')\n"
    )
    assert worker._process.wait(timeout=5) == 0
    assert worker.logs() == ["€"]
    assert redis_output(worker) == "€"


def test_incomplete_utf8_is_flushed_at_eof(subprocess_worker):
    worker = subprocess_worker("import os\nos.write(2, b'\\xe2')\n")
    assert worker._process.wait(timeout=5) == 0
    assert worker.logs() == ["�"]
    assert redis_output(worker) == "�"


def test_bootstrap_uses_only_stdin(subprocess_worker):
    worker = subprocess_worker(
        "import json, os, sys\n"
        "identity = json.loads(sys.stdin.readline())\n"
        "assert identity == {'username': 'alice', 'token': 'private-token'}\n"
        "assert 'private-token' not in repr(dict(os.environ))\n"
        "print('bootstrap received', flush=True)\n",
        worker_id="bootstrap",
        credentials={"username": "alice", "token": "private-token"},
    )
    assert worker._process.wait(timeout=5) == 0
    assert "bootstrap received" in "\n".join(worker.logs())
    assert "private-token" not in redis_output(worker)
    assert "private-token" not in repr(worker._process.args)
    assert not worker._stderr_thread.is_alive()


def test_failed_bootstrap_stops_process_and_readers(subprocess_worker):
    import subprocess

    processes = []
    popen = subprocess.Popen

    def record(*args, **kwargs):
        process = popen(*args, **kwargs)
        processes.append(process)
        return process

    with (
        patch("bec_server.procedures.subprocess_worker.subprocess.Popen", side_effect=record),
        patch.object(
            SubProcessWorker, "_send_command", side_effect=RuntimeError("bootstrap failed")
        ),
    ):
        with pytest.raises(RuntimeError, match="bootstrap failed"):
            subprocess_worker(
                "import sys\nsys.stdin.readline()\n",
                worker_id="failed",
                credentials={"username": "alice", "token": "private-token"},
            )
    assert processes and processes[0].poll() is not None


def test_large_status_and_concurrent_output_use_separate_pipes(subprocess_worker):
    worker = subprocess_worker(
        "import json, sys, threading\n"
        "sys.stdin.readline()\n"
        "def output():\n"
        "    for _ in range(100):\n"
        "        sys.stderr.write('x' * 4096)\n"
        "        sys.stderr.flush()\n"
        "thread = threading.Thread(target=output)\n"
        "thread.start()\n"
        "status = {'worker_queue': 'large', 'status': 2, 'completed_execution_id': 'job', 'error': 'e' * 100000}\n"
        "sys.stdout.write('\\x1eBEC_WORKER_STATUS ' + json.dumps(status) + '\\n')\n"
        "sys.stdout.flush()\n"
        "thread.join()\n",
        worker_id="large",
        credentials={"username": "alice", "token": "private-token"},
    )
    assert worker._process.wait(timeout=10) == 0
    worker.logs()  # Wait for both readers.
    update = worker._status_updates.get(timeout=1)
    assert update.completed_execution_id == "job"
    assert update.error == "e" * 100000
    assert redis_output(worker) == "x" * (4096 * 100)


@pytest.mark.parametrize("terminal, expired", [("FINISHED", True), ("DEAD", False)])
@pytest.mark.parametrize("authentication_failure", [False, True])
def test_closed_assignment_pipe_distinguishes_expiry_from_failed_startup(
    terminal, expired, authentication_failure
):
    from queue import Queue
    from unittest.mock import MagicMock

    from bec_lib.messages import (
        ProcedureExecutionMessage,
        ProcedureWorkerStatus,
        ProcedureWorkerStatusMessage,
    )
    from bec_server.procedures.constants import ProcedureWorkerError, ProcedureWorkerFailure
    from bec_server.procedures.oop_worker_base import WorkerExpired

    worker = SubProcessWorker.__new__(SubProcessWorker)
    worker._status_updates = Queue()
    failure = (
        ProcedureWorkerFailure.AUTHENTICATION
        if authentication_failure
        else ProcedureWorkerFailure.STARTUP
    )
    worker._status_updates.put(
        ProcedureWorkerStatusMessage(
            worker_queue="worker",
            status=ProcedureWorkerStatus[terminal],
            error=failure.value if authentication_failure else None,
        )
    )
    worker._send_command = MagicMock(side_effect=BrokenPipeError())
    worker._process = MagicMock()
    worker._join_output = MagicMock()
    expected = WorkerExpired if expired else ProcedureWorkerError
    with pytest.raises(expected) as caught:
        worker.execute(
            ProcedureExecutionMessage(
                identifier="sleep", queue="test", execution_id="test-execution"
            ),
            MagicMock(),
        )
    if not expired:
        assert caught.value.failure == failure


@pytest.mark.parametrize("terminal", ["FINISHED", "DEAD", "completed", "error", "missing"])
def test_exited_worker_drains_delayed_terminal_status(terminal):
    from collections import deque
    from io import BytesIO
    from queue import Queue
    from threading import Event, Lock, Thread
    from unittest.mock import MagicMock

    from bec_lib.messages import (
        ProcedureExecutionMessage,
        ProcedureWorkerStatus,
        ProcedureWorkerStatusMessage,
    )
    from bec_server.procedures.constants import ProcedureWorkerError
    from bec_server.procedures.oop_worker_base import _STATUS_PREFIX, WorkerExpired

    execution = ProcedureExecutionMessage(identifier="sleep", queue="test", execution_id="job")
    worker = SubProcessWorker.__new__(SubProcessWorker)
    worker._worker_id = "worker"
    worker._queue = "test"
    worker._aborted = Event()
    worker._output_lock = Lock()
    worker._output_logs = deque()
    worker._reader_count = 1
    worker._status_updates = Queue()
    worker._send_command = MagicMock()
    release = Event()
    conn = MagicMock()

    def relay(*args, **kwargs):
        assert release.wait(5), "Test did not release the status relay"

    conn.rpush.side_effect = relay
    completed = terminal in ("completed", "error")
    updates = [
        ProcedureWorkerStatusMessage(
            worker_queue="worker",
            status=ProcedureWorkerStatus.RUNNING if completed else ProcedureWorkerStatus.IDLE,
            current_execution_id="job" if completed else None,
        )
    ]
    if terminal != "missing":
        updates.append(
            ProcedureWorkerStatusMessage(
                worker_queue="worker",
                status=ProcedureWorkerStatus.IDLE if completed else ProcedureWorkerStatus[terminal],
                completed_execution_id="job" if completed else None,
                error="procedure failed" if terminal == "error" else None,
            )
        )
    frames = "".join(_STATUS_PREFIX + update.model_dump_json() + "\n" for update in updates)
    worker._output_thread = Thread(
        target=worker._capture_output, args=(MagicMock(), conn, BytesIO(frames.encode()), True)
    )

    def exited():
        # Only release the reader after execute has exhausted its local status queue.
        release.set()
        return True

    worker._ending_or_ended = exited
    on_started = MagicMock()
    worker._output_thread.start()
    try:
        if completed:
            assert worker.execute(execution, on_started) == (
                "procedure failed" if terminal == "error" else None
            )
            on_started.assert_called_once_with()
        else:
            expected = WorkerExpired if terminal == "FINISHED" else ProcedureWorkerError
            with pytest.raises(expected):
                worker.execute(execution, on_started)
            on_started.assert_not_called()
    finally:
        release.set()
        worker._output_thread.join(timeout=5)
    assert not worker._output_thread.is_alive()


@pytest.mark.parametrize("startup_fails", [False, True])
def test_pool_runtime_logs_are_writable_and_cleaned_up(subprocess_worker, startup_fails):
    from pathlib import Path
    from unittest.mock import MagicMock

    from bec_lib.messages import ProcedureExecutionMessage
    from bec_server.procedures.constants import ProcedureWorkerError

    worker = subprocess_worker(
        "from pathlib import Path\n"
        "from bec_lib.service_config import DEFAULT_BASE_PATH\n"
        "from bec_server.procedures import oop_worker_base as runtime\n"
        "class Client:\n"
        "    def __init__(self, config, **kwargs):\n"
        "        self.directory = Path(config.model.log_writer.base_path)\n"
        "        if self.directory == Path(DEFAULT_BASE_PATH) / 'logs':\n"
        "            raise OSError(30, 'Read-only deployment')\n"
        "        (self.directory / 'worker.log').write_text('runtime output')\n"
        "        print('runtime log directory: ' + str(self.directory), flush=True)\n"
        "    def start(self):\n"
        f"        self.started = {not startup_fails}\n"
        "    def shutdown(self, **kwargs):\n"
        "        assert self.directory.exists(), 'Removed logs before client shutdown'\n"
        "        assert (self.directory / 'worker.log').read_text() == 'runtime output'\n"
        "runtime.BECIPythonClient = Client\n"
        "runtime.main(output_to_redis=False)\n",
        worker_id="writable-logs",
        credentials={"username": "alice", "token": "private-token"},
    )
    execution = ProcedureExecutionMessage(
        identifier="_log_msg_args", queue="output", execution_id="job"
    )
    if startup_fails:
        with pytest.raises(ProcedureWorkerError, match="startup failed"):
            worker.execute(execution, MagicMock())
    else:
        assert worker.execute(execution, MagicMock()) is None
    worker._process.stdin.close()
    worker._process.wait(timeout=5)
    output = worker.logs()
    log_directory = next(
        line.removeprefix("runtime log directory: ")
        for line in output
        if line.startswith("runtime log directory: ")
    )
    assert not Path(log_directory).exists()
    assert "private-token" not in "\n".join(output)


@pytest.mark.parametrize("authentication_failure", [False, True])
def test_pool_startup_reports_safe_failure_category(subprocess_worker, authentication_failure):
    """Startup frames distinguish authentication without forwarding exception text."""
    from unittest.mock import MagicMock

    from bec_lib.messages import ProcedureExecutionMessage
    from bec_server.procedures.constants import ProcedureWorkerError, ProcedureWorkerFailure

    exception = "AuthenticationError" if authentication_failure else "RuntimeError"
    worker = subprocess_worker(
        "from redis.exceptions import AuthenticationError\n"
        "from bec_server.procedures import oop_worker_base as runtime\n"
        "def create(*args, **kwargs):\n"
        f"    raise {exception}('private-token in internal details')\n"
        "runtime._create_client = create\n"
        "runtime.main(output_to_redis=False)\n",
        worker_id="startup-category",
        credentials={"username": "alice", "token": "private-token"},
    )
    expected = (
        ProcedureWorkerFailure.AUTHENTICATION
        if authentication_failure
        else ProcedureWorkerFailure.STARTUP
    )
    with pytest.raises(ProcedureWorkerError) as caught:
        worker.execute(
            ProcedureExecutionMessage(identifier="sleep", queue="output", execution_id="job"),
            MagicMock(),
        )
    assert caught.value.failure == expected
    assert str(caught.value) == expected.value


def test_worker_error_defaults_to_category_and_preserves_explicit_details():
    """Categorized errors need no repeated text; existing exception arguments still work."""
    from bec_server.procedures.constants import ProcedureWorkerError, ProcedureWorkerFailure

    for failure in ProcedureWorkerFailure:
        error = ProcedureWorkerError(failure=failure)
        assert str(error) == failure.value
        assert error.failure is failure
        detailed = ProcedureWorkerError("internal details", failure=failure)
        assert str(detailed) == "internal details"
        assert detailed.failure is failure
    assert ProcedureWorkerError().args == ()
    assert str(ProcedureWorkerError("existing message")) == "existing message"
    assert str(ProcedureWorkerError("", failure=ProcedureWorkerFailure.EXITED)) == ""


@pytest.mark.parametrize("error", [None, "private-token in unexpected startup error"])
def test_unknown_startup_details_are_not_forwarded(error):
    """Old workers and arbitrary child errors retain a safe generic startup category."""
    from bec_server.procedures.constants import ProcedureWorkerFailure
    from bec_server.procedures.oop_worker_base import _startup_error

    assert _startup_error(error).failure == ProcedureWorkerFailure.STARTUP


def test_pool_procedure_stdin_is_eof_and_worker_accepts_next_assignment(subprocess_worker):
    """Procedure input must not block or consume the pooled assignment channel."""
    from threading import Thread
    from unittest.mock import MagicMock

    from bec_lib.messages import ProcedureExecutionMessage

    worker = subprocess_worker(
        "import os, sys, subprocess\n"
        "from bec_server.procedures import oop_worker_base as runtime\n"
        "class Client:\n"
        "    started = True\n"
        "    def start(self): pass\n"
        "    def shutdown(self, **kwargs): pass\n"
        "runtime._create_client = lambda *args, **kwargs: Client()\n"
        "def task(client, item):\n"
        "    if item.execution_id == 'read-stdin':\n"
        "        try:\n"
        "            input()\n"
        "        except EOFError:\n"
        "            pass\n"
        "        else:\n"
        "            raise AssertionError('input() did not raise EOFError')\n"
        "        assert sys.stdin.read() == ''\n"
        "        assert os.read(0, 1) == b''\n"
        "        subprocess.run([sys.executable, '-c', "
        "\"import sys; assert sys.stdin.read() == ''\"], check=True, timeout=3)\n"
        "runtime._run_task = task\n"
        "runtime.main(output_to_redis=False)\n",
        worker_id="stdin-eof",
        credentials={"username": "alice", "token": "private-token"},
    )
    results = []

    def execute_both():
        try:
            for execution_id in ("read-stdin", "next-assignment"):
                execution = ProcedureExecutionMessage(
                    identifier="_log_msg_args", queue="output", execution_id=execution_id
                )
                results.append(worker.execute(execution, MagicMock()))
        except Exception as exc:
            results.append(exc)

    runner = Thread(target=execute_both)
    runner.start()
    try:
        runner.join(timeout=10)
        assert not runner.is_alive(), "Procedure stdin blocked the pooled worker"
        assert results == [None, None], results
        assert worker._process.poll() is None
    finally:
        worker.abort()
        runner.join(timeout=5)


@pytest.mark.parametrize("client_type", ["BECClient", "BECIPythonClient"])
def test_worker_client_uses_supplied_log_directory(tmp_path, client_type):
    from bec_server.procedures.oop_worker_base import _create_client

    with patch(f"bec_server.procedures.oop_worker_base.{client_type}") as client:
        _create_client(
            client_type, {"host": "localhost", "port": "6379"}, log_directory=str(tmp_path)
        )
    config = client.call_args.kwargs["config"]
    assert config.model.log_writer.base_path == str(tmp_path)
    assert not config.model.procedures.enable_procedures
