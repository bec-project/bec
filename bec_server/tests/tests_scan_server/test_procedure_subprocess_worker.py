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

    def create(script):
        worker_file.write_text(script, encoding="utf-8")
        worker = SubProcessWorker("localhost:6379", "output")
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


def test_verbose_output_does_not_block(subprocess_worker):
    worker = subprocess_worker(
        "import sys\n"
        "for _ in range(100):\n"
        "    sys.stdout.write('a' * 4096)\n"
        "    sys.stderr.write('b' * 4096)\n"
        "sys.stdout.write(' Unicode: λ完\\n')\n"
    )
    assert worker._process.wait(timeout=10) == 0
    captured = "\n".join(worker.logs())
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
