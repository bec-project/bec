from __future__ import annotations

import base64
import inspect
import json
import os
import select
import subprocess
import sys
import traceback
from codecs import getincrementaldecoder
from collections import deque
from contextlib import redirect_stderr, redirect_stdout
from queue import Empty, Queue
from tempfile import TemporaryDirectory
from threading import Lock, Thread
from typing import AnyStr, Callable, TextIO

from bec_ipython_client.main import BECIPythonClient
from bec_ipython_client.signals import OperationMode
from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import LogLevel, bec_logger
from bec_lib.messages import ProcedureExecutionMessage, ProcedureWorkerStatus, RawMessage
from bec_lib.procedures.helper import BackendProcedureHelper
from bec_lib.redis_connector import RedisConnector
from bec_lib.serialization import MsgpackSerialization
from bec_lib.service_config import ServiceConfig
from bec_server.procedures import procedure_registry
from bec_server.procedures.constants import (
    PROCEDURE,
    BecClientType,
    OopWorkerEnv,
    ProcedureWorkerError,
)
from bec_server.procedures.worker_base import ProcedureWorker

logger = bec_logger.logger
_STATUS_PREFIX = "\x1eBEC_WORKER_STATUS "
PROCESS_TIMEOUT = 3


class RedisOutputDiverter(TextIO):
    def __init__(self, conn: RedisConnector, queue: str, *, clear: bool = True):

        self._conn = conn
        self._ep = MessageEndpoints.procedure_logs(queue)
        if clear:
            self._conn.delete(self._ep)

    def write(self, data: AnyStr):
        if data:
            self._conn.xadd(
                self._ep, {"data": RawMessage(data=str(data))}, max_size=10000, expire=3600
            )
        return len(data)

    def flush(self): ...

    @property
    def encoding(self):
        return "utf-8"

    def close(self):
        return


class OutOfProcessWorkerBase(ProcedureWorker):
    """A worker which runs in a separate process"""

    def _worker_environment(self):
        """Used to pass information to the container as environment variables - should be the
        minimum necessary, or things which are only necessary for the functioning of the worker,
        and other information should be passed through redis"""
        env = {
            "redis_server": self._redis_server,
            "queue": self._queue,
            "timeout_s": str(self._lifetime_s),
        }
        if self._worker_id is not None:
            env.update(worker_id=self._worker_id, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        return env

    def execute(
        self, item: ProcedureExecutionMessage, on_started: Callable[[], None]
    ) -> str | None:
        """Run one assigned execution while retaining the process for this queue.

        Args:
            item (ProcedureExecutionMessage): Execution selected by the owning queue.
            on_started (Callable[[], None]): Callback after the child reports RUNNING.
        Returns:
            str | None: Procedure error, or None on success.
        """
        try:
            self._send_command(
                {"execution": base64.b64encode(MsgpackSerialization.dumps(item)).decode()}
            )
        except (BrokenPipeError, ValueError):
            self._process.wait(PROCESS_TIMEOUT)
            self._join_output()
            while not self._status_updates.empty():
                update = self._status_updates.get_nowait()
                if update.status == ProcedureWorkerStatus.DEAD:
                    raise ProcedureWorkerError("Procedure worker startup failed") from None
                if update.status == ProcedureWorkerStatus.FINISHED:
                    raise WorkerExpired() from None
            raise ProcedureWorkerError("Procedure worker input closed unexpectedly") from None
        started = False
        while not self._aborted.is_set():
            try:
                update = self._status_updates.get(timeout=0.2)
            except Empty:
                update = None
            if update is not None:
                self.status = update.status
                self._current_execution_id = update.current_execution_id
                if update.completed_execution_id == item.execution_id:
                    return update.error
                if update.current_execution_id == item.execution_id and not started:
                    started = True
                    on_started()
                if update.status == ProcedureWorkerStatus.DEAD:
                    raise ProcedureWorkerError("Procedure worker startup failed")
                if update.status == ProcedureWorkerStatus.FINISHED and not started:
                    raise WorkerExpired()
            elif self._ending_or_ended():
                # Process exit can precede the control reader draining its final frames.
                # Keep waiting while that reader is active, and consume queued statuses
                # before deciding whether the assignment failed or needs an expiry retry.
                self._output_thread.join(timeout=0.2)
                if self._output_thread.is_alive() or not self._status_updates.empty():
                    continue
                raise ProcedureWorkerError("Procedure worker exited unexpectedly")
        raise ProcedureWorkerError("Procedure worker was aborted")

    def _send_command(self, payload: dict) -> None:
        """Write bootstrap or assignments to the process pipe, without logging the payload."""
        self._process.stdin.write((json.dumps(payload) + "\n").encode())
        self._process.stdin.flush()

    def _start_output_capture(self, conn) -> None:
        if not hasattr(self, "_output_logs"):
            self._output_logs = deque(maxlen=10000)
            self._output_lock = Lock()
        self._output_error_logged = False
        self._status_updates = Queue()
        self._reader_count = 2 if self._worker_id is not None else 1
        self._output_thread = Thread(
            target=self._capture_output,
            args=(self._process, conn, self._process.stdout, self._worker_id is not None),
            name=f"ProcedureWorker-{self._queue} output",
            daemon=True,
        )
        self._output_thread.start()
        if self._worker_id is not None:
            self._stderr_thread = Thread(
                target=self._capture_output,
                args=(self._process, conn, self._process.stderr, False),
                name=f"ProcedureWorker-{self._queue} stderr",
                daemon=True,
            )
            self._stderr_thread.start()
            self._send_command(self._credentials)

    def _parse_output(self, text, conn):
        while text:
            index = text.find(_STATUS_PREFIX)
            if index < 0:
                keep = next(
                    (
                        size
                        for size in range(len(_STATUS_PREFIX) - 1, 0, -1)
                        if text.endswith(_STATUS_PREFIX[:size])
                    ),
                    0,
                )
                self._publish_output(text[:-keep] if keep else text, conn)
                return text[-keep:] if keep else ""
            self._publish_output(text[:index], conn)
            text = text[index:]
            end = text.find("\n")
            if end < 0:
                return text
            frame, text = text[len(_STATUS_PREFIX) : end], text[end + 1 :]
            try:
                update = messages.ProcedureWorkerStatusMessage.model_validate_json(frame)
                if update.worker_queue != self._worker_id:
                    raise ValueError()
            except ValueError:
                self._publish_output(_STATUS_PREFIX + frame + "\n", conn)
                continue
            self._status_updates.put(update)
            try:
                conn.rpush(
                    MessageEndpoints.procedure_worker_status_update(self._worker_id),
                    update,
                    max_size=1000,
                    expire=3600,
                )
            except Exception:
                logger.warning("Failed to publish procedure worker status")
        return ""

    def _capture_output(
        self, process: subprocess.Popen, conn: RedisConnector, stream=None, control=False
    ) -> None:
        """Drain both streams even if Redis rejects an output write."""
        pending = ""
        decoder = getincrementaldecoder("utf-8")(errors="replace")
        try:
            with stream if stream is not None else process.stdout as output:
                while True:
                    chunk = output.read(4096)
                    if not chunk:
                        self._publish_output(pending + decoder.decode(b"", final=True), conn)
                        break
                    text = decoder.decode(chunk)
                    if not control:
                        self._publish_output(text, conn)
                    else:
                        pending = self._parse_output(pending + text, conn)
        finally:
            with self._output_lock:
                self._reader_count -= 1
                last_reader = self._reader_count == 0
            if last_reader:
                conn.shutdown()

    def _publish_output(self, text: str, conn: RedisConnector) -> None:
        if not text:
            return
        with self._output_lock:
            self._output_logs.append(text)
        try:
            conn.xadd(
                MessageEndpoints.procedure_logs(self._queue),
                {"data": RawMessage(data=text)},
                max_size=10000,
                expire=3600,
            )
        except Exception as exc:
            if not self._output_error_logged:
                logger.error(f"Failed to publish procedure output: {exc}")
            self._output_error_logged = True

    def _join_output(self) -> None:
        for name in ("_output_thread", "_stderr_thread"):
            thread = getattr(self, name, None)
            if thread is not None:
                thread.join(timeout=PROCESS_TIMEOUT)
                if thread.is_alive():
                    raise ProcedureWorkerError("Procedure output reader failed to stop")

    def _cleanup_execution_environment(self) -> None:
        try:
            super()._cleanup_execution_environment()
        finally:
            if hasattr(self, "_process") and self._process.stdin is not None:
                self._process.stdin.close()
            self._join_output()

    def work(self):
        """block until the external process is finished, listen for status updates in the meantime"""
        # BLPOP from ProcWorkerStatus and set status
        # on timeout check if container is still running

        status_update = None
        while not self._aborted.is_set():
            status_update = self._conn.blocking_list_pop(
                MessageEndpoints.procedure_worker_status_update(self._queue), timeout_s=0.2
            )
            if status_update is not None:
                if not isinstance(status_update, messages.ProcedureWorkerStatusMessage):
                    raise ProcedureWorkerError(f"Received unexpected message {status_update}")
                self.status = status_update.status
                self._current_execution_id = status_update.current_execution_id
                logger.info(
                    f"Procedure worker '{self._queue}' status update: {status_update.status.name}"
                )
            elif self._ending_or_ended():
                break
            # TODO: we probably do want to handle some kind of timeout here but we don't know how
            # long a running procedure should actually take - it could theoretically be infinite
        if self.status != ProcedureWorkerStatus.FINISHED:
            self.status = ProcedureWorkerStatus.DEAD
            if not self._aborted.is_set():
                logs = "\n".join(self.logs())
                raise ProcedureWorkerError(
                    f"Procedure worker for queue {self._queue} exited unexpectedly.\n{logs}"
                )


def _default_env() -> dict[str, str]:
    """Default values for optional environment vars"""
    return {"client_class": BecClientType.BECIPythonClient}


def get_env() -> OopWorkerEnv:
    try:
        needed_keys = OopWorkerEnv.__required_keys__
        logger.debug(f"Checking for environment variables: {needed_keys}")
        supplied_env = {k: os.environ[k] for k in needed_keys}
    except KeyError as e:
        logger.error(f"Missing environment variable needed by container worker: {e}")
        exit(1)
    optional = {key: os.environ[key] for key in ("worker_id",) if key in os.environ}
    return _default_env() | supplied_env | optional  # type: ignore


def _resolve_timeout(timeout_s: str) -> float:
    try:
        return float(timeout_s)
    except ValueError as e:
        logger.error(
            f"{e} \n Failed to convert supplied timeout argument to an int. \n "
            f"Using default timeout of {PROCEDURE.WORKER.QUEUE_TIMEOUT_S} s."
        )
        return PROCEDURE.WORKER.QUEUE_TIMEOUT_S


def _create_client(
    client_type: BecClientType,
    redis: dict[str, str],
    name=None,
    credentials=None,
    log_directory: str | None = None,
):
    settings = {"procedures": {"enable_procedures": False}}
    if log_directory is not None:
        settings["log_writer"] = {"base_path": log_directory}
    config = ServiceConfig(redis=redis, config=settings)

    kwargs = {}
    if credentials is not None:
        username, token = credentials["username"] or "default", credentials["token"]

        class AuthenticatedConnector(RedisConnector):
            """Bind every client connector to the submitted identity without login fallback."""

            def __init__(self, *args, **options):
                options.update(username=username, password=token)
                super().__init__(*args, **options)

            def authenticate(self, *, username="default", password="null"):
                if (username, password) != (credentials["username"] or "default", token):
                    raise ValueError("Procedure connector cannot change its authenticated identity")
                return super().authenticate(username=username, password=password)

        kwargs["connector_cls"] = AuthenticatedConnector
    if client_type == BecClientType.BECIPythonClient:
        return BECIPythonClient(
            config=config, wait_for_server=False, mode=OperationMode.Procedure, **kwargs
        )
    elif client_type == BecClientType.BECClient:
        return BECClient(wait_for_server=False, config=config, **kwargs)
    else:
        raise ValueError(f"BEC client class {client_type} not recognised!")


def setup(env: OopWorkerEnv):
    logger.info("Procedure worker starting up")

    logger.debug(f"Starting with environment: {env}")
    logger.debug("Configuring logger...")
    bec_logger.level = LogLevel.DEBUG
    bec_logger._console_log = True
    bec_logger.configure(
        bootstrap_server=env["redis_server"],  # type: ignore
        connector_cls=RedisConnector,
        service_name=f"Procedure worker for queue {env['queue']}",
        service_config={"log_writer": {"base_path": "/tmp/"}},
    )
    logger.debug("Done.")
    host, port = env["redis_server"].split(":")
    redis = {"host": host, "port": port}

    client = _create_client(
        env["client_class"], redis=redis, name=f"[OOP worker client: {env['queue']}]"
    )

    logger.debug("starting client")
    client.start()
    if not client.started:
        exit(1)

    logger.success(f"Procedure worker started container for queue {env['queue']}")
    conn = RedisConnector(env["redis_server"])
    logger.debug(f"Procedure worker {env['queue']} connected to Redis at {conn.host}:{conn.port}")
    helper = BackendProcedureHelper(conn)

    return env, helper, client, conn


def _run_task(client: BECClient | BECIPythonClient, item: ProcedureExecutionMessage):
    logger.success(f"Executing procedure {item.identifier}.")
    kwargs = dict(item.args_kwargs[1])
    proc_func = procedure_registry.callable_from_execution_message(item)
    if bec_arg := inspect.signature(proc_func).parameters.get("bec"):
        if bec_arg.kind == bec_arg.KEYWORD_ONLY and bec_arg.annotation.__name__ == "BECClient":
            logger.debug(f"Injecting BEC client argument for {item}")
            kwargs["bec"] = client
    proc_func(*item.args_kwargs[0], **kwargs)


def push_status(
    conn: RedisConnector, queue: str, status: ProcedureWorkerStatus, id: str | None = None
):
    status_endpoint = MessageEndpoints.procedure_worker_status_update(queue)
    logger.debug(f"Updating worker status to {status.name}")
    conn.rpush(
        status_endpoint,
        messages.ProcedureWorkerStatusMessage(
            worker_queue=queue, status=status, current_execution_id=id
        ),
    )


def _main(
    env, helper: BackendProcedureHelper, client: BECClient | BECIPythonClient, conn: RedisConnector
):
    exec_endpoint = MessageEndpoints.procedure_execution(env["queue"])
    active_procs_endpoint = MessageEndpoints.active_procedure_executions()
    timeout_s = _resolve_timeout(env["timeout_s"])
    queue = env["queue"]
    push_status(conn, queue, ProcedureWorkerStatus.IDLE)
    item = None
    try:
        logger.success(f"Worker waiting for instructions on queue {env['queue']}")
        while (
            item := conn.blocking_list_pop_to_set_add(
                exec_endpoint, active_procs_endpoint, timeout_s=timeout_s
            )
        ) is not None:
            push_status(conn, queue, ProcedureWorkerStatus.RUNNING, item.execution_id)
            helper.status_update(item.execution_id, "Started")
            helper.notify_watchers(env["queue"], queue_type="execution")
            logger.debug(f"running task {item!r}")
            try:
                _run_task(client, item)
            except Exception as e:
                logger.error(f"Encountered error running procedure {item}")
                helper.status_update(item.execution_id, "Finished", traceback.format_exc())
                logger.error(e)
            else:
                helper.status_update(item.execution_id, "Finished")
                logger.success(f"Finished procedure {item}")
            finally:
                helper.remove_from_active.by_exec_id(item.execution_id)
            push_status(conn, queue, ProcedureWorkerStatus.IDLE)
    except KeyboardInterrupt:
        if item is not None:
            logger.error("Procedure cancelled by user")
            helper.status_update(item.execution_id, "Aborted", error="Aborted by user.")
            # The rest of cleanup is handled in 'finally'
    except Exception as e:
        logger.error(e)  # don't stop ProcedureManager.spawn from cleaning up
    finally:
        logger.success("Procedure runner shutting down")
        push_status(conn, queue, ProcedureWorkerStatus.FINISHED)
        client.shutdown(per_thread_timeout_s=1)
        if item is not None:  # in this case we are here due to an exception, not a timeout
            helper.remove_from_active.by_exec_id(item.execution_id)


class WorkerExpired(ProcedureWorkerError):
    """An idle worker exited before starting an assignment; the queue may retry it."""


def _pool_main(env):
    """Receive credentials and serial assignments through the process input, never Redis."""
    sys.stdout.flush()
    control_output = os.fdopen(os.dup(sys.stdout.fileno()), "w", encoding="utf-8", buffering=1)
    # Reserve original stdout for control; all procedure output, including native writes,
    # goes through the separate stderr pipe drained by the scan server.
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    input_stream = os.fdopen(os.dup(sys.stdin.fileno()), "rb", buffering=0)
    client = None
    ready = False

    def status(state, current=None, completed=None, error=None):
        update = messages.ProcedureWorkerStatusMessage(
            worker_queue=env["worker_id"],
            status=state,
            current_execution_id=current,
            completed_execution_id=completed,
            error=error,
        )
        control_output.write(_STATUS_PREFIX + update.model_dump_json() + "\n")
        control_output.flush()

    # Parse without exposing the bootstrap payload in validation errors or startup logs.
    try:
        credentials = json.loads(input_stream.readline())
        if (
            not isinstance(credentials, dict)
            or not isinstance(credentials.get("username"), str)
            or not isinstance(credentials.get("token"), (str, type(None)))
        ):
            raise ValueError()
    except Exception:
        raise RuntimeError("Invalid procedure worker bootstrap") from None
    # The code deployment may be mounted read-only; only runtime logs go here.
    with TemporaryDirectory(prefix="bec-procedure-") as log_directory:
        try:
            with redirect_stdout(sys.stderr):
                host, port = env["redis_server"].rsplit(":", 1)
                client = _create_client(
                    env["client_class"],
                    {"host": host, "port": port},
                    credentials=credentials,
                    log_directory=log_directory,
                )
                client.start()
                if not client.started:
                    raise RuntimeError("Procedure worker client did not start")
                ready = True
                status(ProcedureWorkerStatus.IDLE)
                while select.select([input_stream], [], [], _resolve_timeout(env["timeout_s"]))[0]:
                    line = input_stream.readline()
                    if not line:
                        break
                    item = MsgpackSerialization.loads(
                        base64.b64decode(json.loads(line)["execution"])
                    )
                    status(ProcedureWorkerStatus.RUNNING, current=item.execution_id)
                    error = None
                    try:
                        _run_task(client, item)
                    except Exception:
                        error = traceback.format_exc()
                        logger.error(error)
                    status(ProcedureWorkerStatus.IDLE, completed=item.execution_id, error=error)
        finally:
            try:
                status(ProcedureWorkerStatus.FINISHED if ready else ProcedureWorkerStatus.DEAD)
            finally:
                input_stream.close()
                control_output.close()
                if client is not None:
                    client.shutdown(per_thread_timeout_s=1)


def main(*, output_to_redis: bool = True) -> None:
    """Run the worker entry point.

    Args:
        output_to_redis (bool): Redirect procedure output to Redis in this process. Subprocess
            workers disable this because their parent streams both output pipes instead.
    """

    env = get_env()
    if "worker_id" in env:
        _pool_main(env)
        return
    env, helper, client, conn = setup(env)
    if not output_to_redis:
        try:
            _main(env, helper, client, conn)
        finally:
            conn.shutdown()
        return
    logger_connector = RedisConnector(env["redis_server"])
    output_diverter = RedisOutputDiverter(logger_connector, env["queue"])
    sink = None
    try:
        with redirect_stdout(output_diverter), redirect_stderr(output_diverter):
            sink = logger.add(
                output_diverter,
                level=LogLevel.SUCCESS,
                format=bec_logger.formatting(is_container=True),
                filter=bec_logger.filter(),
            )
            _main(env, helper, client, conn)
    finally:
        if sink is not None:
            logger.remove(sink)
        conn.shutdown()
        logger_connector.shutdown()
