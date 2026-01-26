import inspect
import os
import traceback
from abc import abstractmethod
from contextlib import redirect_stdout
from typing import AnyStr, TextIO

from bec_ipython_client.main import BECIPythonClient
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import LogLevel, bec_logger
from bec_lib.messages import ProcedureExecutionMessage, ProcedureWorkerStatus, RawMessage
from bec_lib.procedures.helper import BackendProcedureHelper
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig
from bec_server.procedures import procedure_registry
from bec_server.procedures.constants import PROCEDURE, ContainerWorkerEnv, ProcedureWorkerError
from bec_server.procedures.worker_base import ProcedureWorker

logger = bec_logger.logger


class RedisOutputDiverter(TextIO):
    def __init__(self, conn: RedisConnector, queue: str):

        self._conn = conn
        self._ep = MessageEndpoints.procedure_logs(queue)
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

    # The Podman client is a thin wrapper around the libpod API
    # documented at https://docs.podman.io/en/latest/_static/api.html
    # which is more detailed than the podman-py documentation

    def _worker_environment(self) -> ContainerWorkerEnv:
        """Used to pass information to the container as environment variables - should be the
        minimum necessary, or things which are only necessary for the functioning of the worker,
        and other information should be passed through redis"""
        return {
            "redis_server": f"{PROCEDURE.REDIS_HOST}:{self._conn.port}",
            "queue": self._queue,
            "timeout_s": str(self._lifetime_s),
        }

    def _run_task(self, item: ProcedureExecutionMessage):
        raise ProcedureWorkerError(
            f"Out of process worker worker _run_task() called with {item} - this should never happen!"
        )

    @abstractmethod
    def _ending_or_ended(self) -> bool: ...

    def work(self):
        """block until the container is finished, listen for status updates in the meantime"""
        # BLPOP from PocWorkerStatus and set status
        # on timeout check if container is still running

        status_update = None
        while not self._ending_or_ended():
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
            # TODO: we probably do want to handle some kind of timeout here but we don't know how
            # long a running procedure should actually take - it could theoretically be infinite
        if self.status != ProcedureWorkerStatus.FINISHED:
            self.status = ProcedureWorkerStatus.DEAD


def _setup():
    logger.info("Procedure worker starting up")
    try:
        needed_keys = ContainerWorkerEnv.__annotations__.keys()
        logger.debug(f"Checking for environment variables: {needed_keys}")
        env: ContainerWorkerEnv = {k: os.environ[k] for k in needed_keys}  # type: ignore
    except KeyError as e:
        logger.error(f"Missing environment variable needed by container worker: {e}")
        exit(1)

    logger.debug(f"Starting with environment: {env}")
    logger.debug(f"Configuring logger...")
    bec_logger.level = LogLevel.DEBUG
    bec_logger._console_log = True
    bec_logger.configure(
        bootstrap_server=env["redis_server"],  # type: ignore
        connector_cls=RedisConnector,
        service_name=f"Procedure worker for queue {env['queue']}",
        service_config={"log_writer": {"base_path": "/tmp/"}},
    )
    logger.debug(f"Done.")
    host, port = env["redis_server"].split(":")
    redis = {"host": host, "port": port}

    client = BECIPythonClient(
        config=ServiceConfig(redis=redis, config={"procedures": {"enable_procedures": False}})
    )
    logger.debug("starting client")
    client.start()

    logger.success(f"Procedure worker started container for queue {env['queue']}")
    conn = RedisConnector(env["redis_server"])
    logger.debug(f"Procedure worker {env['queue']} connected to Redis at {conn.host}:{conn.port}")
    helper = BackendProcedureHelper(conn)

    return env, helper, client, conn


def _main(env, helper: BackendProcedureHelper, client: BECIPythonClient, conn):

    exec_endpoint = MessageEndpoints.procedure_execution(env["queue"])
    active_procs_endpoint = MessageEndpoints.active_procedure_executions()
    status_endpoint = MessageEndpoints.procedure_worker_status_update(env["queue"])

    try:
        timeout_s = int(env["timeout_s"])
    except ValueError as e:
        logger.error(
            f"{e} \n Failed to convert supplied timeout argument to an int. \n Using default timeout of 10 s."
        )
        timeout_s = PROCEDURE.WORKER.QUEUE_TIMEOUT_S

    def _push_status(status: ProcedureWorkerStatus, id: str | None = None):
        logger.debug(f"Updating worker status to {status.name}")
        conn.rpush(
            status_endpoint,
            messages.ProcedureWorkerStatusMessage(
                worker_queue=env["queue"], status=status, current_execution_id=id
            ),
        )

    def _run_task(item: ProcedureExecutionMessage):
        logger.success(f"Executing procedure {item.identifier}.")
        kwargs = item.args_kwargs[1]
        proc_func = procedure_registry.callable_from_execution_message(item)
        if bec_arg := inspect.signature(proc_func).parameters.get("bec"):
            if bec_arg.kind == bec_arg.KEYWORD_ONLY and bec_arg.annotation.__name__ == "BECClient":
                logger.debug(f"Injecting BEC client argument for {item}")
                kwargs["bec"] = client
        procedure_registry.callable_from_execution_message(item)(*item.args_kwargs[0], **kwargs)

    _push_status(ProcedureWorkerStatus.IDLE)
    item = None
    try:
        logger.success(f"Worker waiting for instructions on queue {env['queue']}")
        while (
            item := conn.blocking_list_pop_to_set_add(
                exec_endpoint, active_procs_endpoint, timeout_s=timeout_s
            )
        ) is not None:
            _push_status(ProcedureWorkerStatus.RUNNING, item.execution_id)
            helper.status_update(item.execution_id, "Started")
            helper.notify_watchers(env["queue"], queue_type="execution")
            logger.debug(f"running task {item!r}")
            try:
                _run_task(item)
            except Exception as e:
                logger.error(f"Encountered error running procedure {item}")
                helper.status_update(item.execution_id, "Finished", traceback.format_exc())
                logger.error(e)
            else:
                helper.status_update(item.execution_id, "Finished")
                logger.success(f"Finished procedure {item}")
            finally:
                helper.remove_from_active.by_exec_id(item.execution_id)
            _push_status(ProcedureWorkerStatus.IDLE)
    except Exception as e:
        logger.error(e)  # don't stop ProcedureManager.spawn from cleaning up
    finally:
        logger.success(f"Procedure runner shutting down")
        _push_status(ProcedureWorkerStatus.FINISHED)
        client.shutdown(per_thread_timeout_s=1)
        if item is not None:  # in this case we are here due to an exception, not a timeout
            helper.remove_from_active.by_exec_id(item.execution_id)


def main():
    """Replaces the main contents of Worker.work() - should be called as the container entrypoint or command"""

    env, helper, client, conn = _setup()
    logger_connector = RedisConnector(env["redis_server"])
    output_diverter = RedisOutputDiverter(logger_connector, env["queue"])
    with redirect_stdout(output_diverter):
        logger.add(
            output_diverter,
            level=LogLevel.SUCCESS,
            format=bec_logger.formatting(is_container=True),
            filter=bec_logger.filter(),
        )
        _main(env, helper, client, conn)
    conn.shutdown()
    logger_connector.shutdown()
