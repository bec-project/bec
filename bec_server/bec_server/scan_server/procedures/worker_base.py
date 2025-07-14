from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import cast

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import ProcedureExecutionMessage, ProcedureWorkerStatus
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.procedures.constants import PROCEDURE

logger = bec_logger.logger


class ProcedureWorker(ABC):
    """Base class for a worker which automatically dies when there is nothing in the queue for TIMEOUT s.
    Implement _setup_execution_environment(), _kill_process(), and _run_task() to create a functional worker.
    """

    def __init__(self, server: str, queue: str, lifetime_s: float | None = None):
        """Start a worker to run procedures on the queue identified by `queue`. Should be used as a
        context manager to ensure that cleanup is handled on destruction. E.g.:
        ```
        with ProcedureWorker(args...) as worker:
            worker.work() # blocks for the lifetime of the worker
        ```

        Args:
            server (str): BEC Redis server in the format "server:port"
            queue (str): name of the queue to listen to execution messages on
            lifetime_s (int): how long to stay alive with nothing in the queue"""

        self._queue = queue
        self.key = MessageEndpoints.procedure_execution(queue)
        self._active_procs_endpoint = MessageEndpoints.active_procedure_executions()
        self.status = ProcedureWorkerStatus.IDLE
        self._conn = RedisConnector([server])
        self._lifetime_s = lifetime_s or PROCEDURE.WORKER.QUEUE_TIMEOUT_S
        self.client_id = self._conn.client_id()

        self._setup_execution_environment()

    def __enter__(self):
        return self

    @abstractmethod
    def _kill_process(self):
        """Clean up the execution environment, e.g. kill container or running subprocess.
        Should be safe to call multiple times, as it could be called in abort() and again on
        __exit__()."""

    @abstractmethod
    def _run_task(self, item: ProcedureExecutionMessage):
        """Actually cause the procedure to be executed.
        Should block until the procedure is complete."""

        # for a single scan procedure, this can just send the message,
        # then block for the scan to appear in the history

    @abstractmethod
    def _setup_execution_environment(self): ...

    def abort(self):
        self._kill_process()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._kill_process()

    def work(self):
        item = None
        try:
            # if podman container worker, this should probably monitor the queue there
            while (
                item := self._conn.blocking_list_pop_to_set_add(
                    self.key, self._active_procs_endpoint, timeout_s=self._lifetime_s
                )
            ) is not None:
                self.status = ProcedureWorkerStatus.RUNNING
                self._run_task(cast(ProcedureExecutionMessage, item))
                self.status = ProcedureWorkerStatus.IDLE
        except Exception as e:
            logger.error(e)  # don't stop ProcedureManager.spawn from cleaning up
        finally:
            if item is not None:
                self._conn.remove_from_set(self._active_procs_endpoint, item)
            self.status = ProcedureWorkerStatus.FINISHED
