from __future__ import annotations

from abc import ABC, abstractmethod
from threading import Event
from typing import cast

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import ProcedureExecutionMessage, ProcedureWorkerStatus
from bec_lib.procedures.helper import BackendProcedureHelper
from bec_lib.redis_connector import RedisConnector
from bec_server.procedures.constants import PROCEDURE

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
        self._redis_server = server
        self._conn = RedisConnector([server])
        self._helper = BackendProcedureHelper(self._conn)
        self._lifetime_s = lifetime_s or PROCEDURE.WORKER.QUEUE_TIMEOUT_S
        self.client_id = self._conn.client_id()
        self._current_execution_id: str | None = None
        self._aborted = Event()

        self._setup_execution_environment()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.shutdown()
        self._kill_process()

    @abstractmethod
    def _kill_process(self):
        """Clean up the execution environment, e.g. kill container or running subprocess.
        Should be safe to call multiple times, as it could be called in abort() and again on
        __exit__()."""

    @abstractmethod
    def _setup_execution_environment(self):
        """Get everything ready such that the worker is waiting for instructions"""
        ...

    @abstractmethod
    def _ending_or_ended(self) -> bool:
        """Is the worker in the process of shutting down or has it finished?"""
        ...

    @abstractmethod
    def logs(self) -> list[str]:
        """Retrieve logs from the worker through whatever communication means it has"""
        ...

    def abort(self):
        """Abort the entire worker"""
        self._aborted.set()
        self._kill_process()

    @abstractmethod
    def abort_execution(self, execution_id: str):
        """Abort the execution with the given id"""
        ...

    @abstractmethod
    def work(self) -> None:
        """Run the external process and communicate with it until it ends"""
        ...
