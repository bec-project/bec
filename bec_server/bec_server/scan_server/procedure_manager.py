from abc import ABC, abstractmethod
import atexit
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum, auto
from threading import RLock
from typing import TypedDict
from unittest.mock import MagicMock
from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import ProcedureExecutionMessage
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.scan_server import ScanServer
from pydantic import ValidationError

MAX_WORKERS = 10
PROCEDURE_QUEUE_TIMEOUT_S = 10

procedure_list = ["test print"]


class ProcedureWorkerStatus(Enum):
    RUNNING = auto()
    IDLE = auto()


class ProcedureWorker(ABC):
    """Worker which automatically dies when there is nothing in the queue for TIMEOUT s"""

    def __init__(self, server: str, queue: str):
        """Start a worker to run procedures on the queue identified by `queue`."""
        self.key = MessageEndpoints.procedure_execution(queue).endpoint
        self.status = ProcedureWorkerStatus.IDLE
        self._conn = RedisConnector([server])
        self.client_id = self._conn.client_id()

        self._setup_execution_environment()

    def __enter__(self):
        return self

    @abstractmethod
    def _kill_process(self):
        """Clean up the execution environment, e.g. kill container or running subprocess.
        Should be safe to call multiple times, as it could be called in abort() and again on
        __exit__()."""
        ...

    @abstractmethod
    def _run_task(self, item: ProcedureExecutionMessage):
        """Actually cause the procedure to be executed.
        Should block until the procedure is complete."""

        # for a single scan procedure, this can just send the message,
        # then block for the scan to appear in the history
        ...

    @abstractmethod
    def _setup_execution_environment(self): ...

    def abort(self):
        self._kill_process()
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._kill_process()
        ...

    def work(self):
        # if podman container worker, this should probably monitor the queue there
        while (
            item := self._conn._redis_conn.brpop([self.key], timeout=PROCEDURE_QUEUE_TIMEOUT_S)
        ) is not None:  # blmove self.key active_procedures RIGHT LEFT :
            self.status = ProcedureWorkerStatus.RUNNING
            self._run_task(item)
            self.status = ProcedureWorkerStatus.IDLE


class PrintTestProcedureWorker(ProcedureWorker):
    def _kill_process(self):
        print(
            f"procedure worker for queue {self.key} timed out after {PROCEDURE_QUEUE_TIMEOUT_S} s, shutting down"
        )

    def _run_task(self, item):
        print(item)

    def _setup_execution_environment(self):
        print(f"procedure worker for queue {self.key} spinning up")


class ProcedureWorkerEntry(TypedDict):
    worker: ProcedureWorker | None
    future: Future


class ProcedureManager:
    """Watch the request queue and push to worker queues"""

    def __init__(self, parent: ScanServer):
        self._parent = parent
        self.lock = RLock()
        self.active_workers: dict[str, ProcedureWorkerEntry] = {}
        self.executor = ThreadPoolExecutor(
            max_workers=MAX_WORKERS, thread_name_prefix="user_procedure_"
        )
        atexit.register(self.executor.shutdown)

        self._worker_cls = PrintTestProcedureWorker
        self._conn = RedisConnector([self._parent.bootstrap_server])
        self._reply_endpoint = MessageEndpoints.procedure_request_response()
        self._server = f"{self._conn.host}:{self._conn.port}"

        self._conn.register(MessageEndpoints.procedure_request(), None, self.process_queue_request)

    def _ack(self, accepted: bool, msg: str):

        self._conn.send(
            self._reply_endpoint, messages.RequestResponseMessage(accepted=accepted, message=msg)
        )

    def process_queue_request(self, msg):
        try:
            message_obj = messages.ProcedureRequestMessage.model_validate(msg)
            proc_id = message_obj.procedure_identifier
            if proc_id not in procedure_list:
                self._ack(
                    False,
                    f"Procedure {proc_id} not known to the server. Available: {procedure_list}",
                )
        except ValidationError as e:
            self._ack(False, f"{e}")
            return
        self._ack(True, f"Running procedure {proc_id}")
        queue = message_obj.procedure_queue or "primary"
        endpoint = MessageEndpoints.procedure_execution(queue)
        self._conn.lpush(endpoint, endpoint.message_type(procedure_identifier=proc_id))
        if queue not in self.active_workers.keys():
            new_worker = self.executor.submit(self.spawn, queue=queue)
            self.active_workers[queue] = {"worker": None, "future": new_worker}

    def spawn(self, queue: str):
        """Spawn a procedure worker future which listens to a given queue, i.e. procedure
        queue list in Redis."""

        if queue in self.active_workers.keys():
            raise ValueError(f"A worker for queue {queue} has already been created!")

        with self._worker_cls(self._server, queue) as worker:
            with self.lock:
                self.active_workers[queue]["worker"] = worker

            worker.work()

        with self.lock:
            del self.active_workers[queue]

    def shutdown(self):
        """cancel any procedure workers which haven't started and abort any which have"""
        for entry in self.active_workers.values():
            cancelled = entry["future"].cancel()
            if not cancelled:
                # unblock any waiting workers and let them shutdown
                if worker := entry["worker"]:
                    # redis unblock executor.client_id
                    worker.abort()


if __name__ == "__main__":  # pragma: no cover

    server = MagicMock()
    server.bootstrap_server = "localhost:6379"

    manager = ProcedureManager(server)

    while True:
        ...
