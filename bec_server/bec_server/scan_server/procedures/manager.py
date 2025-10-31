from __future__ import annotations

import atexit
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from threading import RLock
from typing import Any, Callable, TypedDict

from pydantic import ValidationError

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import (
    ProcedureAbortMessage,
    ProcedureClearUnhandledMessage,
    ProcedureExecutionMessage,
    ProcedureRequestMessage,
    ProcedureWorkerStatus,
    RequestResponseMessage,
)
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.procedures import procedure_registry
from bec_server.scan_server.procedures.constants import PROCEDURE, WorkerAlreadyExists
from bec_server.scan_server.procedures.helper import BackendProcedureHelper
from bec_server.scan_server.procedures.worker_base import ProcedureWorker
from bec_server.scan_server.scan_server import ScanServer

logger = bec_logger.logger


# TODO garbage collect IDs


class ProcedureWorkerEntry(TypedDict):
    worker: ProcedureWorker | None
    future: Future


def _log_on_end(future: Future):
    """Use as a callback so that future is always done."""
    if e := future.exception():
        logger.error(f"Worker failed with exception: {e}")
    else:
        logger.success(f"Procedure worker {future} shut down gracefully")


class ProcedureManager:

    def __init__(self, parent: ScanServer, worker_type: type[ProcedureWorker]):
        """Watches the request queue and pushes to worker execution queues. Manages
        instantiating and cleaning up after workers.

        Args:
            parent (ScanServer): the scan server to get the Redis server address from.
            worker_type (type[ProcedureWorker]): which kind of worker to use."""
        self._parent = parent
        self.lock = RLock()

        logger.success("Initialising procedure manager...")

        self._conn = RedisConnector([self._parent.bootstrap_server])
        self._helper = BackendProcedureHelper(self._conn)
        self._startup()

        self._active_workers: dict[str, ProcedureWorkerEntry] = {}
        self._messages_by_ids: dict[str, ProcedureExecutionMessage] = {}
        self.executor = ThreadPoolExecutor(
            max_workers=PROCEDURE.WORKER.MAX_WORKERS, thread_name_prefix="user_procedure_"
        )
        atexit.register(self.executor.shutdown)

        self._callbacks: dict[str, list[Callable[[ProcedureWorker], Any]]] = {}
        self._worker_cls = worker_type
        self._reply_endpoint = MessageEndpoints.procedure_request_response()
        self._server = f"{self._conn.host}:{self._conn.port}"

        self._conn.register(MessageEndpoints.procedure_abort(), None, self._process_abort)
        self._conn.register(
            MessageEndpoints.procedure_clear_unhandled(), None, self._process_clear_unhandled
        )
        self._conn.register(MessageEndpoints.procedure_request(), None, self._process_queue_request)
        logger.success("Done initialising procedure manager.")

    def _startup(self):
        # If the server is restarted, clear any pending requests, they'll have to be resubmitted
        self._conn.delete(MessageEndpoints.procedure_request())
        previous_queues = self._helper.get.active_and_pending_queue_names()
        logger.debug(f"Clearing previous procedure queues {previous_queues}...")
        self._helper.move.all_execution_queues_to_unhandled()
        self._helper.move.all_active_to_unhandled()
        for queue in previous_queues:
            self._helper.notify_watchers(queue, "execution")
        self._helper.notify_all("unhandled")

    def _ack(self, accepted: bool, msg: str):
        logger.info(f"procedure accepted: {accepted}, message: {msg}")
        self._conn.send(
            self._reply_endpoint, RequestResponseMessage(accepted=accepted, message=msg)
        )

    def _validate_request(self, msg: dict[str, Any] | ProcedureRequestMessage):
        try:
            message_obj = ProcedureRequestMessage.model_validate(msg)
            if not procedure_registry.is_registered(message_obj.identifier):
                self._ack(
                    False,
                    f"Procedure {message_obj.identifier} not known to the server. Available: {list(procedure_registry.available())}",
                )
                return None
        except ValidationError as e:
            self._ack(False, f"{e}")
            return None
        return message_obj

    def add_callback(self, queue: str, cb: Callable[[ProcedureWorker], Any]):
        """Add a callback to run on the worker when it is finished."""
        if self._callbacks.get(queue) is None:
            self._callbacks[queue] = []
        self._callbacks[queue].append(cb)

    def _run_callbacks(self, queue: str):
        with self.lock:
            if queue not in self._active_workers:
                logger.error(f"Attempted to run callbacks for nonexistent worker {queue}")
                return
            if (worker := self._active_workers[queue]["worker"]) is None:
                return
        for cb in self._callbacks.get(queue, []):
            cb(worker)
        self._callbacks[queue] = []

    def _process_queue_request(self, msg: dict[str, Any] | ProcedureRequestMessage):
        """Read a `ProcedureRequestMessage` and if it is valid, create a corresponding `ProcedureExecutionMessage`.
        If there is already a worker for the queue for that request message, add the execution message to that queue,
        otherwise create a new queue and a new worker.

        Args:
            msg (dict[str, Any]): dict corresponding to a ProcedureRequestMessage"""

        logger.debug(f"Procedure manager got request message {msg}")
        if (message := self._validate_request(msg)) is None:
            return
        self._ack(True, f"Running procedure {message.identifier}")
        queue = message.queue or PROCEDURE.WORKER.DEFAULT_QUEUE
        exec_message = ProcedureExecutionMessage(
            identifier=message.identifier, queue=queue, args_kwargs=message.args_kwargs or ((), {})
        )
        logger.debug(f"active workers: {self._active_workers}, worker requested: {queue}")
        self._helper.push.exec(queue, exec_message)

        def cleanup_worker(fut):
            with self.lock:
                logger.debug(f"cleaning up worker {fut} for queue {queue}...")
                self._helper.remove_from_active.by_queue(queue)
                self._run_callbacks(queue)
                self._helper.notify_watchers(queue, "execution")
                del self._active_workers[queue]

        with self.lock:
            if queue not in self._active_workers:
                new_worker = self.executor.submit(self.spawn, queue=queue)
                new_worker.add_done_callback(_log_on_end)
                new_worker.add_done_callback(cleanup_worker)
                self._active_workers[queue] = {"worker": None, "future": new_worker}
            self._messages_by_ids[exec_message.execution_id] = exec_message

    def _process_abort(self, msg: dict[str, Any] | ProcedureAbortMessage):
        message = ProcedureAbortMessage.model_validate(msg)
        with self.lock:
            if message.abort_all:
                self._abort_all()
            if message.queue is not None:
                self._abort_queue(message.queue)
            if message.execution_id is not None:
                self._abort_execution(message.execution_id)

    def _abort_execution(self, execution_id: str):
        if (msg := self._messages_by_ids.get(execution_id)) is None:
            logger.warning(f"Procedure execution with ID {execution_id} not known.")
            return
        # Remove it from the queue if not yet started
        if self._conn.lrem(MessageEndpoints.procedure_execution(msg.queue), 0, msg) > 0:
            logger.debug(f"Removed execution {msg} from queue.")
            self._helper.notify_watchers(msg.queue, "execution")
        # Otherwise try to remove it from whichever worker has it
        for entry in self._active_workers.values():
            if (worker := entry["worker"]) is not None and worker:
                worker.abort_execution(execution_id)
        # Move it to unhandled and stop tracking
        self._helper.push.unhandled(msg.queue, msg)
        del self._messages_by_ids[execution_id]

    def _abort_queue(self, queue: str):
        self._helper.move.execution_queue_to_unhandled(queue)
        if (entry := self._active_workers.get(queue)) is not None:
            if entry["worker"] is not None:
                entry["worker"].abort()
            entry["future"].cancel()
            futures.wait((entry["future"],), PROCEDURE.MANAGER_SHUTDOWN_TIMEOUT_S)
        else:
            logger.warning(f"Received abort request for unknown queue {queue}!")
        self._helper.notify_watchers(queue, "execution")

    def _abort_all(self):
        for entry in self._active_workers.values():
            if entry["worker"] is not None:
                entry["worker"].abort()
        for entry in self._active_workers.values():
            entry["future"].cancel()
        self._helper.move.all_execution_queues_to_unhandled()
        self._wait_for_all_futures()

    def _process_clear_unhandled(self, msg: dict[str, Any] | ProcedureClearUnhandledMessage):
        message = ProcedureClearUnhandledMessage.model_validate(msg)
        with self.lock:
            if message.abort_all:
                self._helper.clear.all_unhandled()
            if message.queue is not None:
                self._helper.clear.unhandled_queue(message.queue)
            if message.execution_id is not None:
                self._helper.clear.unhandled_execution(message.execution_id)

    def _wait_for_all_futures(self):
        futures.wait(
            (entry["future"] for entry in self._active_workers.values()),
            timeout=PROCEDURE.MANAGER_SHUTDOWN_TIMEOUT_S,
        )

    def spawn(self, queue: str):
        """Spawn a procedure worker future which listens to a given queue, i.e. procedure queue list in Redis.

        Args:
            queue (str): name of the queue to spawn a worker for"""

        if queue in self._active_workers and self._active_workers[queue]["worker"] is not None:
            raise WorkerAlreadyExists(
                f"Queue {queue} already has an active worker in {self._active_workers}!"
            )
        with self._worker_cls(self._server, queue, PROCEDURE.WORKER.QUEUE_TIMEOUT_S) as worker:
            with self.lock:
                self._active_workers[queue]["worker"] = worker
            worker.work()

    def shutdown(self):
        """Shutdown the procedure manager. Unregisters from the request endpoint, cancel any
        procedure workers which haven't started, and abort any which have."""
        self._conn.unregister(
            MessageEndpoints.procedure_request(), None, self._process_queue_request
        )
        self._conn.shutdown()
        # cancel futures by hand to give us the opportunity to detatch them from redis if they have started
        for entry in self._active_workers.values():
            cancelled = entry["future"].cancel()
            if not cancelled:
                # unblock any waiting workers and let them shutdown
                if worker := entry["worker"]:
                    # redis unblock executor.client_id
                    worker.abort()
        self._wait_for_all_futures()
        self.executor.shutdown()

    def active_workers(self) -> list[str]:
        with self.lock:
            return list(self._active_workers.keys())

    def worker_statuses(self) -> dict[str, ProcedureWorkerStatus]:
        with self.lock:
            return {
                q: w["worker"].status if w["worker"] is not None else ProcedureWorkerStatus.NONE
                for q, w in self._active_workers.items()
            }
