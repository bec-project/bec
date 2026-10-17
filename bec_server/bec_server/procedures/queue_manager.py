"""Logical procedure queues with serial dispatch and their own credential-bound workers."""

from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
from threading import BoundedSemaphore, Event, Thread
from typing import Any
from uuid import uuid4

from redis.exceptions import RedisError

from bec_lib.acl_login import BECAccess
from bec_lib.endpoints import MessageEndpoints as ME
from bec_lib.logger import bec_logger
from bec_lib.messages import (
    AvailableResourceMessage,
    ProcedureExecutionMessage,
    ProcedureWorkerStatus,
)
from bec_lib.procedures.helper import BackendProcedureHelper
from bec_lib.serialization import MsgpackSerialization
from bec_server.procedures.constants import PROCEDURE
from bec_server.procedures.oop_worker_base import WorkerExpired
from bec_server.procedures.worker_base import ProcedureWorker

logger = bec_logger.logger


@dataclass
class QueueWorker:
    """One worker owned by a logical queue, permanently bound to its credentials."""

    credentials: dict[str, str | None] = field(repr=False)
    environment: dict[str, str] = field(default_factory=dict)
    worker: Any = None
    busy: bool = False
    stopping: bool = False


@dataclass
class QueueAdmission:
    """An in-memory admission whose Redis enqueue may need an idempotent retry."""

    execution: ProcedureExecutionMessage
    enqueued: bool = False


@dataclass
class ProcedureQueue:
    """A stable queue name, a serial runner, and that queue's worker pool."""

    name: str
    workers: dict[str, QueueWorker] = field(default_factory=dict)
    future: Future | None = None
    admissions: deque[QueueAdmission] = field(default_factory=deque)
    current: ProcedureExecutionMessage | None = None
    current_worker: str | None = None
    runner_error: str | None = None
    cancelled: Event = field(default_factory=Event)
    wake: Event = field(default_factory=Event)


class QueueWorkerPools:
    """Queue-local worker selection; actor managers keep their existing lifetime policy."""

    def __init__(
        self,
        redis: str,
        worker_type: type[ProcedureWorker],
        thread_prefix: str = "user_procedure_",
        redis_credentials: dict[str, str | None] | None = None,
    ) -> None:
        """Initialise stable queues and their credential-bound worker pools.

        Args:
            redis (str): Redis server address.
            worker_type (type[ProcedureWorker]): Subprocess or container worker implementation.
            thread_prefix (str): Prefix for serial queue runner threads.
            redis_credentials (dict[str, str | None] | None): Parent service credentials.
        """
        super().__init__(redis, worker_type, thread_prefix, redis_credentials, _defer_startup=True)
        self._helper = BackendProcedureHelper(self._conn, monitor_responses=False)
        self._messages_by_ids = {}
        self._credentials_by_id = {}
        self._stopping = Event()
        self._worker_slots = BoundedSemaphore(PROCEDURE.WORKER.MAX_WORKERS)
        credentials = BECAccess(self._conn).current_credentials()
        self._control_credentials = credentials
        registry = self._conn.get(ME.procedure_queues())
        self._queues = (
            {name: ProcedureQueue(name) for name in registry.resource}
            if isinstance(registry, AvailableResourceMessage)
            else {}
        )
        self._startup()
        self._publish_pools()
        self._register_endpoints()
        self._publish_available()
        self._maintenance = Thread(
            target=self._maintain_workers, name="procedure-pool-maintenance", daemon=True
        )
        self._maintenance.start()

    def _process_queue_request(self, msg, *, token=None):
        message = self._validate_request(msg.value)
        if message is None:
            return None
        with self.lock:
            if self._stopping.is_set():
                return None
            if message.execution_id in self._messages_by_ids:
                self._ack(False, "Execution ID is already pending or running", message.execution_id)
                return None
            name = message.queue or PROCEDURE.WORKER.DEFAULT_QUEUE
            if any(character in name for character in "/\\*?[]"):
                self._ack(False, "Queue name contains reserved characters", message.execution_id)
                return None
            info = message.metadata.get("client_info", {})
            credentials = {
                "username": info.get("acl_user") or "default",
                "token": info.pop("acl_token", token),
            }
            execution = self._respond_to_valid_request(message)
            self._messages_by_ids[execution.execution_id] = execution
            self._credentials_by_id[execution.execution_id] = credentials
            queue = self._queues.setdefault(name, ProcedureQueue(name))
            queue.admissions.append(QueueAdmission(execution))
            try:
                self._flush_admissions(queue)
            except RedisError:
                logger.warning("Procedure admission will retry after Redis recovers")
            finally:
                self._start_queue(queue)
                queue.wake.set()
            self._publish_pools()
            return execution

    def _flush_admissions(self, queue):
        """Keep credentials and FIFO order when enqueue, notification, or ACK fails."""
        while queue.admissions:
            admission = queue.admissions[0]
            execution = admission.execution
            if not admission.enqueued:
                pipe = self._conn.pipeline()
                endpoint = ME.procedure_execution(queue.name)
                self._conn.lrem(endpoint, 0, execution, pipe=pipe)
                self._conn.rpush(endpoint, execution, pipe=pipe)
                self._conn.execute_pipeline(pipe)
                admission.enqueued = True
            self._helper.notify_watchers(queue.name, "execution")
            self._ack(True, f"Running procedure {execution.identifier}", execution.execution_id)
            queue.admissions.popleft()

    def _start_queue(self, queue):
        if not self._stopping.is_set() and (queue.future is None or queue.future.done()):
            queue.future = self.executor.submit(self._run_queue, queue)
            queue.future.add_done_callback(self._cleanup_worker_function(queue.name))

    def _cleanup_worker_function(self, name):
        def finished(future):
            with self.lock:
                queue = self._queues[name]
                if not future.cancelled() and future.exception() is not None:
                    logger.error(
                        f"Procedure queue {name} runner failed: {type(future.exception()).__name__}"
                    )
                    if queue.current is not None:
                        queue.runner_error = (
                            f"Procedure queue runner failed ({type(future.exception()).__name__})"
                        )
                if not self._stopping.is_set() and (
                    queue.current is not None or self._conn.llen(ME.procedure_execution(name))
                ):
                    self._start_queue(queue)

        return finished

    def _publish_pools(self):
        self._conn.set(
            ME.procedure_queues(),
            AvailableResourceMessage(resource={name: {} for name in self._queues}),
        )
        self._conn.set(
            ME.procedure_workers(),
            AvailableResourceMessage(
                resource={
                    identifier: {
                        "queue": queue.name,
                        "status": entry.worker.status.name,
                        "execution_id": (
                            queue.current.execution_id
                            if queue.current_worker == identifier and queue.current
                            else None
                        ),
                    }
                    for queue in self._queues.values()
                    for identifier, entry in queue.workers.items()
                }
            ),
        )

    def _claim(self, queue):
        with self.lock:
            self._flush_admissions(queue)
            pending = self._conn.lrange(ME.procedure_execution(queue.name), 0, 0)
            if not pending and queue.current is None:
                return None
            execution = queue.current or pending[0]
            if execution.execution_id not in self._credentials_by_id:
                raise RuntimeError(
                    "Execution was not admitted through the procedure request endpoint"
                )
            if queue.current is None:
                queue.current = execution
                queue.current_worker = None
                queue.cancelled.clear()
            pipe = self._conn.pipeline()
            self._conn.lrem(ME.procedure_execution(queue.name), 1, execution, pipe=pipe)
            pipe.sadd(
                ME.active_procedure_executions().endpoint, MsgpackSerialization.dumps(execution)
            )
            self._conn.execute_pipeline(pipe)
            self._helper.notify_watchers(queue.name, "execution")
            return execution

    def _close_worker(self, queue, identifier):
        with self.lock:
            entry = queue.workers.get(identifier)
            if entry is not None:
                entry.stopping = True
        if entry is None:
            return
        entry.worker.abort()
        try:
            self._logs.extend(entry.worker.logs())
        except Exception:
            logger.warning("Could not retrieve stopped procedure worker output")
        with self.lock:
            if queue.workers.pop(identifier, None) is None:
                return
        self._worker_slots.release()
        self._conn.delete(ME.procedure_worker_status_update(identifier))
        with self.lock:
            self._publish_pools()

    def _select_worker(self, queue, execution):
        credentials = self._credentials_by_id[execution.execution_id]
        environment = execution.env or {}
        with self.lock:
            for identifier, entry in queue.workers.items():
                if (
                    not entry.stopping
                    and not entry.worker._ending_or_ended()
                    and entry.credentials == credentials
                    and entry.environment == environment
                ):
                    entry.busy = True
                    return identifier, entry.worker
        while not self._worker_slots.acquire(timeout=0.1):
            if queue.cancelled.is_set() or self._stopping.is_set():
                return None, None
            # Retire an incompatible idle worker in this queue to make room.
            with self.lock:
                idle = next(
                    (
                        identifier
                        for identifier, entry in queue.workers.items()
                        if not entry.busy and not entry.stopping
                    ),
                    None,
                )
                if idle is not None:
                    queue.workers[idle].stopping = True
            if idle is not None:
                self._close_worker(queue, idle)
        identifier = uuid4().hex
        try:
            worker = self._worker_cls(
                self._server,
                queue.name,
                PROCEDURE.WORKER.QUEUE_TIMEOUT_S,
                environment,
                worker_id=identifier,
                credentials=credentials,
                control_credentials=self._control_credentials,
            )
        except BaseException:
            self._worker_slots.release()
            raise
        with self.lock:
            queue.workers[identifier] = QueueWorker(credentials, environment, worker, busy=True)
        return identifier, worker

    def _started(self, execution):
        with self.lock:
            self._helper.status_update(execution.execution_id, "Started")
            self._publish_pools()

    def _maintain_workers(self):
        while not self._stopping.wait(0.1):
            with self.lock:
                for queue in self._queues.values():
                    if queue.admissions:
                        try:
                            self._flush_admissions(queue)
                            self._start_queue(queue)
                            queue.wake.set()
                        except RedisError:
                            pass
                waiting = any(
                    queue.future is not None
                    and not queue.future.running()
                    and not queue.future.done()
                    for queue in self._queues.values()
                )
                expired = []
                for queue in self._queues.values():
                    for identifier, entry in queue.workers.items():
                        if not entry.busy and (
                            entry.stopping or waiting or entry.worker._ending_or_ended()
                        ):
                            entry.stopping = True
                            expired.append((queue, identifier))
            for queue, identifier in expired:
                try:
                    self._close_worker(queue, identifier)
                    queue.wake.set()
                except Exception:
                    logger.error("Failed to retire procedure worker")

    def _execute(self, queue, execution):
        if queue.runner_error is not None:
            return queue.runner_error
        while not queue.cancelled.is_set() and not self._stopping.is_set():
            identifier, worker = self._select_worker(queue, execution)
            if worker is None:
                return "Aborted by user."
            with self.lock:
                queue.current_worker = identifier
                cancelled = queue.cancelled.is_set() or self._stopping.is_set()
                self._publish_pools()
            if cancelled:
                self._close_worker(queue, identifier)
                return "Aborted by user."
            try:
                return worker.execute(execution, lambda: self._started(execution))
            except WorkerExpired:
                # Idle expiry raced assignment; no procedure started, so retry in place.
                self._close_worker(queue, identifier)
            except Exception:
                self._close_worker(queue, identifier)
                raise
        return "Aborted by user."

    def _finish(self, queue, execution, error):
        with self.lock:
            aborted = queue.cancelled.is_set() or self._stopping.is_set()
            error = "Aborted by user." if aborted else error
            pipe = self._conn.pipeline()
            self._conn.remove_from_set(ME.active_procedure_executions(), execution, pipe=pipe)
            if error:
                endpoint = ME.unhandled_procedure_execution(queue.name)
                self._conn.lrem(endpoint, 0, execution, pipe=pipe)
                self._conn.rpush(endpoint, execution, pipe=pipe)
            self._conn.execute_pipeline(pipe)
            self._helper.status_update(
                execution.execution_id, "Aborted" if aborted else "Finished", error=error
            )
            self._messages_by_ids.pop(execution.execution_id, None)
            self._credentials_by_id.pop(execution.execution_id, None)
            queue.current = None
            queue.runner_error = None
            self._helper.notify_watchers(queue.name, "execution")
            if error:
                self._helper.notify_watchers(queue.name, "unhandled")
            self._publish_pools()

    def _run_queue(self, queue):
        try:
            while not self._stopping.is_set():
                with self.lock:
                    blocked = [
                        identifier
                        for identifier, entry in queue.workers.items()
                        if entry.busy and entry.stopping
                    ]
                if blocked:
                    try:
                        for identifier in blocked:
                            self._close_worker(queue, identifier)
                    except Exception:
                        self._stopping.wait(0.1)
                        continue
                try:
                    execution = self._claim(queue)
                except RedisError:
                    if self._stopping.wait(0.1):
                        raise
                    continue
                if execution is None:
                    if not queue.workers:
                        return
                    queue.wake.wait(0.1)
                    queue.wake.clear()
                    continue
                try:
                    error = self._execute(queue, execution)
                except Exception as exc:
                    error = f"Procedure worker failed ({type(exc).__name__})"
                with self.lock:
                    current_worker = queue.current_worker
                    cancel_current = queue.cancelled.is_set()
                if cancel_current and current_worker is not None:
                    self._close_worker(queue, current_worker)
                while True:
                    try:
                        self._finish(queue, execution, error)
                        break
                    except RedisError:
                        if self._stopping.wait(0.1):
                            raise
                with self.lock:
                    worker = queue.workers.get(queue.current_worker)
                    if worker is not None:
                        worker.busy = False
                    callbacks = self._callbacks.pop(queue.name, [])
                    queue.current_worker = None
                for callback in callbacks:
                    try:
                        callback(worker.worker if worker else None)
                    except Exception:
                        logger.exception("Procedure completion callback failed")
        finally:
            for identifier in list(queue.workers):
                self._close_worker(queue, identifier)

    def active_workers(self) -> list[str]:
        """Return independent IDs of workers across all queue-owned pools.

        Returns:
            list[str]: Worker identifiers.
        """
        with self.lock:
            return [
                identifier
                for queue in self._queues.values()
                for identifier, entry in queue.workers.items()
            ]

    def worker_status(self, worker_id: str) -> ProcedureWorkerStatus:
        """Return a worker's current status.

        Args:
            worker_id (str): Worker identifier from the public registry.
        Returns:
            ProcedureWorkerStatus: Current state, or NONE if the worker is absent.
        """
        with self.lock:
            for queue in self._queues.values():
                if worker_id in queue.workers:
                    return queue.workers[worker_id].worker.status
        return ProcedureWorkerStatus.NONE

    def worker_statuses(self) -> dict[str, ProcedureWorkerStatus]:
        """Return status by independent worker ID.

        Returns:
            dict[str, ProcedureWorkerStatus]: Current worker states across queues.
        """
        return {identifier: self.worker_status(identifier) for identifier in self.active_workers()}

    def _cancel(self, identifiers):
        workers = []
        with self.lock:
            for identifier in identifiers:
                execution = self._messages_by_ids.get(identifier)
                if execution is None:
                    continue
                queue = self._queues[execution.queue]
                if queue.current and queue.current.execution_id == identifier:
                    queue.cancelled.set()
                    worker = queue.workers.get(queue.current_worker)
                    if worker is not None:
                        worker.stopping = True
                        workers.append(worker.worker)
                else:
                    self._conn.lrem(ME.procedure_execution(queue.name), 1, execution)
                    self._helper.push.unhandled(queue.name, execution)
                    self._helper.status_update(identifier, "Aborted", error="Aborted by user.")
                    self._messages_by_ids.pop(identifier, None)
                    self._credentials_by_id.pop(identifier, None)
                    queue.admissions = deque(
                        admission
                        for admission in queue.admissions
                        if admission.execution.execution_id != identifier
                    )
                    self._helper.notify_watchers(queue.name, "execution")
        for worker in workers:
            try:
                worker.abort()
            except Exception:
                logger.error("Procedure worker abort failed; queue cleanup will retry")

    def _process_abort(self, msg):
        message = msg.value
        with self.lock:
            identifiers = [
                identifier
                for identifier, execution in self._messages_by_ids.items()
                if message.abort_all
                or message.execution_id == identifier
                or message.queue == execution.queue
            ]
        self._cancel(identifiers)

    def _abort_execution(self, execution_id):
        self._cancel([execution_id])

    def _abort_queue(self, name):
        with self.lock:
            identifiers = [
                identifier
                for identifier, execution in self._messages_by_ids.items()
                if execution.queue == name
            ]
        self._cancel(identifiers)

    def _abort_all(self):
        with self.lock:
            identifiers = list(self._messages_by_ids)
        self._cancel(identifiers)

    def shutdown(self) -> None:
        """Stop queue runners and their worker processes before closing Redis."""
        if self._stopping.is_set():
            return
        self._unregister_endpoints()
        self._stopping.set()
        self._maintenance.join()
        self._abort_all()
        for queue in self._queues.values():
            queue.wake.set()
        self.executor.shutdown(wait=True)
        self._publish_pools()
        self._conn.shutdown()
