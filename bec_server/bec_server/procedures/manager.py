from __future__ import annotations

import atexit
import re
from abc import ABC, abstractmethod
from collections import deque
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from threading import RLock
from typing import TYPE_CHECKING, Any, Callable, Generic, NotRequired, Protocol, TypedDict, TypeVar

from pydantic import ValidationError

from bec_lib.endpoints import EndpointInfo, MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import (
    AvailableResourceMessage,
    BECMessage,
    ProcedureClearUnhandledMessage,
    ProcedureExecutionMessage,
    ProcedureQNotifMessage,
    ProcedureRequestMessage,
    ProcedureWorkerStatus,
    RequestResponseMessage,
)
from bec_lib.redis_connector import MessageObject, RedisConnector
from bec_server.procedures import procedure_registry
from bec_server.procedures.constants import PROCEDURE, WorkerAlreadyExists
from bec_server.procedures.queue_manager import QueueWorkerPools

if TYPE_CHECKING:
    from bec_server.procedures.worker_base import ProcedureWorker

logger = bec_logger.logger


class ProcedureWorkerEntry(TypedDict):
    worker: ProcedureWorker | None
    future: Future
    abort_requested: NotRequired[bool]


class RedisCredentials(TypedDict):
    username: str | None
    token: str | None


def _log_on_end(future: Future):
    """Use as a callback so that future is always done."""
    if future.cancelled():
        return
    if e := future.exception():
        logger.error(f"Worker failed with exception: {e}")
    else:
        logger.success(f"Procedure worker {future} shut down gracefully")


class _ExecutionMsgProtocol(Protocol):
    queue: str
    execution_id: str
    env: dict[str, str] | None


_T = TypeVar("_T", bound=BECMessage)
_ReqMsgT = TypeVar("_ReqMsgT", bound=BECMessage)
_ExecMsgT = TypeVar("_ExecMsgT", bound=_ExecutionMsgProtocol)


class ProcedureManagerBase(ABC, Generic[_ReqMsgT, _ExecMsgT]):
    request_msg_type: _ReqMsgT
    exec_msg_type: _ExecMsgT

    def __init__(
        self,
        redis: str,
        worker_type: type[ProcedureWorker],
        thread_prefix: str = "user_procedure_",
        redis_credentials: RedisCredentials | None = None,
        *,
        _defer_startup: bool = False,
    ):
        """Watches the request queue and pushes to worker execution queues. Manages
        instantiating and cleaning up after workers.

        Args:
            redis (str): the scan redis server connection string "host:port".
            worker_type (type[ProcedureWorker]): which kind of worker to use.
            thread_prefix (str): an identifier to preprend to the manager thread names.
        """
        self.lock = RLock()
        self._define_endpoints()

        logger.success(
            f"Initialising {self.__class__.__name__}, using worker type '{worker_type.__name__}'..."
        )

        self._logs = deque([], maxlen=1000)

        # NOTE: I'm not convinced that we should really instantiate a new RedisConnector here
        # but I will keep it for now. We should probably simply pass in the existing RedisConnector
        # from the ScanServer, but that would require some refactoring of the ScanServer and ProcedureManager.
        self._conn = RedisConnector([redis], name="ProcedureManager RedisConnector")
        if redis_credentials is not None:
            self._conn.authenticate(
                username=redis_credentials["username"] or "default",
                password=redis_credentials.get("token"),
            )

        self._active_workers: dict[str, ProcedureWorkerEntry] = {}
        self.executor = ThreadPoolExecutor(
            max_workers=PROCEDURE.WORKER.MAX_WORKERS, thread_name_prefix=thread_prefix
        )
        atexit.register(self.executor.shutdown)

        self._callbacks: dict[str, list[Callable[[ProcedureWorker], Any]]] = {}
        self._worker_cls = worker_type
        self._server = f"{self._conn.host}:{self._conn.port}"

        self._reply_ep: EndpointInfo
        if not _defer_startup:
            self._register_endpoints()
            self._publish_available()
        logger.success("Done initialising procedure manager.")

    def _ack(self, accepted: bool, msg: str, exec_id: str):
        logger.info(f"procedure accepted: {accepted}, message: {msg}")
        self._conn.send(
            self._reply_ep,
            RequestResponseMessage(
                accepted=accepted, message={"message": msg, "execution_id": exec_id}
            ),
        )

    def _process_queue_request(self, msg: MessageObject) -> _ExecMsgT | None:
        """Read a request message, and if it is valid, create a corresponding `ProcedureExecutionMessage`.
        If there is already a worker for the queue for that request message, add the execution message to that queue,
        otherwise create a new queue and a new worker.

        Args:
            msg (dict[str, Any]): dict corresponding to a ProcedureRequestMessage"""

        logger.debug(f"Procedure manager got queue request on topic {msg.topic}")
        if (message := self._validate_request(msg.value)) is None:
            return
        exec_msg = self._respond_to_valid_request(message)
        queue, env = exec_msg.queue, exec_msg.env or {}
        with self.lock:
            if exec_msg.queue not in self._active_workers:
                new_worker = self.executor.submit(self.spawn, queue=queue, env=env)
                self._active_workers[queue] = {"worker": None, "future": new_worker}
                new_worker.add_done_callback(_log_on_end)
                new_worker.add_done_callback(self._cleanup_worker_function(queue))
        return exec_msg

    def spawn(self, queue: str, env: dict[str, str]):
        """Spawn a procedure worker future which listens to a given queue, i.e. procedure queue list in Redis.

        Args:
            queue (str): name of the queue to spawn a worker for"""

        if queue in self._active_workers and self._active_workers[queue]["worker"] is not None:
            raise WorkerAlreadyExists(
                f"Queue {queue} already has an active worker in {self._active_workers}!"
            )
        worker = None
        try:
            with self._worker_cls(
                self._server, queue, PROCEDURE.WORKER.QUEUE_TIMEOUT_S, env
            ) as worker:
                with self.lock:
                    entry = self._active_workers[queue]
                    entry["worker"] = worker
                    abort_requested = entry.get("abort_requested", False)
                if abort_requested:
                    worker.abort()
                else:
                    worker.work()
        finally:
            if worker is not None:
                logs = worker.logs()
                self._logs.extend(logs)
                for line in logs:
                    logger.info(f"Procedure worker '{queue}': {line}")

    def _abort_workers(
        self, entries: list[ProcedureWorkerEntry], before_wait: Callable[[], None] | None = None
    ) -> None:
        """Stop selected workers, optionally draining queues before waiting for startup."""
        with self.lock:
            for entry in entries:
                entry["abort_requested"] = True
            workers = [entry["worker"] for entry in entries if entry["worker"] is not None]
        for entry in entries:
            entry["future"].cancel()
        for worker in workers:
            worker.abort()
        if before_wait is not None:
            before_wait()
        # A cancelled future need not have been picked up by an executor thread yet.
        futs = [entry["future"] for entry in entries if not entry["future"].cancelled()]
        futures.wait(futs, timeout=PROCEDURE.MANAGER_SHUTDOWN_TIMEOUT_S)

    def active_workers(self) -> list[str]:
        with self.lock:
            return list(self._active_workers.keys())

    def worker_status(self, queue):
        with self.lock:
            worker = self._active_workers.get(queue)
            return (
                ProcedureWorkerStatus.NONE
                if worker is None or worker["worker"] is None
                else worker["worker"].status
            )

    def worker_statuses(self) -> dict[str, ProcedureWorkerStatus]:
        with self.lock:
            return {
                q: (w["worker"].status if w["worker"] is not None else ProcedureWorkerStatus.NONE)
                for q, w in self._active_workers.items()
            }

    def shutdown(self):
        """Shutdown the procedure manager. Unregisters from the request endpoint, cancel any
        procedure workers which haven't started, and abort any which have."""
        logger.debug(f"shutting down {self.__class__.__name__}")
        self._unregister_endpoints()
        self._conn.shutdown()
        with self.lock:
            entries = list(self._active_workers.values())
        self._abort_workers(entries)
        self.executor.shutdown()

    @abstractmethod
    def _define_endpoints(self):
        """Set instance variables for easy access to frequently used Redis endpoints."""

    @abstractmethod
    def _register_endpoints(self):
        """Attach callbacks to any Redis endpoints which require them, in particular _process_queue_request()"""

    @abstractmethod
    def _unregister_endpoints(self):
        """Remove any callbacks from any Redis endpoints"""

    @abstractmethod
    def _publish_available(self):
        """Publish information to Redis about the available procedures."""

    @abstractmethod
    def _startup(self):
        """Clean up any state which may have been left behind by an incorrectly terminated manager."""

    @abstractmethod
    def _validate_request(self, msg: _ReqMsgT) -> _ReqMsgT | None:
        """Check if the information in the supplied messasge or dict is a valid request. If yes,
        return it as a message, otherwise, return None."""

    @abstractmethod
    def _respond_to_valid_request(self, message: _ReqMsgT) -> _ExecMsgT:
        """If _validate_request passed successfully, the valid request message received here should
        be propagated to Redis, and the execution message returned to be passed on to spawn().
        """

    @abstractmethod
    def _cleanup_worker_function(self, queue: str) -> Callable[[Future], None]:
        """Returns a function which will be attached as a callback to finishing workers."""


class ProcedureManager(
    QueueWorkerPools, ProcedureManagerBase[ProcedureRequestMessage, ProcedureExecutionMessage]
):

    def _define_endpoints(self):
        self._reply_ep = MessageEndpoints.procedure_request_response()
        self._abort_ep = MessageEndpoints.procedure_abort()
        self._clear_uh_ep = MessageEndpoints.procedure_clear_unhandled()
        self._request_ep = MessageEndpoints.procedure_request("*")
        self._q_notif_ep = MessageEndpoints.procedure_queue_notif()
        self._exec_ep = MessageEndpoints.procedure_execution

    def _publish_available(self):
        self._conn.set(
            MessageEndpoints.available_procedures(),
            AvailableResourceMessage(
                resource={
                    name: procedure_registry.get_info(name)
                    for name in procedure_registry.available()
                }
            ),
        )

    def _register_endpoints(self):
        self._conn.register(self._abort_ep, None, self._process_abort)
        self._conn.register(self._clear_uh_ep, None, self._process_clear_unhandled)
        self._conn.register(patterns=self._request_ep, cb=self._procedure_request_callback)
        self._conn.register(self._q_notif_ep, None, self._publish_metrics)
        self._conn.set(
            MessageEndpoints.available_procedures(),
            AvailableResourceMessage(
                resource={
                    name: procedure_registry.get_info(name)
                    for name in procedure_registry.available()
                }
            ),
        )

    def _procedure_request_callback(self, msg: MessageObject):
        """Validate the personal topic against the submitted ACL identity before enqueueing."""
        message = msg.value
        if not isinstance(message, ProcedureRequestMessage):
            logger.warning("Received an invalid procedure request.")
            return
        client_info = message.metadata.get("client_info")
        token = client_info.pop("acl_token", None) if isinstance(client_info, dict) else None
        if token is not None and not isinstance(token, str):
            self._ack(False, "Invalid procedure ACL token type.", message.execution_id)
            return
        username_regex = MessageEndpoints.procedure_request("([^/]+)").endpoint
        match = re.fullmatch(username_regex, msg.topic)
        if not match:
            self._ack(False, "Could not extract username from topic.", message.execution_id)
            return
        if not isinstance(client_info, dict) or not client_info:
            self._ack(False, "Missing client info in request metadata.", message.execution_id)
            return
        acl_user = client_info.get("acl_user") or "default"
        if acl_user != match.group(1):
            self._ack(False, "Username in topic does not match client info.", message.execution_id)
            return
        return self._process_queue_request(msg, token=token)

    def _respond_to_valid_request(self, message: ProcedureRequestMessage):
        queue = message.queue or PROCEDURE.WORKER.DEFAULT_QUEUE
        exec_message = ProcedureExecutionMessage(
            identifier=message.identifier,
            queue=queue,
            args_kwargs=message.args_kwargs or ((), {}),
            execution_id=message.execution_id,
        )
        logger.debug(f"Procedure submitted to logical queue {queue}")
        return exec_message

    def _publish_metrics(self, msg: ProcedureQNotifMessage):
        self._conn.publish_metrics(
            "pending_procedure_queues",
            {
                name: self._conn.llen(self._exec_ep(name))
                for name in self._helper.get.active_and_pending_queue_names()
            },
        )

    def _startup(self):
        # If the server is restarted, clear any pending requests, they'll have to be resubmitted
        previous_queues = self._helper.get.active_and_pending_queue_names()
        logger.debug(f"Clearing previous procedure queues {previous_queues}...")
        self._helper.move.all_execution_queues_to_unhandled()
        self._helper.move.all_active_to_unhandled()
        for queue in previous_queues:
            self._helper.notify_watchers(queue, "execution")
        self._helper.notify_all("unhandled")

    def _validate_request(self, msg: ProcedureRequestMessage):
        try:
            if not procedure_registry.is_registered(msg.identifier):
                self._ack(
                    False,
                    f"Procedure {msg.identifier} not known to the server. Available: {list(procedure_registry.available())}",
                    exec_id=msg.execution_id,
                )
                return None
        except ValidationError as e:
            exec_id = (
                msg.get("execution_id", "ID UNKNOWN") if isinstance(msg, dict) else msg.execution_id
            )
            self._ack(False, f"{e}", exec_id)
            return None
        return msg

    def add_callback(self, queue: str, cb: Callable[[ProcedureWorker], Any]):
        """Add a callback to run on the worker when it is finished."""
        if self._callbacks.get(queue) is None:
            self._callbacks[queue] = []
        self._callbacks[queue].append(cb)

    def _process_clear_unhandled(self, msg: MessageObject):
        message: ProcedureClearUnhandledMessage = msg.value  # type: ignore
        with self.lock:
            if message.abort_all:
                self._helper.clear.all_unhandled()
            if message.queue is not None:
                self._helper.clear.unhandled_queue(message.queue)
            if message.execution_id is not None:
                self._helper.clear.unhandled_execution(message.execution_id)

    def _unregister_endpoints(self):
        """Remove any callbacks from any Redis endpoints"""
        self._conn.unregister(self._abort_ep, None, self._process_abort)
        self._conn.unregister(self._clear_uh_ep, None, self._process_clear_unhandled)
        self._conn.unregister(patterns=self._request_ep, cb=self._procedure_request_callback)
        self._conn.unregister(self._q_notif_ep, None, self._publish_metrics)
