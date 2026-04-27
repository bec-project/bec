"""A manager for BEC Actors, based on the same infrastructure as Procedures."""

import threading
from concurrent.futures import Future
from typing import Any
from uuid import uuid4

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import ActorExecutionMessage, ActorStartRequestMessage

from bec_server.actors.worker import ActorProcedureWorker
from bec_server.procedures.manager import ProcedureManagerBase, _resolve_dict

logger = bec_logger.logger


class ActorManager(ProcedureManagerBase[ActorStartRequestMessage, ActorExecutionMessage]):
    """A specialised procedure manager for running Actors."""

    def _define_endpoints(self):
        self._reply_ep = MessageEndpoints.actor_request_response()
        self._request_ep = MessageEndpoints.actor_start_request()
        self._abort_ep = MessageEndpoints.actor_stop_request()

    def __init__(self, redis: str, thread_prefix: str = "actor_"):
        super().__init__(redis, ActorProcedureWorker, thread_prefix)

    def _register_endpoints(self):
        self._conn.register(self._request_ep, None, self._process_queue_request)

    def _unregister_endpoints(self):
        self._conn.unregister(self._request_ep, None, self._process_queue_request)

    def _publish_available(self): ...

    def _startup(self): ...

    def _validate_request(self, msg: dict[str, Any] | ActorStartRequestMessage):
        return _resolve_dict(msg, ActorStartRequestMessage)

    def _respond_to_valid_request(self, message: ActorStartRequestMessage):
        queue = f"{message.actor_module}.{message.actor_class_name}"
        exec_id = str(uuid4())
        return ActorExecutionMessage(
            execution_id=exec_id,
            queue=queue,
            env={
                "actor_module": message.actor_module,
                "actor_class_name": message.actor_class_name,
                "actor_exec_id": exec_id,
            },
        )

    def _cleanup_worker_function(self, queue: str):
        def cleanup_worker(fut: Future): ...

        return cleanup_worker


if __name__ == "__main__":
    e = threading.Event()
    manager = ActorManager(redis="localhost:6379")
    try:
        e.wait()
    except KeyboardInterrupt:
        ...
