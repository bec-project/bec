import os
from typing import cast

import podman
from podman.domain.containers import Container

from bec_lib import messages
from bec_lib.client import BECClient
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.messages import ProcedureExecutionMessage
from bec_lib.redis_connector import RedisConnector
from bec_server.scan_server.procedures.constants import (
    PODMAN_URI,
    ContainerWorkerEnv,
    ProcedureWorkerError,
    ProcedureWorkerStatus,
)
from bec_server.scan_server.procedures.worker_base import ProcedureWorker

logger = bec_logger.logger


class ContainerProcedureWorker(ProcedureWorker):
    def _worker_environment(self) -> ContainerWorkerEnv:
        return {
            "redis_server": "localhost:6379",
            "queue": self._queue,
            "timeout_s": str(self._lifetime_s),
        }

    def _setup_execution_environment(self):
        with podman.PodmanClient(base_url=PODMAN_URI) as client:
            self._container: Container = client.containers.run(
                "bec:latest",
                "bec-procedure-worker",
                detach=True,
                environment=self._worker_environment(),
                pod="local_bec",
            )  # type: ignore # running with detach returns container object

    def _run_task(self, item: ProcedureExecutionMessage):
        raise ProcedureWorkerError(
            f"Container worker _run_task() called with {item} - this should never happen!"
        )

    def _kill_process(self):
        self._container.kill()

    def work(self):
        """block until the container is killed - update status in the meantime"""
        # BLPOP from PocWorkerStatus and set status
        # on timeout check if container is still running

        status_update = None
        while self._container.status not in ["exited", "stopped"]:
            status_update = self._conn.blocking_list_pop(
                MessageEndpoints.procedure_worker_status_update(self._queue)
            )
            if status_update is not None:
                if not isinstance(status_update, messages.ProcedureWorkerStatusMessage):
                    raise ProcedureWorkerError(f"Received unexpected message {status_update}")
                self.status = status_update.status
                logger.debug(f"Container worker status update: {status_update}")


def main():
    """Replaces the main contents of Worker.work() - should be called as the container entrypoint"""
    from bec_lib.logger import bec_logger

    logger = bec_logger.logger
    try:
        needed_keys = ContainerWorkerEnv.__annotations__.keys()
        logger.debug(f"Checking for environment variables: {needed_keys}")
        env: ContainerWorkerEnv = {k: os.environ[k] for k in needed_keys}  # type: ignore
    except KeyError as e:
        logger.error(f"Missing environment variable needed by container worker: {e}")
        return

    logger.info(f"ContainerWorker started container for queue {env['queue']}")
    logger.debug(f"ContainerWorker environment: {env}")

    endpoint_info = MessageEndpoints.procedure_execution(env["queue"])
    conn = RedisConnector(env["redis_server"])
    active_procs_endpoint = MessageEndpoints.active_procedure_executions()
    status_endpoint = MessageEndpoints.procedure_worker_status_update(env["queue"])

    logger.debug(f"ContainerWorker connecting to Redis at {conn.host}:{conn.port}")
    client = BECClient()
    client.start()
    logger.debug(f"ContainerWorker client started")

    def _push_status(status: ProcedureWorkerStatus):
        conn.rpush(
            status_endpoint,
            messages.ProcedureWorkerStatusMessage(worker_queue=env["queue"], status=status),
        )

    def _run_task(item: ProcedureExecutionMessage):
        # evaluate procedure
        logger.success(f"Running procedure {item.identifier} in container")

    _push_status(ProcedureWorkerStatus.IDLE)
    item = None
    try:
        logger.debug(f"ContainerWorker waiting for instructions on {endpoint_info}")
        while (
            item := conn.blocking_list_pop_to_set_add(
                endpoint_info, active_procs_endpoint, timeout_s=int(env["timeout_s"])
            )
        ) is not None:
            _push_status(ProcedureWorkerStatus.RUNNING)
            _run_task(cast(ProcedureExecutionMessage, item))
            _push_status(ProcedureWorkerStatus.IDLE)
    except Exception as e:
        logger.error(e)  # don't stop ProcedureManager.spawn from cleaning up
    finally:
        client.shutdown()
        if item is not None:  # in this case we are here due to an exception, not a timeout
            conn.remove_from_set(active_procs_endpoint, item)
        _push_status(ProcedureWorkerStatus.FINISHED)


if __name__ == "__main__":
    main()
