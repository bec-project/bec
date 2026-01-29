import time

from bec_lib.logger import bec_logger
from bec_server.procedures.constants import PROCEDURE, PodmanContainerStates
from bec_server.procedures.container_utils import get_backend
from bec_server.procedures.oop_worker_base import (
    main,  # temporarily needed before update of base image on release
)
from bec_server.procedures.oop_worker_base import PROCESS_TIMEOUT, OutOfProcessWorkerBase
from bec_server.procedures.protocol import ContainerCommandBackend

logger = bec_logger.logger


class ContainerProcedureWorker(OutOfProcessWorkerBase):
    """A worker which runs scripts in a container with a full BEC environment,
    mounted from the filesystem, and only access to Redis"""

    # The Podman client is a thin wrapper around the libpod API
    # documented at https://docs.podman.io/en/latest/_static/api.html
    # which is more detailed than the podman-py documentation

    def _setup_execution_environment(self):
        self._backend: ContainerCommandBackend = get_backend()
        image_tag = f"{PROCEDURE.CONTAINER.IMAGE_NAME}:v{PROCEDURE.BEC_VERSION}"
        self.container_name = f"bec_procedure_{PROCEDURE.BEC_VERSION}_{self._queue}"
        if not self._backend.image_exists(image_tag):
            self._backend.build_worker_image()
        self._container_id = self._backend.run(
            image_tag,
            self._worker_environment(),
            [
                {
                    "source": str(PROCEDURE.CONTAINER.DEPLOYMENT_PATH),
                    "target": "/bec",
                    "type": "bind",
                    "read_only": True,
                }
            ],
            PROCEDURE.CONTAINER.COMMAND,
            pod_name=PROCEDURE.CONTAINER.POD_NAME,
            container_name=self.container_name,
        )

    def _ending_or_ended(self):
        return self._backend.state(self._container_id) in [
            PodmanContainerStates.EXITED,
            PodmanContainerStates.STOPPED,
            PodmanContainerStates.STOPPING,
        ]

    def _kill_process(self):
        if not self._ending_or_ended():
            self._backend.interrupt(self.container_name)
            start = time.monotonic()
            while time.monotonic() < start + PROCESS_TIMEOUT:
                if self._ending_or_ended():
                    return
                time.sleep(0.2)
            logger.warning(
                f"Procedure worker {self._container_id} for queue {self._queue} failed to shut down, killing."
            )
            self._backend.kill(self.container_name)

    def abort_execution(self, execution_id: str):
        """Abort the execution with the given id. Has no effect if the given ID is not the current job"""
        if execution_id == self._current_execution_id:
            self._backend.kill(self._container_id)
            self._helper.remove_from_active.by_exec_id(execution_id)
            logger.info(
                f"Aborting execution {execution_id}, restarting worker for queue: {self._queue}"
            )
            self._setup_execution_environment()

    def logs(self):
        if self._container_id is None:
            return [""]
        return self._backend.logs(self._container_id)
