import os
from dataclasses import dataclass
from enum import Enum, StrEnum
from importlib.metadata import version
from pathlib import Path
from typing import NotRequired, ParamSpec, Protocol, TypedDict, runtime_checkable

import bec_lib


class BecClientType(StrEnum):
    BECClient = "BECClient"
    BECIPythonClient = "BECIPythonClient"


class OopWorkerEnv(TypedDict):
    redis_server: str
    queue: str
    timeout_s: str
    client_class: NotRequired[BecClientType]
    worker_id: NotRequired[str]


P = ParamSpec("P")


@runtime_checkable
class BecProcedure(Protocol[P]):
    """A procedure should not return anything, because it could be run in an isolated environment
    and data needs to be extracted in other ways. It may be a simple function, but it can also be
    a class instance which implements __call__ and has its state initialised by its worker class.
    """

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> None: ...


class ProcedureWorkerFailure(str, Enum):
    """Fixed, credential-free descriptions suitable for public execution status."""

    STARTUP = "Procedure worker startup failed"
    AUTHENTICATION = "Procedure worker authentication failed"
    EXITED = "Procedure worker exited unexpectedly"
    INPUT_CLOSED = "Procedure worker input closed unexpectedly"
    ABORTED = "Procedure worker was aborted"


class ProcedureWorkerError(RuntimeError):
    """Worker failure with an optional safe category for public status."""

    def __init__(self, *args: object, failure: ProcedureWorkerFailure | None = None) -> None:
        """Default to the public failure description unless internal details are supplied."""
        if not args and failure is not None:
            args = (failure.value,)
        super().__init__(*args)
        self.failure = failure


class WorkerAlreadyExists(ProcedureWorkerError): ...


class NoPodman(ProcedureWorkerError): ...


class NoImage(ProcedureWorkerError): ...


@dataclass(frozen=True)
class _WORKER:
    MAX_WORKERS = 10
    QUEUE_TIMEOUT_S = 60
    DEFAULT_QUEUE = "primary"


@dataclass(frozen=True)
class _CONTAINER:
    PODMAN_URI = "unix:///run/user/1000/podman/podman.sock"
    IMAGE_NAME = "bec_procedure_worker"
    DEPLOYMENT_PATH = Path(os.path.dirname(bec_lib.__file__)) / "../../"
    CONTAINERFILE_LOCATION = (  # Directory where `Containerfile` lives
        DEPLOYMENT_PATH / "bec_server/bec_server/procedures/"
    )
    REQUIREMENTS_CONTAINERFILE_NAME = "Containerfile.requirements"
    REQUIREMENTS_IMAGE_NAME = "bec_requirements"
    WORKER_CONTAINERFILE_NAME = "Containerfile.worker"
    COMMAND = "bec-procedure-worker"
    POD_NAME = "local_bec"
    CONTAINERFILE_WORKER_TARGET = "procedure_worker"


@dataclass(frozen=True)
class _PROCEDURE:
    WORKER = _WORKER()
    CONTAINER = _CONTAINER()
    MANAGER_SHUTDOWN_TIMEOUT_S: float | None = None
    BEC_VERSION = version("bec_lib")
    REDIS_HOST = "redis"


PROCEDURE = _PROCEDURE()


class PodmanContainerStates(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    STOPPING = "stopping"
    EXITED = "exited"
