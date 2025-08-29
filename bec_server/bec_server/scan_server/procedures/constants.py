import os
from dataclasses import dataclass
from enum import Enum, auto
from importlib.metadata import version
from pathlib import Path
from typing import ParamSpec, Protocol, TypedDict, runtime_checkable

import bec_lib


class ContainerWorkerEnv(TypedDict):
    redis_server: str
    queue: str
    timeout_s: str


P = ParamSpec("P")


@runtime_checkable
class BecProcedure(Protocol[P]):
    """A procedure should not return anything, because it could be run in an isolated environment
    and data needs to be extracted in other ways. It may be a simple function, but it can also be
    a class instance which implements __call__ and has its state initialised by its worker class."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> None: ...


class ProcedureWorkerError(RuntimeError): ...


class WorkerAlreadyExists(ProcedureWorkerError): ...


class NoPodman(ProcedureWorkerError): ...


class NoImage(ProcedureWorkerError): ...


@dataclass(frozen=True)
class _WORKER:
    MAX_WORKERS = 10
    QUEUE_TIMEOUT_S = 10
    DEFAULT_QUEUE = "primary"


@dataclass(frozen=True)
class _CONTAINER:
    PODMAN_URI = "unix:///run/user/1000/podman/podman.sock"
    IMAGE_NAME = "bec_procedure_worker"
    DEPLOYMENT_PATH = Path(os.path.dirname(bec_lib.__file__)) / "../../"
    CONTAINERFILE_LOCATION = (  # Directory where `Containerfile` lives
        DEPLOYMENT_PATH / "bec_server/bec_server/scan_server/procedures/"
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
    MANAGER_SHUTDOWN_TIMEOUT_S = 2
    BEC_VERSION = version("bec_lib")


PROCEDURE = _PROCEDURE()


class PodmanContainerStates(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    EXITED = "exited"
