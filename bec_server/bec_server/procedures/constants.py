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


P = ParamSpec("P")


@runtime_checkable
class BecProcedure(Protocol[P]):
    """A procedure should not return anything, because it could be run in an isolated environment
    and data needs to be extracted in other ways. It may be a simple function, but it can also be
    a class instance which implements __call__ and has its state initialised by its worker class.
    """

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


def _deployment_path() -> Path:
    """Locate the source tree mounted into procedure containers, including wheel deployments."""
    configured_path = os.environ.get("BEC_PROCEDURE_DEPLOYMENT_PATH")
    if configured_path:
        return Path(configured_path).expanduser().resolve()
    return Path(bec_lib.__file__).resolve().parents[2]


@dataclass(frozen=True)
class _CONTAINER:
    PODMAN_URI = "unix:///run/user/1000/podman/podman.sock"
    IMAGE_NAME = "bec_procedure_worker"
    # Procedure images install editable packages from the source tree mounted at /bec.
    # Wheel installations can supply that tree via BEC_PROCEDURE_DEPLOYMENT_PATH.
    DEPLOYMENT_PATH = _deployment_path()
    CONTAINERFILE_LOCATION = Path(__file__).resolve().parent
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
