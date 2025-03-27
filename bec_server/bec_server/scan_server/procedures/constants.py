from enum import Enum, auto
from typing import ParamSpec, Protocol, TypedDict, runtime_checkable


class ProcedureWorkerStatus(Enum):
    RUNNING = auto()
    IDLE = auto()
    FINISHED = auto()
    DEAD = auto()  # worker lost communication with the container


class ContainerWorkerEnv(TypedDict):
    redis_server: str
    queue: str
    timeout_s: str


P = ParamSpec("P")


@runtime_checkable
class BecProcedure(Protocol[P]):
    """A procedure should not return anything, because it could be run in an isolated environment
    and data needs to be extracted in other ways. It may be a simple function, but it can also be
    a class instance which implements __call__ and does some"""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> None: ...


class ProcedureWorkerError(RuntimeError): ...


class WorkerAlreadyExists(ProcedureWorkerError): ...


MAX_WORKERS = 10
QUEUE_TIMEOUT_S = 10
MANAGER_SHUTDOWN_TIMEOUT_S = 2
DEFAULT_QUEUE = "primary"
PODMAN_URI = "unix:///run/user/1000/podman/podman.sock"
IMAGE_NAME = "bec:latest"
