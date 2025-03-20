from enum import Enum, auto
from typing import TypedDict


class ProcedureWorkerStatus(Enum):
    RUNNING = auto()
    IDLE = auto()
    FINISHED = auto()
    DEAD = auto()  # worker lost communication with the container


class ContainerWorkerEnv(TypedDict):
    redis_server: str
    queue: str
    timeout_s: str


class ProcedureWorkerError(RuntimeError): ...


class WorkerAlreadyExists(ProcedureWorkerError): ...


MAX_WORKERS = 10
QUEUE_TIMEOUT_S = 10
MANAGER_SHUTDOWN_TIMEOUT_S = 2
DEFAULT_QUEUE = "primary"
PODMAN_URI = "unix:///run/user/1000/podman/podman.sock"
