from typing import Literal, Protocol, TypedDict

from bec_server.scan_server.procedures.constants import ContainerWorkerEnv, PodmanContainerStates


class VolumeSpec(TypedDict):
    source: str
    target: str
    type: Literal["bind"]
    read_only: bool


class ContainerCommandOutput(Protocol):

    def pretty_print(self) -> str: ...


class ContainerCommandBackend(Protocol):

    def _build_image(
        self, buildargs: dict, path: str, file: str, volume: str, tag: str
    ) -> ContainerCommandOutput: ...
    def build_requirements_image(self) -> ContainerCommandOutput: ...
    def build_worker_image(self) -> ContainerCommandOutput: ...
    def image_exists(self, image_tag) -> bool: ...
    def run(
        self,
        image_tag: str,
        environment: ContainerWorkerEnv,
        volumes: list[VolumeSpec],
        command: str,
        pod_name: str | None = None,
    ) -> str: ...
    def kill(self, id: str): ...
    def logs(self, id: str) -> list[str]: ...
    def state(self, id: str) -> PodmanContainerStates | None: ...
