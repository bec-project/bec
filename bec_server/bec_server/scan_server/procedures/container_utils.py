"""Utilities to build BEC container images"""

import json
import subprocess
import traceback
from http import HTTPStatus
from itertools import chain
from typing import Iterator, cast

from podman import PodmanClient
from podman.domain.containers import Container
from podman.errors import APIError

from bec_lib.logger import bec_logger
from bec_server.scan_server.procedures.constants import (
    PROCEDURE,
    ContainerWorkerEnv,
    NoPodman,
    PodmanContainerStates,
    ProcedureWorkerError,
)
from bec_server.scan_server.procedures.protocol import (
    ContainerCommandBackend,
    ContainerCommandOutput,
)

logger = bec_logger.logger


def get_backend() -> ContainerCommandBackend:
    return PodmanApiUtils()


class PodmanApiOutput(ContainerCommandOutput):
    def __init__(self, command_output: Iterator[bytes]):
        self._command_output = command_output

    def pretty_print(self) -> str:
        return "\n".join(str(json.loads(line).values()) for line in self._command_output)


class PodmanApiUtils(ContainerCommandBackend):

    # See https://docs.podman.io/en/latest/_static/api.html#tag/images/operation/ImageBuildLibpod
    # for libpod API specs

    def __init__(self, uri: str = PROCEDURE.CONTAINER.PODMAN_URI):
        self.uri = uri
        self._container: Container | None = None

    def _build_image(
        self, buildargs: dict, path: str, file: str, volume: str, tag: str
    ):  # pragma: no cover
        with PodmanClient(base_url=self.uri) as client:
            build_kwargs = {
                "buildargs": buildargs,
                "path": path,
                "dockerfile": file,
                "volume": [volume],
                "tag": tag,
            }
            logger.info(f"Building container: {build_kwargs}")
            return PodmanApiOutput(client.images.build(**build_kwargs)[1])

    def build_requirements_image(self):  # pragma: no cover
        """Build the procedure worker requirements image"""
        return self._build_image(
            buildargs={"BEC_VERSION": PROCEDURE.BEC_VERSION},
            path=str(PROCEDURE.CONTAINER.CONTAINERFILE_LOCATION),
            file=PROCEDURE.CONTAINER.REQUIREMENTS_CONTAINERFILE_NAME,
            volume=f"{PROCEDURE.CONTAINER.DEPLOYMENT_PATH}:/bec:ro",
            tag=f"{PROCEDURE.CONTAINER.REQUIREMENTS_IMAGE_NAME}:v{PROCEDURE.BEC_VERSION}",
        )

    def build_worker_image(self):  # pragma: no cover
        """Build the procedure worker image"""
        return self._build_image(
            buildargs={"BEC_VERSION": PROCEDURE.BEC_VERSION},
            path=str(PROCEDURE.CONTAINER.CONTAINERFILE_LOCATION),
            file=PROCEDURE.CONTAINER.WORKER_CONTAINERFILE_NAME,
            volume=f"{PROCEDURE.CONTAINER.DEPLOYMENT_PATH}:/bec:ro",
            tag=f"{PROCEDURE.CONTAINER.IMAGE_NAME}:v{PROCEDURE.BEC_VERSION}",
        )

    def run(self, image_tag: str, environment: ContainerWorkerEnv):
        with PodmanClient(base_url=self.uri) as client:
            try:
                self._container = client.containers.run(
                    image_tag,
                    PROCEDURE.CONTAINER.COMMAND,
                    detach=True,
                    environment=environment,
                    mounts=[
                        {
                            "source": str(PROCEDURE.CONTAINER.DEPLOYMENT_PATH),
                            "target": "/bec",
                            "type": "bind",
                            "read_only": True,
                        }
                    ],
                    pod="local_bec",
                )  # type: ignore # running with detach returns container object
            except APIError as e:
                if e.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
                    raise ProcedureWorkerError(
                        f"Got an internal server error from Podman service: {traceback.print_exception(e)}"
                    ) from e
                # TODO handle a few more categories
                raise NoPodman(
                    f"Could not connect to podman socket at {PROCEDURE.CONTAINER.PODMAN_URI} - is the systemd service running? Try `systemctl --user start podman.socket`."
                ) from e
        return cast(str, self._container.id)  # type: ignore # _container is set above or we raise before here

    def image_exists(self, image_tag) -> bool:
        with PodmanClient(base_url=self.uri) as client:
            return client.images.exists(image_tag)

    def kill(self, id: str):
        with PodmanClient(base_url=self.uri) as client:
            client.containers.get(id).kill()

    def state(self, id: str) -> PodmanContainerStates | None:
        with PodmanClient(base_url=self.uri) as client:
            status = client.containers.get(id).status
            if status == "unknown":
                return None
            return PodmanContainerStates(status)


def _build_args_from_dict(buildargs: dict[str, str]) -> list[str]:
    return list(chain(*(("--build-arg", f"{k}={v}") for k, v in buildargs.items())))


class PodmanCliOutput(ContainerCommandOutput):
    def __init__(self, command_output: str):
        self._command_output = command_output

    def pretty_print(self) -> str:
        return self._command_output


class PodmanCliUtils(ContainerCommandBackend):
    def _build_image(
        self, buildargs: dict, path: str, file: str, volume: str, tag: str
    ) -> PodmanCliOutput:
        _buildargs = ...
        output = subprocess.run(["podman", "build", "/dev/null"], capture_output=True)
        return PodmanCliOutput(output.stdout.decode())

    def build_requirements_image(self) -> ContainerCommandOutput: ...
    def build_worker_image(self) -> ContainerCommandOutput: ...
