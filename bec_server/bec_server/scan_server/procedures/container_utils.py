"""Utilities to build and run BEC container images"""

import json
import subprocess
import traceback
from http import HTTPStatus
from itertools import chain
from pathlib import Path
from typing import Iterator, Literal, cast

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
    VolumeSpec,
)

logger = bec_logger.logger


def get_backend() -> ContainerCommandBackend:
    """Get the currently selected backend for executing podman commands"""
    return PodmanCliUtils()
    # return PodmanApiUtils()


class _PodmanUtilsBase(ContainerCommandBackend):

    def build_requirements_image(self):  # pragma: no cover
        """Build the procedure worker requirements image"""
        return self._build_image(
            buildargs={"BEC_VERSION": PROCEDURE.BEC_VERSION},
            path=str(PROCEDURE.CONTAINER.CONTAINERFILE_LOCATION),
            file=PROCEDURE.CONTAINER.REQUIREMENTS_CONTAINERFILE_NAME,
            volume=f"{PROCEDURE.CONTAINER.DEPLOYMENT_PATH}:/bec:ro:z",
            tag=f"{PROCEDURE.CONTAINER.REQUIREMENTS_IMAGE_NAME}:v{PROCEDURE.BEC_VERSION}",
        )

    def build_worker_image(self):  # pragma: no cover
        """Build the procedure worker image"""
        return self._build_image(
            buildargs={"BEC_VERSION": PROCEDURE.BEC_VERSION},
            path=str(PROCEDURE.CONTAINER.CONTAINERFILE_LOCATION),
            file=PROCEDURE.CONTAINER.WORKER_CONTAINERFILE_NAME,
            volume=f"{PROCEDURE.CONTAINER.DEPLOYMENT_PATH}:/bec:ro:z",
            tag=f"{PROCEDURE.CONTAINER.IMAGE_NAME}:v{PROCEDURE.BEC_VERSION}",
        )


class PodmanApiOutput(ContainerCommandOutput):
    def __init__(self, command_output: Iterator[bytes]):
        self._command_output = command_output

    def pretty_print(self) -> str:
        return "\n".join(str(json.loads(line).values()) for line in self._command_output)


class PodmanApiUtils(_PodmanUtilsBase):

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

    def run(
        self,
        image_tag: str,
        environment: ContainerWorkerEnv,
        volumes: list[VolumeSpec],
        command: str,
        pod_name: str | None = None,
    ) -> str:
        with PodmanClient(base_url=self.uri) as client:
            try:
                self._container = client.containers.run(
                    image_tag,
                    command,
                    detach=True,
                    environment=environment,
                    mounts=volumes,
                    pod=pod_name,
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

    def logs(self, id: str) -> list[str]:
        return NotImplemented


def _multi_args_from_dict(argname: str, args: dict[str, str]) -> list[str]:
    return list(chain(*((argname, f"{k}={v}") for k, v in args.items())))


class PodmanCliOutput(ContainerCommandOutput):
    def __init__(self, command_output: str):
        self._command_output = command_output

    def pretty_print(self) -> str:
        return self._command_output


class PodmanCliUtils(_PodmanUtilsBase):

    def _run_and_capture_error(self, *args: str):
        logger.debug(f"Running {args}")
        output = subprocess.run([*args], capture_output=True)
        if output.returncode != 0:
            raise ProcedureWorkerError(
                "Container shell command: \n"
                f"    {args}"
                "\n failed with output:"
                f"{output.stderr}"
            )
        return output

    def _podman_ls_json(self, subcom: Literal["image", "container"] = "container"):
        return json.loads(
            self._run_and_capture_error(
                "podman", subcom, "list", "--all", "--format", "json"
            ).stdout
        )

    def _build_image(
        self, buildargs: dict, path: str, file: str, volume: str, tag: str
    ) -> PodmanCliOutput:
        _buildargs = _multi_args_from_dict("--build-arg", buildargs)
        _containerfile = str(Path(path) / file)
        output = self._run_and_capture_error(
            "podman", "build", *_buildargs, "-f", _containerfile, "-t", tag, "-v", volume
        )
        return PodmanCliOutput(output.stdout.decode())

    def image_exists(self, image_tag) -> bool:
        def _matches_tag(names: list[str]):
            return any(name.split("/")[-1] == image_tag for name in names)

        return any(_matches_tag(image.get("Names", [])) for image in self._podman_ls_json("image"))

    def run(
        self,
        image_tag: str,
        environment: ContainerWorkerEnv,
        volumes: list[VolumeSpec],
        command: str,
        pod_name: str | None = None,
    ) -> str:
        _volumes = [
            f"{vol['source']}:{vol['target']}{':ro' if vol['read_only'] else ''}" for vol in volumes
        ]
        _volume_args = list(chain(*(("-v", vol) for vol in _volumes)))
        _environment = _multi_args_from_dict("-e", environment)  # type: ignore # this is actually a dict[str, str]
        _pod_arg = ["--pod", pod_name] if pod_name else []
        return (
            self._run_and_capture_error(
                "podman", "run", *_environment, "-d", *_volume_args, *_pod_arg, image_tag, command
            )
            .stdout.decode()
            .strip()
        )

    def kill(self, id: str):
        self._run_and_capture_error("podman", "kill", id)
        self._run_and_capture_error("podman", "rm", id)

    def logs(self, id: str) -> list[str]:
        return self._run_and_capture_error("podman", "logs", id).stderr.decode().splitlines()

    def state(self, id: str) -> PodmanContainerStates | None:
        for container in self._podman_ls_json():
            if container["Id"] == id or container["Id"].startswith(id):
                return PodmanContainerStates(container["State"])
