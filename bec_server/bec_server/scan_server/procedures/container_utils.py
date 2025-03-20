"""Utilities to build BEC container images"""

from podman import PodmanClient

from bec_lib.logger import bec_logger
from bec_server.scan_server.procedures.constants import PROCEDURE

logger = bec_logger.logger


# See https://docs.podman.io/en/latest/_static/api.html#tag/images/operation/ImageBuildLibpod
# for libpod API specs


def _build_image(
    client: PodmanClient, buildargs: dict, path: str, file: str, volume: str, tag: str
):
    build_kwargs = {
        "buildargs": buildargs,
        "path": path,
        "dockerfile": file,
        "volume": volume,
        "tag": tag,
    }
    logger.info(f"Building container: {build_kwargs}")
    client.images.build(**build_kwargs)


def build_base_image(client: PodmanClient):
    """Build the procedure worker requirements image

    Args:
        client (PodmanClient): an active podman socket client to execute the build"""
    _build_image(
        client,
        buildargs={"BEC_VERSION": PROCEDURE.BEC_VERSION},
        path=str(PROCEDURE.CONTAINER.CONTAINERFILE_LOCATION),
        file=PROCEDURE.CONTAINER.REQUIREMENTS_CONTAINERFILE_NAME,
        volume=f"{PROCEDURE.CONTAINER.DEPLOYMENT_PATH}:/bec:ro",
        tag=f"{PROCEDURE.CONTAINER.REQUIREMENTS_IMAGE_NAME}:{PROCEDURE.BEC_VERSION}",
    )


def build_worker_image(client: PodmanClient):
    """Build the procedure worker image

    Args:
        client (PodmanClient): an active podman socket client to execute the build"""
    _build_image(
        client,
        buildargs={"BEC_VERSION": PROCEDURE.BEC_VERSION},
        path=str(PROCEDURE.CONTAINER.CONTAINERFILE_LOCATION),
        file=PROCEDURE.CONTAINER.WORKER_CONTAINERFILE_NAME,
        volume=f"{PROCEDURE.CONTAINER.DEPLOYMENT_PATH}:/bec:ro",
        tag=f"{PROCEDURE.CONTAINER.IMAGE_NAME}:{PROCEDURE.BEC_VERSION}",
    )


def build_base_image_standalone():
    """Build an image with the requirements for BEC installed, to reduce time
    spent building the final image"""
    with PodmanClient(base_url=PROCEDURE.CONTAINER.PODMAN_URI) as client:
        build_base_image(client)
