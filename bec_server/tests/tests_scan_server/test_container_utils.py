from unittest.mock import ANY, MagicMock, patch

import pytest

from bec_server.scan_server.procedures.constants import PROCEDURE, PodmanContainerStates
from bec_server.scan_server.procedures.container_utils import (
    PodmanApiUtils,
    PodmanCliUtils,
    _multi_args_from_dict,
)

TEST_IMAGE_JSON = b"""[{
"Id": "0b14a859cdba15104c5f194ef813fcccbf2749d74bc7be4550c06a0fc0d482e6",
"ParentId": "",
"RepoTags": null,
"RepoDigests": [
    "docker.io/library/python@sha256:139020233cc412efe4c8135b0efe1c7569dc8b28ddd88bddb109b764f8977e30",
    "docker.io/library/python@sha256:153bae509ec02c9ac789e2e35f3cbe94be446b59c3afcfbbc88c96a344d2eb73"
],
"Size": 134970967,
"SharedSize": 0,
"VirtualSize": 134970967,
"Labels": null,
"Containers": 0,
"Digest": "sha256:139020233cc412efe4c8135b0efe1c7569dc8b28ddd88bddb109b764f8977e30",
"History": [
    "docker.io/library/python:3.11-slim"
],
"Names": [
    "docker.io/library/python:3.11-slim"
],
"Created": 1748991773,
"CreatedAt": "2025-06-03T23:02:53Z"
}]"""

TEST_CONTAINER_JSON = b"""[{
    "AutoRemove": false,
    "Command": [
      "redis-server"
    ],
    "CreatedAt": "11 days ago",
    "CIDFile": "",
    "Exited": false,
    "ExitedAt": -62135596800,
    "ExitCode": 0,
    "ExposedPorts": {
      "6379": [
  "tcp"
]
    },
    "Id": "13826d25a737b733a5d87975b50a9c1efb5d087365d956cb7db2ccbe6aca07c4",
    "Image": "docker.io/library/redis:latest",
    "ImageID": "43724892d6db0fd681c7309bd458ce636c637a027f2d203a4932668ba8ffd97c",
    "IsInfra": false,
    "Labels": null,
    "Mounts": [
      "/var/redis/data",
      "/data"
    ],
    "Names": [
      "bec_redis"
    ],
    "Namespaces": {

    },
    "Networks": [],
    "Pid": 451555,
    "Pod": "8085e859ad58ee868f534961dd7fc62c5c5e80f00d1014b10ff76a8d323062c1",
    "PodName": "",
    "Ports": [
      {
        "host_ip": "",
        "container_port": 6379,
        "host_port": 6379,
        "range": 1,
        "protocol": "tcp"
      }
    ],
    "Restarts": 0,
    "Size": null,
    "StartedAt": 1752829321,
    "State": "running",
    "Status": "Up 11 days",
    "Created": 1752829321
  }
]"""


@pytest.fixture
def api_utils():
    with patch("bec_server.scan_server.procedures.container_utils.PodmanClient") as client:
        yield PodmanApiUtils(), client


@pytest.fixture
def cli_utils():
    with patch("bec_server.scan_server.procedures.container_utils.subprocess") as subprocess:
        subprocess.run().returncode = 0
        subprocess.run().stdout = b"success"
        yield PodmanCliUtils(), subprocess.run


@pytest.fixture
def cli_utils_with_fake_image_json():
    with patch("bec_server.scan_server.procedures.container_utils.subprocess") as subprocess:
        subprocess.run().returncode = 0
        subprocess.run().stdout = TEST_IMAGE_JSON
        yield PodmanCliUtils()


@pytest.fixture
def cli_utils_with_fake_container_json():
    with patch("bec_server.scan_server.procedures.container_utils.subprocess") as subprocess:
        subprocess.run().returncode = 0
        subprocess.run().stdout = TEST_CONTAINER_JSON
        yield PodmanCliUtils()


def test_api_utils_build(api_utils: tuple[PodmanApiUtils, MagicMock]):
    utils, client = api_utils
    utils.build_worker_image()
    client.assert_called_with(base_url=PROCEDURE.CONTAINER.PODMAN_URI)
    client().__enter__().images.build.assert_called_once()


def test_api_utils_run(api_utils: tuple[PodmanApiUtils, MagicMock]):
    utils, client = api_utils
    utils.run(
        "test_tag",
        {"a": "b"},
        [{"source": "a", "target": "b", "read_only": True, "type": "bind"}],
        "run",
    )
    client().__enter__().containers.run.assert_called_once_with(
        "test_tag",
        "run",
        detach=True,
        environment={"a": "b"},
        mounts=[{"source": "a", "target": "b", "read_only": True, "type": "bind"}],
        pod=None,
    )


def test_api_utils_image_exists(api_utils: tuple[PodmanApiUtils, MagicMock]):
    utils, client = api_utils
    utils.image_exists("test")
    client().__enter__().images.exists.assert_called_once_with("test")


def test_build_args_from_dict():
    assert _multi_args_from_dict("--build-arg", {"a": "b", "c": "d"}) == [
        "--build-arg",
        "a=b",
        "--build-arg",
        "c=d",
    ]


def test_cli_build_req(cli_utils: tuple[PodmanCliUtils, MagicMock]):
    utils, run_mock = cli_utils
    run_mock.reset_mock()
    utils.build_requirements_image()
    run_mock.assert_called_with(
        ["podman", "build", "--build-arg", ANY, "-f", ANY, "-t", ANY, "-v", ANY],
        capture_output=True,
    )


def test_cli_build_worker(cli_utils: tuple[PodmanCliUtils, MagicMock]):
    utils, run_mock = cli_utils
    run_mock.reset_mock()
    utils.build_worker_image()
    run_mock.assert_called_with(
        ["podman", "build", "--build-arg", ANY, "-f", ANY, "-t", ANY, "-v", ANY],
        capture_output=True,
    )


def test_find_images(cli_utils_with_fake_image_json: PodmanCliUtils):
    assert cli_utils_with_fake_image_json.image_exists("python:3.11-slim")
    assert not cli_utils_with_fake_image_json.image_exists("python:3.12")


def test_cli_kill(cli_utils: tuple[PodmanCliUtils, MagicMock]):
    utils, run_mock = cli_utils
    run_mock.reset_mock()
    utils.kill("test")
    assert run_mock.call_count == 2


def test_cli_get_state(cli_utils_with_fake_container_json: PodmanCliUtils):
    assert cli_utils_with_fake_container_json.state(
        "13826d25a737b733a5d87975b50a9c1efb5d087365d956cb7db2ccbe6aca07c4"
    ) == PodmanContainerStates("running")
