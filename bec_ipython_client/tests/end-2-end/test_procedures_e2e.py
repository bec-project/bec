from __future__ import annotations

import os
import time
from importlib.metadata import version
from typing import Callable, Generator
from unittest.mock import patch
from uuid import uuid4

import pytest
from pytest_redis import factories as redis_factories
from redis.exceptions import NoPermissionError

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.procedures.helper import FrontendProcedureHelper, ProcedureState
from bec_lib.redis_connector import RedisConnector
from bec_lib.utils.user_acls_test import BECAccessDemo
from bec_server.procedures.constants import PROCEDURE
from bec_server.procedures.container_utils import (
    _run_and_capture_error,
    get_backend,
    podman_available,
)
from bec_server.procedures.container_worker import ContainerProcedureWorker
from bec_server.procedures.manager import ProcedureManager
from bec_server.procedures.subprocess_worker import SubProcessWorker

logger = bec_logger.logger

# pylint: disable=protected-access

# Random order disabled for this module so that the test for building the worker container runs first
# and we can use lower timeouts for the remaining tests
pytestmark = pytest.mark.random_order(disabled=True)


# Isolate test managers from the scan server's manager, which subscribes to the same requests.
proc_redis = redis_factories.redis_proc()


@pytest.fixture
def procedure_manager(proc_redis) -> Generator[ProcedureManager, None, None]:
    """Provide a manager whose request channels are isolated from running BEC services."""
    manager = ProcedureManager(f"{proc_redis.host}:{proc_redis.port}", SubProcessWorker)
    try:
        yield manager
    finally:
        manager.shutdown()


def _wait_while(cond: Callable[[], bool], timeout_s):
    start = time.monotonic()
    while cond():
        if (time.monotonic() - start) > timeout_s:
            raise TimeoutError()
        time.sleep(0.01)


@pytest.mark.timeout(100)
def test_building_worker_image():
    podman_utils = get_backend()
    build = podman_utils.build_worker_image()
    assert len(build._command_output.splitlines()[-1]) == 64  # type: ignore
    assert podman_utils.image_exists(f"bec_procedure_worker:v{version('bec_lib')}")


@pytest.mark.timeout(30)
def test_procedure_runner_spawns_worker(procedure_manager):
    manager = procedure_manager
    assert manager.active_workers() == []
    status = manager._helper.request.procedure(
        "sleep", args_kwargs=((), {"time_s": 0.1}), queue="test"
    )
    _wait_while(lambda: not manager.active_workers(), 10)
    status.wait(timeout_s=10, raise_on_failure=True)
    assert status.state == ProcedureState.SUCCESS
    # A completed execution leaves the queue-owned worker available for reuse.
    assert len(manager.active_workers()) == 1


@pytest.mark.timeout(30)
def test_happy_path_container_procedure_runner(procedure_manager):
    test_args = (1, 2, 3)
    test_kwargs = {"a": "b", "c": "d"}
    manager = procedure_manager
    status = manager._helper.request.procedure(
        "_log_msg_args", args_kwargs=(test_args, test_kwargs)
    )
    status.wait(timeout_s=10, raise_on_failure=True)
    assert status.state == ProcedureState.SUCCESS
    manager.shutdown()
    assert any("Executing procedure _log_msg_args" in line for line in manager._logs)
    expected = (
        f"Builtin procedure log_message_args_kwargs called with args: {test_args} "
        f"and kwargs: {test_kwargs}"
    )
    assert any(expected in line for line in manager._logs)


@pytest.mark.timeout(30)
def test_personal_procedure_submission_acl(proc_redis):
    """Only the submitting ACL user can publish or subscribe to their request channel."""
    server = f"{proc_redis.host}:{proc_redis.port}"
    manager = ProcedureManager(server, SubProcessWorker)
    alice = RedisConnector(server)
    bob = RedisConnector(server)
    try:
        access = BECAccessDemo(manager._conn)
        access.add_account("alice", None, "user")
        access.add_account("bob", None, "user")
        alice.authenticate(username="alice", password=None)
        bob.authenticate(username="bob", password=None)
        with (
            patch.object(manager, "_process_queue_request") as process,
            patch.object(manager, "_ack", wraps=manager._ack) as acknowledge,
        ):
            helper = FrontendProcedureHelper(alice)
            helper.request.procedure("sleep")
            _wait_while(lambda: process.call_count < 1, 5)
            received = process.call_args.args[0]
            assert received.topic == MessageEndpoints.procedure_request("alice").endpoint
            assert received.value.metadata["client_info"]["acl_user"] == "alice"

            # A later login must change both the personal channel and the supplied identity.
            alice.authenticate(username="bob", password=None)
            helper.request.procedure("sleep")
            _wait_while(lambda: process.call_count < 2, 5)
            assert process.call_args.args[0].value.metadata["client_info"]["acl_user"] == "bob"

            forged = messages.ProcedureRequestMessage(
                identifier="sleep", metadata={"client_info": {"acl_user": "alice"}}
            )
            bob.send(MessageEndpoints.procedure_request("bob"), forged)
            _wait_while(lambda: not acknowledge.called, 5)
            acknowledge.assert_called_once_with(
                False, "Username in topic does not match client info.", forged.execution_id
            )
            assert process.call_count == 2
            with pytest.raises(NoPermissionError):
                bob.send(MessageEndpoints.procedure_request("alice"), forged)
            with bob._managed_connection._redis_conn.pubsub() as subscription:
                subscription.subscribe(MessageEndpoints.procedure_request("alice").endpoint)
                with pytest.raises(NoPermissionError):
                    subscription.get_message(timeout=1)
    finally:
        alice.shutdown()
        bob.shutdown()
        manager.shutdown()


@pytest.mark.timeout(120)
@pytest.mark.parametrize(
    "worker_type", [ContainerProcedureWorker, SubProcessWorker], ids=["podman", "subprocess"]
)
def test_procedure_execution(proc_redis, monkeypatch, worker_type):
    """Execute a real procedure with either worker backend against isolated Redis.

    Podman requires the worker image and a host-networked ``local_bec`` pod. For a remote
    Podman VM, BEC_TEST_CONTAINER_REDIS_HOST can supply a container-reachable host.
    """
    uses_container = worker_type is ContainerProcedureWorker
    if uses_container and not podman_available():
        pytest.skip("Podman is not available")
    host, port = proc_redis.host, proc_redis.port
    queue = f"procedure_test_{uuid4().hex}"
    if uses_container and (container_host := os.environ.get("BEC_TEST_CONTAINER_REDIS_HOST")):
        worker_environment = ContainerProcedureWorker._worker_environment
        monkeypatch.setattr(
            ContainerProcedureWorker,
            "_worker_environment",
            lambda worker: worker_environment(worker)
            | {"redis_server": f"{container_host}:{port}"},
        )
    manager = ProcedureManager(f"{host}:{port}", worker_type)
    try:
        status = manager._helper.request.procedure(
            "_log_msg_args", args_kwargs=(("procedure regression",), {}), queue=queue
        )
        status.wait(timeout_s=90)
        if not status.done or status.state != ProcedureState.SUCCESS:
            logs = manager._conn.xrange(MessageEndpoints.procedure_logs(queue), "-", "+") or []
            output = "".join(entry["data"].data for entry in logs)
            pytest.fail(
                f"Procedure state: {status.state.value}; error: {status.error}\n"
                f"Worker output:\n{output}"
            )
        _wait_while(lambda: bool(manager.active_workers()), PROCEDURE.WORKER.QUEUE_TIMEOUT_S + 10)
        logs = manager._conn.xrange(MessageEndpoints.procedure_logs(queue), "-", "+") or []
        assert any("procedure regression" in entry["data"].data for entry in logs)
        assert any("procedure regression" in line for line in manager._logs)
    finally:
        try:
            manager.shutdown()
        finally:
            if uses_container:
                container_name = f"bec_procedure_{PROCEDURE.BEC_VERSION}_{queue}"
                _run_and_capture_error("podman", "rm", "--force", "--ignore", container_name)


@pytest.mark.timeout(120)
def test_queue_pool_credentials_and_reuse(proc_redis):
    """Use only the submitted identity in clients, with service control confined to the parent."""
    from bec_lib.script_executor import upload_script

    server = f"{proc_redis.host}:{proc_redis.port}"
    admin = RedisConnector(server)
    alice = RedisConnector(server)
    manager = None
    try:
        access = BECAccessDemo(admin)
        access.add_account("procedure_service", "service-secret", "admin")
        access.add_account("procedure_alice", None, "user")
        admin._managed_connection._redis_conn.acl_setuser(
            "procedure_alice", enabled=True, reset_passwords=True, passwords=["+first-token"]
        )
        admin.authenticate(username="procedure_service", password="service-secret")
        admin._managed_connection._redis_conn.acl_setuser(
            "procedure_alice", enabled=True, passwords=["+second-token"]
        )
        admin._managed_connection._redis_conn.acl_setuser("default", enabled=False)
        alice.authenticate(username="procedure_alice", password="first-token")
        manager = ProcedureManager(
            server,
            SubProcessWorker,
            redis_credentials={"username": "procedure_service", "token": "service-secret"},
        )
        helper = FrontendProcedureHelper(alice)

        def execute(queue):
            script_id = upload_script(
                alice,
                "import os, sys\n"
                "assert 'credentials_file' not in os.environ\n"
                "assert self.connector.username == 'procedure_alice'\n"
                "try:\n"
                "    self.connector._managed_connection._redis_conn.acl_list()\n"
                "except Exception:\n"
                "    pass\n"
                "else:\n"
                "    raise AssertionError('Procedure obtained service privileges')\n"
                "print('stdout marker', end='', flush=True)\n"
                "sys.stderr.write('stderr marker')\n"
                "sys.stderr.flush()\n",
            )
            status = helper.request.procedure(
                "run_script", args_kwargs=((script_id,), {}), queue=queue
            )
            status.wait(timeout_s=30, raise_on_failure=True)
            assert status.state == ProcedureState.SUCCESS
            _wait_while(lambda: manager._queues[queue].current is not None, 5)
            return status

        execute("credential_queue")
        (first_id,) = manager._queues["credential_queue"].workers
        first_pid = manager._queues["credential_queue"].workers[first_id].worker._process.pid
        execute("credential_queue")
        assert list(manager._queues["credential_queue"].workers) == [first_id]
        alice.authenticate(username="procedure_alice", password="second-token")
        execute("credential_queue")
        assert len(manager._queues["credential_queue"].workers) == 2
        (second_id,) = set(manager._queues["credential_queue"].workers) - {first_id}
        assert (
            manager._queues["credential_queue"].workers[second_id].worker._process.pid != first_pid
        )
        alice.authenticate(username="procedure_alice", password="first-token")
        execute("credential_queue")
        assert len(manager._queues["credential_queue"].workers) == 2
        assert (
            manager._queues["credential_queue"].workers[first_id].worker._process.pid == first_pid
        )
        execute("different_queue")
        assert not set(manager._queues["different_queue"].workers) & set(
            manager._queues["credential_queue"].workers
        )
        logs = (
            manager._conn.xrange(MessageEndpoints.procedure_logs("credential_queue"), "-", "+")
            or []
        )
        text = "".join(entry["data"].data for entry in logs)
        assert "stdout marker" in text and "stderr marker" in text
        assert all(
            secret not in text for secret in ("first-token", "second-token", "service-secret")
        )
    finally:
        if manager is not None:
            manager.shutdown()
        BECAccessDemo(admin).set_default_limited(False)
        alice.shutdown()
        admin.shutdown()


@pytest.mark.timeout(60)
def test_invalid_procedure_token_cannot_fall_back_to_default(proc_redis):
    """Invalid forwarded credentials must fail even when default has full privileges."""
    server = f"{proc_redis.host}:{proc_redis.port}"
    manager = ProcedureManager(server, SubProcessWorker)
    try:
        BECAccessDemo(manager._conn).add_account("invalid_token_alice", None, "user")
        manager._conn._managed_connection._redis_conn.acl_setuser(
            "invalid_token_alice", enabled=True, reset_passwords=True, passwords=["+correct"]
        )
        request = messages.ProcedureRequestMessage(
            identifier="_log_msg_args",
            args_kwargs=(("must not execute",), {}),
            queue="invalid_credentials",
            metadata={
                "client_info": {"acl_user": "invalid_token_alice", "acl_token": "incorrect-token"}
            },
        )
        manager._conn.send(MessageEndpoints.procedure_request("invalid_token_alice"), request)
        _wait_while(lambda: not manager._helper.get.unhandled_queue("invalid_credentials"), 30)
        assert (
            manager._helper.get.unhandled_queue("invalid_credentials")[0].execution_id
            == request.execution_id
        )
        output = "\n".join(manager._logs)
        assert "must not execute" not in output
        assert "incorrect-token" not in output
    finally:
        manager.shutdown()
