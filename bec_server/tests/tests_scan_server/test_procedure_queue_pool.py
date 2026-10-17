"""Queue ownership, serial execution, credentials, and worker lifecycle regressions."""

from functools import partial
from threading import Event
from time import monotonic, sleep
from unittest.mock import patch

import fakeredis
import pytest

from bec_lib.endpoints import MessageEndpoints as ME
from bec_lib.messages import ProcedureRequestMessage, ProcedureWorkerStatus
from bec_lib.procedures.helper import FrontendProcedureHelper, ProcedureState, ProcedureStatus
from bec_lib.redis_connector import MessageObject, RedisConnector
from bec_server.procedures.manager import ProcedureManager


def wait_for(condition):
    deadline = monotonic() + 5
    while not condition():
        assert monotonic() < deadline, "Condition did not become true"
        sleep(0.01)


class ControlledWorker:
    """A worker whose individual executions are released explicitly by the test."""

    instances = []

    def __init__(
        self, server, queue, lifetime, env, *, worker_id, credentials, control_credentials
    ):
        self.queue = queue
        self.id = worker_id
        self.credentials = credentials
        self.status = ProcedureWorkerStatus.IDLE
        self.executions = []
        self.completed = Event()
        self.aborted = Event()
        self.error = None
        self.instances.append(self)

    def execute(self, execution, on_started):
        self.executions.append(execution)
        self.status = ProcedureWorkerStatus.RUNNING
        on_started()
        self.completed.clear()
        while not self.completed.wait(0.01):
            if self.aborted.is_set():
                raise RuntimeError("aborted")
        self.status = ProcedureWorkerStatus.IDLE
        if isinstance(self.error, Exception):
            raise self.error
        return self.error

    def abort(self):
        self.aborted.set()
        self.status = ProcedureWorkerStatus.DEAD

    def _ending_or_ended(self):
        return self.aborted.is_set()

    def logs(self):
        return ["worker output"]


@pytest.fixture
def manager():
    ControlledWorker.instances = []
    server = fakeredis.FakeServer()
    connector = partial(RedisConnector, redis_cls=partial(fakeredis.FakeRedis, server=server))
    with patch("bec_server.procedures.manager.RedisConnector", connector):
        instance = ProcedureManager("localhost:6379", ControlledWorker)
    try:
        yield instance
    finally:
        instance.shutdown()
    assert all(worker.aborted.is_set() for worker in ControlledWorker.instances)


def submit(manager, queue="primary", token="secret", username="alice", execution_id=None):
    kwargs = {"execution_id": execution_id} if execution_id else {}
    message = ProcedureRequestMessage(
        identifier="sleep",
        queue=queue,
        metadata={"client_info": {"acl_user": username, "acl_token": token}},
        **kwargs,
    )
    result = manager._procedure_request_callback(
        MessageObject(topic=ME.procedure_request(username).endpoint, value=message)
    )
    assert "acl_token" not in message.metadata["client_info"]
    return result


def workers(queue):
    return [entry.worker for entry in queue.workers.values()]


def test_fifo_serial_per_queue_and_parallel_between_queues(manager):
    first = submit(manager, "a")
    wait_for(
        lambda: len(ControlledWorker.instances) == 1 and ControlledWorker.instances[0].executions
    )
    worker = ControlledWorker.instances[0]
    second = submit(manager, "a")
    other = submit(manager, "b")
    wait_for(
        lambda: len(ControlledWorker.instances) == 2 and ControlledWorker.instances[1].executions
    )
    assert worker.executions == [first]
    assert manager._helper.get.exec_queue("a") == [second]
    assert ControlledWorker.instances[1].executions == [other]
    worker.completed.set()
    wait_for(lambda: worker.executions == [first, second])
    worker.completed.set()
    ControlledWorker.instances[1].completed.set()
    wait_for(lambda: not manager._messages_by_ids)


def test_token_rotation_selects_queue_local_worker_and_reuses_old_identity(manager):
    first = submit(manager, token="first")
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    a = ControlledWorker.instances[0]
    second = submit(manager, token="second")
    third = submit(manager, token="first")
    assert len(ControlledWorker.instances) == 1
    a.completed.set()
    wait_for(
        lambda: len(ControlledWorker.instances) == 2 and ControlledWorker.instances[1].executions
    )
    b = ControlledWorker.instances[1]
    assert a.executions == [first]
    assert b.executions == [second]
    b.completed.set()
    wait_for(lambda: a.executions == [first, third])
    assert len(ControlledWorker.instances) == 2
    assert {worker.queue for worker in ControlledWorker.instances} == {"primary"}
    assert a.credentials["token"] == "first"
    assert b.credentials["token"] == "second"
    a.completed.set()
    wait_for(lambda: not manager._messages_by_ids)
    assert FrontendProcedureHelper(manager._conn).get.queue_names() == ["primary"]


def test_same_credentials_do_not_share_workers_across_queues(manager):
    submit(manager, "a")
    submit(manager, "b")
    wait_for(lambda: len(ControlledWorker.instances) == 2)
    assert {worker.queue for worker in ControlledWorker.instances} == {"a", "b"}
    assert len(manager.active_workers()) == 2


def test_username_is_part_of_worker_lookup(manager):
    first = submit(manager, username="alice")
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    submit(manager, username="bob")
    ControlledWorker.instances[0].completed.set()
    wait_for(lambda: len(ControlledWorker.instances) == 2)
    assert [worker.credentials["username"] for worker in ControlledWorker.instances] == [
        "alice",
        "bob",
    ]


def test_duplicate_execution_id_cannot_replace_credentials(manager):
    first = submit(manager, token="original")
    with patch.object(manager, "_ack") as ack:
        duplicate = submit(manager, token="other", username="bob", execution_id=first.execution_id)
    assert duplicate is None
    assert ack.call_args.args[0] is False
    assert manager._credentials_by_id[first.execution_id] == {
        "username": "alice",
        "token": "original",
    }


@pytest.mark.parametrize("scope", ["execution", "queue", "all"])
def test_abort_running_and_pending(manager, scope):
    first = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    second = submit(manager)
    if scope == "execution":
        manager._abort_execution(first.execution_id)
    elif scope == "queue":
        manager._abort_queue("primary")
    else:
        manager._abort_all()
    wait_for(lambda: first.execution_id not in manager._messages_by_ids)
    assert first in manager._helper.get.unhandled_queue("primary")
    if scope != "execution":
        assert second in manager._helper.get.unhandled_queue("primary")
    else:
        wait_for(
            lambda: len(ControlledWorker.instances) == 2
            and ControlledWorker.instances[1].executions
        )
        assert ControlledWorker.instances[1].executions == [second]


def test_cancel_pending_does_not_stop_running_worker(manager):
    first = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    second = submit(manager)
    manager._abort_execution(second.execution_id)
    assert not ControlledWorker.instances[0].aborted.is_set()
    assert manager._helper.get.unhandled_queue("primary") == [second]
    assert manager._queues["primary"].current == first


def test_worker_failure_only_fails_current_execution(manager):
    first = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    second = submit(manager)
    ControlledWorker.instances[0].error = RuntimeError("lost process")
    ControlledWorker.instances[0].completed.set()
    wait_for(
        lambda: len(ControlledWorker.instances) == 2 and ControlledWorker.instances[1].executions
    )
    assert manager._helper.get.unhandled_queue("primary") == [first]
    assert ControlledWorker.instances[1].executions == [second]


def test_worker_startup_failure_preserves_later_pending_jobs(manager):
    original = manager._worker_cls
    with patch.object(manager, "_worker_cls", side_effect=RuntimeError("failed startup")):
        first = submit(manager)
        wait_for(lambda: first.execution_id not in manager._messages_by_ids)
    assert manager._helper.get.unhandled_queue("primary") == [first]
    submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    assert manager._worker_cls is original


def test_abort_during_worker_startup(manager):
    entering = Event()
    release = Event()
    original = manager._worker_cls

    def create(*args, **kwargs):
        entering.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    with patch.object(manager, "_worker_cls", side_effect=create):
        first = submit(manager)
        assert entering.wait(5)
        manager._abort_queue("primary")
        release.set()
        wait_for(lambda: first.execution_id not in manager._messages_by_ids)
    assert ControlledWorker.instances[0].executions == []
    assert ControlledWorker.instances[0].aborted.is_set()


def test_public_registries_and_executions_do_not_contain_tokens(manager):
    execution = submit(manager, token="hidden-token")
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    assert execution.metadata == {}
    for endpoint in (ME.procedure_workers(), ME.procedure_queues()):
        assert "hidden-token" not in manager._conn.get(endpoint).model_dump_json()


def test_delayed_abort_cannot_stop_next_execution(manager):
    from threading import Thread

    first = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    second = submit(manager)
    old = ControlledWorker.instances[0]
    entered = Event()
    release = Event()
    original_abort = old.abort

    def delayed_abort():
        entered.set()
        assert release.wait(5)
        original_abort()

    old.abort = delayed_abort
    cancelling = Thread(target=manager._abort_execution, args=(first.execution_id,))
    cancelling.start()
    try:
        assert entered.wait(5)
        old.completed.set()
        sleep(0.1)
        assert old.executions == [first]
    finally:
        release.set()
        cancelling.join(5)
    wait_for(
        lambda: len(ControlledWorker.instances) == 2 and ControlledWorker.instances[1].executions
    )
    assert ControlledWorker.instances[1].executions == [second]
    assert not ControlledWorker.instances[1].aborted.is_set()


def test_expired_idle_worker_releases_capacity_while_sibling_is_running(manager):
    from threading import BoundedSemaphore

    manager._worker_slots = BoundedSemaphore(2)
    submit(manager, "a", token="first")
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    first = ControlledWorker.instances[0]
    submit(manager, "a", token="second")
    first.completed.set()
    wait_for(
        lambda: len(ControlledWorker.instances) == 2 and ControlledWorker.instances[1].executions
    )
    submit(manager, "b", token="third")
    first.aborted.set()  # The idle process exits while its queue's other worker is still busy.
    wait_for(
        lambda: len(ControlledWorker.instances) == 3 and ControlledWorker.instances[2].executions
    )
    assert ControlledWorker.instances[2].queue == "b"
    assert not ControlledWorker.instances[1].aborted.is_set()


def test_worker_registry_reports_running(manager):
    execution = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    (worker_id,) = manager.active_workers()
    wait_for(
        lambda: manager._conn.get(ME.procedure_workers()).resource[worker_id]["status"] == "RUNNING"
    )
    assert (
        manager._conn.get(ME.procedure_workers()).resource[worker_id]["execution_id"]
        == execution.execution_id
    )


def test_completion_redis_failure_is_retried_without_executing_twice(manager):
    from redis.exceptions import ConnectionError as RedisConnectionError

    execution = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    worker = ControlledWorker.instances[0]
    finish = manager._finish
    attempts = []

    def temporarily_unavailable(*args):
        attempts.append(1)
        if len(attempts) == 1:
            raise RedisConnectionError("temporary")
        return finish(*args)

    with patch.object(manager, "_finish", side_effect=temporarily_unavailable):
        worker.completed.set()
        wait_for(lambda: execution.execution_id not in manager._messages_by_ids)
    assert len(attempts) == 2
    assert worker.executions == [execution]


def test_idle_retirement_failure_is_retried(manager):
    execution = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    worker = ControlledWorker.instances[0]
    worker.completed.set()
    wait_for(lambda: execution.execution_id not in manager._messages_by_ids)
    original = worker.abort
    attempts = []

    def transient_failure():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("temporary backend failure")
        original()

    worker.abort = transient_failure
    worker.aborted.set()  # Expire an idle worker.
    wait_for(lambda: not manager.active_workers())
    assert len(attempts) == 2


def test_bad_token_type_is_rejected_and_removed(manager):
    with patch.object(manager, "_ack") as ack:
        result = submit(manager, token=b"invalid-token")
    assert result is None
    assert ack.call_args.args[0] is False
    assert not ControlledWorker.instances


def test_client_worker_listing_can_filter_by_queue(manager):
    submit(manager, "a")
    submit(manager, "b")
    wait_for(lambda: len(manager.active_workers()) == 2)
    helper = FrontendProcedureHelper(manager._conn)
    wait_for(lambda: len(helper.get.workers()) == 2)
    assert {entry["queue"] for entry in helper.get.workers().values()} == {"a", "b"}
    assert len(helper.get.workers("a")) == 1
    assert helper.get.workers("missing") == {}


def test_failed_abort_recovers_claimed_execution_after_backend_recovers(manager):
    execution = submit(manager)
    wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
    worker = ControlledWorker.instances[0]
    original_abort = worker.abort
    failed = Event()

    def unavailable():
        failed.set()
        raise RuntimeError("temporary backend failure")

    first_runner = manager._queues["primary"].future
    worker.abort = unavailable
    try:
        manager._abort_execution(execution.execution_id)
        worker.completed.set()
        wait_for(first_runner.done)
    finally:
        worker.abort = original_abort
    # Recovery must include the claimed execution, even with no pending list.
    wait_for(lambda: execution.execution_id not in manager._messages_by_ids)
    assert manager._helper.get.unhandled_queue("primary") == [execution]
    assert worker.aborted.is_set()


@pytest.mark.parametrize("failure", ["enqueue", "uncertain_enqueue", "notification", "ack"])
def test_admission_recovers_without_losing_credentials_or_fifo(manager, failure):
    from redis.exceptions import ConnectionError as RedisConnectionError

    recovered = Event()
    target = (
        manager._conn
        if "enqueue" in failure
        else manager._helper if failure == "notification" else manager
    )
    method = (
        "execute_pipeline"
        if "enqueue" in failure
        else "notify_watchers" if failure == "notification" else "_ack"
    )
    original = getattr(target, method)

    def unavailable(*args, **kwargs):
        if not recovered.is_set():
            if failure == "uncertain_enqueue":
                original(*args, **kwargs)
            raise RedisConnectionError("temporary admission failure")
        return original(*args, **kwargs)

    with patch.object(target, method, side_effect=unavailable):
        first = submit(manager, token="first")
        second = submit(manager, token="second")
        assert manager._credentials_by_id[first.execution_id]["token"] == "first"
        assert manager._credentials_by_id[second.execution_id]["token"] == "second"
        assert not ControlledWorker.instances
        recovered.set()
        wait_for(lambda: ControlledWorker.instances and ControlledWorker.instances[0].executions)
        assert ControlledWorker.instances[0].executions == [first]
        wait_for(lambda: manager._helper.get.exec_queue("primary") == [second])
        ControlledWorker.instances[0].completed.set()
        wait_for(
            lambda: len(ControlledWorker.instances) == 2
            and ControlledWorker.instances[1].executions
        )
        assert ControlledWorker.instances[1].executions == [second]
        ControlledWorker.instances[1].completed.set()
        wait_for(lambda: not manager._messages_by_ids)
    assert manager._helper.get.running_procedures() == set()


def test_claim_notifies_pending_queue_watchers(manager):
    with manager.lock, patch.object(manager._helper, "notify_watchers") as notify:
        execution = submit(manager)
        notify.reset_mock()
        manager._claim(manager._queues[execution.queue])
        notify.assert_called_once_with(execution.queue, "execution")
        assert manager._helper.get.exec_queue(execution.queue) == []


def test_pending_admission_can_be_cancelled(manager):
    from redis.exceptions import ConnectionError as RedisConnectionError

    helper = FrontendProcedureHelper(manager._conn)
    status = ProcedureStatus(manager._conn, "cancel-before-ack", "sleep")
    helper.request._set_callbacks(status._id, status)
    with patch.object(manager, "_ack", side_effect=RedisConnectionError("temporary")):
        execution = submit(manager, execution_id=status._id)
        manager._abort_execution(execution.execution_id)
    assert not manager._queues["primary"].admissions
    assert execution.execution_id not in manager._credentials_by_id
    assert manager._helper.get.unhandled_queue("primary") == [execution]
    assert manager._helper.get.exec_queue("primary") == []
    wait_for(lambda: status.done)
    assert status.state == ProcedureState.FAILED
    assert status.error == "Aborted by user."
