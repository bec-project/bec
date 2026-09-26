"""Behavioral tests for request aggregation without a hardware library."""

import threading
from dataclasses import FrozenInstanceError
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.request_handler import RequestHandler


class PlainStatus:
    """Independent backend status with no instruction or transport references."""

    status_type = "PlainStatus"
    device_name = "motor"

    def __init__(self, *, done=False, success=None):
        self.done = done
        self.success = success
        self.error = None
        self.callbacks = []

    def exception(self):
        return self.error

    def add_callback(self, callback):
        self.callbacks.append(callback)
        if self.done:
            callback(self)

    def finish(self, success=True, error=None, *, notify=True):
        self.success = success
        self.error = error
        self.done = True
        if notify:
            self.notify()

    def notify(self):
        for callback in self.callbacks:
            callback(self)


@pytest.fixture
def handler():
    return RequestHandler(mock.Mock())


@pytest.fixture
def instruction():
    return messages.DeviceInstructionMessage(
        device="motor",
        action="trigger",
        parameter={},
        metadata={"device_instr_id": "generic", "RID": "rid", "response": True},
    )


def terminal_responses(handler):
    return [
        call.args[1]
        for call in handler.connector.send.call_args_list
        if call.args[1].status != "running"
    ]


def device_responses(handler, request_id="rid"):
    return [
        call.args[1]["data"]
        for call in handler.connector.xadd.call_args_list
        if call.args and call.args[0] == MessageEndpoints.device_req_status(request_id)
    ]


@pytest.mark.parametrize("with_exception", [False, True])
def test_non_ophyd_failure_is_never_reported_as_success(handler, instruction, with_exception):
    handler.add_request(instruction, 1)
    status = PlainStatus()
    assert not hasattr(status, "instruction")
    handler.add_status_object(instruction, status)
    status.finish(False, RuntimeError("hardware failed") if with_exception else None)
    terminal = terminal_responses(handler)
    per_device = device_responses(handler)
    assert len(terminal) == len(per_device) == 1
    assert terminal[0].status == "error"
    assert terminal[0].error_info.device == "motor"
    assert terminal[0].result_is_status is True
    assert per_device[0].success is False
    assert per_device[0].metadata["error_info"] == terminal[0].error_info
    assert "trigger" in terminal[0].error_info.compact_error_message
    assert not handler.has_request("generic")


def test_multiple_immediate_statuses_complete_once(handler, instruction):
    handler.add_request(instruction, 3)
    for index in range(3):
        handler.add_status_object(instruction, PlainStatus(done=True, success=True))
        assert len(terminal_responses(handler)) == (1 if index == 2 else 0)
    assert terminal_responses(handler)[0].status == "completed"
    assert len(device_responses(handler)) == 3


def test_concurrent_statuses_publish_before_aggregate_completion(handler, instruction):
    statuses = [PlainStatus() for _ in range(8)]
    handler.add_request(instruction, len(statuses))
    for status in statuses:
        handler.add_status_object(instruction, status)
    barrier = threading.Barrier(len(statuses))
    order = []
    handler.connector.xadd.side_effect = lambda *_args, **_kwargs: order.append("device")

    def aggregate(_endpoint, response):
        if response.status != "running":
            order.append("aggregate")

    handler.connector.send.side_effect = aggregate

    def finish(status):
        barrier.wait(timeout=5)
        status.finish()

    threads = [threading.Thread(target=finish, args=(status,)) for status in statuses]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert order == ["device"] * len(statuses) + ["aggregate"]
    assert len(terminal_responses(handler)) == 1


def test_done_flags_do_not_replace_completion_callbacks(handler, instruction):
    first, second = PlainStatus(), PlainStatus()
    handler.add_request(instruction, 2)
    handler.add_status_object(instruction, first)
    handler.add_status_object(instruction, second)
    first.finish(notify=False)
    second.finish()
    assert first.done and second.done
    assert len(device_responses(handler)) == 1
    assert terminal_responses(handler) == []
    first.notify()
    assert len(device_responses(handler)) == 2
    assert len(terminal_responses(handler)) == 1


def test_old_status_publishes_without_completing_replacement_request(handler, instruction):
    handler.add_request(instruction, 1)
    old = PlainStatus()
    handler.add_status_object(instruction, old)
    handler.remove_request("generic")
    replacement = instruction.model_copy(deep=True)
    replacement.metadata["RID"] = "replacement"
    handler.add_request(replacement, 1)
    new = PlainStatus()
    handler.add_status_object(replacement, new)
    old.finish(False, RuntimeError("old operation failed"))
    assert handler.has_request("generic")
    assert handler.get_request("generic").status_objects == (new,)
    assert terminal_responses(handler) == []
    assert len(device_responses(handler)) == 1
    new.finish()
    assert terminal_responses(handler)[0].status == "completed"
    assert terminal_responses(handler)[0].metadata["RID"] == "replacement"
    assert len(device_responses(handler, "replacement")) == 1


@pytest.mark.parametrize("aggregate_state", ["absent", "removed", "cleared"])
@pytest.mark.parametrize("already_done", [False, True])
def test_per_device_completion_survives_missing_aggregate(
    handler, instruction, aggregate_state, already_done
):
    status = PlainStatus(done=already_done, success=True if already_done else None)
    if aggregate_state != "absent":
        handler.add_request(instruction, 1)
        if aggregate_state == "removed":
            handler.remove_request("generic")
        else:
            handler.clear()
    handler.add_status_object(instruction, status)
    if not already_done:
        status.finish()
    assert len(device_responses(handler)) == 1
    assert terminal_responses(handler) == []


def test_registered_callback_publishes_after_aggregate_removal(handler, instruction):
    status = PlainStatus()
    handler.add_request(instruction, 1)
    handler.add_status_object(instruction, status)
    handler.remove_request("generic")
    status.finish()
    assert len(device_responses(handler)) == 1
    assert terminal_responses(handler) == []


@pytest.mark.parametrize("stop_key", ["RID", "scan_id", "queue_id"])
def test_stopped_aggregate_keeps_per_device_completion(handler, instruction, stop_key):
    instruction.metadata[stop_key] = "stopped"
    handler.add_stopped_request("stopped")
    handler.add_request(instruction, 1)
    handler.add_status_object(instruction, PlainStatus(done=True, success=True))
    handler.connector.send.assert_not_called()
    assert len(device_responses(handler, instruction.metadata["RID"])) == 1
    assert not handler.has_request("generic")


def test_shutdown_suppresses_late_publication(handler, instruction):
    status = PlainStatus()
    handler.add_request(instruction, 1)
    handler.add_status_object(instruction, status)
    handler.shutdown()
    handler.connector.reset_mock()
    status.finish()
    handler.add_request(instruction, 1)
    handler.add_status_object(instruction, PlainStatus(done=True, success=True))
    handler.connector.send.assert_not_called()
    handler.connector.xadd.assert_not_called()
    assert not handler.has_request("generic")


def test_shutdown_waits_for_active_publication(handler, instruction):
    status = PlainStatus()
    handler.add_request(instruction, 1)
    handler.add_status_object(instruction, status)
    publishing = threading.Event()
    release = threading.Event()
    shutdown_started = threading.Event()
    shutdown_done = threading.Event()

    def publish(*_args, **_kwargs):
        publishing.set()
        assert release.wait(5)

    def shutdown():
        shutdown_started.set()
        handler.shutdown()
        shutdown_done.set()

    handler.connector.xadd.side_effect = publish
    completion = threading.Thread(target=status.finish)
    closing = threading.Thread(target=shutdown)
    completion.start()
    try:
        assert publishing.wait(2)
        closing.start()
        assert shutdown_started.wait(2)
        assert not shutdown_done.wait(0.05)
    finally:
        release.set()
        completion.join(5)
        if closing.ident is not None:
            closing.join(5)
    assert not completion.is_alive()
    assert not closing.is_alive()
    assert shutdown_done.is_set()
    assert len(terminal_responses(handler)) == 1


def test_publication_error_fails_aggregate_without_mutating_status(handler, instruction):
    handler.add_request(instruction, 1)
    handler.connector.xadd.side_effect = ConnectionError("status transport unavailable")
    status = PlainStatus(done=True, success=True)
    handler.add_status_object(instruction, status)
    terminal = terminal_responses(handler)
    assert len(terminal) == 1
    assert terminal[0].status == "error"
    assert terminal[0].error_info.exception_type == "ConnectionError"
    assert "status transport unavailable" in terminal[0].error_info.error_message
    assert status.success is True
    assert status.exception() is None
    assert not handler.has_request("generic")


def test_finish_if_untracked_respects_status_ownership(handler, instruction):
    handler.add_request(instruction, 1)
    assert handler.finish_if_untracked("generic", True, result=42)
    assert terminal_responses(handler)[0].result == 42
    assert not handler.finish_if_untracked("generic", True)
    handler.add_request(instruction, 1)
    status = PlainStatus()
    handler.add_status_object(instruction, status)
    assert not handler.finish_if_untracked("generic", True)
    assert handler.has_request("generic")
    status.finish()
    assert len(terminal_responses(handler)) == 2


@pytest.mark.parametrize("actual_count", [0, 1])
def test_patched_count_completes_after_observed_callbacks(handler, instruction, actual_count):
    handler.add_request(instruction, 3)
    if actual_count:
        handler.add_status_object(instruction, PlainStatus(done=True, success=True))
    assert terminal_responses(handler) == []
    handler.patch_num_status_objects(instruction, actual_count)
    assert len(terminal_responses(handler)) == 1
    assert not handler.has_request("generic")


def test_request_snapshot_is_frozen_and_does_not_track_mutations(handler, instruction):
    handler.add_request(instruction, 1)
    snapshot = handler.get_request("generic")
    assert snapshot.status_objects == ()
    with pytest.raises(FrozenInstanceError):
        snapshot.num_status_objects = 3
    status = PlainStatus()
    handler.add_status_object(instruction, status)
    assert snapshot.status_objects == ()
    assert handler.get_request("generic").status_objects == (status,)


def test_registration_does_not_hold_handler_lock(handler, instruction):
    handler.add_request(instruction, 1)

    class ThreadedRegistrationStatus(PlainStatus):
        def add_callback(self, callback):
            worker = threading.Thread(target=callback, args=(self,))
            worker.start()
            worker.join(2)
            assert not worker.is_alive()

    handler.add_status_object(instruction, ThreadedRegistrationStatus(done=True, success=True))
    assert len(terminal_responses(handler)) == 1


def test_response_opt_out_still_completes_aggregate(handler, instruction):
    instruction.metadata["response"] = False
    handler.add_request(instruction, 1)
    handler.add_status_object(instruction, PlainStatus(done=True, success=True))
    handler.connector.xadd.assert_not_called()
    assert terminal_responses(handler)[0].status == "completed"


def test_synchronous_completion_releases_request(handler, instruction):
    handler.add_request(instruction, 0, done=True, success=True)
    assert not handler.has_request("generic")
    assert len(terminal_responses(handler)) == 1


@pytest.mark.parametrize("already_done", [False, True])
def test_running_publication_failure_still_observes_completion(handler, instruction, already_done):
    handler.add_request(instruction, 1)
    status = PlainStatus(done=already_done, success=True if already_done else None)
    handler.connector.send.side_effect = [ConnectionError("running reply failed"), None]

    handler.add_status_object(instruction, status)
    if not already_done:
        status.finish()

    handler.connector.xadd.assert_called_once()
    response = handler.connector.xadd.call_args.args[1]["data"]
    assert response.success is True
    assert len(terminal_responses(handler)) == 1
    assert not handler.has_request("generic")
