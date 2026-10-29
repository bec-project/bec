"""Regression tests for direct scan device-instruction statuses."""

from __future__ import annotations

import threading
from unittest import mock

import pytest

from bec_lib import messages
from bec_server.scan_server.errors import DeviceInstructionError
from bec_server.scan_server.scans.scan_status import ScanStatus


@pytest.fixture
def instruction_handler():
    """Provide callback registration without a running Redis service."""
    return mock.Mock()


def _response(
    status: str,
    *,
    result=None,
    error_info: messages.ErrorInfo | None = None,
    result_is_status: bool | None = None,
) -> messages.DeviceInstructionResponse:
    instruction = messages.DeviceInstructionMessage(device="samx", action="set", parameter={})
    return messages.DeviceInstructionResponse(
        device="samx",
        status=status,
        error_info=error_info,
        instruction=instruction,
        instruction_id="instruction-1",
        result=result,
        result_is_status=result_is_status,
    )


def test_status_registers_callback_and_completes_from_response(instruction_handler):
    registry = {"instruction-1": object()}
    status = ScanStatus(instruction_handler, device_instr_id="instruction-1", registry=registry)
    instruction_handler.register_callback.assert_called_once_with(
        "instruction-1", status._update_future
    )
    assert status.done is False
    assert status.result is None

    status._update_future(_response("running", result_is_status=True))
    assert status.done is False
    assert status._result_is_status is True

    status._update_future(_response("completed", result=12))
    assert status.wait() is status
    assert status.done is True
    assert status.result == 12
    assert status._done_checked is True
    assert "instruction-1" not in registry
    assert "action=set, devices=samx, done=True" in repr(status)


def test_status_propagates_device_instruction_failure(instruction_handler):
    status = ScanStatus(instruction_handler)
    error_info = messages.ErrorInfo(
        error_message="Motor failed",
        compact_error_message="Motor failed",
        exception_type="RuntimeError",
        device="samx",
    )

    status._update_future(_response("error", error_info=error_info))

    assert status.done is True
    with pytest.raises(DeviceInstructionError, match="Motor failed"):
        status.wait()


def test_container_waits_for_children_and_collects_results(instruction_handler):
    registry = {"first": object(), "second": object()}
    container = ScanStatus(instruction_handler, is_container=True, registry=registry)
    first = ScanStatus(instruction_handler, device_instr_id="first", registry=registry)
    second = ScanStatus(instruction_handler, device_instr_id="second", registry=registry)
    container.add_status(first)
    container.add_status(second)

    instruction_handler.register_callback.assert_any_call("first", first._update_future)
    assert instruction_handler.register_callback.call_count == 2
    assert container.done is False
    assert container.result is None

    first.set_done(1)
    second.set_done(2)
    assert container.wait() is container
    assert container.done is True
    assert container.result == [1, 2]
    assert container._done_checked and first._done_checked and second._done_checked
    assert "first" not in registry and "second" not in registry


def test_child_failure_propagates_through_container(instruction_handler):
    container = ScanStatus(instruction_handler, is_container=True)
    child = ScanStatus(instruction_handler)
    container.add_status(child)
    child.set_failed(
        messages.ErrorInfo(
            error_message="Child failed",
            compact_error_message="Child failed",
            exception_type="RuntimeError",
        )
    )

    with pytest.raises(DeviceInstructionError, match="Child failed"):
        container.wait()


def test_wait_times_out_and_rejects_early_resolution_for_container(instruction_handler):
    status = ScanStatus(instruction_handler)
    with mock.patch("bec_server.scan_server.scans.scan_status.concurrent.futures.wait") as wait:
        wait.return_value = (set(), set())
        with pytest.raises(TimeoutError, match="wait operation timed out"):
            status.wait(timeout=0.1)

    container = ScanStatus(instruction_handler, is_container=True)
    container.add_status(status)
    with pytest.raises(
        ValueError, match="not supported for status objects with sub status objects"
    ):
        container.wait(resolve_on_known_type=True)


def test_wait_can_resolve_when_result_type_is_known(instruction_handler):
    status = ScanStatus(instruction_handler)
    status._update_future(_response("running", result_is_status=True))

    assert status.wait(resolve_on_known_type=True) is status
    assert status.done is False


def test_shutdown_marks_status_checked(instruction_handler):
    shutdown_event = threading.Event()
    status = ScanStatus(instruction_handler, shutdown_event=shutdown_event, name="move")
    shutdown_event.set()

    assert status.done is True
    assert status._done_checked is True
    assert repr(status).startswith("ScanStatus(move, ")


def _instruction(device):
    return messages.DeviceInstructionMessage(device=device, action="set", parameter={"value": 1})


def test_wait_reports_start_without_an_end_notification(instruction_handler):
    updates = []
    status = ScanStatus(
        instruction_handler,
        wait_callback=lambda obj: updates.append((obj.wait_started_at, obj.pending_instructions)),
    )
    status.instruction = _instruction("samx")

    def complete(*args, **kwargs):
        assert updates[0][0] is not None
        status.set_done()
        return {status._future}, set()

    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait", side_effect=complete
    ):
        assert status.wait() is status
    assert len(updates) == 1
    assert updates[0][0] is not None
    assert updates[0][1] == [status.instruction]
    assert status.wait_started_at is None


def test_completed_and_known_type_statuses_do_not_report_a_wait(instruction_handler):
    callback = mock.Mock()
    status = ScanStatus(instruction_handler, wait_callback=callback)
    status.set_done()
    status.wait()
    pending = ScanStatus(instruction_handler, wait_callback=callback)
    pending._update_future(_response("running", result_is_status=True))
    pending.wait(resolve_on_known_type=True)
    callback.assert_not_called()


@pytest.mark.parametrize("exit_reason", ["timeout", "error", "shutdown"])
def test_wait_state_is_reset_without_broadcasting_an_end(instruction_handler, exit_reason):
    updates = []
    status = ScanStatus(
        instruction_handler, wait_callback=lambda obj: updates.append(obj.wait_started_at)
    )

    def unblock(*args, **kwargs):
        if exit_reason == "error":
            status.set_failed(
                messages.ErrorInfo(
                    error_message="failed",
                    compact_error_message="failed",
                    exception_type="RuntimeError",
                )
            )
            return {status._future}, set()
        if exit_reason == "shutdown":
            status._shutdown_event.set()
        return set(), {status._future}

    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait", side_effect=unblock
    ):
        if exit_reason == "timeout":
            with pytest.raises(TimeoutError):
                status.wait(timeout=0.1)
        elif exit_reason == "error":
            with pytest.raises(DeviceInstructionError):
                status.wait()
        else:
            status.wait()
    assert len(updates) == 1
    assert updates[0] is not None
    assert status.wait_started_at is None


def test_container_wait_updates_only_remaining_instructions(instruction_handler):
    updates = []
    container = ScanStatus(
        instruction_handler,
        is_container=True,
        wait_callback=lambda obj: updates.append((obj.wait_started_at, obj.pending_instructions)),
    )
    first, second = ScanStatus(instruction_handler), ScanStatus(instruction_handler)
    first.instruction, second.instruction = _instruction("samx"), _instruction("samy")
    container.add_status(first)
    container.add_status(second)

    def progress(*args, **kwargs):
        if not first._future.done():
            first.set_done()
            return {first._future, container._future}, {second._future}
        second.set_done()
        return {first._future, second._future, container._future}, set()

    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait", side_effect=progress
    ):
        container.wait()
    assert [update[1] for update in updates] == [
        [first.instruction, second.instruction],
        [second.instruction],
    ]
    assert updates[0][0] == updates[1][0]
    assert container.wait_started_at is None


def test_pending_instructions_include_nested_statuses_and_response_fallback(instruction_handler):
    parent = ScanStatus(instruction_handler, is_container=True)
    child = ScanStatus(instruction_handler, is_container=True)
    pending = ScanStatus(instruction_handler)
    pending._update_future(_response("running"))
    child.add_status(pending)
    parent.add_status(child)
    assert parent.pending_instructions == [pending.message.instruction]
    pending.set_done()
    assert parent.pending_instructions == []


def test_minimum_and_nested_waits_share_one_start_time(instruction_handler):
    updates = []
    status = ScanStatus(
        instruction_handler, wait_callback=lambda obj: updates.append(obj.wait_started_at)
    )
    status.set_done()
    with status._wait_context():
        with mock.patch("bec_server.scan_server.scans.scan_status.time.sleep") as sleep:
            status.wait(min_wait=0.01)
            sleep.assert_called_once_with(0.01)
    assert len(updates) == 1
    assert updates[0] is not None
    assert status.wait_started_at is None


def test_failed_wait_notification_does_not_change_result(instruction_handler):
    callback = mock.Mock(side_effect=RuntimeError("notification failed"))
    status = ScanStatus(instruction_handler, wait_callback=callback)
    status.set_done(12)
    with mock.patch("bec_server.scan_server.scans.scan_status.time.sleep"):
        assert status.wait(min_wait=0.01) is status
    assert status.result == 12
    assert callback.call_count == 1
    assert status.wait_started_at is None


def test_wait_does_not_repeat_unchanged_pending_instructions(instruction_handler):
    callback = mock.Mock()
    status = ScanStatus(instruction_handler, wait_callback=callback)
    status.instruction = _instruction("samx")
    polls = 0

    def progress(*args, **kwargs):
        nonlocal polls
        polls += 1
        assert callback.call_count == 1
        if polls == 3:
            status.set_done()
            return {status._future}, set()
        return set(), {status._future}

    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait", side_effect=progress
    ):
        status.wait()
    callback.assert_called_once_with(status)
    assert status.wait_started_at is None


def test_wait_notification_state_resets_for_another_wait(instruction_handler):
    starts = []
    status = ScanStatus(
        instruction_handler, wait_callback=lambda obj: starts.append(obj.wait_started_at)
    )
    status.instruction = _instruction("samx")
    with (
        mock.patch(
            "bec_server.scan_server.scans.scan_status.time.time", side_effect=[1000.0, 1001.0]
        ),
        mock.patch(
            "bec_server.scan_server.scans.scan_status.concurrent.futures.wait",
            return_value=(set(), set()),
        ),
    ):
        for _ in range(2):
            with pytest.raises(TimeoutError):
                status.wait(timeout=0.1)
            assert status.wait_started_at is None
    assert starts == [1000.0, 1001.0]


def test_wait_deduplicates_response_copies_with_array_parameters(instruction_handler):
    import numpy as np

    callback = mock.Mock()
    status = ScanStatus(
        instruction_handler, device_instr_id="instruction-1", wait_callback=callback
    )

    def response():
        message = _response("running")
        message.instruction.parameter = {"value": np.array([1.0, 2.0])}
        message.instruction.metadata = {"device_instr_id": "instruction-1"}
        return message

    status._update_future(response())
    polls = 0

    def progress(*args, **kwargs):
        nonlocal polls
        polls += 1
        if polls == 1:
            status._update_future(response())
            return set(), {status._future}
        status.set_done()
        return {status._future}, set()

    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait", side_effect=progress
    ):
        status.wait()
    callback.assert_called_once_with(status)
