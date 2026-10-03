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
    assert status.wait() is True
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
    assert container.wait() is True
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
        with mock.patch(
            "bec_server.scan_server.scans.scan_status.time.monotonic", side_effect=[0, 0, 0.1]
        ):
            assert status.wait(timeout=0.1) is False
        assert wait.call_args.kwargs["timeout"] == 0.1

    container = ScanStatus(instruction_handler, is_container=True)
    container.add_status(status)
    with pytest.raises(
        ValueError, match="not supported for status objects with sub status objects"
    ):
        container.wait(resolve_on_known_type=True)


def test_wait_can_resolve_when_result_type_is_known(instruction_handler):
    status = ScanStatus(instruction_handler)
    status._update_future(_response("running", result_is_status=True))

    assert status.wait(resolve_on_known_type=True) is False
    assert status.done is False


def test_shutdown_marks_status_checked(instruction_handler):
    shutdown_event = threading.Event()
    status = ScanStatus(instruction_handler, shutdown_event=shutdown_event, name="move")
    shutdown_event.set()

    assert status.done is True
    assert status._done_checked is True
    assert repr(status).startswith("ScanStatus(move, ")


@pytest.mark.parametrize("timeout", [0, -1, 0.01])
def test_timeout_leaves_instruction_pending_and_can_be_retried(instruction_handler, timeout):
    status = ScanStatus(instruction_handler)
    assert status.wait(timeout=timeout) is False
    assert not status._future.done()
    assert status._done_checked
    status.set_done(42)
    assert status.wait(timeout=0) is True
    assert status.result == 42


def test_completion_during_short_wait_wins_over_timeout(instruction_handler):
    status = ScanStatus(instruction_handler)
    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait",
        side_effect=lambda *args, **kwargs: status.set_done(42),
    ) as wait:
        assert status.wait(timeout=0.01) is True
    assert 0 < wait.call_args.kwargs["timeout"] <= 0.01


def test_container_timeout_waits_for_every_child(instruction_handler):
    container = ScanStatus(instruction_handler, is_container=True)
    child = ScanStatus(instruction_handler)
    container.add_status(child)
    assert container.wait(timeout=0) is False
    child.set_done(1)
    assert container.wait(timeout=0) is True


def test_pending_failure_is_propagated_during_wait(instruction_handler):
    status = ScanStatus(instruction_handler)
    error = messages.ErrorInfo(
        error_message="Move failed",
        compact_error_message="Move failed",
        exception_type="RuntimeError",
    )
    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait",
        side_effect=lambda *args, **kwargs: status.set_failed(error),
    ):
        with pytest.raises(DeviceInstructionError, match="Move failed"):
            status.wait(timeout=0.01)


def test_shutdown_returns_false_for_pending_instruction(instruction_handler):
    event = threading.Event()
    status = ScanStatus(instruction_handler, shutdown_event=event)
    event.set()
    assert status.wait(timeout=0) is False
    assert not status._future.done()
    assert status._done_checked


def test_wait_uses_elapsed_time_and_remaining_budget(instruction_handler):
    status = ScanStatus(instruction_handler)
    module = "bec_server.scan_server.scans.scan_status"
    with (
        mock.patch(f"{module}.time.monotonic", side_effect=[10, 10, 10.4, 10.7]),
        mock.patch(f"{module}.concurrent.futures.wait") as wait,
    ):
        assert status.wait(timeout=0.6) is False
    assert wait.call_count == 2
    assert wait.call_args_list[0].kwargs["timeout"] == 0.5
    assert wait.call_args_list[1].kwargs["timeout"] == pytest.approx(0.2)


def test_polling_loop_completes(instruction_handler):
    status = ScanStatus(instruction_handler)
    polls = 0
    while not status.wait(timeout=0):
        polls += 1
        status.set_done()
    assert polls == 1


@pytest.mark.parametrize("timeout", [None, float("inf")])
def test_unbounded_wait_completes(instruction_handler, timeout):
    status = ScanStatus(instruction_handler)
    with mock.patch(
        "bec_server.scan_server.scans.scan_status.concurrent.futures.wait",
        side_effect=lambda *args, **kwargs: status.set_done(),
    ):
        assert status.wait(timeout=timeout) is True


def test_completion_check_does_not_miss_concurrent_failure(instruction_handler):
    status = ScanStatus(instruction_handler)
    error = messages.ErrorInfo(
        error_message="Concurrent failure",
        compact_error_message="Concurrent failure",
        exception_type="RuntimeError",
    )
    original_done = status._future.done

    def complete_after_pending_check():
        pending = not original_done()
        if pending:
            status.set_failed(error)
        return not pending

    with mock.patch.object(status._future, "done", side_effect=complete_after_pending_check):
        with pytest.raises(DeviceInstructionError, match="Concurrent failure"):
            status.wait(timeout=0.01)
