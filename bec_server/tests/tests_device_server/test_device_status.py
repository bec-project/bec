"""Regression coverage for translating native status completion into a neutral status."""

import threading
from unittest import mock

import pytest
from ophyd import Component, Device, Signal, StatusBase

from bec_lib.messages import DeviceInstructionMessage
from bec_server.device_server.ophyd.status import OphydStatus


@pytest.fixture
def instruction():
    return DeviceInstructionMessage(
        device="motor", action="set", parameter={"value": 1}, metadata={"device_instr_id": "id"}
    )


@pytest.fixture
def device():
    obj = Device(name="motor")
    yield obj
    obj.destroy()


@pytest.mark.parametrize("already_done", [False, True])
@pytest.mark.parametrize("success", [False, True])
def test_status_exposes_outcome_only_after_finalization(instruction, device, already_done, success):
    native = StatusBase()
    error = None if success else ValueError("move failed")
    observed = []
    notified = threading.Event()

    def finalize(status):
        observed.append((status.done, status.success, status.exception()))

    def finish():
        if success:
            native.set_finished()
        else:
            native.set_exception(error)

    if already_done:
        finish()
    status = OphydStatus(native, instruction, device, on_complete=finalize)
    status.add_callback(lambda _: notified.set())
    if not already_done:
        assert not status.done
        assert status.success is None
        assert status.exception() is None
        finish()
    assert notified.wait(2)
    assert observed == [(False, None, None)]
    assert status.native_status is native
    assert status.obj is device
    assert status.instruction is instruction
    assert status.device_name == "motor"
    assert status.status_type == "StatusBase"
    assert status.done
    assert status.success is success
    assert status.exception() is error
    late_callback = mock.Mock()
    status.add_callback(late_callback)
    late_callback.assert_called_once_with(status)


def test_finalization_failure_is_a_completed_failure(instruction, device):
    native = StatusBase()
    error = RuntimeError("cache update failed")
    finalize = mock.Mock(side_effect=error)
    status = OphydStatus(native, instruction, device, on_complete=finalize)
    notified = threading.Event()
    status.add_callback(lambda _: notified.set())

    native.set_finished()

    assert notified.wait(2)
    finalize.assert_called_once_with(status)
    assert native.success
    assert status.done
    assert status.success is False
    assert status.exception() is error


def test_one_bad_callback_does_not_block_other_callbacks(instruction, device):
    native = StatusBase()
    status = OphydStatus(native, instruction, device)
    notified = threading.Event()
    failed_callback = mock.Mock(side_effect=RuntimeError("callback failed"))
    status.add_callback(failed_callback)
    status.add_callback(lambda _: notified.set())

    native.set_finished()

    assert notified.wait(2)
    failed_callback.assert_called_once_with(status)
    assert status.success
    assert status.exception() is None


def test_callback_can_register_another_callback(instruction, device):
    native = StatusBase()
    status = OphydStatus(native, instruction, device)
    notified = threading.Event()
    status.add_callback(lambda completed: completed.add_callback(lambda _: notified.set()))

    native.set_finished()

    assert notified.wait(2)


def test_native_completion_waits_for_finalization_of_each_status(instruction, device):
    first_native = StatusBase()
    second_native = StatusBase()
    finalizing = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    observed_completion = []

    def finalize(_status):
        finalizing.set()
        assert release.wait(2)

    first = OphydStatus(first_native, instruction, device, on_complete=finalize)
    second = OphydStatus(second_native, instruction, device)

    def check_both(_status):
        observed_completion.append(first.done and second.done)
        if first.done and second.done:
            finished.set()

    first.add_callback(check_both)
    second.add_callback(check_both)
    worker = threading.Thread(target=first_native.set_finished)
    worker.start()
    try:
        assert finalizing.wait(2)
        assert first_native.done
        assert not first.done
        second_native.set_finished()
        assert second.done
        assert observed_completion == [False]
    finally:
        release.set()
        worker.join(2)
    assert finished.wait(2)
    assert not worker.is_alive()
    assert observed_completion == [False, True]


def test_nested_device_name_is_a_snapshot(instruction):
    class Parent(Device):
        value = Component(Signal, value=0)

    parent = Parent(name="motor")
    try:
        status = OphydStatus(StatusBase(), instruction, parent.value)
        assert status.device_name == "motor.value"
        parent.name = "renamed"
        assert status.device_name == "motor.value"
    finally:
        parent.destroy()
