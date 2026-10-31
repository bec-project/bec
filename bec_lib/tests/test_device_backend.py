"""Observable semantics of BEC's native-independent completion contract."""

import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pytest

from bec_lib.device_backend import Completion, DeviceIdentity, Operation


def test_completion_can_be_slotted_and_request_metadata_is_not_required():
    operation = Operation.finished(3)
    assert isinstance(operation, Completion)
    assert operation.result() == 3
    assert operation.done
    assert not hasattr(operation, "__dict__")
    with pytest.raises(AttributeError):
        operation.instruction = object()


def test_callback_registered_before_and_after_completion_runs_once():
    operation = Operation()
    observed = []
    operation.add_callback(lambda completed: observed.append(completed.result()))
    operation.set_result(5)
    operation.add_callback(lambda completed: observed.append(completed.result() + 1))
    assert observed == [5, 6]


def test_observer_timeout_does_not_cancel_producer():
    operation = Operation()
    with pytest.raises(TimeoutError):
        operation.result(timeout=0)
    assert not operation.done
    operation.set_result(7)
    assert operation.result() == 7


def test_original_exception_is_preserved_without_stop_side_effects():
    error = ValueError("device failed")
    operation = Operation.failed(error)
    assert operation.exception() is error
    with pytest.raises(ValueError) as caught:
        operation.result()
    assert caught.value is error


def test_dispatched_service_callback_does_not_block_producer():
    entered, release, producer_returned = threading.Event(), threading.Event(), threading.Event()
    with ThreadPoolExecutor(max_workers=1) as callbacks:
        operation = Operation(dispatch=lambda callback: callbacks.submit(callback))

        def slow_callback(_completed):
            entered.set()
            assert release.wait(2)

        operation.add_callback(slow_callback)
        producer = threading.Thread(
            target=lambda: (operation.set_result(None), producer_returned.set())
        )
        producer.start()
        try:
            assert entered.wait(2)
            assert producer_returned.wait(2)
            assert operation.done
        finally:
            release.set()
            producer.join(2)


def test_identity_preserves_requested_child_path():
    identity = DeviceIdentity("motor", "axis.readback", "motor_axis_readback")
    assert identity.qualified_name == "motor.axis.readback"
    assert DeviceIdentity("motor").qualified_name == "motor"
