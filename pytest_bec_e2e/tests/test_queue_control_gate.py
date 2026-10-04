"""Lifecycle regressions for the integration-test trigger latch."""

import pytest

from pytest_bec_e2e.queue_control_gate import QueueControlGate


def held_gate():
    gate = QueueControlGate("", name="gate")
    gate.arm()
    gate.trigger().wait(timeout=1)
    gate.trigger().wait(timeout=1)
    status = gate.trigger()
    assert not status.done
    assert gate.waiting.get()
    return gate, status


def test_release_finishes_held_trigger_and_does_not_hold_successors():
    gate, status = held_gate()
    gate.release()
    status.wait(timeout=1)
    assert not gate.waiting.get()
    gate.trigger().wait(timeout=1)


def test_stop_cancels_held_trigger_and_does_not_hold_successors():
    gate, status = held_gate()
    gate.stop()
    with pytest.raises(RuntimeError, match="gate stopped"):
        status.wait(timeout=1)
    assert not gate.waiting.get()
    gate.trigger().wait(timeout=1)


def test_rearm_held_trigger_is_rejected_and_release_is_idempotent():
    gate, status = held_gate()
    with pytest.raises(RuntimeError, match="already held"):
        gate.arm()
    gate.release()
    gate.release()
    status.wait(timeout=1)
