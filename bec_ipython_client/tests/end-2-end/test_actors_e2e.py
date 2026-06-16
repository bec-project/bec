from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from bec_lib.bl_states import DeviceWithinLimitsStateConfig
from bec_lib.builtin_actor_hli import BuiltinActorHli
from bec_lib.logger import bec_logger
from bec_lib.messages import ScanQueueStatus

logger = bec_logger.logger

if TYPE_CHECKING:  # pragma: no cover
    from ophyd_devices.sim.sim_test_devices import SimDeviceWithSignalDelay

    from bec_ipython_client.main import BECIPythonClient


@pytest.fixture
def bec_with_delay_device(bec_ipython_client_fixture):
    bec = bec_ipython_client_fixture
    bec.builtin_actors.scan_interlock.enabled = True
    bec.builtin_actors.scan_interlock.trigger_setting = "restart_scan"
    dev = bec.device_manager.devices
    dev.ramp_up.min_val.put(0)
    dev.ramp_up.max_val.put(400)
    dev.ramp_up.value.put(400)
    dev.ramp_up.delay.put(2)
    dev.ramp_up.rampup_time.put(8)
    yield bec, dev.ramp_up
    dev.ramp_up.stop()


@pytest.fixture
def ramp_up_bl_state(bec_with_delay_device):
    bec, _ = bec_with_delay_device
    ramp_up_state_config = DeviceWithinLimitsStateConfig(
        name="beam_intensity_sufficient", device="ramp_up", low_limit=200
    )
    bec.beamline_states.add(ramp_up_state_config)
    yield bec_with_delay_device
    bec.beamline_states.delete("beam_intensity_sufficient")
    bec.builtin_actors.scan_interlock.clear_all()


def _wait_for(pred, timeout=10, retries=100):
    for i in range(retries):
        if pred():
            return True
        time.sleep(timeout / retries)
    return pred()


# pylint: disable=protected-accesstest
@pytest.mark.timeout(100)
def test_scan_interlock(
    ramp_up_bl_state: tuple[BECIPythonClient, SimDeviceWithSignalDelay], bec_with_delay_device
):
    bec, ramp_up = ramp_up_bl_state
    actors: BuiltinActorHli = bec.builtin_actors
    assert bec.beamline_states.beam_intensity_sufficient.get()["status"] == "valid"
    assert actors.scan_interlock.enabled
    assert actors.scan_interlock.trigger_setting == "restart_scan"
    current_q_status_msg: ScanQueueStatus = bec.queue.queue_storage.current_scan_queue["primary"]
    assert current_q_status_msg.status == "RUNNING"
    actors.scan_interlock.add_state_to_interlock("beam_intensity_sufficient", "valid")

    assert _wait_for(lambda: "beam_intensity_sufficient" in actors.scan_interlock.states_watched)

    def _beam_down():
        return (ramp_up.value.get() < 200) and bec.beamline_states.beam_intensity_sufficient.get()[
            "status"
        ] == "invalid"

    def _beam_up():
        return (ramp_up.value.get() > 200) and bec.beamline_states.beam_intensity_sufficient.get()[
            "status"
        ] == "valid"

    ramp_up.start.set(1)
    scan = bec.scans.line_scan(
        "samx", -5, 5, steps=10, exp_time=0.5, relative=False, hide_report=True
    )

    assert _wait_for(_beam_down)
    assert _wait_for(
        lambda: bec.queue.queue_storage.current_scan_queue["primary"].status == "LOCKED"
    )
    assert scan.status == "STOPPED"
    assert _wait_for(_beam_up)
    assert _wait_for(
        lambda: bec.queue.queue_storage.current_scan_queue["primary"].status == "RUNNING"
    )

    def second_scan_has_run():
        if len(bec.history) < 2:
            return False
        return (
            bec.history[-2].metadata["bec"]["status"] == "aborted"
            and bec.history[-1].metadata["bec"]["status"] == "closed"
        )

    assert _wait_for(second_scan_has_run)
