"""Review probes for bec-project/bec#1041 (device-server side).

Run:

    pytest -q bec_server/tests/tests_device_server/test_review_pr1041_device_server.py

Each test asserts the DESIRED behaviour and therefore FAILS on the current PR
head; the assertion message describes the defect. Once the corresponding change
lands, the test passes and can be kept as a regression test or deleted.

Only the existing `dm_with_devices` fixture is used (real DeviceManagerDS with
simulated devices, mocked redis), so nothing has to be running.
"""

import os
import threading
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.devicemanager import DeviceContainer
from bec_server.device_server.devices.config_update_handler import ConfigUpdateHandler
from bec_server.device_server.devices.devicemanager import DeviceManagerDS


# ---------------------------------------------------------------------------
# ConfigUpdateHandler._update_config (enable path, except block) swaps the OLD
# DSDevice back and destroys the NEW one, while _add_config and
# handle_failed_device_inits keep the NEW DSDevice (disabled, destroyed).
# Observable difference: after a failed enable the container holds an ophyd
# object that was never destroyed. The swap also needs the write
# `failed_device._config["enabled"] = was_enabled` on an object that is
# discarded on the next line, and a `.get()` guard that can never be None.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("device_manager_class", [DeviceManagerDS])
def test_failed_enable_leaves_a_destroyed_object_like_failed_add(dm_with_devices):
    handler = ConfigUpdateHandler(dm_with_devices)
    name = "motor1_disabled"
    old_device = dm_with_devices.devices[name]

    with (
        mock.patch.object(
            dm_with_devices, "connect_device", return_value=ConnectionError("PV is unreachable")
        ),
        mock.patch.object(handler, "send_config_request_reply"),
    ):
        handler.parse_config_request(
            messages.DeviceConfigMessage(action="update", config={name: {"enabled": True}}),
            cancel_event=threading.Event(),
        )

    kept = dm_with_devices.devices[name]
    assert kept.enabled is False  # the fix itself works
    assert kept.obj._destroyed, (
        f"after the failed enable the container holds the {'OLD' if kept is old_device else 'NEW'} "
        "DSDevice whose ophyd object was never destroyed. _add_config and "
        "handle_failed_device_inits leave a disabled DSDevice with a DESTROYED object; the "
        "enable path uses a different strategy (swap-back)."
    )


# ---------------------------------------------------------------------------
# Test quality, bec_ipython_client/tests/end-2-end/test_scans_e2e.py
# (test_unreachable_device_stays_disabled_when_enabled_twice):
#     with pytest.raises(DeviceConfigError):
#         send_config_request(action="add", ...)
#     device = dev[device_name]        # <- outside the raises block
# Every rejection path also raises DeviceConfigError, but then the device is not
# in the client container and the next line dies with a bare KeyError.
# ---------------------------------------------------------------------------
def test_e2e_lookup_after_rejected_add_gives_a_clear_error():
    if os.environ.get("OPHYD_CONTROL_LAYER") == "dummy":
        # The device-server test conftest selects the dummy control layer. A device
        # server started in such an environment rejects the e2e add outright, because
        # the EpicsMotor constructor itself raises:
        import ophyd

        with pytest.raises(NotImplementedError):
            ophyd.EpicsMotor("BEC:E2E:INTENTIONALLY_UNREACHABLE:", name="unreachable_epics_motor")

    # Rejected add -> client container does not contain the device.
    dev = DeviceContainer()
    with pytest.raises(DeviceConfigError):
        # DESIRED: a DeviceConfigError that explains what happened
        #          (add `match=` to both pytest.raises blocks and assert
        #          `device_name in dev` before indexing).
        # ACTUAL:  a bare KeyError from dict.__getitem__, reported as the test
        #          failure instead of the real cause (the add was rejected).
        dev["unreachable_epics_motor"]
