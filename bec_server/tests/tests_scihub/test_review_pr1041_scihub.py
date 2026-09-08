"""Review probes for bec-project/bec#1041 (scihub side).

Run:

    pytest -q bec_server/tests/tests_scihub/test_review_pr1041_scihub.py

Each test asserts the DESIRED behaviour and therefore FAILS on the current PR
head; the assertion message describes the defect. Once the corresponding change
lands, the test passes and can be kept as a regression test or deleted.

Only the existing `config_handler` fixture is used (mocked redis, mocked device
server), so nothing has to be running.
"""

import copy
import threading
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.device import DeviceBaseWithConfig

BASIC = {
    "enabled": True,
    "deviceClass": "TestDevice",
    "readoutPriority": "monitored",
    "deviceConfig": {},
}


def _register(config_handler, name: str, **overrides) -> DeviceBaseWithConfig:
    """Put a device into the scihub's in-memory device manager."""
    devices = config_handler.device_manager.devices
    devices[name] = DeviceBaseWithConfig(name=name, config=BASIC | {"name": name} | overrides)
    return devices[name]


# ---------------------------------------------------------------------------
# ConfigHandler._update_device_config (enabled branch) waits the default 30 s
# for the device server, while the device server honours the per-device
# connectionTimeout, which has no upper bound.
# ---------------------------------------------------------------------------
def test_enable_wait_covers_connection_timeout(config_handler):
    samx = _register(config_handler, "samx", enabled=False, connectionTimeout=60.0)

    with (
        mock.patch.object(config_handler, "_update_device_server"),
        mock.patch.object(
            config_handler, "_wait_for_device_server_update", return_value=(True, mock.MagicMock())
        ) as wait,
    ):
        config_handler._update_device_config(samx, {"enabled": True})

    call = wait.call_args
    timeout_used = call.kwargs.get("timeout_time", call.args[1] if len(call.args) > 1 else 30)
    assert timeout_used >= 60, (
        f"scihub waits {timeout_used}s for the device server reply, but the device "
        "server may legitimately need connectionTimeout=60s (wait_for_connection with "
        "all_signals=True). After 30s scihub raises TimeoutError, keeps enabled=False "
        "and never broadcasts, while the device server can still finish and enable "
        "the device -> device server enabled, scihub/redis/clients disabled."
    )


def test_enable_timeout_cancels_device_server_request(config_handler):
    samx = _register(config_handler, "samx", enabled=False, connectionTimeout=60.0)

    with (
        mock.patch.object(config_handler, "_update_device_server") as send,
        mock.patch.object(
            config_handler,
            "_wait_for_device_server_update",
            side_effect=TimeoutError(
                "Reached timeout whilst waiting for a device server config reply."
            ),
        ),
    ):
        with pytest.raises(TimeoutError):
            config_handler._update_device_config(samx, {"enabled": True})

    actions = [
        c.kwargs.get("action", c.args[2] if len(c.args) > 2 else "update")
        for c in send.call_args_list
    ]
    assert samx.enabled is False  # scihub gave up -> stays disabled (this part is correct)
    assert "cancel" in actions, (
        f"device server requests sent: {actions}. scihub timed out but never cancelled "
        "the in-flight enable, so the device server can still complete it and reply "
        "accepted=True to a key nobody reads (expire=60)."
    )


# ---------------------------------------------------------------------------
# ConfigHandler._add_to_config:
#     for dev in failed_devices: dev_configs[dev]["enabled"] = False
# is unguarded. The device server attaches its shared dm.failed_devices dict by
# reference; the startup loader writes the same dict, so a startup device can
# show up in the reply of a concurrent add.
# ---------------------------------------------------------------------------
def test_foreign_failed_device_does_not_abort_add(config_handler):
    config = {
        "new_dev": {
            "deviceConfig": {},
            "name": "new_dev",
            "enabled": True,
            "readoutPriority": "baseline",
            "deviceClass": "SimPositioner",
        }
    }
    msg = messages.DeviceConfigMessage(action="add", config=config, metadata={"RID": "1"})
    # The device server accepted the add, but its failed_devices dict also carries a
    # startup device that is NOT part of this request.
    response = mock.MagicMock(metadata={"failed_devices": {"startup_motor": "Connection failed"}})

    with (
        mock.patch.object(config_handler, "add_devices_to_redis") as add_devices,
        mock.patch.object(config_handler, "_update_device_server"),
        mock.patch.object(
            config_handler, "_wait_for_device_server_update", return_value=(True, response)
        ),
        mock.patch.object(config_handler, "send_config_request_reply") as reply,
        mock.patch.object(config_handler, "send_config") as broadcast,
    ):
        config_handler.parse_config_request(msg, cancel_event=threading.Event())

    kwargs = reply.call_args.kwargs
    assert kwargs["accepted"] is True, (
        "the device server accepted the add and already registered new_dev, but scihub "
        "rejected it and wrote nothing to redis / broadcast nothing. Reply error tail: "
        f"...{str(kwargs['error_msg'])[-160:]}  (add_devices_to_redis called: "
        f"{add_devices.called}, send_config called: {broadcast.called}). A retry is now "
        "rejected by the device server with 'already exists'."
    )


# ---------------------------------------------------------------------------
# ConfigHandler._update_config, multi-device update: device 1 succeeds (device
# server + redis + scihub memory), device 2 fails -> exception -> no broadcast,
# reply accepted=False.
# ---------------------------------------------------------------------------
def test_partial_multi_device_update_is_broadcast(config_handler):
    samx = _register(config_handler, "samx", enabled=False)
    _register(config_handler, "unreachable", enabled=False)
    msg = messages.DeviceConfigMessage(
        action="update",
        config={"samx": {"enabled": True}, "unreachable": {"enabled": True}},
        metadata={"RID": "1"},
    )
    ok = mock.MagicMock()
    failed = mock.MagicMock(message="PV is unreachable")

    with (
        mock.patch.object(config_handler, "_update_device_server"),
        mock.patch.object(
            config_handler,
            "_wait_for_device_server_update",
            side_effect=[(True, ok), (False, failed)],
        ),
        mock.patch.object(config_handler, "update_config_in_redis") as redis_write,
        mock.patch.object(config_handler, "send_config") as broadcast,
        mock.patch.object(config_handler, "send_config_request_reply") as reply,
    ):
        config_handler.parse_config_request(msg, cancel_event=threading.Event())

    # what already happened for samx before the second device failed:
    assert redis_write.call_args_list == [mock.call(samx)]
    assert samx.enabled is True
    assert broadcast.called, (
        "samx is enabled on the device server, in redis and in scihub memory, but no "
        "device_config_update broadcast was sent: scan server and clients keep "
        f"samx.enabled == False. Reply: accepted={reply.call_args.kwargs['accepted']} "
        "-> the client prints 'Failed to update the config: ... No devices were updated.'"
    )


# ---------------------------------------------------------------------------
# Two writers of the redis device_config key. The device server's
# handle_failed_device_inits writes redis WITHOUT a broadcast, so scihub's
# in-memory copy stays enabled=True. Any later unrelated accepted update
# replaces the whole redis entry with the stale copy.
# ---------------------------------------------------------------------------
def test_stale_scihub_copy_does_not_reenable_device_in_redis(config_handler):
    # scihub memory: still enabled (never told about the device server's startup failure)
    _register(config_handler, "samx", enabled=True)
    # redis: the device server disabled samx at startup via force_update_config_in_redis
    redis_config = [BASIC | {"name": "samx", "enabled": False}]
    written: list[list[dict]] = []

    with (
        mock.patch.object(
            config_handler, "get_config_from_redis", return_value=copy.deepcopy(redis_config)
        ),
        mock.patch.object(
            config_handler,
            "set_config_in_redis",
            side_effect=lambda c: written.append(copy.deepcopy(c)),
        ),
        mock.patch.object(config_handler, "_update_device_server") as device_server,
        mock.patch.object(config_handler, "send_config") as broadcast,
        mock.patch.object(config_handler, "send_config_request_reply"),
    ):
        config_handler._update_config(
            messages.DeviceConfigMessage(
                action="update",
                config={"samx": {"readoutPriority": "baseline"}},
                metadata={"RID": "1"},
            ),
            cancel_event=threading.Event(),
        )

    assert not device_server.called  # readoutPriority needs no device-server round trip
    samx_in_redis = next(d for d in written[-1] if d.get("name") == "samx")
    assert samx_in_redis["enabled"] is False, (
        f"an unrelated readoutPriority update rewrote redis with {samx_in_redis!r}: the "
        "device is re-enabled in redis while the device server still has it disabled and "
        f"unconnected. Broadcast carried only {broadcast.call_args.args[0].config} so live "
        "clients do not flip either; the next reload / new client re-reads redis and "
        "retries the dead PV."
    )


# ---------------------------------------------------------------------------
# Test quality, test_atlas_config_handler.py::test_config_handler_add_to_config_disables_failed_devices:
#     add_devices.assert_called_once_with(config)
# compares the mock's recorded argument with the SAME live dict the handler
# mutates, so it cannot tell whether the enabled=False flip happened before or
# after the redis write.
# ---------------------------------------------------------------------------
def test_by_reference_assertion_detects_a_late_flip():
    config = {"failed_device": {"enabled": True}}
    add_devices = mock.MagicMock()

    add_devices(config)  # "redis written" while the device is still enabled ...
    config["failed_device"]["enabled"] = False  # ... and the flip happens afterwards

    with pytest.raises(AssertionError):
        # DESIRED: the assertion notices that redis was written with enabled=True.
        # ACTUAL:  it passes, because the recorded argument IS the mutated dict.
        #          Use a call-time snapshot instead:
        #          side_effect=lambda c: snapshots.append(copy.deepcopy(c))
        add_devices.assert_called_once_with(config)
