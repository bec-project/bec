import threading
import time
from unittest import mock

import pytest

from bec_server.scan_server.device_lock_registry import DeviceLockRegistry


def test_device_lock_registry_acquire_and_release():
    registry = DeviceLockRegistry()

    acquired = registry.acquire_many("scan-1", {"samx", "samy"})

    assert acquired == ["samx", "samy"]
    assert registry.release_all("scan-1") == ["samx", "samy"]


def test_device_lock_registry_logs_when_waiting_for_device():
    registry = DeviceLockRegistry()
    registry.acquire("scan-1", "samx")
    wait_logged = threading.Event()

    def release_after_wait_logged():
        assert wait_logged.wait(timeout=1)
        registry.release_all("scan-1")

    releaser = threading.Thread(target=release_after_wait_logged, daemon=True)
    releaser.start()

    with mock.patch("bec_server.scan_server.device_lock_registry.logger.info") as log_info:
        log_info.side_effect = lambda *args, **kwargs: wait_logged.set()
        registry.acquire("request-2", "samx")

    releaser.join(timeout=1)

    log_info.assert_called_once()
    assert "waiting for device lock" in log_info.call_args.args[0]


def test_device_lock_registry_wait_log_is_throttled():
    registry = DeviceLockRegistry()
    with mock.patch("bec_server.scan_server.device_lock_registry.logger.info") as log_info:
        next_log_time = 0.0
        with mock.patch("bec_server.scan_server.device_lock_registry.time.monotonic") as monotonic:
            monotonic.side_effect = [10.0, 12.0, 16.0]
            next_log_time = registry._log_waiting_for_device_lock(
                request_id="scan-2", blocked_owners={"samx": "scan-1"}, next_log_time=next_log_time
            )
            next_log_time = registry._log_waiting_for_device_lock(
                request_id="scan-2", blocked_owners={"samx": "scan-1"}, next_log_time=next_log_time
            )
            registry._log_waiting_for_device_lock(
                request_id="scan-2", blocked_owners={"samx": "scan-1"}, next_log_time=next_log_time
            )

    assert log_info.call_count == 2


def test_device_lock_registry_logs_all_waiting_devices():
    registry = DeviceLockRegistry()
    registry.acquire("scan-1", "samx")
    registry.acquire("scan-2", "samy")
    wait_logged = threading.Event()

    def release_after_wait_logged():
        assert wait_logged.wait(timeout=1)
        registry.release_all("scan-1")
        registry.release_all("scan-2")

    releaser = threading.Thread(target=release_after_wait_logged, daemon=True)
    releaser.start()

    with mock.patch("bec_server.scan_server.device_lock_registry.logger.info") as log_info:
        log_info.side_effect = lambda *args, **kwargs: wait_logged.set()
        registry.acquire_many("request-3", ["samx", "samz", "samy"])

    releaser.join(timeout=1)

    log_info.assert_called_once()
    assert "samx, samy" in log_info.call_args.args[0]
    assert "samx held by scan-1" in log_info.call_args.args[0]
    assert "samy held by scan-2" in log_info.call_args.args[0]


def test_device_lock_registry_runs_interruption_callback_outside_condition():
    registry = DeviceLockRegistry()
    registry.acquire("scan-1", "samx")
    callback_states = []

    def interruption_callback():
        callback_states.append(registry._condition._is_owned())

    releaser = threading.Thread(
        target=lambda: (time.sleep(0.05), registry.release_all("scan-1")), daemon=True
    )
    releaser.start()

    registry.acquire("request-2", "samx", interruption_callback=interruption_callback)

    releaser.join(timeout=1)

    assert callback_states
    assert callback_states == [False] * len(callback_states)


def test_device_lock_registry_notifies_queue_update_callback_on_wait_transition():
    registry = DeviceLockRegistry()
    registry.acquire("scan-1", "samx")
    wait_logged = threading.Event()
    wait_states = []

    def queue_update_callback():
        wait_states.append(registry.get_pending_devices("request-2"))
        if wait_states[-1]:
            wait_logged.set()

    releaser = threading.Thread(
        target=lambda: (wait_logged.wait(timeout=1), registry.release_all("scan-1")), daemon=True
    )
    releaser.start()

    registry.acquire("request-2", "samx", queue_update_callback=queue_update_callback)

    releaser.join(timeout=1)

    assert wait_states == [["samx"], []]


def test_device_lock_registry_acquire_many_bundles_queue_update_callback():
    registry = DeviceLockRegistry()
    registry.acquire("scan-1", "samx")
    registry.acquire("scan-2", "samy")
    wait_logged = threading.Event()
    wait_states = []

    def queue_update_callback():
        wait_states.append(registry.get_pending_devices("request-3"))
        assert registry._condition._is_owned() is False
        if wait_states[-1]:
            wait_logged.set()

    releaser = threading.Thread(
        target=lambda: (
            wait_logged.wait(timeout=1),
            registry.release_all("scan-1"),
            registry.release_all("scan-2"),
        ),
        daemon=True,
    )
    releaser.start()

    acquired = registry.acquire_many(
        "request-3", ["samx", "samz", "samy"], queue_update_callback=queue_update_callback
    )

    releaser.join(timeout=1)

    assert acquired == ["samx", "samy", "samz"]
    assert wait_states == [["samx", "samy"], []]


def test_stop_delivery_fences_release_without_blocking_unrelated_devices():
    registry = DeviceLockRegistry()
    registry.acquire("scan", "samx")
    sending, release_send, released = threading.Event(), threading.Event(), threading.Event()
    stopped_devices = []

    def send(devices):
        stopped_devices.extend(devices)
        sending.set()
        assert release_send.wait(2)

    stop = threading.Thread(target=lambda: registry.stop_request("scan", send))
    release = threading.Thread(target=lambda: (registry.release_all("scan"), released.set()))
    stop.start()
    try:
        assert sending.wait(1)
        release.start()
        assert not released.wait(0.02)
        assert registry.get_owned_devices("scan") == ["samx"]
        assert registry.acquire_many("other", ["samy"]) == ["samy"]
    finally:
        release_send.set()
        stop.join(1)
        release.join(1)
    assert released.is_set()
    assert stopped_devices == ["samx"]
    assert registry.acquire_many("other", ["samx"]) == ["samx"]


def test_stop_rejects_new_acquisition_until_cleanup():
    registry = DeviceLockRegistry()
    registry.stop_request("scan", lambda devices: None)

    with pytest.raises(RuntimeError, match="was stopped"):
        registry.acquire_many("scan", ["samx"])
    registry.allow_request("scan")
    assert registry.acquire_many("scan", ["samx"]) == ["samx"]


def test_failed_stop_delivery_still_unblocks_release():
    registry = DeviceLockRegistry()
    registry.acquire("scan", "samx")

    with pytest.raises(RuntimeError, match="transport"):
        registry.stop_request("scan", mock.Mock(side_effect=RuntimeError("transport")))
    assert registry.release_all("scan") == ["samx"]
