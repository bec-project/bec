import threading
import time
from unittest import mock

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


def test_device_lock_registry_notifies_wait_state_callback_on_wait_transition():
    registry = DeviceLockRegistry()
    registry.acquire("scan-1", "samx")
    wait_logged = threading.Event()
    wait_states = []

    def wait_state_callback(waiting_devices):
        wait_states.append(waiting_devices)
        if waiting_devices:
            wait_logged.set()

    releaser = threading.Thread(
        target=lambda: (wait_logged.wait(timeout=1), registry.release_all("scan-1")), daemon=True
    )
    releaser.start()

    registry.acquire("request-2", "samx", wait_state_callback=wait_state_callback)

    releaser.join(timeout=1)

    assert wait_states == [["samx"], []]


def test_device_lock_registry_acquire_many_bundles_wait_state_updates():
    registry = DeviceLockRegistry()
    registry.acquire("scan-1", "samx")
    registry.acquire("scan-2", "samy")
    wait_logged = threading.Event()
    wait_states = []

    def wait_state_callback(waiting_devices):
        wait_states.append(waiting_devices)
        assert registry._condition._is_owned() is False
        if waiting_devices:
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
        "request-3", ["samx", "samz", "samy"], wait_state_callback=wait_state_callback
    )

    releaser.join(timeout=1)

    assert acquired == ["samx", "samy", "samz"]
    assert wait_states == [["samx", "samy"], []]
