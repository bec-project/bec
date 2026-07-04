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
    assert log_info.call_args.args[2] == "samx"


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
