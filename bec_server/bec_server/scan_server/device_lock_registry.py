from __future__ import annotations

import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable

from bec_lib.logger import bec_logger

logger = bec_logger.logger


class DeviceLockRegistry:
    """Registry that tracks per-request device locks for the scan server."""

    WAIT_INTERVAL_S: float = 0.1
    WAIT_LOG_INTERVAL_S: float = 5.0

    def __init__(self) -> None:
        """Initialize the device lock registry."""
        self._condition: threading.Condition = threading.Condition()
        self._device_owners: dict[str, str] = {}
        self._owner_devices: dict[str, set[str]] = defaultdict(set)

    def acquire_many(
        self,
        request_id: str,
        devices: Iterable[str],
        interruption_callback: Callable[[], None] | None = None,
        wait_state_callback: Callable[[list[str]], None] | None = None,
    ) -> list[str]:
        """
        Acquire locks for multiple devices on behalf of a request.

        Args:
            request_id (str): request identifier that will own the device locks.
            devices (Iterable[str]): device names to lock.
            interruption_callback (Callable[[], None] | None, optional):
                callback invoked while waiting for a lock, allowing the caller
                to react to interruptions. Defaults to None.

        Returns:
            list[str]: sorted device names whose locks were acquired.
        """
        device_names = sorted(set(devices))
        if not device_names:
            return []

        waiting_devices: list[str] = []
        next_log_time = 0.0
        while True:
            should_wait = False
            should_broadcast = False
            next_waiting_devices: list[str] = []
            blocked_owners: dict[str, str] = {}

            with self._condition:
                acquirable_devices: list[str] = []
                blocked_devices: list[str] = []

                for device in device_names:
                    current_owner = self._device_owners.get(device)
                    if current_owner == request_id:
                        # we already own this device, so we can skip it
                        continue
                    if current_owner is None:
                        # the device is not owned by anyone, so we can acquire it
                        acquirable_devices.append(device)
                        continue

                    # the device is owned by another request, so we need to wait for it
                    blocked_devices.append(device)
                    blocked_owners[device] = current_owner

                for device in acquirable_devices:
                    self._device_owners[device] = request_id
                    self._owner_devices[request_id].add(device)

                next_waiting_devices = blocked_devices
                if blocked_devices:
                    should_wait = True
                    next_log_time = self._log_waiting_for_device_lock(
                        request_id=request_id,
                        blocked_owners=blocked_owners,
                        next_log_time=next_log_time,
                    )
                    self._condition.wait(timeout=self.WAIT_INTERVAL_S)

                should_broadcast = bool(acquirable_devices) or (
                    next_waiting_devices != waiting_devices
                )

            if should_broadcast and wait_state_callback is not None:
                wait_state_callback(next_waiting_devices.copy())
            waiting_devices = next_waiting_devices

            if not should_wait:
                return device_names

            if interruption_callback is not None:
                interruption_callback()

    def acquire(
        self,
        request_id: str,
        device: str,
        interruption_callback: Callable[[], None] | None = None,
        wait_state_callback: Callable[[list[str]], None] | None = None,
    ) -> None:
        """
        Acquire the lock for a single device on behalf of a request.

        If the device is already owned by another request, this call blocks
        until the lock becomes available.

        Args:
            request_id (str): request identifier that will own the device lock.
            device (str): device name to lock.
            interruption_callback (Callable[[], None] | None, optional):
                callback invoked while waiting for the lock. Defaults to None.
        """
        self.acquire_many(
            request_id=request_id,
            devices=[device],
            interruption_callback=interruption_callback,
            wait_state_callback=wait_state_callback,
        )

    def release_all(self, request_id: str) -> list[str]:
        """
        Release all device locks held by a request.

        Args:
            request_id (str): request identifier whose locks should be released.

        Returns:
            list[str]: sorted device names whose locks were released.
        """
        with self._condition:
            devices = sorted(self._owner_devices.pop(request_id, set()))
            for device in devices:
                if self._device_owners.get(device) == request_id:
                    self._device_owners.pop(device, None)
            self._condition.notify_all()
            return devices

    def get_owned_devices(self, request_id: str) -> list[str]:
        """
        Get the devices currently locked by a request.

        Args:
            request_id (str): request identifier whose locked devices should be returned.

        Returns:
            list[str]: sorted device names currently owned by the request.
        """
        with self._condition:
            return sorted(self._owner_devices.get(request_id, set()))

    def _log_waiting_for_device_lock(
        self, request_id: str, blocked_owners: dict[str, str], next_log_time: float
    ) -> float:
        now = time.monotonic()
        if now < next_log_time:
            return next_log_time

        waiting_devices = ", ".join(sorted(blocked_owners))
        owners = ", ".join(
            f"{device} held by {owner}" for device, owner in sorted(blocked_owners.items())
        )
        logger.info(f"Request {request_id} waiting for device locks on {waiting_devices}; {owners}")
        return now + self.WAIT_LOG_INTERVAL_S
