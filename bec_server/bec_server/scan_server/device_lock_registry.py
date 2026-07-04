from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Callable, Iterable

from bec_lib.logger import bec_logger

logger = bec_logger.logger


class DeviceLockRegistry:
    """Registry that tracks per-request device locks for the scan server."""

    WAIT_INTERVAL_S: float = 0.1

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
        acquired: list[str] = []
        for device in sorted(set(devices)):
            self.acquire(request_id, device, interruption_callback=interruption_callback)
            acquired.append(device)
        return acquired

    def acquire(
        self, request_id: str, device: str, interruption_callback: Callable[[], None] | None = None
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
        has_logged_wait = False
        while True:
            with self._condition:
                current_owner = self._device_owners.get(device)
                if current_owner in (None, request_id):
                    self._device_owners[device] = request_id
                    self._owner_devices[request_id].add(device)
                    return

                if not has_logged_wait:
                    logger.info(
                        "Request %s waiting for device lock on %s held by %s",
                        request_id,
                        device,
                        current_owner,
                    )
                    has_logged_wait = True

                self._condition.wait(timeout=self.WAIT_INTERVAL_S)

            if interruption_callback is not None:
                interruption_callback()

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
