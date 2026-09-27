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
        self._stopped_requests: set[str] = set()
        self._stops_in_flight: dict[str, int] = defaultdict(int)

        # Maps device names to the request ID that currently owns the lock.
        self._device_owners: dict[str, str] = {}

        # Maps request IDs to the set of device names they currently own.
        self._owner_devices: dict[str, set[str]] = defaultdict(set)

        # Maps request IDs to the set of device names they are currently waiting for.
        self._pending_device_locks: dict[str, set[str]] = defaultdict(set)

    def acquire_many(
        self,
        request_id: str,
        devices: Iterable[str],
        interruption_callback: Callable[[], None] | None = None,
        queue_update_callback: Callable[[], None] | None = None,
    ) -> list[str]:
        """
        Acquire locks for multiple devices on behalf of a request.

        Args:
            request_id (str): request identifier that will own the device locks.
            devices (Iterable[str]): device names to lock.
            interruption_callback (Callable[[], None] | None, optional):
                callback invoked while waiting for a lock, allowing the caller
                to react to interruptions. Defaults to None.
            queue_update_callback (Callable[[], None] | None, optional):
                callback invoked when queue-visible lock state changes, e.g.
                owned or waiting devices. Defaults to None.

        Returns:
            list[str]: sorted device names whose locks were acquired.
        """
        device_names = sorted(set(devices))
        if not device_names:
            return []

        next_log_time = 0.0
        while True:
            should_queue_update = False
            blocked_owners: dict[str, str] = {}
            waiting_devices = sorted(self._pending_device_locks.get(request_id, set()))

            if interruption_callback is not None:
                interruption_callback()
            with self._condition:
                if request_id in self._stopped_requests:
                    raise RuntimeError(f"Request {request_id} was stopped")
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

                self._pending_device_locks[request_id] = set(blocked_devices)
                if blocked_devices:
                    next_log_time = self._log_waiting_for_device_lock(
                        request_id=request_id,
                        blocked_owners=blocked_owners,
                        next_log_time=next_log_time,
                    )
                    self._condition.wait(timeout=self.WAIT_INTERVAL_S)

                next_waiting_devices = sorted(self._pending_device_locks[request_id])
                should_queue_update = bool(acquirable_devices) or (
                    next_waiting_devices != waiting_devices
                )

            if should_queue_update and queue_update_callback is not None:
                queue_update_callback()

            if not blocked_devices:
                return device_names

            if interruption_callback is not None:
                interruption_callback()

    def acquire(
        self,
        request_id: str,
        device: str,
        interruption_callback: Callable[[], None] | None = None,
        queue_update_callback: Callable[[], None] | None = None,
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
            queue_update_callback (Callable[[], None] | None, optional):
                callback invoked when queue-visible lock state changes. Defaults to None.
        """
        self.acquire_many(
            request_id=request_id,
            devices=[device],
            interruption_callback=interruption_callback,
            queue_update_callback=queue_update_callback,
        )

    def stop_request(self, request_id: str, send_stop: Callable[[list[str]], None]) -> None:
        """Freeze acquisition and send stop before the caller releases device ownership.

        The worker waits for the queue's stop receipt before cleanup or release. The
        registry lock is only needed to capture ownership, never for network I/O.
        """
        with self._condition:
            self._stopped_requests.add(request_id)
            self._stops_in_flight[request_id] += 1
            devices = sorted(self._owner_devices.get(request_id, set()))
            self._condition.notify_all()
        try:
            send_stop(devices)
        finally:
            with self._condition:
                self._stops_in_flight[request_id] -= 1
                if not self._stops_in_flight[request_id]:
                    self._stops_in_flight.pop(request_id)
                self._condition.notify_all()

    def allow_request(self, request_id: str) -> None:
        """Allow a stopped request to acquire locks during its exception hook."""
        with self._condition:
            self._stopped_requests.discard(request_id)
            self._condition.notify_all()

    def release_all(self, request_id: str) -> list[str]:
        """
        Release all device locks held by a request.

        Args:
            request_id (str): request identifier whose locks should be released.

        Returns:
            list[str]: sorted device names whose locks were released.
        """
        with self._condition:
            self._condition.wait_for(lambda: not self._stops_in_flight.get(request_id))
            self._pending_device_locks.pop(request_id, None)
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

    def get_pending_devices(self, request_id: str) -> list[str]:
        """
        Get the devices that a request is currently waiting to acquire.

        Args:
            request_id (str): request identifier whose pending devices should be returned.
        """
        with self._condition:
            return sorted(self._pending_device_locks.get(request_id, set()))

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
