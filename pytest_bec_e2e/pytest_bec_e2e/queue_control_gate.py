"""A thread-free detector latch for queue-control integration tests."""

from __future__ import annotations

import threading
from typing import Any

from ophyd import Component, Device, DeviceStatus, Signal


class QueueControlGate(Device):
    """Hold the third trigger after arming; release or stop resolves the held status."""

    USER_ACCESS = ["arm", "release"]
    value = Component(Signal, value=0)
    waiting = Component(Signal, value=False, kind="omitted")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._gate_lock = threading.RLock()
        self._armed = False
        self._count = 0
        self._held: DeviceStatus | None = None

    def arm(self) -> None:
        """Hold the third upcoming trigger until released or stopped."""
        with self._gate_lock:
            if self._held is not None:
                raise RuntimeError("A trigger is already held")
            self._armed = True
            self._count = 0

    def trigger(self) -> DeviceStatus:
        """Return a pending status for the armed third trigger, otherwise complete promptly."""
        with self._gate_lock:
            status = DeviceStatus(self)
            self._count += 1
            self.value.put(self._count)
            if self._armed and self._count >= 3:
                self._held = status
                self.waiting.put(True)
            else:
                status.set_finished()
            return status

    def _resolve(self, stopped: bool) -> None:
        with self._gate_lock:
            self._armed = False
            status, self._held = self._held, None
            self.waiting.put(False)
        if status is not None:
            if stopped:
                status.set_exception(RuntimeError("Queue control gate stopped"))
            else:
                status.set_finished()

    def release(self) -> None:
        """Release the held trigger and leave subsequent triggers unblocked."""
        self._resolve(stopped=False)

    def stop(self, *, success: bool = False) -> None:
        """Cancel a held trigger promptly, without starting or waiting on a thread."""
        self._resolve(stopped=not success)
