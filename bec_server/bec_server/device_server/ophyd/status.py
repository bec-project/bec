"""Translate Ophyd operation statuses into the device server's neutral contract."""

from __future__ import annotations

import threading
from collections.abc import Callable

from ophyd import OphydObject, StatusBase

from bec_lib.logger import bec_logger
from bec_lib.messages import DeviceInstructionMessage
from bec_server.device_server.device_status import DeviceStatus

logger = bec_logger.logger


class OphydStatus:
    """Settle a native status only after its backend completion hook has finished."""

    def __init__(
        self,
        native_status: StatusBase,
        instruction: DeviceInstructionMessage,
        obj: OphydObject,
        *,
        on_complete: Callable[[OphydStatus], None] | None = None,
    ) -> None:
        self.native_status = native_status
        self.instruction = instruction
        self.obj = obj
        root_name = getattr(getattr(obj, "root", None), "name", None)
        dotted_name = getattr(obj, "dotted_name", None)
        self.device_name = (
            f"{root_name}.{dotted_name}"
            if root_name and dotted_name
            else root_name or getattr(obj, "name", None)
        )
        self.status_type = native_status.__class__.__name__
        self._on_complete = on_complete
        self._lock = threading.Lock()
        self._started = False
        self._done = False
        self._success: bool | None = None
        self._exception: Exception | None = None
        self._callbacks: list[Callable[[DeviceStatus], None]] = []
        native_status.add_callback(self._native_completed)

    @property
    def done(self) -> bool:
        """Whether native completion and backend side effects have finished."""
        with self._lock:
            return self._done

    @property
    def success(self) -> bool | None:
        """Return the native outcome, including any failure in backend finalization."""
        with self._lock:
            return self._success

    def exception(self) -> Exception | None:
        """Return the captured failure without waiting for native completion."""
        with self._lock:
            return self._exception

    def add_callback(self, callback: Callable[[DeviceStatus], None]) -> None:
        """Invoke a callback with this adapter after finalization.

        Args:
            callback (Callable[[DeviceStatus], None]): Callback receiving the settled status.
        """
        with self._lock:
            if not self._done:
                self._callbacks.append(callback)
                return
        self._invoke_callback(callback)

    def _native_completed(self, _native_status: StatusBase) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True

        # Keep the outcome local until backend finalization has also completed.
        try:
            error = self.native_status.exception()
            success = self.native_status.success
        except Exception as exc:  # pylint: disable=broad-except
            error = exc
            success = False
        try:
            if self._on_complete is not None:
                self._on_complete(self)
        except Exception as exc:  # pylint: disable=broad-except
            error = error or exc
            success = False
            logger.exception(f"Status finalization failed for {self.device_name}.")
        finally:
            with self._lock:
                self._exception = error
                self._success = success
                self._done = True
                callbacks = self._callbacks
                self._callbacks = []
                self._on_complete = None
            for callback in callbacks:
                self._invoke_callback(callback)

    def _invoke_callback(self, callback: Callable[[DeviceStatus], None]) -> None:
        try:
            callback(self)
        except Exception:  # pylint: disable=broad-except
            # A failing consumer must not prevent another observing completion.
            logger.exception(f"Status callback failed for {self.device_name}.")
