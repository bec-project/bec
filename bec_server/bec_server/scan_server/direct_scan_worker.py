"""Execute one v4 scan without reading or changing its scheduling queue."""

from __future__ import annotations

import threading
import traceback
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Literal

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.logger import bec_logger

from .device_lock_registry import DeviceLockRegistry
from .errors import DeviceInstructionError, ScanAbortion, UserScanInterruption
from .scan_queue.types import ExitInfoType, InstructionQueueStatus
from .scans.scan_base import ScanBase

# ScanActions and DeviceManager expose the existing scan hooks as internal methods.
# pylint: disable=protected-access

logger = bec_logger.logger

SCAN_SEQUENCE = (
    "prepare_scan",
    "open_scan",
    "stage",
    "pre_scan",
    "scan_core",
    "post_scan",
    "unstage",
    "close_scan",
)
ScanOutcome = Literal["completed", "aborted", "shutdown"]


@dataclass
class ScanControl:
    """Thread-safe queue-to-task controls for one executing scan."""

    run_on_exception_hook: bool
    exit_info: ExitInfoType | None = None
    _requested_action: Literal["pause", "abort", "halt"] | None = None
    _in_cleanup: bool = False
    _terminal: bool = False
    _device_stops: list[Future[messages.DeviceStopResponse]] = field(
        default_factory=list, repr=False
    )
    device_stop_timeout: float = 30.0
    on_execution_status: Callable[[InstructionQueueStatus], None] | None = None
    _shutdown: bool = False
    shutdown_event: threading.Event | None = None
    _condition: threading.Condition = field(default_factory=threading.Condition, repr=False)

    @property
    def requested_action(self) -> Literal["pause", "abort", "halt"] | None:
        """Read the pending instruction independently of actual execution status."""
        with self._condition:
            return self._requested_action

    @property
    def terminal(self) -> bool:
        """Whether the terminal decision is closed to further interruption."""
        with self._condition:
            return self._terminal

    def complete(self, success: bool = True, stop_confirmed: bool = True) -> bool:
        """Commit a terminal decision without blocking the queue owner.

        Args:
            success (bool): Whether execution completed successfully rather than interrupted.
            stop_confirmed (bool): Whether ownership may be released after stop confirmation.

        Returns:
            bool: Whether the decision committed; False requires another stop acknowledgement.

        Raises:
            ScanAbortion: An interruption won the terminal decision.
            UserScanInterruption: An interruption has terminal information.
            RuntimeError: A newly accepted stop failed before terminal commitment.
        """
        with self._condition:
            if success and self._requested_action in ("abort", "halt"):
                if self.exit_info is None:
                    raise ScanAbortion()
                raise UserScanInterruption(exit_info=self.exit_info)
            if stop_confirmed:
                for stop in self._device_stops:
                    if not stop.done():
                        return False
                    if stop.exception() is not None or not stop.result().success:
                        raise RuntimeError("Device interruption failed; retaining ownership")
            self._terminal = True
            return True

    def request_pause(self) -> None:
        """Request a pause unless interruption or cleanup has already begun."""
        with self._condition:
            if self._requested_action not in ("abort", "halt") and not self._in_cleanup:
                self._requested_action = "pause"
                self._condition.notify_all()

    def resume(self) -> None:
        """Withdraw a pause request without undoing an abort or halt."""
        with self._condition:
            if self._requested_action == "pause":
                self._requested_action = None
                self._condition.notify_all()

    def set_cleanup_enabled(self, enabled: bool) -> None:
        """Update the exception-cleanup policy, for example when halting.

        Args:
            enabled (bool): Whether to run the scan exception cleanup hook.
        """
        with self._condition:
            self.run_on_exception_hook = enabled

    def stop(
        self,
        exit_info: ExitInfoType | None = None,
        *,
        shutdown: bool = False,
        device_stop: Future[messages.DeviceStopResponse] | None = None,
    ) -> bool:
        """Request interruption once; halt supersedes abort and interrupts cleanup.

        Args:
            exit_info (ExitInfoType | None): Terminal status and interruption source.
            shutdown (bool): Whether queue shutdown should skip exception cleanup.
            device_stop (Future[messages.DeviceStopResponse] | None): Correlated interruption
                acknowledgement to await before cleanup or lock release.

        Returns:
            bool: Whether a new interruption or escalation was accepted.
        """
        action = "halt" if shutdown or (exit_info and exit_info[0] == "halted") else "abort"
        with self._condition:
            if self._terminal:
                return False
            self._shutdown |= shutdown
            if (
                (self._in_cleanup and action == "abort")
                or self._requested_action == "halt"
                or (self._requested_action == "abort" and action == "abort" and not shutdown)
            ):
                return False
            self.exit_info = exit_info or self.exit_info
            if device_stop is not None:
                self._device_stops.append(device_stop)
            self._requested_action = action
            self.run_on_exception_hook = action != "halt"
            if self.shutdown_event is not None:
                self.shutdown_event.set()
            self._condition.notify_all()
            return True

    def wait_for_device_stops(self) -> None:
        """Require every accepted interruption to complete before relinquishing ownership.

        Raises:
            RuntimeError: A device stop failed or its acknowledgement did not arrive.
        """
        checked = 0
        while True:
            with self._condition:
                stops = tuple(self._device_stops[checked:])
                if not stops:
                    return
            for stop in stops:
                try:
                    response = stop.result(timeout=self.device_stop_timeout)
                except Exception as exc:
                    raise RuntimeError(
                        "Device interruption was not confirmed; retaining ownership"
                    ) from exc
                if not response.success:
                    raise RuntimeError(
                        f"Device interruption failed; retaining ownership: {response.errors}"
                    )
            checked += len(stops)

    def prepare_cleanup(self) -> bool:
        """Enter cleanup without clearing the acquisition's interruption request.

        Returns:
            bool: Whether finalization may proceed instead of queue shutdown.
        """
        with self._condition:
            if self._shutdown:
                return False
            self._in_cleanup = True
            if self.run_on_exception_hook and self.shutdown_event is not None:
                self.shutdown_event.clear()
            if self._requested_action == "pause":
                self._requested_action = None
                self._condition.notify_all()
            return True

    def checkpoint(self, on_pause: Callable[[], None]) -> None:
        """Acknowledge pause and apply interruption at a worker checkpoint.

        Args:
            on_pause (Callable[[], None]): Callback publishing the scan pause notification.

        Raises:
            ScanAbortion: Interruption was requested without terminal information.
            UserScanInterruption: Interruption was requested with terminal information.
        """
        with self._condition:
            paused = self._requested_action == "pause"
        if paused:
            if self.on_execution_status is not None:
                self.on_execution_status(InstructionQueueStatus.PAUSED)
            on_pause()
        with self._condition:
            self._condition.wait_for(lambda: self._requested_action != "pause")
            interrupted = self._requested_action == "halt" or (
                self._requested_action == "abort" and not self._in_cleanup
            )
            if interrupted:
                if self.exit_info is None:
                    raise ScanAbortion()
                raise UserScanInterruption(exit_info=self.exit_info)
        if paused and self.on_execution_status is not None:
            self.on_execution_status(InstructionQueueStatus.RUNNING)


@dataclass(frozen=True)
class ScanTask:
    """The future and control channel for one submitted scan."""

    future: Future[ScanOutcome]
    control: ScanControl
    thread: threading.Thread


class DirectScanWorker:
    """Run the v4 lifecycle and report its terminal outcome to a future."""

    def __init__(
        self,
        *,
        scan: ScanBase,
        control: ScanControl,
        on_status: Callable[[], None],
        device_lock_registry: DeviceLockRegistry | None = None,
        on_failure: Callable[[], None] | None = None,
        on_complete: Callable[[bool, bool], bool] | None = None,
    ) -> None:
        """Initialize execution of one scan with an independent control channel.

        Args:
            scan (ScanBase): Scan instance to execute or describe.
            control (ScanControl): Channel carrying pause, stop, and cleanup instructions.
            on_status (Callable[[], None]): Callback requesting publication of updated queue
                status.
            device_lock_registry (DeviceLockRegistry | None): Registry used to release device locks
                owned by the scan.
            on_failure (Callable[[], None] | None): Acknowledge failure on the queue owner before
                cleanup. None supports independent worker execution.
            on_complete (Callable[[bool, bool], bool] | None): Commit the terminal decision on
                the queue owner with success and stop-confirmation flags. None uses local control.
        """
        self.scan = scan
        self.control = control
        control.shutdown_event = scan._shutdown_event
        self.on_status = on_status
        self.device_lock_registry = device_lock_registry
        self.connector = scan.redis_connector
        self.device_manager = scan.device_manager
        self.on_failure = on_failure
        self.on_complete = on_complete or control.complete

    def run(self, prepare: Callable[[], None] | None = None) -> ScanOutcome:
        """Execute the scan and all cleanup before completing the future.

        Args:
            prepare (Callable[[], None] | None): Optional startup callback run before scan
                initialization.

        Returns:
            ScanOutcome: Terminal outcome after execution and lock release.
        """
        scan = self.scan
        scan.actions._interruption_callback = self.check_for_interruption
        scan.actions._update_queue_info_callback = self.on_status
        initialized = False
        abortion_exc: BaseException | None = None
        stop_confirmed = True
        try:
            with self.device_manager._rpc_method(scan.actions.rpc_call):
                self.check_for_interruption()
                if prepare is not None:
                    prepare()
                self.check_for_interruption()
                scan.actions._initialize_scan()
                initialized = True
                for step in SCAN_SEQUENCE:
                    method = getattr(scan, step, None)
                    if method is None:
                        raise ScanAbortion(f"Scan is missing required method: {step}")
                    self.check_for_interruption()
                    method()
            self.on_complete(True, True)
            return "completed"
        except BaseException as exc:
            if not (
                isinstance(exc, ScanAbortion) and self.control.requested_action in ("abort", "halt")
            ):
                if self.on_failure is not None:
                    try:
                        self.on_failure()
                    except Exception:  # pylint: disable=broad-except
                        logger.exception("Failed to coordinate scan failure; continuing cleanup")
                self._raise_alarm(exc)
            try:
                self.control.wait_for_device_stops()
            except Exception as stop_exc:  # pylint: disable=broad-except
                stop_confirmed = False
                abortion_exc = exc
                self._raise_alarm(stop_exc)
                return "aborted"
            if not self.control.prepare_cleanup():
                return "shutdown"
            if initialized and self.control.on_execution_status is not None:
                self.control.on_execution_status(InstructionQueueStatus.RUNNING)
            if initialized and isinstance(exc, Exception):
                self._run_on_exception_hook(exc)
            abortion_exc = exc
            return "aborted"
        finally:
            if stop_confirmed and not self.control.terminal:
                try:
                    while True:
                        self.control.wait_for_device_stops()
                        if self.on_complete(False, True):
                            break
                except Exception as stop_exc:  # pylint: disable=broad-except
                    stop_confirmed = False
                    self._raise_alarm(stop_exc)
            if stop_confirmed:
                self._release_scan_locks()
            else:
                self.on_complete(False, False)
            scan.actions._interruption_callback = None
            scan.actions._update_queue_info_callback = None
            if abortion_exc is not None:
                self._publish_abortion(abortion_exc)

    def check_for_interruption(self) -> None:
        """Apply pause and stop commands at a scan checkpoint."""
        self.control.checkpoint(lambda: self.scan.actions._send_scan_status("paused"))

    #############################################
    ############### Helper Methods ##############
    #############################################

    def _run_on_exception_hook(self, exc: Exception) -> bool:
        """Run scan cleanup and report a failure in the cleanup hook.

        Args:
            exc (Exception): Exception that interrupted scan execution.

        Returns:
            bool: Whether cleanup completed without an additional exception.
        """
        if not self.control.run_on_exception_hook:
            return True
        hook = getattr(self.scan, "on_exception", None)
        if not callable(hook):
            return True
        try:
            self.scan.actions._metadata_suffix = "__on-exception"
            with self.device_manager._rpc_method(self.scan.actions.rpc_call):
                hook(exc.__cause__ or exc)
        except Exception as cleanup_exc:
            if (
                isinstance(cleanup_exc, UserScanInterruption)
                and self.control.requested_action == "halt"
            ):
                return True
            self.scan.actions.send_client_info("")
            logger.exception("Failed to run direct scan on_exception hook")
            self._raise_alarm(cleanup_exc)
            return False
        return True

    def _publish_abortion(self, exc: BaseException) -> None:
        """Publish the terminal status of an interrupted scan.

        Args:
            exc (BaseException): Exception that interrupted scan execution.
        """
        exit_info = self.control.exit_info or (
            exc.exit_info if isinstance(exc, UserScanInterruption) else None
        )
        if exit_info:
            self.scan.actions._send_scan_status(exit_info[0], reason=exit_info[1])
        else:
            status = "aborted" if self.control.run_on_exception_hook else "halted"
            self.scan.actions._send_scan_status(status, reason="alarm")

    def _raise_alarm(self, exc: BaseException) -> None:
        """Report a scan failure with its request, queue, and scan metadata.

        Args:
            exc (BaseException): Exception that interrupted scan execution.
        """
        error_info = (
            exc.error_info
            if isinstance(exc, DeviceInstructionError)
            else messages.ErrorInfo(
                error_message=traceback.format_exc(),
                compact_error_message=f"{type(exc).__name__}: {exc}",
                exception_type=type(exc).__name__,
                device=None,
            )
        )
        metadata = {
            "queue": getattr(self.scan.scan_info, "scan_queue", "primary"),
            "RID": self.scan.scan_info.metadata.get("RID"),
            **{
                key: value
                for key, value in {
                    "scan_id": self.scan.scan_info.scan_id,
                    "scan_number": self.scan.scan_info.scan_number,
                }.items()
                if value is not None
            },
        }
        try:
            self.connector.raise_alarm(severity=Alarms.MAJOR, info=error_info, metadata=metadata)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to publish scan failure alarm")

    def _release_scan_locks(self) -> None:
        """Release device locks held by the scan request."""
        request_id = self.scan.scan_info.metadata.get("RID")
        if self.device_lock_registry is not None and request_id is not None:
            self.device_lock_registry.release_all(request_id)
