"""Monitor the ophyd callback queue for stalled callbacks."""

from __future__ import annotations

import sys
import threading
import traceback

import ophyd

from bec_lib.logger import bec_logger

logger = bec_logger.logger


class OphydCallbackMonitor:
    """Warn when the ophyd monitor callback queue exceeds a configured size."""

    def __init__(self, *, queue_threshold: int = 1000, sample_interval: float = 5) -> None:
        """Initialize the monitor.

        Args:
            queue_threshold (int): Maximum queue size before a warning is logged.
            sample_interval (float): Time in seconds between queue samples.
        """
        if queue_threshold < 0:
            raise ValueError("queue_threshold must be non-negative")
        if sample_interval <= 0:
            raise ValueError("sample_interval must be positive")

        self.queue_threshold = queue_threshold
        self.sample_interval = sample_interval
        self._stop_event = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._stopping = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background monitor if it is not already running."""
        with self._lifecycle_lock:
            if self._stopping or (self._thread is not None and self._thread.is_alive()):
                return

            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run, name="ophyd_callback_monitor", daemon=True
            )
            self._thread.start()

    def request_stop(self) -> None:
        """Ask the monitor to stop sampling."""
        with self._lifecycle_lock:
            self._stopping = True
            self._stop_event.set()

    def join(self, timeout: float | None = None) -> None:
        """Wait for the monitor thread to finish.

        A bounded join keeps starts suspended until a final unbounded join completes.

        Args:
            timeout (float | None): Maximum wait in seconds, or None to wait indefinitely.
        """
        with self._lifecycle_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        if timeout is None:
            with self._lifecycle_lock:
                if thread is self._thread:
                    self._thread = None
                self._stopping = False

    def _run(self) -> None:
        """Sample the ophyd monitor queue until shutdown is requested."""
        while not self._stop_event.wait(self.sample_interval):
            self._sample()

    def _sample(self) -> None:
        """Log a warning if the ophyd monitor queue exceeds the threshold."""
        dispatcher = ophyd.get_cl().get_dispatcher()
        if dispatcher is None:
            return
        monitor = dispatcher.threads.get("monitor")
        if monitor is None:
            return
        queue_size = monitor.queue.qsize()
        if queue_size <= self.queue_threshold:
            return

        frame = sys._current_frames().get(monitor.ident)  # pylint: disable=protected-access
        stack = (
            "".join(traceback.format_stack(frame, limit=20)) if frame is not None else "<no stack>"
        )
        del frame
        logger.warning(
            f"Ophyd callback monitor queue exceeds {self.queue_threshold}: "
            f"queued={queue_size}, last_started={monitor.current_callback}\n{stack}"
        )
