"""Broadcast the latest scan lifecycle or instruction update to clients."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger


class ScanHookBroadcaster:
    """Send only the latest update in each 0.1 second broadcast window."""

    def __init__(self, connector: RedisConnector) -> None:
        self._connector = connector
        self._lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._latest_update: messages.ScanHookMessage | None = None
        self._timer: threading.Timer | None = None
        self._active = False

    def start(self) -> None:
        """Enable event collection when the scan worker begins execution."""
        with self._lock:
            self._active = True

    def stop(self) -> None:
        """Disable collection and attempt final delivery without affecting the scan outcome."""
        with self._lock:
            self._active = False
        try:
            self.flush()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to broadcast final scan hook update")

    def add(self, event: messages.ScanHookMessage) -> None:
        """Replace the pending update and schedule a flush when the first event arrives.

        Args:
            event (messages.ScanHookMessage): Lifecycle or instruction event to broadcast.
        """
        with self._lock:
            if not self._active:
                return
            self._latest_update = event
            if self._timer is None:
                self._timer = threading.Timer(0.1, self._flush_scheduled)
                self._timer.daemon = True
                self._timer.start()

    def _flush_scheduled(self) -> None:
        try:
            self.flush()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to broadcast scan hook events")

    def flush(self) -> None:
        """Send the latest pending update and cancel the pending timer.

        Scans call this on every exit path to deliver their final update.
        Failed publications retain the update for retry unless a newer one arrives.
        """
        timer = None
        try:
            # Serialize publications without blocking producers on network I/O.
            with self._publish_lock:
                with self._lock:
                    timer = self._timer
                    self._timer = None
                    if timer is not None:
                        timer.cancel()
                    event = self._latest_update
                    self._latest_update = None
                if event is None:
                    return
                try:
                    self._connector.send(MessageEndpoints.scan_hook(), event)
                except Exception:
                    with self._lock:
                        if self._latest_update is None:
                            self._latest_update = event
                    raise
        finally:
            # A timer may itself be waiting to publish, so release both locks first.
            if timer is not None and timer is not threading.current_thread():
                timer.join()
