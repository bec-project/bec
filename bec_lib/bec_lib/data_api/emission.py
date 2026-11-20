"""
Emission scheduler of the DataAPI.

Coalesced ("live") updates are not delivered on every notification: a
subscription emits at most once per ``min_emit_interval`` per correlation
group and guarantees a trailing emission carrying the final state. The
trailing emissions of *all* subscriptions of one client are delivered by one
:class:`EmissionScheduler` — a single lazily started daemon thread holding a
deadline heap — instead of one ``threading.Timer`` per window.

Lock discipline: the scheduler's condition is a *leaf* lock. ``schedule`` and
``cancel`` may be called with the DataAPI lock held; the scheduler thread
always releases the condition before invoking a flush, and a flush takes the
DataAPI lock itself. A flush that raises is logged and never kills the thread.
"""

from __future__ import annotations

import heapq
import threading
import time
from typing import Callable, Hashable

from bec_lib.logger import bec_logger

logger = bec_logger.logger

#: A scheduled trailing emission: (deadline, sequence, owner, key, flush).
_Entry = tuple[float, int, object, Hashable, Callable[[Hashable], None]]


class EmissionScheduler:
    """
    One deadline heap and one daemon thread delivering trailing emissions.

    Args:
        name (str): Thread name (one scheduler per client).
    """

    def __init__(self, name: str = "data-api-emit") -> None:
        self._name = name
        self._cv = threading.Condition()
        self._heap: list[_Entry] = []
        self._seq = 0
        self._thread: threading.Thread | None = None
        self._stopped = False

    @property
    def thread(self) -> threading.Thread | None:
        """The scheduler thread, or ``None`` until the first deferred emission."""
        return self._thread

    @property
    def pending(self) -> int:
        """Number of scheduled (not yet delivered) trailing emissions."""
        with self._cv:
            return len(self._heap)

    @property
    def stopped(self) -> bool:
        """Whether :meth:`stop` was called (entries are dropped, ``schedule`` refuses)."""
        return self._stopped

    def schedule(
        self, owner: object, key: Hashable, deadline: float, flush: Callable[[Hashable], None]
    ) -> bool:
        """
        Schedule ``flush(key)`` at ``deadline`` (``time.monotonic()`` based).

        Args:
            owner (object): The subscription the entry belongs to (for cancel).
            key (Hashable): The correlation group, handed back to ``flush``.
            deadline (float): Monotonic time at which to flush.
            flush (Callable[[Hashable], None]): Called on the scheduler thread.

        Returns:
            bool: ``False`` once the scheduler is stopped — the caller must
                then deliver immediately rather than lose the state.
        """
        with self._cv:
            if self._stopped:
                return False
            self._seq += 1
            heapq.heappush(self._heap, (deadline, self._seq, owner, key, flush))
            if self._thread is None or not self._thread.is_alive():
                # First use, or the previous thread died of a BaseException
                # escaping a flush: never leave entries queued on a dead thread.
                self._thread = threading.Thread(target=self._run, name=self._name, daemon=True)
                self._thread.start()
            else:
                self._cv.notify()
            return True

    def cancel(self, owner: object, key: Hashable | None = None) -> None:
        """Drop the entries of ``owner`` (all groups, or just ``key``)."""
        with self._cv:
            kept = [e for e in self._heap if not (e[2] is owner and (key is None or e[3] == key))]
            if len(kept) != len(self._heap):
                self._heap = kept
                heapq.heapify(self._heap)
                self._cv.notify()

    def stop(self, timeout: float = 1.0) -> None:
        """Drop every entry and join the thread (unless called from it)."""
        with self._cv:
            self._stopped = True
            self._heap.clear()
            thread = self._thread
            self._cv.notify_all()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
            if thread.is_alive():
                logger.warning(f"{self._name}: still delivering after {timeout}s")

    def _run(self) -> None:
        while True:
            with self._cv:
                while not self._stopped:
                    if not self._heap:
                        self._cv.wait()
                        continue
                    delay = self._heap[0][0] - time.monotonic()
                    if delay <= 0:
                        break
                    self._cv.wait(delay)
                if self._stopped:
                    return
                _, _, _, key, flush = heapq.heappop(self._heap)
            try:
                flush(key)
            except Exception:  # pylint: disable=broad-except
                logger.exception(f"{self._name}: trailing emission of group {key!r} failed")
            except BaseException:
                with self._cv:
                    self._thread = None  # a fresh thread is started by the next schedule()
                raise
