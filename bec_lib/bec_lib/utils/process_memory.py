"""
Process-level memory tuning helpers.

These address a platform-specific symptom: long-lived, multi-threaded BEC
processes (e.g. the IPython console) use *far* more resident memory on Linux
(glibc) than on macOS, even with identical Python-level allocations. The cause is
the glibc allocator, not a Python leak:

* glibc creates up to ``8 * nproc`` malloc *arenas* and assigns threads to them.
  The BEC client runs ~30 background threads (redis listeners/dispatchers, stream
  readers, a ``ThreadPoolExecutor``, the metrics emitter, ...), so on a many-core
  console host this can reserve a very large heap.
* glibc rarely returns freed memory (including freed numpy buffers) to the OS, so
  RSS ratchets up and plateaus high.

macOS uses a completely different allocator, which is why the same workload looks
fine there. These helpers are safe no-ops off Linux/glibc.
"""

from __future__ import annotations

import ctypes
import os
import sys

# mallopt parameter numbers (from <malloc.h>); negative by glibc convention.
_M_ARENA_MAX = -8
_M_TRIM_THRESHOLD = -1


def limit_malloc_arenas(max_arenas: int = 2) -> bool:
    """Cap the number of glibc malloc arenas for this process.

    Reduces RSS for multi-threaded Python+numpy processes on Linux, at the cost of
    a little malloc concurrency (negligible for the I/O-bound BEC client). Must be
    called early, before many threads/arenas are created, to be fully effective.

    Disable via the environment variable ``BEC_DISABLE_MALLOC_TUNING=1``.

    Args:
        max_arenas: maximum number of arenas (glibc ``M_ARENA_MAX``). Default 2.

    Returns:
        True if the limit was applied, False if skipped (non-glibc, disabled, or
        the C call failed).
    """
    if sys.platform != "linux" or os.environ.get("BEC_DISABLE_MALLOC_TUNING"):
        return False
    arenas = os.environ.get("BEC_MALLOC_ARENA_MAX")
    if arenas is not None:
        try:
            max_arenas = int(arenas)
        except ValueError:
            pass
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        # mallopt returns 1 on success, 0 on failure.
        return libc.mallopt(_M_ARENA_MAX, int(max_arenas)) == 1
    except (OSError, AttributeError):  # pragma: no cover - non-glibc / no libc
        return False


def trim_malloc() -> bool:
    """Ask glibc to return as much free heap memory to the OS as possible.

    Thin wrapper around ``malloc_trim(0)``. Useful to call at natural idle points
    (e.g. after a scan completes) so freed numpy buffers do not stay resident.
    Safe no-op off Linux/glibc.

    Returns:
        True if memory was released, False otherwise.
    """
    if sys.platform != "linux" or os.environ.get("BEC_DISABLE_MALLOC_TUNING"):
        return False
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        return libc.malloc_trim(0) == 1
    except (OSError, AttributeError):  # pragma: no cover - non-glibc / no libc
        return False
