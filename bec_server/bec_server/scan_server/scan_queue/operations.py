"""Marshal public queue operations onto their owning coordinator."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any


def coordinated(fcn: Callable[..., Any]) -> Callable[..., Any]:
    """Run an entire queue operation on its sole owning thread.

    Args:
        fcn (Callable[..., Any]): Queue operation to wrap.

    Returns:
        Callable[..., Any]: Wrapper that executes the operation on its queue coordinator.
    """

    @functools.wraps(fcn)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if hasattr(self, "_coordinator"):
            manager = self
        elif hasattr(self, "queue_manager"):
            manager = self.queue_manager
        else:
            manager = self.parent.queue_manager
        return manager._coordinator.call(fcn, self, *args, **kwargs)

    return wrapper
