"""Marshal manager commands onto their owning coordinator."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any


def coordinated(fcn: Callable[..., Any]) -> Callable[..., Any]:
    """Run a manager command on its sole owning thread.

    Args:
        fcn (Callable[..., Any]): Queue operation to wrap.

    Returns:
        Callable[..., Any]: Wrapper that executes the operation on its queue coordinator.
    """

    @functools.wraps(fcn)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        return self._coordinator.call(fcn, self, *args, **kwargs)

    return wrapper
