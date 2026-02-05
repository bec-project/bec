from typing import Literal

ReadoutPriorities = dict[
    Literal["monitored", "baseline", "async", "continuous", "on_request"], list[str]
]
