from enum import Enum
from typing import Literal


class EnumTest(int, Enum):
    a = 1
    b = 2


def func(a: Literal["a", "b", "c"] | EnumTest):
    return a
