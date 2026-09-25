from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from jinja2.ext import Extension

if TYPE_CHECKING:  # pragma: no cover
    from copier import YieldEnvironment


_T = TypeVar("_T")


def _snake_to_pascal(value: str) -> str:
    return "".join(map(str.capitalize, value.split("_")))


def _snake_to_camel(value: str) -> str:
    res = _snake_to_pascal(value)
    if len(res) >= 1:
        res = res[0].lower() + res[1:]
    return res


def _debug(value: _T) -> _T:
    print(value)
    return value


class CopierFilters(Extension):
    identifier = "BEC copier jinja filters"

    def __init__(self, env: YieldEnvironment) -> None:
        self._env = env

        self._env.filters["snake_to_pascal"] = _snake_to_pascal
        self._env.filters["snake_to_camel"] = _snake_to_camel
        self._env.filters["debug"] = _debug
