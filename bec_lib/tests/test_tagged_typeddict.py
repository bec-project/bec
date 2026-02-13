from typing import Literal, TypedDict

from pydantic import BaseModel


class C1(TypedDict):
    _discrim: Literal["float", "int"] = "float"
    val: float
    ts: float | None
