from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Type

from bec_lib.device import DeviceBase


class BECCodec(ABC):
    """Abstract base class for custom encoders"""

    obj_type: Type | list[Type]

    @staticmethod
    @abstractmethod
    def encode(obj: Any) -> Any:
        """Encode an object into a serializable format."""


class BECDeviceEncoder(BECCodec):
    obj_type = DeviceBase

    @staticmethod
    def encode(obj: DeviceBase) -> str:
        if hasattr(obj, "_compile_function_path"):
            # pylint: disable=protected-access
            return obj._compile_function_path()
        return obj.name
