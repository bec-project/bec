from __future__ import annotations

from functools import lru_cache
from typing import Callable, Type

from bec_lib import codecs as bec_codecs
from bec_lib.logger import bec_logger

logger = bec_logger.logger


class NoCodec(Exception): ...


class SerializationRegistry:
    """Registry for serialization codecs"""

    def __init__(self):
        self._registry: dict[str, tuple[Type, Callable]] = {}

        self.register_codec(bec_codecs.BECDeviceEncoder)

    def register_codec(self, codec: Type[bec_codecs.BECCodec]):
        """
        Register a codec for a specific BECCodec subclass.
        This method allows for easy registration of custom encoders and decoders
        for BECMessage and other types.

        Args:
            codec: A subclass of BECCodec that implements encode and decode methods.
        Raises:
            ValueError: If a codec for the specified type is already registered.
        """
        if isinstance(codec.obj_type, list):
            for cls in codec.obj_type:
                self.register(cls, codec.encode)
        else:
            self.register(codec.obj_type, codec.encode)

    def register(self, cls: Type, encoder: Callable, *_):  # hacky fix for BW compat
        """Register a codec for a specific type."""

        if cls.__name__ in self._registry:
            raise ValueError(f"Codec for {cls} already registered.")
        self._registry[cls.__name__] = (cls, encoder)
        self.get_codec.cache_clear()  # Clear the cache when a new codec is registered

    @lru_cache(maxsize=2000)
    def get_codec(self, cls: Type) -> tuple[Type, Callable] | None:
        """Get the codec for a specific type."""
        codec = self._registry.get(cls.__name__)
        if codec:
            return codec
        for _, (registered_cls, encoder) in self._registry.items():
            if issubclass(cls, registered_cls):
                return registered_cls, encoder
        return None

    def is_registered(self, cls: Type) -> bool:
        """
        Check if a codec is registered for a specific type.
        Args:
            cls: The class type to check for a registered codec.
        Returns:
            bool: True if a codec is registered for the type, False otherwise.
        """
        return self.get_codec(cls) is not None

    def encode(self, obj):
        """Encode an object using the registered codec."""
        codec = self.get_codec(type(obj))
        if not codec:
            raise NoCodec()  # No codec registered for this type
        try:
            _, encoder = codec
            return encoder(obj)
        except Exception as e:
            raise ValueError(
                f"Serialization failed: Failed to encode {obj.__class__.__name__} with codec {codec}: {e}"
            ) from e
