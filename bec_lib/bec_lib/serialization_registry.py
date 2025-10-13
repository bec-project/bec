from __future__ import annotations

from functools import lru_cache
from typing import Callable, Type

from bec_lib import codecs as bec_codecs
from bec_lib.logger import bec_logger

logger = bec_logger.logger


class SerializationRegistry:
    """Registry for serialization codecs"""

    use_json = False

    def __init__(self):
        self._registry: dict[str, tuple[Type, Callable, Callable]] = {}
        self._legacy_codecs = []  # can be removed in future versions, see issue #516

        self.register_codec(bec_codecs.BECMessageEncoder)
        self.register_codec(bec_codecs.BECDeviceEncoder)
        self.register_codec(bec_codecs.EndpointInfoEncoder)
        self.register_codec(bec_codecs.SetEncoder)
        self.register_codec(bec_codecs.BECTypeEncoder)
        self.register_codec(bec_codecs.PydanticEncoder)
        self.register_codec(bec_codecs.EnumEncoder)

        if self.use_json:
            self.register_codec(bec_codecs.NumpyEncoderList)
        else:
            self.register_codec(bec_codecs.NumpyEncoder)

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
                self.register(cls, codec.encode, codec.decode)
        else:
            self.register(codec.obj_type, codec.encode, codec.decode)

    def register(self, cls: Type, encoder: Callable, decoder: Callable):
        """Register a codec for a specific type."""

        if cls.__name__ in self._registry:
            raise ValueError(f"Codec for {cls} already registered.")
        self._registry[cls.__name__] = (cls, encoder, decoder)
        self.get_codec.cache_clear()  # Clear the cache when a new codec is registered

    @lru_cache(maxsize=2000)
    def get_codec(self, cls: Type) -> tuple[Type, Callable, Callable] | None:
        """Get the codec for a specific type."""
        codec = self._registry.get(cls.__name__)
        if codec:
            return codec
        for _, (registered_cls, encoder, decoder) in self._registry.items():
            if issubclass(cls, registered_cls):
                return registered_cls, encoder, decoder
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
            return obj  # No codec registered for this type
        cls, encoder, _ = codec
        try:
            return {
                "__bec_codec__": {
                    "encoder_name": cls.__name__,
                    "type_name": obj.__class__.__name__,
                    "data": encoder(obj),
                }
            }
        except Exception as e:
            raise ValueError(
                f"Serialization failed: Failed to encode {obj.__class__.__name__} with codec {encoder}: {e}"
            ) from e

    def decode(self, data):
        """Decode an object using the registered codec."""
        if not isinstance(data, dict) or "__bec_codec__" not in data:
            return data
        codec_info = data["__bec_codec__"]
        codec_type = codec_info.pop("encoder_name")
        if not codec_type or codec_type not in self._registry:
            return data
        _, _, decoder = self._registry[codec_type]
        try:
            return decoder(**codec_info)
        except Exception as e:
            raise ValueError(
                f"Deserialization failed: Failed to decode {codec_type} with codec {decoder}: {e}"
            ) from e
