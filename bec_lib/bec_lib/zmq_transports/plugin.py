"""Load receiver plugins for ZMQ signal transports.

Custom receivers are classes in an installed BEC plugin package. For example,
a descriptor can advertise ``encoder_plugin="beamline_plugin.receivers.CustomStream"``
when ``beamline_plugin`` is installed and declares a ``bec`` entry point.
"""

from __future__ import annotations

from functools import cache, lru_cache
from importlib import import_module
from importlib.metadata import entry_points

from bec_lib.messages import ZMQSignalTransport

from .adzmq import ADZMQ_ENCODERS, ADZMQStream
from .protocol import SignalStream


def load_encoder_plugin(transport: ZMQSignalTransport) -> type[SignalStream]:
    """Resolve a transport descriptor's encoder plugin to a receiver class.

    Args:
        transport (ZMQSignalTransport): Descriptor containing the encoder name.
            ADZMQ uses the built-in receiver. Other encoders must advertise a
            receiver class from a locally installed BEC plugin package.

    Returns:
        type[SignalStream]: Receiver class to instantiate before calling ``start``.

    Raises:
        ValueError: If a custom plugin path is missing, invalid, or outside
            all installed BEC plugin packages.
        ImportError: If the module cannot be imported.
        AttributeError: If the named class is missing from the module.
        TypeError: If the named object is not a receiver class.
    """
    if transport.encoder in ADZMQ_ENCODERS:
        return ADZMQStream
    path = transport.encoder_plugin
    if path is None:
        raise ValueError(f"Encoder {transport.encoder!r} requires an encoder_plugin")

    if not any(path.startswith(f"{package}.") for package in _bec_plugin_package_names()):
        raise ValueError(f"Encoder plugin {path!r} must be in an installed BEC plugin package")
    return _load_encoder_plugin(path)


@cache
def _bec_plugin_package_names() -> frozenset[str]:
    """Discover installed BEC plugin packages once per process."""
    return frozenset(entry.module for entry in entry_points(group="bec"))


@lru_cache(maxsize=20)
def _load_encoder_plugin(path: str) -> type[SignalStream]:
    module_name, separator, class_name = path.rpartition(".")
    if not separator or not all(part.isidentifier() for part in path.split(".")):
        raise ValueError(f"Expected a fully qualified encoder plugin path, got {path!r}")

    plugin = getattr(import_module(module_name), class_name)
    if (
        not isinstance(plugin, type)
        or plugin is SignalStream
        or not issubclass(plugin, SignalStream)
    ):
        raise TypeError(f"Encoder plugin {path!r} must be a class implementing SignalStream")
    return plugin
