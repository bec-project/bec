"""
Plugin contract of the DataAPI.

A plugin claims a whole scan (``resolve`` returns specs) or declines it
(``resolve`` returns ``None``); the facade routes each subscription to the
first claiming plugin in ascending ``priority`` order. Within a claimed scan,
individual sources may still be unavailable (``SourceSpec.available=False``) —
the facade exposes them via ``Subscription.unbound_sources``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

from .alignment import Bundle
from .models import SourceKey, SourceKind, UpdateReason


@dataclass
class SourceSpec:
    """Resolved description of one requested source within one scan."""

    device: str
    entry: str
    kind: SourceKind | None = None
    acquisition_group: str | None = None
    storage_name: str | None = None
    available: bool = True

    @property
    def key(self) -> SourceKey:
        """Return the (device, entry) key of this spec."""
        return (self.device, self.entry)


@dataclass
class SourceRequest:
    """
    One active binding of a subscription to a plugin.

    The plugin inserts points into ``bundle`` and calls ``notify(reason)``;
    the facade owns emission (rate limiting, callback invocation). ``state``
    is plugin-private storage for this request.
    """

    scan_id: str
    specs: list[SourceSpec]
    bundle: Bundle
    notify: Callable[[UpdateReason], None]
    state: dict[str, Any] = field(default_factory=dict)


class DataSourcePlugin(ABC):
    """Base class for DataAPI source plugins."""

    #: Routing order; lower numbers are consulted first.
    priority: int = 100

    def connect(self) -> None:
        """Attach plugin-wide feeds (called once at registration)."""

    def disconnect(self) -> None:
        """Detach all feeds and drop all state."""

    @abstractmethod
    def resolve(self, sources: list[SourceKey], scan_id: str) -> list[SourceSpec] | None:
        """
        Claim or decline a scan for the given sources.

        Args:
            sources (list[SourceKey]): Requested (device, entry) pairs.
            scan_id (str): Identifier of the scan.

        Returns:
            list[SourceSpec] | None: One spec per requested source when this
                plugin serves the scan (individual specs may be marked
                unavailable), or ``None`` when the plugin does not serve it.
        """

    @abstractmethod
    def open(self, request: SourceRequest) -> None:
        """Start feeding the request (backfill + live wiring as applicable)."""

    @abstractmethod
    def close(self, request: SourceRequest) -> None:
        """Stop feeding the request and release per-request resources."""
