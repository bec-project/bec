"""Catalog from BEC's advertised metadata, using its existing input validator."""

from __future__ import annotations

import hashlib
import inspect
import json
from typing import TYPE_CHECKING

from bec_lib.scan_input_validator import ScanInputValidator
from bec_lib.scans import ScanObject

if TYPE_CHECKING:
    from bec_lib.client import BECClient
    from bec_lib.devicemanager import DeviceManagerBase


def canonical(value: object) -> str:
    """Stable JSON encoding for advertised metadata fingerprints.

    Args:
        value (object): Value to validate, resolve or encode.

    Returns:
        str: Deterministic JSON representation of the metadata.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(info: dict) -> str:
    """Fingerprint advertised metadata; this cannot fingerprint unadvertised code.

    Args:
        info (dict): Advertised scan metadata.

    Returns:
        str: SHA-256 fingerprint of advertised metadata.
    """
    return hashlib.sha256(canonical(info).encode()).hexdigest()


# pylint: disable=protected-access
_METADATA_ARGUMENT = inspect.signature(ScanObject._run).parameters["metadata"]


class ScanCatalog:
    """Expose advertised signatures, defaults, docs and validation metadata as-is.

    Args:
        entries (dict[str, dict]): Advertised scan metadata indexed by scan name.
        device_manager (DeviceManagerBase | None): Optional live proxy manager for resolving
            device names.
    """

    def __init__(
        self, entries: dict[str, dict], device_manager: DeviceManagerBase | None = None
    ) -> None:
        """Initialize ScanCatalog parameters.

        Args:
            entries (dict[str, dict]): Advertised scan metadata indexed by scan name.
            device_manager (DeviceManagerBase | None): Optional live proxy manager for resolving
                device names.
        """
        self._entries = {
            name: info for name, info in entries.items() if not info.get("is_internal")
        }
        self.device_manager = device_manager

    @classmethod
    def from_client(cls, client: BECClient) -> ScanCatalog:
        """Read the client's already subscribed scan info.

        Args:
            client (BECClient): Existing BEC client providing scan info and device proxies.

        Returns:
            ScanCatalog: Catalog using the client's subscribed scan metadata and proxy manager.
        """
        # Scans exposes scan-info only through its subscribed registry.
        # pylint: disable=protected-access
        return cls(
            {name: obj.scan_info for name, obj in client.scans._available_scans.items()},
            client.device_manager,
        )

    @property
    def entries(self) -> dict[str, dict]:
        """Return the catalog metadata.

        Returns:
            dict[str, dict]: Advertised metadata indexed by public scan name.
        """
        return self._entries

    def info(self, name: str) -> dict:
        """Look up one public scan.

        Args:
            name (str): Device, signal or scan name to look up.

        Returns:
            dict: The supplied metadata for the requested public scan.
        """
        if name not in self._entries:
            raise ValueError(f"Unknown scan: {name}")
        return self._entries[name]

    def entry(self, name: str) -> dict:
        """Expose editor metadata and its deterministic definition fingerprint.

        Args:
            name (str): Device, signal or scan name to look up.

        Returns:
            dict: Scan name, advertised metadata, frontend inputs and fingerprint.
        """
        info = self.info(name)
        # ScanObject consumes metadata before passing kwargs to scan-specific validation.
        return {
            "name": name,
            "metadata": info,
            "fingerprint": fingerprint(info),
            "client_arguments": {
                "metadata": {
                    "annotation": str(_METADATA_ARGUMENT.annotation),
                    "default": _METADATA_ARGUMENT.default,
                }
            },
        }

    def validate(self, name: str, positional: list, arguments: dict) -> None:
        """Validate with BEC metadata; leave defaults to the scan implementation.

        Args:
            name (str): Device, signal or scan name to look up.
            positional (list): Resolved positional scan arguments.
            arguments (dict): Resolved keyword arguments.
        """
        if not isinstance(arguments, dict):
            raise ValueError("Scan arguments must resolve to a mapping")
        info = self.info(name)
        scan_arguments = arguments.copy()
        metadata = scan_arguments.pop("metadata", None)
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError(f"Invalid client metadata for scan {name}: expected a mapping or None")
        # Offline catalogs can validate device-name types without creating device objects.
        # Live validation additionally resolves names through the device manager.
        if self.device_manager is None:

            def names(value):
                """Replace device annotations with name types for offline validation.

                Args:
                    value (object): Value to validate, resolve or encode.

                Returns:
                    object: Metadata with device annotations represented as name types.
                """
                if value == "DeviceBase":
                    return "str"
                if isinstance(value, list):
                    return [names(v) for v in value]
                if isinstance(value, dict):
                    return {k: names(v) for k, v in value.items()}
                return value

            info = names(info)
        ScanInputValidator(self.device_manager).validate(name, info, positional, scan_arguments)
