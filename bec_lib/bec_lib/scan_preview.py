"""Resolve advertised scan inputs into device-independent position previews."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any

import numpy as np


def generate_scan_positions(
    scan_info: Mapping[str, Any], parameters: Mapping[str, Any]
) -> np.ndarray:
    """Generate a trajectory from an advertised scan definition and bound scan inputs.

    Args:
        scan_info (Mapping[str, Any]): Entry from ``available_scans`` containing
            ``position_preview``, ``position_preview_parameter_mapping`` and
            ``arg_bundle_size``. Preview functions use fully qualified import paths, e.g.
            ``bec_lib.position_generators.line_scan_positions`` or a plugin module function.
            Mapping values are scan parameter names, nested lists of names, or
            ``{"bundle": [indices]}`` for fields of each repeated positional bundle.
            Bundle indices are zero-based and include the motor at index zero.
        parameters (Mapping[str, Any]): Scan inputs keyed by constructor parameter name,
            including scan defaults. Repeated positional inputs use the key ``args``.
            For example, line scans use ``{"args": ["samx", -1, 1], "steps": 5}``.

    Returns:
        np.ndarray: Positions in scan motor order, before relative motor offsets or
            server-side trajectory optimization. For relative scans, the GUI must
            add the current motor positions to obtain absolute positions.

    Raises:
        ValueError: If the scan does not advertise a fully qualified preview or bundles are incomplete.
        TypeError: If the imported preview is not callable.
        KeyError: If a required scan parameter is missing.
    """
    name = scan_info.get("position_preview")
    if not name:
        raise ValueError("This scan does not advertise a position generator")
    module_name, separator, function_name = name.rpartition(".")
    if not separator or not module_name or not function_name:
        raise ValueError("Position preview must specify a fully qualified function import path")
    generator = getattr(import_module(module_name), function_name)
    if not callable(generator):
        raise TypeError(f"Position preview {name!r} is not callable")

    def resolve(source):
        if isinstance(source, str):
            return parameters[source]
        if isinstance(source, list):
            return [resolve(item) for item in source]
        if isinstance(source, dict) and set(source) == {"bundle"}:
            size = scan_info["arg_bundle_size"]["bundle"]
            args = parameters["args"]
            indices = source["bundle"]
            if size <= 0 or len(args) % size or any(i < 0 or i >= size for i in indices):
                raise ValueError("Invalid or incomplete positional scan bundles")
            return [[args[offset + i] for i in indices] for offset in range(0, len(args), size)]
        raise ValueError(f"Unsupported generator parameter mapping: {source!r}")

    kwargs = {
        key: resolve(source)
        for key, source in scan_info["position_preview_parameter_mapping"].items()
    }
    return generator(**kwargs)
