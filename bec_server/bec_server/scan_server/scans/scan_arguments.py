"""Argument serialization helpers shared by direct scans and the GUI schema."""

from __future__ import annotations

import enum
from typing import Any


class ScanArgType(str, enum.Enum):
    """Serialized argument labels retained for GUI and plugin signatures."""

    DEVICE = "device"
    FLOAT = "float"
    INT = "int"
    BOOL = "boolean"
    STR = "str"
    LIST = "list"
    DICT = "dict"


def unpack_scan_args(scan_args: dict[str, Any]) -> list:
    """Unpack named argument bundles into the flat argument list.

    Args:
        scan_args (dict[str, Any]): scan arguments

    Returns:
        list: list of arguments
    """
    args = []
    if not scan_args:
        return args
    if not isinstance(scan_args, dict):
        return scan_args
    for cmd_name, cmd_args in scan_args.items():
        args.append(cmd_name)
        args.extend(cmd_args)
    return args
