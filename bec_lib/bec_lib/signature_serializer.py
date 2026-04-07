"""
This module contains functions to serialize and deserialize function signatures.
"""

from __future__ import annotations

import builtins
import inspect
import itertools
import types
from collections.abc import Callable
from typing import Annotated, Generator, Literal, Union, get_args, get_origin, get_type_hints

import numpy as np
from pydantic import ValidationError

from bec_lib.device import DeviceBase
from bec_lib.scan_args import ScanArgument
from bec_lib.scan_items import ScanItem

_special_types = {DeviceBase, ScanItem}


def _serialize_dtype(dtype: object) -> Generator[str | dict, None, None]:
    """
    Yield serialized dtype entries for simple, union, and literal annotations.

    Args:
        dtype (object): Type annotation to serialize.

    Yields:
        str | dict: Serialized dtype entries.
    """
    if dtype is None or dtype is types.NoneType:
        yield "NoneType"
    if (name := getattr(dtype, "__name__", None)) and isinstance(dtype, type):
        if name in builtins.__dict__ or name in np.__dict__ or dtype in _special_types:
            yield name
    if dtype.__class__.__name__ == "_UnionGenericAlias" or dtype.__class__ == types.UnionType:
        yield from itertools.chain.from_iterable(_serialize_union_arg(x) for x in dtype.__args__)  # type: ignore
    if dtype.__class__.__name__ == "_LiteralGenericAlias":
        yield {"Literal": dtype.__args__}  # type: ignore


def _serialize_union_arg(dtype: object) -> list[str | dict]:
    """
    Serialize one member of a union annotation.

    Args:
        dtype (object): Union member annotation to serialize.

    Returns:
        list[str | dict]: Serialized dtype entries for the union member.
    """
    if annotated_dtype := _serialize_annotated_dtype(dtype):
        return [annotated_dtype]
    return list(_merge_literals(_serialize_dtype(dtype)))


def _get_scan_argument(metadata: tuple) -> ScanArgument | None:
    """
    Return ScanArgument metadata from an Annotated metadata tuple, if present.

    Args:
        metadata (tuple): Metadata entries from an Annotated annotation.

    Returns:
        ScanArgument | None: ScanArgument metadata if present, otherwise None.
    """
    for entry in metadata:
        if isinstance(entry, ScanArgument):
            return entry
    return None


def _serialize_annotated_dtype(dtype: object) -> dict | str | list[str | dict] | None:
    """
    Serialize Annotated dtype metadata supported by the scan argument schema.

    Args:
        dtype (object): Type annotation to serialize.

    Returns:
        dict | str | list[str | dict] | None: Serialized annotation, base annotation, or None if
        dtype is not Annotated.
    """
    if get_origin(dtype) is not Annotated:
        return None

    dtype_arg, *metadata = get_args(dtype)
    serialized_dtype = serialize_dtype(dtype_arg)
    scan_argument = _get_scan_argument(tuple(metadata))
    if scan_argument is None:
        return serialized_dtype
    return {
        "Annotated": {
            "type": serialized_dtype,
            "metadata": {"ScanArgument": scan_argument.model_dump()},
        }
    }


def _merge_literals(vals: Generator[str | dict, None, None]) -> Generator[str | dict, None, None]:
    """
    Merge serialized Literal entries while preserving non-literal annotation payloads.

    Args:
        vals (Generator[str | dict, None, None]): Serialized dtype values to merge.

    Yields:
        str | dict: Serialized dtype values with Literal entries merged.
    """
    _literal_args = []
    for val in vals:
        if val == "NoneType":
            _literal_args.append(None)
            continue
        if not isinstance(val, dict):
            yield val
        elif "Literal" not in val:
            yield val
        else:
            _literal_args.extend(val["Literal"])
    if _literal_args == [None]:
        yield "NoneType"
    elif _literal_args:
        yield {"Literal": tuple(_literal_args)}


def serialize_dtype(dtype: object) -> list[str | dict] | str | dict:
    """
    Serialize a Python type annotation.

    Args:
        dtype (object): Type annotation to serialize.

    Returns:
        list[str | dict] | str | dict: Serialized dtype payload.
    """
    if annotated_dtype := _serialize_annotated_dtype(dtype):
        return annotated_dtype
    type_list = list(_merge_literals(_serialize_dtype(dtype)))
    return (type_list[0] if len(type_list) == 1 else type_list) or "_empty"


def _deser_simple_type(dtype: str) -> type | None:
    """
    Deserialize a simple named type, if it is supported.

    Args:
        dtype (str): Serialized type name.

    Returns:
        type | None: Deserialized type if supported, otherwise None.
    """
    if hasattr(builtins, dtype):
        return getattr(builtins, dtype)
    if hasattr(np, dtype):
        return getattr(np, dtype)
    if dtype == "DeviceBase":
        return DeviceBase
    if dtype == "ScanItem":
        return ScanItem


def _deserialize_annotated_dtype(annotated_dtype: object) -> object:
    """
    Deserialize an Annotated payload if it contains valid ScanArgument metadata.

    Args:
        annotated_dtype (object): Serialized Annotated payload.

    Returns:
        object: Annotated type for valid ScanArgument metadata, otherwise the base type.
    """
    if not isinstance(annotated_dtype, dict):
        # pylint: disable=protected-access
        return inspect._empty

    base_dtype = deserialize_dtype(annotated_dtype.get("type", "_empty"))
    metadata = annotated_dtype.get("metadata", {})
    if not isinstance(metadata, dict):
        return base_dtype

    scan_argument_data = metadata.get("ScanArgument")
    if scan_argument_data is None:
        return base_dtype

    try:
        scan_argument = ScanArgument.model_validate(scan_argument_data)
    except ValidationError:
        return base_dtype
    return Annotated[base_dtype, scan_argument]


def deserialize_dtype(dtype: list | dict | str) -> object:
    """
    Convert a serialized dtype to a type.

    Args:
        dtype (str): String representation of the data type

    Returns:
        type: Data type
    """
    if isinstance(dtype, list):
        return Union[*(deserialize_dtype(t) for t in dtype)]
    if isinstance(dtype, dict):
        if "Annotated" in dtype:
            annotated_dtype = dtype.get("Annotated")
            return _deserialize_annotated_dtype(annotated_dtype)
        return Literal[*dtype["Literal"]]
    if dtype == "_empty":
        # pylint: disable=protected-access
        return inspect._empty
    if dtype == "NoneType":
        return None
    if simple_dtype := _deser_simple_type(dtype):
        return simple_dtype


def signature_to_dict(func: Callable, include_class_obj=False) -> list[dict]:
    """
    Convert a function signature to a dictionary.
    The dictionary can be used to reconstruct the signature using dict_to_signature.

    Args:
        func (Callable): Function to be converted

    Returns:
        list[dict]: List of dictionaries representing the function signature
    """
    out = []
    params = inspect.signature(func).parameters
    try:
        type_hints = get_type_hints(func, include_extras=True)
    except NameError as e:
        raise TypeError(
            f"Couldn't find annotated type {e.name}. The type you annotate with must be available in the local scope! Check it is not hidden by TYPE_CHECKING."
        ) from e
    for param_name, param in params.items():
        if not include_class_obj and param_name == "self" or param_name == "cls":
            continue
        # pylint: disable=protected-access
        param_typehint = type_hints.get(param_name)
        out.append(
            {
                "name": param_name,
                "kind": param.kind.name,
                "default": param.default if param.default != inspect._empty else "_empty",
                "annotation": serialize_dtype(param_typehint) if param_typehint else "_empty",
            }
        )
    return out


def dict_to_signature(params: list[dict]) -> inspect.Signature:
    """
    Convert a dictionary representation of a function signature to a signature object.

    Args:
        params (list[dict]): List of dictionaries representing the function signature

    Returns:
        inspect.Signature: Signature object
    """
    out = []
    for param in params:
        # pylint: disable=protected-access
        out.append(
            inspect.Parameter(
                name=param["name"],
                kind=getattr(inspect.Parameter, param["kind"]),
                default=param["default"] if param["default"] != "_empty" else inspect._empty,
                annotation=deserialize_dtype(param["annotation"]),
            )
        )
    return inspect.Signature(out)
