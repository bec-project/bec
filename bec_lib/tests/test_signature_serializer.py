import inspect
from typing import Literal, Union
from unittest import mock

import numpy as np
import pytest

from bec_lib.signature_serializer import (
    deserialize_dtype,
    dict_to_signature,
    serialize_dtype,
    signature_to_dict,
)


def test_signature_serializer():
    def test_func(a, b, c=1, d=2, e: int = 3):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {"name": "a", "kind": "POSITIONAL_OR_KEYWORD", "default": "_empty", "annotation": "_empty"},
        {"name": "b", "kind": "POSITIONAL_OR_KEYWORD", "default": "_empty", "annotation": "_empty"},
        {"name": "c", "kind": "POSITIONAL_OR_KEYWORD", "default": 1, "annotation": "_empty"},
        {"name": "d", "kind": "POSITIONAL_OR_KEYWORD", "default": 2, "annotation": "_empty"},
        {"name": "e", "kind": "POSITIONAL_OR_KEYWORD", "default": 3, "annotation": "int"},
    ]

    sig = dict_to_signature(params)
    assert sig == inspect.signature(test_func)


def test_signature_serializer_with_literals():
    def test_func(
        a, b: Literal[1, 2, 3] = 1, c: None | np.ndarray = None, d: None | np.ndarray | float = None
    ):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {"name": "a", "kind": "POSITIONAL_OR_KEYWORD", "default": "_empty", "annotation": "_empty"},
        {
            "name": "b",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": 1,
            "annotation": {"Literal": (1, 2, 3)},
        },
        {
            "name": "c",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": None,
            "annotation": "NoneType | ndarray",
        },
        {
            "name": "d",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": None,
            "annotation": "NoneType | ndarray | float",
        },
    ]

    sig = dict_to_signature(params)
    assert sig == inspect.signature(test_func)


@pytest.mark.parametrize(
    "dtype_in,dtype_out",
    [
        (int, "int"),
        (str, "str"),
        (float, "float"),
        (bool, "bool"),
        (inspect._empty, "_empty"),
        # (Literal[1, 2, 3], {"Literal": (1, 2, 3)}),
        # (Union[int, str], "int | str"),
    ],
)
def test_serialize_dtype(dtype_in, dtype_out):
    assert dtype_out == serialize_dtype(dtype_in)


@pytest.mark.parametrize(
    "dtype_in,dtype_out",
    [
        ("int", int),
        ("str", str),
        ("float", float),
        ("bool", bool),
        ("_empty", inspect._empty),
        ({"Literal": (1, 2, 3)}, Literal[1, 2, 3]),
        ("int | str", Union[int, str]),
    ],
)
def test_deserialize_dtype(dtype_in, dtype_out):
    assert dtype_out == deserialize_dtype(dtype_in)
