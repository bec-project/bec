from __future__ import annotations

import inspect
from typing import Literal, Union

from bec_lib.signature_serializer import serialize_dtype, signature_to_dict

from ._additional_for_signature_serializer_test import EnumTest
from ._additional_for_signature_serializer_test import func as literal_union_test_func


def test_signature_serializer_merged_literals():
    def test_func(a: Literal[1, 2, 3] | None = None):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "a",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": None,
            "annotation": {"Literal": (1, 2, 3, None)},
        }
    ]


def test_signature_serializer_merged_literals_different_types():
    def test_func(a: Literal[1, 2, 3] | None | Literal["a", "b", "c"]):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "a",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": {"Literal": (1, 2, 3, None, "a", "b", "c")},
        }
    ]


class SomeUnknownType: ...


def test_signature_serializer_merged_literals_different_types_with_forwardref():
    def test_func(a: Literal[1, 2, 3] | "SomeUnknownType" | Literal["a", "b", "c"]):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "a",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": {"Literal": (1, 2, 3, "a", "b", "c")},
        }
    ]


def test_serialize_dtype_imported_imported_func_arg():
    sig = inspect.signature(literal_union_test_func)
    anno = sig.parameters["a"].annotation
    assert serialize_dtype(anno) == serialize_dtype(Union[Literal["a", "b", "c"], EnumTest])
    assert serialize_dtype(anno) == {"Literal": ("a", "b", "c")}


def test_signature_serializer_parses_untion_on_imported_func():
    params = signature_to_dict(literal_union_test_func)
    assert params == [
        {
            "name": "a",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": {"Literal": ("a", "b", "c")},
        }
    ]


def test_signature_serializer_only_custom_type():
    def test_func(a: SomeUnknownType):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {"name": "a", "kind": "POSITIONAL_OR_KEYWORD", "default": "_empty", "annotation": "_empty"}
    ]
