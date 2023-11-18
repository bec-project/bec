import inspect
import typing

from bec_lib.signature_serializer import dict_to_signature, signature_to_dict


def test_signature_serializer():
    def test_func(a, b, c=1, d=2, e: int = 3):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "a",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": "_empty",
        },
        {
            "name": "b",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": "_empty",
        },
        {"name": "c", "kind": "POSITIONAL_OR_KEYWORD", "default": 1, "annotation": "_empty"},
        {"name": "d", "kind": "POSITIONAL_OR_KEYWORD", "default": 2, "annotation": "_empty"},
        {"name": "e", "kind": "POSITIONAL_OR_KEYWORD", "default": 3, "annotation": "int"},
    ]

    sig = dict_to_signature(params)
    assert sig == inspect.signature(test_func)


def test_signature_serializer_with_literals():
    def test_func(a, b: typing.Literal[1, 2, 3] = 1):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "a",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": "_empty",
        },
        {
            "name": "b",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": 1,
            "annotation": {"Literal": (1, 2, 3)},
        },
    ]

    sig = dict_to_signature(params)
    assert sig == inspect.signature(test_func)
