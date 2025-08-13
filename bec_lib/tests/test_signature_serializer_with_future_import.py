from __future__ import annotations

from typing import Literal

from bec_lib.signature_serializer import signature_to_dict


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
