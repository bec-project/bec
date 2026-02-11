import inspect
from typing import Annotated, Literal, Optional, Union

import numpy as np
import pytest
from pydantic import ValidationError

from bec_lib.device import DeviceBase
from bec_lib.scan_args import ScanArgument, Units
from bec_lib.scan_items import ScanItem
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


def test_signature_serializer_merged_literals():
    def test_func(a: Literal[1, 2, 3] | None = None):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "a",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": None,
            "annotation": {"Literal": [1, 2, 3, None]},
        }
    ]


def test_signature_serializer_with_unpack():
    def test_func(a, b: Literal["test", None], *args, **kwargs):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {"name": "a", "kind": "POSITIONAL_OR_KEYWORD", "default": "_empty", "annotation": "_empty"},
        {
            "name": "b",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": {"Literal": ["test", None]},
        },
        {"name": "args", "kind": "VAR_POSITIONAL", "default": "_empty", "annotation": "_empty"},
        {"name": "kwargs", "kind": "VAR_KEYWORD", "default": "_empty", "annotation": "_empty"},
    ]


def test_signature_serializer_with_literals():
    def test_func(
        a,
        b: Literal["test", None],
        c: Literal[1, 2, 3] = 1,
        d: None | np.ndarray = None,
        e: None | np.ndarray | float = None,
    ):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {"name": "a", "kind": "POSITIONAL_OR_KEYWORD", "default": "_empty", "annotation": "_empty"},
        {
            "name": "b",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": {"Literal": ["test", None]},
        },
        {
            "name": "c",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": 1,
            "annotation": {"Literal": [1, 2, 3]},
        },
        {
            "name": "d",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": None,
            "annotation": ["ndarray", "NoneType"],
        },
        {
            "name": "e",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": None,
            "annotation": ["ndarray", "float", "NoneType"],
        },
    ]

    sig = dict_to_signature(params)
    assert sig == inspect.signature(test_func)


def test_signature_serializer_with_scan_argument_annotation():
    scan_argument = ScanArgument(
        description="Step size", tooltip="Motor step size", expert=True, units=Units.mm
    )

    def test_func(step: Annotated[float, scan_argument]):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "step",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": "_empty",
            "annotation": {
                "Annotated": {
                    "type": "float",
                    "metadata": {
                        "ScanArgument": {
                            "description": "Step size",
                            "display_name": None,
                            "tooltip": "Motor step size",
                            "expert": True,
                            "alternative_group": None,
                            "units": "mm",
                            "reference_units": None,
                            "gt": None,
                            "ge": None,
                            "lt": None,
                            "le": None,
                            "reference_limits": None,
                        }
                    },
                }
            },
        }
    ]

    sig = dict_to_signature(params)
    assert sig == inspect.signature(test_func)


def test_scan_argument_accepts_units():
    scan_argument = ScanArgument(units=Units.mm)

    assert scan_argument.units == "mm"


def test_scan_argument_accepts_compound_units():
    scan_argument = ScanArgument(units=Units.mm / Units.s)

    assert scan_argument.units == "mm/s"


def test_scan_argument_accepts_quantity_units():
    scan_argument = ScanArgument(units=1 * Units.mm / Units.s)

    assert scan_argument.units == "mm/s"


def test_scan_argument_accepts_reference_units():
    scan_argument = ScanArgument(reference_units="motor")

    assert scan_argument.reference_units == "motor"


def test_scan_argument_accepts_alternative_group():
    scan_argument = ScanArgument(alternative_group="scan_resolution")

    assert scan_argument.alternative_group == "scan_resolution"


def test_scan_argument_accepts_display_name_and_limits():
    scan_argument = ScanArgument(
        display_name="Step Size", gt=0, ge=0.1, lt=10, le=9.9, reference_limits="motor"
    )

    assert scan_argument.display_name == "Step Size"
    assert scan_argument.gt == 0
    assert scan_argument.ge == 0.1
    assert scan_argument.lt == 10
    assert scan_argument.le == 9.9
    assert scan_argument.reference_limits == "motor"


def test_scan_argument_unit_and_reference_units_are_exclusive():
    with pytest.raises(ValidationError, match="units and reference_units are mutually exclusive"):
        ScanArgument(units=Units.mm, reference_units="motor")


def test_scan_argument_reference_units_rejects_units():
    with pytest.raises(ValidationError):
        ScanArgument(reference_units=Units.T / Units.min)


def test_signature_serializer_ignores_unknown_annotated_metadata():
    def test_func(a: Annotated[float, "unknown metadata"]):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {"name": "a", "kind": "POSITIONAL_OR_KEYWORD", "default": "_empty", "annotation": "float"}
    ]


def test_signature_serializer_with_optional_scan_argument_annotation():
    scan_argument = ScanArgument(description="Optional step size")

    def test_func(step: Annotated[float, scan_argument] | None = None):
        pass

    params = signature_to_dict(test_func)
    assert params == [
        {
            "name": "step",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": None,
            "annotation": [
                {
                    "Annotated": {
                        "type": "float",
                        "metadata": {
                            "ScanArgument": {
                                "description": "Optional step size",
                                "display_name": None,
                                "tooltip": None,
                                "expert": False,
                                "alternative_group": None,
                                "units": None,
                                "reference_units": None,
                                "gt": None,
                                "ge": None,
                                "lt": None,
                                "le": None,
                                "reference_limits": None,
                            }
                        },
                    }
                },
                "NoneType",
            ],
        }
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
        (Literal[1, 2, 3], {"Literal": [1, 2, 3]}),
        (Union[int, str], ["int", "str"]),
        (Optional[str], ["str", "NoneType"]),
        (DeviceBase, "DeviceBase"),
        (ScanItem, "ScanItem"),
        (np.ndarray, "ndarray"),
        (
            Annotated[
                float, ScanArgument(description="Step size", alternative_group="scan_resolution")
            ],
            {
                "Annotated": {
                    "type": "float",
                    "metadata": {
                        "ScanArgument": {
                            "description": "Step size",
                            "display_name": None,
                            "tooltip": None,
                            "expert": False,
                            "alternative_group": "scan_resolution",
                            "units": None,
                            "reference_units": None,
                            "gt": None,
                            "ge": None,
                            "lt": None,
                            "le": None,
                            "reference_limits": None,
                        }
                    },
                }
            },
        ),
        (
            Annotated[float, ScanArgument(units=Units.mm / Units.s)],
            {
                "Annotated": {
                    "type": "float",
                    "metadata": {
                        "ScanArgument": {
                            "description": None,
                            "display_name": None,
                            "tooltip": None,
                            "expert": False,
                            "alternative_group": None,
                            "units": "mm/s",
                            "reference_units": None,
                            "gt": None,
                            "ge": None,
                            "lt": None,
                            "le": None,
                            "reference_limits": None,
                        }
                    },
                }
            },
        ),
        (Annotated[float, "unknown metadata"], "float"),
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
        ({"Literal": [1, 2, 3]}, Literal[1, 2, 3]),
        (["int", "str"], Union[int, str]),
        (["str", "NoneType"], Optional[str]),
        ("NoneType", None),
        ("DeviceBase", DeviceBase),
        ("ScanItem", ScanItem),
        ("ndarray", np.ndarray),
        (
            {
                "Annotated": {
                    "type": "float",
                    "metadata": {
                        "ScanArgument": {
                            "description": "Step size",
                            "display_name": None,
                            "tooltip": None,
                            "expert": False,
                            "alternative_group": "scan_resolution",
                            "units": None,
                            "reference_units": None,
                            "gt": None,
                            "ge": None,
                            "lt": None,
                            "le": None,
                            "reference_limits": None,
                        }
                    },
                }
            },
            Annotated[
                float, ScanArgument(description="Step size", alternative_group="scan_resolution")
            ],
        ),
        ({"Annotated": {"type": "float", "metadata": {"Other": {}}}}, float),
        ({"Annotated": {"type": "float", "metadata": {}}}, float),
        ({"Annotated": {"type": "float"}}, float),
        (
            {
                "Annotated": {
                    "type": "float",
                    "metadata": {"ScanArgument": {"units": "mm", "reference_units": "motor"}},
                }
            },
            float,
        ),
    ],
)
def test_deserialize_dtype(dtype_in, dtype_out):
    assert dtype_out == deserialize_dtype(dtype_in)
