from __future__ import annotations

import re
from contextvars import ContextVar
from typing import Any, Literal, Optional, Type

from pydantic import BaseModel, Field, field_validator
from pydantic_core import PydanticCustomError

from bec_lib.signature_serializer import signature_to_dict
from bec_server.scan_server.scans import ScanBase

context_signature = ContextVar("context_signature")
context_docstring = ContextVar("context_docstring")


class GUIInput(BaseModel):
    """
    InputConfig is a data model for the input configuration of a scan.

    Args:
        name (str): name of the input, has to be same as in the function signature.
    """

    arg: bool = Field(False)
    name: str = Field(None, validate_default=True)
    type: Optional[
        Literal["DeviceBase", "device", "float", "int", "bool", "str", "list", "dict"]
    ] = Field(None, validate_default=True)
    display_name: Optional[str] = Field(None, validate_default=True)
    tooltip: Optional[str] = Field(None, validate_default=True)
    default: Optional[Any] = Field(None, validate_default=True)
    expert: Optional[bool] = Field(False)  # TODO decide later how to implement

    @field_validator("name")
    @classmethod
    def validate_name(cls, v, values):
        # args cannot be validated with the current implementation of signature of scans
        if values.data["arg"]:
            return v
        signature = context_signature.get()
        available_args = [entry["name"] for entry in signature]
        if v not in available_args:
            raise PydanticCustomError(
                "wrong argument name",
                "The argument name is not available in the function signature",
                {"wrong_value": v},
            )
        return v

    @field_validator("type")
    @classmethod
    def validate_field(cls, v, values):
        # args cannot be validated with the current implementation of signature of scans
        if values.data["arg"]:
            return v
        signature = context_signature.get()
        if v is None:
            name = values.data.get("name", None)
            if name is None:
                raise PydanticCustomError(
                    "missing argument name",
                    "The argument name is required to infer the type",
                    {"wrong_value": v},
                )
            for entry in signature:
                if entry["name"] == name:
                    v = entry["annotation"]
                    return v

    @field_validator("tooltip")
    @classmethod
    def validate_tooltip(cls, v, values):
        # args cannot be validated with the current implementation of signature of scans
        if values.data["arg"]:
            return v
        if v is not None:
            return v

        docstring = context_docstring.get()
        name = values.data.get("name", None)
        if name is None:
            raise PydanticCustomError(
                "missing argument name",
                "The argument name is required to infer the tooltip",
                {"wrong_value": v},
            )

        try:
            args_part = docstring.split("Args:")[1].split("Returns:")[0]
        except IndexError:
            return None

        pattern = re.compile(r"\s*" + re.escape(name) + r" \(([^)]+)\): (.+?)(?:\.|\n|$)")
        match = pattern.search(args_part)

        if match:
            description = match.group(2)
            first_sentence = description.split(".")[0].strip()
            if first_sentence:
                v = first_sentence[0].upper() + first_sentence[1:]
                return v
        return None

    @field_validator("display_name")
    @classmethod
    def validate_display_name(cls, v, values):
        if v is not None:
            return v
        name = values.data.get("name", None)
        if name is None:
            raise PydanticCustomError(
                "missing argument name",
                "The argument name is required to infer the display name",
                {"wrong_value": v},
            )
        parts = re.split(r"(_|\d+)", name)
        formatted_parts = []
        for part in parts:
            if part.isdigit():
                formatted_parts.append("" + part)
            elif part.isalpha():
                formatted_parts.append(part.capitalize())
        v = " ".join(formatted_parts).strip()
        return v

    @field_validator("default")
    @classmethod
    def validate_default(cls, v, values):
        # args cannot be validated with the current implementation of signature of scans
        if values.data["arg"]:
            return v
        if v is not None:
            return v
        signature = context_signature.get()
        name = values.data.get("name", None)
        if name is None:
            raise PydanticCustomError(
                "missing argument name",
                "The argument name is required to infer the type",
                {"wrong_value": v},
            )
        for entry in signature:
            if entry["name"] == name:
                v = entry["default"]
                return v


class GUIGroup(BaseModel):
    """
    GUIGroup is a data model for the GUI group configuration of a scan.
    """

    name: str
    inputs: list[GUIInput]


class GUIArgGroup(BaseModel):
    """
    GUIArgGroup is a data model for the GUI group configuration of a scan.
    """

    name: str = "Scan Arguments"
    bundle: int = Field(None)
    arg_inputs: dict
    inputs: Optional[list[GUIInput]] = Field(None, validate_default=True)
    min: Optional[int] = Field(None)
    max: Optional[int] = Field(None)

    @field_validator("inputs")
    @classmethod
    def validate_inputs(cls, v, values):
        if v is not None:
            return v
        arg_inputs = values.data["arg_inputs"]
        arg_inputs_str = {key: value.value for key, value in arg_inputs.items()}
        v = []
        for name, type_ in arg_inputs_str.items():
            v.append(GUIInput(name=name, type=type_, arg=True))
        return v


class GUIConfig(BaseModel):
    """
    GUIConfig is a data model for the GUI configuration of a scan.
    """

    scan_class_name: str
    arg_group: Optional[GUIArgGroup] = Field(None)
    kwarg_groups: list[GUIGroup] = Field(None)
    signature: list[dict] = Field(..., exclude=True)
    docstring: str = Field(..., exclude=True)

    @classmethod
    def from_dict(cls, scan_cls: Type[ScanBase]) -> GUIConfig:
        """
        Create a GUIConfig object from a scan class.

        Args:
            scan_cls(Type[ScanBase]): scan class

        Returns:
            GUIConfig: GUIConfig object
        """

        groups = []
        config = scan_cls.gui_config
        signature = signature_to_dict(scan_cls.__init__)
        signature_token = context_signature.set(signature)
        docstring = scan_cls.__doc__ or scan_cls.__init__.__doc__
        docstring_token = context_docstring.set(docstring)
        # kwargs from gui config
        for group_name, input_names in config.items():
            inputs = [GUIInput(name=name, arg=False) for name in input_names]
            group = GUIGroup(name=group_name, inputs=inputs)
            groups.append(group)
        # args from arg_input if available
        arg_group = None
        if hasattr(scan_cls, "arg_input"):
            arg_input = scan_cls.arg_input
            if hasattr(scan_cls, "arg_bundle_size"):
                arg_group = GUIArgGroup(
                    bundle=scan_cls.arg_bundle_size.get("bundle"),
                    min=scan_cls.arg_bundle_size.get("min"),
                    max=scan_cls.arg_bundle_size.get("max"),
                    arg_inputs=arg_input,
                )
            else:
                arg_group = GUIArgGroup(inputs=scan_cls.arg_input)

        context_signature.reset(signature_token)
        context_docstring.reset(docstring_token)
        return cls(
            scan_class_name=scan_cls.__name__,
            signature=signature,
            docstring=docstring,
            kwarg_groups=groups,
            arg_group=arg_group,
        )
