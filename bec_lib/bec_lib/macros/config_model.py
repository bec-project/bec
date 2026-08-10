from __future__ import annotations

from typing import Annotated, Self, TextIO, TypeAliasType

import yaml
from pydantic import BaseModel, Field, TypeAdapter, ValidationError, model_validator

MacroNsSpec = TypeAliasType(
    "MacroNsSpec", "dict[str, Annotated[list[str], Field(min_length=3)] | MacroNsSpec]"
)


class MacroConfig(BaseModel):
    macros: Annotated[
        MacroNsSpec,
        Field(
            description="""key is the macro name; value is the import location - the last index of the list is the
function and the preceding are the modules. E.g. the following entry:
`"run_alignment_scan": ["pxii_bec", "macros", "scans", "alignment_scan"]`
would result in
`bec.macros.run_alignment_scan` pointing to the function at `pxii_bec.macros.scans.alignment_scan`.
Namespaces can be nested and result in
"""
        ),
    ]
    add_to_builtins: Annotated[
        list[str],
        Field(
            description="A list of macro names (keys) to import into builtins in an interactive BEC Client session. Must exist in `macros`.",
            default_factory=list,
        ),
    ]

    @model_validator(mode="after")
    def _validate_added_to_builtins(self) -> Self:
        def _validate_nested_contains(needle: str, haystack: MacroNsSpec, nested_name: str):
            if "." in needle:
                head, tail = needle.split(".", 1)
                assert head in haystack, f"No category {head} in macro namespace {nested_name}"
                assert isinstance((category := haystack[head]), dict), (
                    f"{nested_name}.{head} is not a macro category!"
                )
                return _validate_nested_contains(tail, category, f"{nested_name}.{head}")
            assert needle in haystack, (
                f"No macro {needle} in macro namespace {nested_name} to add to builtins."
            )
            assert isinstance(haystack.get(needle), list), (
                f"Cannot add macro namespace {macro_ref} to builtins. Please add individual macros instead."
            )

        for macro_ref in self.add_to_builtins:
            _validate_nested_contains(macro_ref, self.macros, "macros")
        return self


_NestedStrDict = TypeAliasType("_NestedStrDict", dict[str, "str | _NestedStrDict"])
_NestedStrListDict = TypeAliasType(
    "_NestedStrListDict", "dict[str, list[str] | _NestedStrListDict]"
)
_NestedStrDictAdapter = TypeAdapter(_NestedStrDict)


def parse_macro_config(config_stream: TextIO) -> MacroConfig:
    """Parse a text buffer or file handler for a TOML macro config into a MacroConfig object."""

    def _split_strings(d: _NestedStrDict) -> _NestedStrListDict:
        return {k: _split_strings(v) if isinstance(v, dict) else v.split(".") for k, v in d.items()}

    data = yaml.safe_load(config_stream)
    assert "macros" in data, "No macros entry found in config!"
    data["macros"] = _split_strings(_NestedStrDictAdapter.validate_python(data["macros"]))
    try:
        return MacroConfig.model_validate(data)
    except ValidationError as e:
        for error in e.errors():
            if error["type"] == "too_short":
                raise ValueError(
                    f"{'.'.join(error['input'])} is too short to identify a macro! Please reference macros in the format <plugin_repo>.<module>[.<optionally nested module>].<function_name>."
                ) from e
        raise
