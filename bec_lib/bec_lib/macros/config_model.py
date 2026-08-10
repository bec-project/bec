from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Self, TextIO, TypeAliasType

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    Discriminator,
    Field,
    Tag,
    TypeAdapter,
    model_validator,
)


def _is_identifier(s: str) -> str:
    if not s.isidentifier():
        raise ValueError(f"{s!r} is not a valid Python identifier")
    return s


def _is_dotted_path(ref: str) -> str:
    if not all(part.isidentifier() for part in ref.split(".")):
        raise ValueError(f"{ref!r} is not a valid module path")
    return ref


class MacroRefBase(BaseModel):
    func_name: Annotated[str, AfterValidator(_is_identifier)] | None


class ModuleImportMacro(MacroRefBase):
    reference: Annotated[str, AfterValidator(_is_dotted_path)]


class FileImportMacro(MacroRefBase):
    reference: Path


def _disc_module_file(input: dict[str, str] | ModuleImportMacro | FileImportMacro | Any):
    if isinstance(input, dict):
        if "reference" not in input:
            return None
        return "file" if input["reference"].endswith(".py") else "module"
    if isinstance(input, (ModuleImportMacro, FileImportMacro)):
        return "module" if isinstance(input.reference, str) else "file"


MacroRef = Annotated[
    Annotated[ModuleImportMacro, Tag("module")] | Annotated[FileImportMacro, Tag("file")],
    Discriminator(_disc_module_file),
]


MacroNsSpec = TypeAliasType("MacroNsSpec", dict[str, MacroRef | "MacroNsSpec"])


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
                assert isinstance(
                    (category := haystack[head]), dict
                ), f"{nested_name}.{head} is not a macro category!"
                return _validate_nested_contains(tail, category, f"{nested_name}.{head}")
            assert (
                needle in haystack
            ), f"No macro {needle} in macro namespace {nested_name} to add to builtins."
            assert isinstance(
                haystack.get(needle), MacroRefBase
            ), f"Cannot add macro namespace {macro_ref} to builtins. Please add individual macros instead."

        for macro_ref in self.add_to_builtins:
            _validate_nested_contains(macro_ref, self.macros, "macros")
        return self


_NestedStrInDict = TypeAliasType("_NestedStrInDict", dict[str, "str | _NestedStrInDict"])
_NestedStrOutDict = TypeAliasType(
    "_NestedStrOutDict", dict[str, "dict[str, str | None] | _NestedStrOutDict"]
)
_NestedStrDictAdapter = TypeAdapter(_NestedStrInDict)


def parse_macro_config(config_stream: TextIO) -> MacroConfig:
    """Parse a text buffer or file handler for a TOML macro config into a MacroConfig object."""

    def _split_strings(d: _NestedStrInDict) -> _NestedStrOutDict:
        def _split_entry(entry: str) -> dict[str, str | None]:
            split = entry.split("::")
            if len(split) == 1:
                return {"reference": split[0], "func_name": None}
            assert (
                len(split) == 2
            ), f"Invalid macro reference: '{entry}'. Please reference in the format 'package.module::function' or '/path/to/file.py::function'."
            return {"reference": split[0], "func_name": split[1]}

        return {
            k: _split_strings(v) if isinstance(v, dict) else _split_entry(v) for k, v in d.items()
        }

    data = yaml.safe_load(config_stream)
    assert "macros" in data, "No macros entry found in config!"
    data["macros"] = _split_strings(_NestedStrDictAdapter.validate_python(data["macros"]))
    return MacroConfig.model_validate(data)
