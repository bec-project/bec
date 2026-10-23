from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Self, TextIO

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
from typing_extensions import TypeAliasType  # only for 3.11

# Directory references which are resolved on each client when the config is loaded, since the
# actual paths differ between machines.
USER_MACROS_DIR_PLACEHOLDER = "<user_macros>"
PLUGIN_MACROS_DIR_PLACEHOLDER = "<plugin_macros>"


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


class DirImportMacro(MacroRefBase):
    """A directory of macro files. The key it is listed under is only a label: the directory's
    contents are merged into the namespace containing the reference. Each `.py` file in the
    directory becomes a namespace of its public functions and each subdirectory a nested namespace.
    The reference may be one of the placeholders defined in this module. It is kept as text and only
    resolved to a path on loading, so that it survives serialization unchanged."""

    reference: str

    @model_validator(mode="after")
    def _no_func_name(self) -> Self:
        if self.func_name is not None:
            raise ValueError("A directory reference cannot select a single function")
        return self


_MACRO_REF_TAGS = {ModuleImportMacro: "module", FileImportMacro: "file", DirImportMacro: "dir"}


def _disc_macro_ref(value: dict[str, Any] | MacroRefBase | Any) -> str | None:
    if isinstance(value, MacroRefBase):
        return _MACRO_REF_TAGS.get(type(value))
    if not isinstance(value, dict) or "reference" not in value:
        return None
    reference = str(value["reference"])
    if reference.endswith(".py"):
        return "file"
    # dotted module paths contain neither path separators nor placeholder brackets
    if "/" in reference or reference.startswith(("<", ".", "~")):
        return "dir"
    return "module"


MacroRef = Annotated[
    Annotated[ModuleImportMacro, Tag("module")]
    | Annotated[FileImportMacro, Tag("file")]
    | Annotated[DirImportMacro, Tag("dir")],
    Discriminator(_disc_macro_ref),
]

# Pydantic handles this correctly. From python 3.13 it should be replaced with
# `type MacroNsSpec = dict[str, MacroRef | "MacroNsSpec"]` which is also interpreted correctly by pyright
MacroNsSpec = TypeAliasType("MacroNsSpec", dict[str, MacroRef | "MacroNsSpec"])  # type: ignore


class MacroConfig(BaseModel):
    macros: Annotated[
        MacroNsSpec,
        Field(description="""key is the macro name; value is the import location, given as a dotted
module path or a path to a .py file, optionally followed by `::function`. Without a function, the
whole module becomes a namespace of its public functions. E.g. the entry
`run_alignment_scan: pxii_bec.macros.scans::alignment_scan` results in
`bec.macros.run_alignment_scan` pointing to the function at `pxii_bec.macros.scans.alignment_scan`.
Nested mappings result in nested namespaces. A value which is a directory path or one of the
directory placeholders merges the directory's contents into the containing namespace: each .py
file becomes a namespace of its functions and each subdirectory a nested namespace.
"""),
    ]
    global_in_interactive_shell: Annotated[
        list[str],
        Field(
            description="A list of macro names (keys) to import into builtins in an interactive BEC Client session. Must exist in `macros`.",
            default_factory=list,
        ),
    ]

    @model_validator(mode="after")
    def _validate_added_to_shell(self) -> Self:
        def _has_dir_ref(haystack: MacroNsSpec) -> bool:
            # the contents of a directory reference are only known once it is loaded
            return any(isinstance(macro, DirImportMacro) for macro in haystack.values())

        def _validate_nested_contains(needle: str, haystack: MacroNsSpec, nested_name: str):
            if "." in needle:
                head, tail = needle.split(".", 1)
                if head not in haystack:
                    assert _has_dir_ref(
                        haystack
                    ), f"No category {head} in macro namespace {nested_name}"
                    return
                assert isinstance(
                    (category := haystack[head]), dict
                ), f"{nested_name}.{head} is not a macro category!"
                _validate_nested_contains(tail, category, f"{nested_name}.{head}")
                return
            if needle not in haystack:
                assert _has_dir_ref(
                    haystack
                ), f"No macro {needle} in macro namespace {nested_name} to add to builtins."
                return
            assert isinstance(
                haystack.get(needle), MacroRefBase
            ), f"Cannot add macro namespace {macro_ref} as a global in the interactive session. Please add individual macros instead."

        for macro_ref in self.global_in_interactive_shell:
            _validate_nested_contains(macro_ref, self.macros, "macros")
        return self

    def get_modules(self): ...


_NestedStrInDict = TypeAliasType("_NestedStrInDict", dict[str, "str | _NestedStrInDict"])
_NestedStrOutDict = TypeAliasType(
    "_NestedStrOutDict", dict[str, "dict[str, str | None] | _NestedStrOutDict"]
)
_NestedStrDictAdapter = TypeAdapter(_NestedStrInDict)


def parse_macro_config(config_stream: TextIO) -> MacroConfig:
    """Parse a text buffer or file handler for a YAML macro config into a MacroConfig object."""

    def _split_strings(d: _NestedStrInDict) -> _NestedStrOutDict:
        def _split_entry(entry: str) -> dict[str, str | None]:
            split = entry.split("::")
            if len(split) == 1:
                return {"reference": split[0], "func_name": None}
            assert (
                len(split) == 2
            ), f"Invalid macro reference: '{entry}'. Please reference in the format 'package.module::function', '/path/to/file.py::function' or '/path/to/directory'."
            return {"reference": split[0], "func_name": split[1]}

        return {
            k: _split_strings(v) if isinstance(v, dict) else _split_entry(v) for k, v in d.items()
        }

    data = yaml.safe_load(config_stream)
    assert "macros" in data, "No macros entry found in config!"
    data["macros"] = _split_strings(_NestedStrDictAdapter.validate_python(data["macros"]))
    return MacroConfig.model_validate(data)
