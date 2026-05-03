"""
Helpers for applying scan signature overrides from scan modifier plugins.

This module builds the user-facing scan signature published by the scan manager
and applies the same override-derived defaults again in the scan assembler
before a direct scan class is instantiated. This keeps the published signature,
input validation, and runtime construction behavior aligned.
"""

from __future__ import annotations

import inspect
from functools import lru_cache
from typing import TYPE_CHECKING, Annotated, Any, Type, get_args, get_origin, get_type_hints

from bec_lib.plugin_helper import get_scan_modifier_plugin
from bec_lib.scan_args import ScanArgument
from bec_lib.signature_serializer import deserialize_dtype, dict_to_signature, signature_to_dict
from bec_server.scan_server.scans.scan_base import ScanBase

if TYPE_CHECKING:
    from bec_server.scan_server.scans.scan_modifier import ScanModifier


@lru_cache(maxsize=None)
def get_scan_modifier() -> Type[ScanModifier] | None:
    """
    Load the available scan argument modifier from the plugin.

    Returns:
        ScanModifier: The scan modifier class
    """
    modifier = get_scan_modifier_plugin()
    return modifier


def _convert_annotation(annotation: object) -> Annotated[Any, ScanArgument] | None:
    """
    Normalize an annotation to ``Annotated[..., ScanArgument(...)]``.

    The scan signature override API operates on annotations that always carry
    ``ScanArgument`` metadata so modifier code can treat original and injected
    arguments consistently. Plain annotations are wrapped in a default
    ``ScanArgument`` instance, while existing ``ScanArgument`` metadata is
    preserved.

    This is mainly done to keep the interface for scan signature modifiers consistent.

    Args:
        annotation (object): Serialized or native annotation object.

    Returns:
        Annotated[Any, ScanArgument] | None: Normalized annotation with
            ``ScanArgument`` metadata, or ``None`` if the source annotation is
            empty.
    """
    if annotation is None or annotation == "_empty":
        return None

    converted = (
        deserialize_dtype(annotation) if isinstance(annotation, (str, dict, list)) else annotation
    )
    if converted is None or converted is inspect._empty:
        return None

    if get_origin(converted) is Annotated:
        base_annotation, *metadata = get_args(converted)
        for entry in metadata:
            if isinstance(entry, ScanArgument):
                return converted
        return Annotated[base_annotation, ScanArgument()]

    return Annotated[converted, ScanArgument()]


def _get_annotations_and_defaults(
    scan_cls: Type[ScanBase],
) -> tuple[dict[str, Annotated[Any, ScanArgument] | None], dict[str, Any]]:
    """
    Collect normalized annotations and Python defaults from ``scan_cls.__init__``.

    Args:
        scan_cls (Type[ScanBase]): Scan class whose constructor should be
            inspected.

    Returns:
        tuple: A tuple ``(arguments, defaults)`` where ``arguments`` maps input
        names to normalized annotations and ``defaults`` maps input names to
        their constructor defaults.
    """
    signature = inspect.signature(scan_cls.__init__)
    type_hints = get_type_hints(scan_cls.__init__, include_extras=True)
    annotations = {}
    defaults = {}
    for arg_name, parameter in signature.parameters.items():
        if arg_name in {"self", "cls", "args", "kwargs"}:
            continue
        annotations[arg_name] = _convert_annotation(type_hints.get(arg_name, parameter.annotation))
        if parameter.default is not inspect.Parameter.empty:
            defaults[arg_name] = parameter.default
    return annotations, defaults


def _build_signature_from_arguments(
    scan_cls: Type[ScanBase],
    arguments: dict[str, Annotated[Any, ScanArgument] | None],
    defaults: dict[str, Any],
) -> inspect.Signature:
    """
    Build a modified scan signature from override-derived arguments and defaults.

    Existing constructor parameters are copied from ``scan_cls.__init__`` and
    updated in-place with override-derived annotations and defaults.
    Additional override-only arguments are appended as keyword-only parameters
    before any trailing ``**kwargs`` entry.

    Args:
        scan_cls (Type[ScanBase]): Scan class whose constructor shape is used as
            the base signature.
        arguments (dict): Normalized scan argument annotations after overrides.
        defaults (dict): Default values after overrides.

    Returns:
        inspect.Signature: Effective signature after applying scan overrides.
    """
    parameters: list[inspect.Parameter] = []
    trailing_var_keyword = None
    seen_names: set[str] = set()

    for name, param in inspect.signature(scan_cls.__init__).parameters.items():
        if name in {"self", "cls"}:
            continue
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            parameters.append(param)
            continue
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            trailing_var_keyword = param
            continue
        if name not in arguments:
            continue
        annotation = arguments[name]
        parameters.append(
            param.replace(
                annotation=annotation if annotation is not None else inspect.Parameter.empty,
                default=defaults.get(name, inspect.Parameter.empty),
            )
        )
        seen_names.add(name)

    for name, annotation in arguments.items():
        if name in seen_names:
            continue
        parameters.append(
            inspect.Parameter(
                name=name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=defaults.get(name, inspect.Parameter.empty),
                annotation=annotation if annotation is not None else inspect.Parameter.empty,
            )
        )

    if trailing_var_keyword is not None and trailing_var_keyword.name in arguments:
        annotation = arguments[trailing_var_keyword.name]
        parameters.append(
            trailing_var_keyword.replace(
                annotation=annotation if annotation is not None else inspect.Parameter.empty,
                default=defaults.get(trailing_var_keyword.name, inspect.Parameter.empty),
            )
        )
    elif trailing_var_keyword is not None:
        parameters.append(trailing_var_keyword)

    return inspect.Signature(parameters)


def get_scan_argument_overrides(
    scan_cls: Type[ScanBase],
) -> tuple[dict[str, Annotated[Any, ScanArgument] | None], dict[str, Any]]:
    """
    Return the effective scan arguments and defaults after applying overrides.

    The helper first inspects ``scan_cls.__init__`` and then, when available,
    calls the installed scan modifier plugin. Both the current
    ``scan_argument_overrides`` name and the legacy
    ``scan_signature_overrides`` name are supported.

    Args:
        scan_cls (Type[ScanBase]): Scan class to inspect.

    Returns:
        tuple: A tuple ``(arguments, defaults)`` containing the effective
        annotation and default maps after modifier processing.
    """
    arguments, defaults = _get_annotations_and_defaults(scan_cls)
    if not issubclass(scan_cls, ScanBase):
        return arguments, defaults
    modifier = get_scan_modifier()
    if modifier is None:
        return arguments, defaults
    override_func = getattr(modifier, "scan_argument_overrides", None)
    if override_func is None:
        override_func = getattr(modifier, "scan_signature_overrides", None)
    if override_func is None:
        return arguments, defaults
    return override_func(scan_cls.scan_name, arguments, defaults)


def scan_signature_with_modifiers(scan_cls: Type[ScanBase]) -> list[dict[str, Any]]:
    """
    Build the published scan signature after applying modifier overrides.

    Args:
        scan_cls (Type[ScanBase]): Scan class whose signature should be
            published.

    Returns:
        list[dict[str, Any]]: Serialized signature entries describing the
        user-facing scan inputs.
    """
    arguments, defaults = get_scan_argument_overrides(scan_cls)
    return signature_to_dict(_build_signature_from_arguments(scan_cls, arguments, defaults))


def gui_config_with_modifiers(
    scan_cls: Type[ScanBase], gui_config: dict[str, list[str]]
) -> dict[str, list[str]]:
    """
    Build the published GUI configuration after applying modifier overrides.

    Args:
        scan_cls (Type[ScanBase]): Scan class whose GUI config should be published.
        gui_config (dict[str, list[str]]): Validated GUI config for the scan.

    Returns:
        dict[str, list[str]]: Modifier-adjusted GUI config for publication.
    """
    if not issubclass(scan_cls, ScanBase):
        return gui_config

    modifier = get_scan_modifier()
    if modifier is None:
        # no plugin installed, return original config
        return gui_config

    return modifier.gui_config_overrides(scan_cls.scan_name, gui_config)


def _annotation_to_doc_type(annotation: object) -> str:
    """
    Convert a normalized annotation to a compact docstring type label.

    Args:
        annotation (object): Annotation object to stringify.

    Returns:
        str: Human-readable type label for generated argument docs.
    """
    if annotation is None:
        return "Any"
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]

    origin = get_origin(annotation)
    if origin is None:
        name = getattr(annotation, "__name__", None)
        return name if name is not None else str(annotation)

    args = get_args(annotation)
    origin_name = getattr(origin, "__name__", str(origin))
    if args:
        return f"{origin_name}[{', '.join(_annotation_to_doc_type(arg) for arg in args)}]"
    return origin_name


def _argument_to_doc_line(
    name: str, annotation: Annotated[Any, ScanArgument] | None, defaults: dict[str, Any]
) -> str:
    """
    Build one ``Args:`` line for the generated scan docstring.

    Args:
        name (str): Argument name.
        annotation (Annotated[Any, ScanArgument] | None): Normalized argument
            annotation.
        defaults (dict[str, Any]): Effective default values.

    Returns:
        str: Single formatted docstring line.
    """
    description = name.replace("_", " ")
    if annotation is not None and get_origin(annotation) is Annotated:
        _, *metadata = get_args(annotation)
        for entry in metadata:
            if isinstance(entry, ScanArgument):
                if entry.description:
                    description = entry.description
                break

    line = f"    {name} ({_annotation_to_doc_type(annotation)}): {description}"
    if name in defaults:
        line += f". Default: {defaults[name]!r}"
    return line


def _annotation_metadata(annotation: Annotated[Any, ScanArgument] | None) -> ScanArgument | None:
    """Return the ``ScanArgument`` metadata attached to an annotation."""
    if annotation is None or get_origin(annotation) is not Annotated:
        return None
    _, *metadata = get_args(annotation)
    return next((entry for entry in metadata if isinstance(entry, ScanArgument)), None)


def _example_literal(name: str, annotation: object, bundle_index: int | None = None) -> str:
    """Build a compact Python literal for a generated example call."""
    metadata = _annotation_metadata(annotation)
    if metadata is not None and metadata.example is not None:
        return repr(metadata.example)

    if annotation is None:
        return repr(name)
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]

    origin = get_origin(annotation)
    if origin is list:
        return "[1.0, 2.0, 3.0]"
    if origin is tuple:
        return "(1.0, 2.0)"
    if origin is dict:
        return "{'key': 'value'}"
    if origin is set:
        return "{1.0, 2.0}"
    if origin is not None:
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if args:
            return _example_literal(name, args[0], bundle_index=bundle_index)

    type_name = getattr(annotation, "__name__", str(annotation))
    if type_name == "DeviceBase":
        return f"dev.{name}{bundle_index + 1}" if bundle_index is not None else f"dev.{name}"
    if annotation is bool:
        return "False"
    if annotation is int:
        return "10" if "step" in name else "1"
    if annotation is float:
        if "start" in name:
            return "-1.0"
        if "stop" in name or "target" in name:
            return "1.0"
        return "1.0"
    if annotation is str:
        return repr(name)
    return repr(name)


def _build_example_calls(
    scan_cls: Type[ScanBase],
    arguments: dict[str, Annotated[Any, ScanArgument] | None],
    defaults: dict[str, Any],
) -> tuple[str, str]:
    """Build minimum and full generated example calls from the effective inputs."""
    minimum_parts: list[str] = []
    full_parts: list[str] = []

    if getattr(scan_cls, "arg_input", None) and getattr(scan_cls, "arg_bundle_size", {}).get(
        "bundle", 0
    ):
        bundle_count = getattr(scan_cls, "arg_bundle_size", {}).get("min") or 1
        for bundle_index in range(bundle_count):
            for arg_name, arg_annotation in scan_cls.arg_input.items():
                literal = _example_literal(
                    arg_name, _convert_annotation(arg_annotation), bundle_index=bundle_index
                )
                minimum_parts.append(literal)
                full_parts.append(literal)

    signature = inspect.signature(scan_cls.__init__)
    for name, parameter in signature.parameters.items():
        if name in {"self", "args", "kwargs"} or name not in arguments:
            continue
        value = defaults[name] if name in defaults else _example_literal(name, arguments[name])
        rendered = repr(value) if not isinstance(value, str) else value
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY:
            if name not in defaults:
                minimum_parts.append(f"{name}={rendered}")
            full_parts.append(f"{name}={rendered}")
        else:
            if name not in defaults:
                minimum_parts.append(rendered)
            full_parts.append(rendered)

    existing_parameters = {
        name
        for name, parameter in signature.parameters.items()
        if name not in {"self", "args", "kwargs"}
        and parameter.kind is not inspect.Parameter.VAR_KEYWORD
    }
    for name, annotation in arguments.items():
        if name in existing_parameters:
            continue
        value = defaults[name] if name in defaults else _example_literal(name, annotation)
        rendered = repr(value) if not isinstance(value, str) else value
        if name not in defaults:
            minimum_parts.append(f"{name}={rendered}")
        full_parts.append(f"{name}={rendered}")

    minimum_call = f"        >>> scans.{scan_cls.scan_name}({', '.join(minimum_parts)})"
    full_call = f"        >>> scans.{scan_cls.scan_name}({', '.join(full_parts)})"
    return minimum_call, full_call


def _get_scan_raw_doc(scan_cls: Type[ScanBase]) -> str:
    """Return the scan-specific raw docstring, preferring non-generic class docs."""
    class_doc = inspect.getdoc(scan_cls)
    base_class_doc = inspect.getdoc(ScanBase)
    base_init_doc = inspect.getdoc(ScanBase.__init__)

    if class_doc and class_doc not in {base_class_doc, base_init_doc}:
        return class_doc

    return inspect.getdoc(scan_cls.__init__) or class_doc or ""


def scan_doc_with_modifiers(scan_cls: Type[ScanBase]) -> str:
    """
    Build a published scan docstring from the effective scan inputs.

    The helper preserves the introductory and trailing sections from the
    original class or constructor docstring while rebuilding the ``Args:``
    and ``Examples:`` sections from the effective argument and default maps
    after modifier overrides have been applied.

    Args:
        scan_cls (Type[ScanBase]): Scan class whose published docstring should
            be generated.

    Returns:
        str: Docstring aligned with the effective scan signature.
    """
    if not issubclass(scan_cls, ScanBase):
        # For legacy scans, we simply return the original docstring
        return scan_cls.__doc__ or scan_cls.__init__.__doc__ or ""

    raw_doc = _get_scan_raw_doc(scan_cls)
    arguments, defaults = get_scan_argument_overrides(scan_cls)

    prefix = raw_doc.strip()
    suffix = ""
    if "Args:" in raw_doc:
        prefix, remainder = raw_doc.split("Args:", 1)
        prefix = prefix.rstrip()
        trailing_markers = ["Returns:", "Examples:", "Raises:"]
        split_positions = [
            remainder.find(marker) for marker in trailing_markers if remainder.find(marker) != -1
        ]
        if split_positions:
            suffix = remainder[min(split_positions) :].strip()
            if "Examples:" in suffix:
                example_pos = suffix.find("Examples:")
                next_section_positions = [
                    suffix.find(marker, example_pos + len("Examples:"))
                    for marker in ("Returns:", "Raises:")
                    if suffix.find(marker, example_pos + len("Examples:")) != -1
                ]
                if next_section_positions:
                    end_pos = min(next_section_positions)
                    suffix = (suffix[:example_pos] + suffix[end_pos:]).strip()
                else:
                    suffix = suffix[:example_pos].rstrip()

    arg_lines = []
    if getattr(scan_cls, "arg_input", None) and getattr(scan_cls, "arg_bundle_size", {}).get(
        "bundle", 0
    ):
        bundle_items = []
        for arg_name, arg_annotation in scan_cls.arg_input.items():
            bundle_items.append(
                f"{arg_name}: {_annotation_to_doc_type(_convert_annotation(arg_annotation))}"
            )
        arg_lines.append(f"    *args ({', '.join(bundle_items)}): repeated scan argument bundles.")

    for name, annotation in arguments.items():
        arg_lines.append(_argument_to_doc_line(name, annotation, defaults))

    minimum_example, full_example = _build_example_calls(scan_cls, arguments, defaults)
    example_section = "\n".join(
        ["Examples:", "    Minimum:", minimum_example, "    Full:", full_example]
    )
    sections = [
        section
        for section in [prefix, "Args:\n" + "\n".join(arg_lines), suffix, example_section]
        if section
    ]
    return "\n\n".join(sections)


def apply_scan_argument_defaults(
    scan_cls: Type[ScanBase],
    signature: list[dict[str, Any]],
    args: tuple | list,
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """
    Apply published and override-derived defaults before scan construction.

    This helper reuses the published signature to materialize missing defaulted
    values before ``scan_cls(...)`` is called. Values for parameters that do not
    exist on the real constructor are moved into
    ``additional_scan_parameters`` so they remain available through
    ``scan.scan_info.additional_scan_parameters`` without leaking into
    ``ScanBase.__init__``.

    Args:
        scan_cls (Type[ScanBase]): Scan class that will be instantiated.
        signature (list[dict[str, Any]]): Published serialized signature for the
            scan.
        args (tuple | list): Positional arguments after validation and device
            resolution.
        kwargs (dict[str, Any]): Keyword arguments after validation and device
            resolution.

    Returns:
        dict[str, Any]: Constructor kwargs updated with any missing defaults and
        ``additional_scan_parameters`` entries implied by signature overrides.
    """
    public_signature = dict_to_signature(signature)
    original_signature = inspect.signature(scan_cls)
    _, modifier_defaults = get_scan_argument_overrides(scan_cls)

    known_kwargs = {
        key: value for key, value in kwargs.items() if key in public_signature.parameters
    }
    original_known_kwargs = {
        key: value for key, value in kwargs.items() if key in original_signature.parameters
    }
    try:
        bound = public_signature.bind_partial(*args, **known_kwargs)
        original_bound = original_signature.bind_partial(*args, **original_known_kwargs)
    except TypeError:
        return kwargs

    bound.apply_defaults()
    provided_original_arguments = set(original_bound.arguments)
    scan_kwargs = dict(kwargs)
    additional_scan_parameters = dict(scan_kwargs.get("additional_scan_parameters") or {})

    for name, value in bound.arguments.items():
        if name not in original_signature.parameters:
            additional_scan_parameters[name] = value
            scan_kwargs.pop(name, None)
            continue
        if name in provided_original_arguments:
            continue
        param = original_signature.parameters[name]
        if param.kind not in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }:
            continue
        scan_kwargs[name] = value

    for name, value in modifier_defaults.items():
        if name in provided_original_arguments or name not in original_signature.parameters:
            continue
        param = original_signature.parameters[name]
        if param.kind not in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }:
            continue
        scan_kwargs.setdefault(name, value)

    if additional_scan_parameters:
        scan_kwargs["additional_scan_parameters"] = additional_scan_parameters

    return scan_kwargs
