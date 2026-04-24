from __future__ import annotations

import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, get_args, get_origin

import jinja2
import questionary
import typer

from bec_lib.logger import bec_logger
from bec_lib.plugin_helper import get_scan_component_plugins, plugin_package_name, plugin_repo_path
from bec_lib.scan_args import DefaultArgType, ScanArgument, Units
from bec_lib.utils.copier_jinja_filters import CopierFilters
from bec_lib.utils.plugin_manager._util import run_formatters

logger = bec_logger.logger
_app = typer.Typer(rich_markup_mode="rich")
_ARGUMENT_TYPES = ("float", "int", "bool", "str", "DeviceBase")
_SCAN_TYPES = ("SOFTWARE_TRIGGERED", "HARDWARE_TRIGGERED")
_QUESTIONARY_STYLE = questionary.Style(
    [
        ("qmark", "fg:#e76f51 bold"),
        ("question", "fg:#f4a261 bold"),
        ("answer", "fg:#2a9d8f bold"),
        ("pointer", "fg:#e9c46a bold"),
        ("highlighted", "fg:#ffffff bg:#264653 bold"),
        ("selected", "fg:#2a9d8f bold"),
        ("separator", "fg:#6c757d"),
        ("instruction", "fg:#8d99ae italic"),
        ("text", "fg:#f8f9fa"),
    ]
)
_HIDDEN_BUILTIN_ARGUMENT_NAMES = frozenset(
    {"num_points", "num_monitored_readouts", "positions", "run_on_exception_hook"}
)


def _select_option(prompt: str, choices: list[str] | tuple[str, ...]) -> str:
    """Prompt the user to select one option from a list of choices."""
    selected = questionary.select(prompt, choices=list(choices), style=_QUESTIONARY_STYLE).ask()
    if selected is None:
        raise typer.Exit(code=1)
    return selected


def _scan_argument_metadata(annotation: Any) -> tuple[Any, ScanArgument]:
    """Extract the base type and :class:`ScanArgument` metadata from an annotation."""
    if get_origin(annotation) is not Annotated:
        raise ValueError(f"Expected Annotated type, got {annotation!r}")

    base_type, *metadata = get_args(annotation)
    for item in metadata:
        if isinstance(item, ScanArgument):
            return base_type, item
    raise ValueError(f"No ScanArgument metadata found in {annotation!r}")


def _python_type_name(annotation: Any) -> str:
    """Return a readable Python type name for docstrings and prompt labels."""
    if annotation is type(None):
        return "None"
    return getattr(annotation, "__name__", repr(annotation))


@dataclass(frozen=True)
class BaseArgumentSpec:
    """Common interface for template-rendered scan arguments."""

    name: str
    default: str | None = None
    gui_group: str = "Scan Parameters"

    @property
    def requires_default_arg_type_import(self) -> bool:
        return False

    @property
    def requires_scan_argument_import(self) -> bool:
        return False

    @property
    def requires_units_import(self) -> bool:
        return False

    @property
    def unit_expression(self) -> str | None:
        """Return the generated ``Units`` expression for the configured unit.

        Returns:
            str | None: Python expression using ``Units.<name>`` attributes, or ``None``.
        """
        units = getattr(self, "units", None)
        if not units:
            return None

        parsed_unit = Units.parse_units(units)
        numerator: list[str] = []
        denominator: list[str] = []
        for unit_name, exponent in parsed_unit._units.items():
            compact_name = f"{Units.parse_units(unit_name):~P}"
            unit_term = f"Units.{compact_name}"
            if abs(exponent) != 1:
                unit_term = f"{unit_term} ** {abs(exponent)}"
            target = numerator if exponent > 0 else denominator
            target.append(unit_term)

        expression = " * ".join(numerator) if numerator else "1"
        if denominator:
            expression += " / " + " / ".join(denominator)
        return expression

    @property
    def annotation(self) -> str:
        raise NotImplementedError

    @property
    def display_name(self) -> str:
        """Return a title-cased label derived from the argument name.

        Returns:
            str: Display label for prompts and generated metadata.
        """
        return self.name.replace("_", " ").title()

    @property
    def doc_type(self) -> str:
        raise NotImplementedError

    @property
    def doc_line(self) -> str:
        raise NotImplementedError

    @property
    def parameter(self) -> str:
        """Render the full parameter definition for the template.

        Returns:
            str: Function parameter including annotation and optional default.
        """
        parameter = f"{self.name}: {self.annotation}"
        if self.default is not None:
            parameter += f" = {self.default}"
        return parameter

    @property
    def selection_label(self) -> str:
        """Return the label shown in the built-in argument selector.

        Returns:
            str: Human-readable label for interactive selection prompts.
        """
        return f"{self.name} ({self.type}): {self.doc_line}"


@dataclass(frozen=True)
class CustomArgumentSpec(BaseArgumentSpec):
    """User-defined scan argument rendered as ``Annotated[..., ScanArgument(...)]``."""

    type: str = "float"
    doc: str | None = None
    units: str | None = None

    @property
    def requires_scan_argument_import(self) -> bool:
        return True

    @property
    def requires_units_import(self) -> bool:
        return self.units is not None

    @property
    def annotation(self) -> str:
        units = f", units={self.unit_expression}" if self.unit_expression else ""
        annotations = {
            "float": (
                f"Annotated[float, ScanArgument(display_name={self.display_name!r}, "
                f"description={self.doc_line!r}{units})]"
            ),
            "int": (
                f"Annotated[int, ScanArgument(display_name={self.display_name!r}, "
                f"description={self.doc_line!r}{units})]"
            ),
            "bool": (
                f"Annotated[bool, ScanArgument(display_name={self.display_name!r}, "
                f"description={self.doc_line!r})]"
            ),
            "str": (
                f"Annotated[str, ScanArgument(display_name={self.display_name!r}, "
                f"description={self.doc_line!r})]"
            ),
            "DeviceBase": (
                f"Annotated[DeviceBase, ScanArgument(display_name={self.display_name!r}, "
                f"description={self.doc_line!r})]"
            ),
        }
        return annotations[self.type]

    @property
    def doc_type(self) -> str:
        return self.type

    @property
    def doc_line(self) -> str:
        return self.doc or f"{self.display_name}."


@dataclass(frozen=True)
class BuiltinArgumentSpec(BaseArgumentSpec):
    """Builtin scan argument backed directly by :class:`DefaultArgType`."""

    default_arg_type: str = ""

    @property
    def requires_default_arg_type_import(self) -> bool:
        return True

    @property
    def default_annotation(self) -> Any:
        return getattr(DefaultArgType, self.default_arg_type)

    @property
    def type(self) -> str:
        base_type, _ = _scan_argument_metadata(self.default_annotation)
        return _python_type_name(base_type)

    @property
    def metadata(self) -> ScanArgument:
        _, metadata = _scan_argument_metadata(self.default_annotation)
        return metadata

    @property
    def annotation(self) -> str:
        return f"DefaultArgType.{self.default_arg_type}"

    @property
    def display_name(self) -> str:
        return self.metadata.display_name or super().display_name

    @property
    def doc_type(self) -> str:
        return self.type

    @property
    def doc_line(self) -> str:
        return self.metadata.description or f"{self.display_name}."


@dataclass(frozen=True)
class ScanConfig:
    """Collect the values required to render a new scan template.

    Args:
        name (str): Snake-case scan name used for the file, class, and scan identifier.
        description (str): Module docstring description for the generated scan.
        scan_type (str): Selected v4 scan type enum member name.
        template_arguments (list[BaseArgumentSpec]): Arguments rendered into the template.
        plugin_components_class (str | None): Optional plugin-local ScanComponents subclass.
        plugin_components_import (str | None): Import path for the plugin-local ScanComponents subclass.
    """

    name: str
    description: str
    scan_type: str
    template_arguments: list[BaseArgumentSpec]
    plugin_components_class: str | None = None
    plugin_components_import: str | None = None


def _render_scan(template_dir: Path, config: ScanConfig) -> str:
    """Render the v4 scan template for the provided configuration."""
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(template_dir),
        extensions=[CopierFilters],
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = environment.get_template("v4_scan.py.jinja")
    return (
        template.render(
            name=config.name,
            description=config.description,
            scan_type=config.scan_type,
            arguments=config.template_arguments,
            plugin_components_class=config.plugin_components_class,
            plugin_components_import=config.plugin_components_import,
        ).rstrip()
        + "\n"
    )


def _get_plugin_components_config() -> tuple[str | None, str | None]:
    """Return the plugin-local ScanComponents subclass and absolute import path."""
    component_plugins = get_scan_component_plugins()
    if not component_plugins:
        return None, None

    plugin_package = plugin_package_name()
    for component_cls in component_plugins:
        module_name = component_cls.__module__
        if not module_name.startswith(f"{plugin_package}."):
            continue
        return component_cls.__name__, module_name
    return None, None


_BUILTIN_ARGUMENTS = (
    BuiltinArgumentSpec(
        name="exp_time",
        default_arg_type="ExposureTime",
        default="0",
        gui_group="Acquisition Parameters",
    ),
    BuiltinArgumentSpec(
        name="frames_per_trigger",
        default_arg_type="FramesPerTrigger",
        default="1",
        gui_group="Acquisition Parameters",
    ),
    BuiltinArgumentSpec(
        name="settling_time",
        default_arg_type="SettlingTime",
        default="0",
        gui_group="Acquisition Parameters",
    ),
    BuiltinArgumentSpec(
        name="settling_time_after_trigger",
        default_arg_type="SettlingTimeAfterTrigger",
        default="0",
        gui_group="Acquisition Parameters",
    ),
    BuiltinArgumentSpec(
        name="readout_time",
        default_arg_type="ReadoutTime",
        default="0",
        gui_group="Acquisition Parameters",
    ),
    BuiltinArgumentSpec(
        name="burst_at_each_point",
        default_arg_type="BurstAtEachPoint",
        default="1",
        gui_group="Acquisition Parameters",
    ),
    BuiltinArgumentSpec(name="relative", default_arg_type="Relative", gui_group="Scan Parameters"),
)
_RESERVED_ARGUMENT_NAMES = frozenset(argument.name for argument in _BUILTIN_ARGUMENTS) | (
    _HIDDEN_BUILTIN_ARGUMENT_NAMES
)


def _snake_to_pascal(value: str) -> str:
    """Convert a snake-case identifier to PascalCase.

    Args:
        value (str): Identifier in snake_case.

    Returns:
        str: PascalCase version of ``value``.
    """
    return "".join(part.capitalize() for part in value.split("_"))


def _normalize_identifier(name: str, target: str) -> str:
    """Normalize and validate a user-provided identifier.

    Args:
        name (str): Raw identifier entered by the user.
        target (str): Human-readable target name for error messages.

    Returns:
        str: Lowercase identifier with dashes replaced by underscores.

    Raises:
        typer.Exit: If the normalized name is not a valid Python identifier.
    """
    formatted_name = name.lower().replace("-", "_")
    if formatted_name != name:
        logger.warning(f"Adjusting {target} name from {name} to {formatted_name}")
    if not formatted_name.isidentifier():
        logger.error(
            f"{name} is not a valid name for a {target} (even after converting to {formatted_name}). "
            "Please enter a valid Python identifier using only letters, numbers and underscores, and not "
            "starting with a number, e.g. 'my_scan', 'scan1', 'my_scan_v2', ..."
        )
        raise typer.Exit(code=1)
    return formatted_name


def _normalize_unit(unit: str) -> str | None:
    """Validate and normalize a user-provided unit string.

    Args:
        unit (str): Raw unit text entered by the user.

    Returns:
        str | None: Unit attribute name from :class:`Units`, or ``None`` if omitted.

    Raises:
        typer.Exit: If the provided unit does not exist on :class:`Units`.
    """
    stripped = unit.strip()
    if not stripped or stripped.lower() == "none":
        return None

    try:
        parsed_unit = Units.parse_units(stripped)
    except Exception:
        logger.error(f"{unit} is not a valid unit.")
        raise typer.Exit(code=1) from None

    return f"{parsed_unit:~P}"


def _prompt_for_builtin_arguments() -> list[BuiltinArgumentSpec]:
    """Prompt the user to select built-in scan arguments.

    Returns:
        list[BuiltinArgumentSpec]: Selected built-in argument specifications.

    Raises:
        typer.Exit: If the interactive questionary prompt is cancelled.
    """
    if not typer.confirm(
        "Do you want to include built-in scan arguments (e.g. exp_time, relative, ...)?",
        default=False,
    ):
        return []

    selected = questionary.checkbox(
        "Select built-in scan arguments",
        choices=[
            questionary.Choice(title=argument.selection_label, value=argument)
            for argument in _BUILTIN_ARGUMENTS
        ],
        style=_QUESTIONARY_STYLE,
        instruction="Use space to toggle, arrows to move, enter to confirm",
    ).ask()
    if selected is None:
        raise typer.Exit(code=1)

    # we need to sort the selected arguments based on default arguments as we have to specify
    # kwargs without defaults before kwargs with defaults in the generated function signature
    selected.sort(key=lambda argument: argument.default is not None)
    return selected


def _prompt_for_custom_arguments() -> list[CustomArgumentSpec]:
    """Prompt the user for custom scan arguments.

    Returns:
        list[CustomArgumentSpec]: Custom argument specifications in prompt order.

    Raises:
        typer.Exit: If the user provides an invalid name, duplicate name, invalid unit,
            or cancels one of the interactive selectors.
    """
    arguments: list[CustomArgumentSpec] = []
    if not typer.confirm("Do you want to set up scan arguments?", default=True):
        return arguments

    while True:
        while True:
            try:
                arg_name = _normalize_identifier(typer.prompt("Enter argument name"), "argument")
            except typer.Exit:
                continue
            if arg_name in _RESERVED_ARGUMENT_NAMES:
                logger.error(
                    f"Argument {arg_name} is already provided as a built-in scan argument."
                )
                continue
            if any(existing.name == arg_name for existing in arguments):
                logger.error(f"Argument {arg_name} already exists.")
                continue
            break
        arg_type = _select_option("Select argument type", _ARGUMENT_TYPES)
        arg_units = None
        if arg_type in {"float", "int"}:
            while True:
                try:
                    arg_units = _normalize_unit(
                        typer.prompt(
                            "Enter argument units, e.g. 's', 'microns', 'mm', 'T/min', ...",
                            default="None",
                        )
                    )
                except typer.Exit:
                    continue
                break
        arg_description = typer.prompt(
            "Enter argument description", default=arg_name.replace("_", " ").capitalize() + "."
        )
        arguments.append(
            CustomArgumentSpec(name=arg_name, type=arg_type, doc=arg_description, units=arg_units)
        )
        if not typer.confirm("Add another argument?", default=False):
            return arguments


def _ensure_scan_export(init_file: Path, name: str) -> None:
    """Ensure that the generated scan is exported from the scans package.

    Args:
        init_file (Path): Path to the package ``__init__.py`` file.
        name (str): Snake-case scan name to export.
    """
    class_name = _snake_to_pascal(name)
    import_line = f"from .{name} import {class_name}\n"
    if init_file.exists():
        content = init_file.read_text(encoding="utf-8")
    else:
        content = ""

    if import_line in content.splitlines(keepends=True):
        return

    new_content = content
    if new_content and not new_content.endswith("\n"):
        new_content += "\n"
    new_content += import_line
    init_file.parent.mkdir(parents=True, exist_ok=True)
    init_file.write_text(new_content, encoding="utf-8")


def verify_dependencies() -> None:
    """
    Verify that all dependencies for scan creation are available.

    We verify the dependencies manually to avoid that the user writes the
    scan and only finds out at the end that they forgot to install one of the required packages.
    """
    try:
        # pylint: disable=import-outside-toplevel,unused-import
        import black
        import isort
    except ImportError as e:
        logger.error(f"Missing dependency: {e.name}. Please install it to use this command.")
        raise typer.Exit(code=1) from None


@_app.command()
def scan(
    name: Annotated[
        str | None, typer.Argument(help="Enter a name for your scan in snake_case")
    ] = None,
):
    """Create a new v4 scan plugin.

    Args:
        name (str | None): Optional scan name in snake_case. If omitted, the command
            prompts for it interactively.

    Raises:
        typer.Exit: If the user cancels an interactive prompt or provides invalid input.
    """
    try:
        verify_dependencies()
        repo = Path(plugin_repo_path())
        template_dir = Path(__file__).with_name("templates")
        plugin_components_class, plugin_components_import = _get_plugin_components_config()
        scan_name = _normalize_identifier(name or typer.prompt("Scan name"), "scan")
        scans_dir = repo / repo.name / "scans"
        scan_file = scans_dir / f"{scan_name}.py"
        init_file = scans_dir / "__init__.py"
        if scan_file.exists():
            if not typer.confirm(f"Scan {scan_name} already exists. Override it?", default=False):
                raise typer.Exit(code=1)

        description = typer.prompt("Scan description", default="Scan implementation.")
        scan_type = _select_option("Scan type", _SCAN_TYPES)
        builtin_arguments = _prompt_for_builtin_arguments()
        custom_arguments = _prompt_for_custom_arguments()
        config = ScanConfig(
            name=scan_name,
            description=description,
            scan_type=scan_type,
            template_arguments=custom_arguments + builtin_arguments,
            plugin_components_class=plugin_components_class,
            plugin_components_import=plugin_components_import,
        )

        logger.info(f"Adding new scan {config.name}...")
        scan_file.parent.mkdir(parents=True, exist_ok=True)
        scan_file.write_text(_render_scan(template_dir, config), encoding="utf-8")
        _ensure_scan_export(init_file, config.name)
        run_formatters(repo, [str(scan_file.relative_to(repo)), str(init_file.relative_to(repo))])
    except typer.Exit:
        raise
    except Exception:
        logger.error(traceback.format_exc())
        logger.error("exiting...")
        raise typer.Exit(code=1) from None

    logger.success(f"Added scan {config.name}!")
