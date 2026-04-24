import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import jinja2
import questionary
import typer

from bec_lib.logger import bec_logger
from bec_lib.plugin_helper import plugin_repo_path
from bec_lib.scan_args import Units
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


@dataclass(frozen=True)
class ScanArgumentSpec:
    """Describe a scan argument to be rendered into the generated template.

    Args:
        name (str): Argument name used in the generated scan signature.
        type (str): Logical argument type selected by the user.
        annotation_override (str | None): Explicit annotation string for template output.
        default (str | None): Default value rendered into the generated signature.
        doc (str | None): Human-readable argument description for docstrings and UI metadata.
        units (str | None): Optional unit name from :class:`bec_lib.scan_args.Units`.
        gui_group (str): GUI group label used in the generated ``gui_config``.
    """

    name: str
    type: str
    annotation_override: str | None = None
    default: str | None = None
    doc: str | None = None
    units: str | None = None
    gui_group: str = "Scan Parameters"

    @property
    def unit_expression(self) -> str | None:
        """Return the generated ``Units`` expression for the configured unit.

        Returns:
            str | None: Python expression using ``Units.<name>`` attributes, or ``None``.
        """
        if not self.units:
            return None

        parsed_unit = Units.parse_units(self.units)
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
        """Build the annotation string for the generated scan argument.

        Returns:
            str: Rendered ``Annotated[...]`` expression for the argument.
        """
        if self.annotation_override:
            return self.annotation_override
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
                f"description={self.doc_line!r}{units})]"
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
    def display_name(self) -> str:
        """Return a title-cased label derived from the argument name.

        Returns:
            str: Display label for prompts and generated metadata.
        """
        return self.name.replace("_", " ").title()

    @property
    def doc_type(self) -> str:
        """Return the type name used in generated docstrings.

        Returns:
            str: Type representation shown in the generated ``Args`` section.
        """
        return {"DeviceBase": "DeviceBase", "np.ndarray": "np.ndarray"}.get(self.type, self.type)

    @property
    def doc_line(self) -> str:
        """Return the description line used in prompts and generated docs.

        Returns:
            str: Argument description with a fallback derived from ``name``.
        """
        return self.doc or f"{self.display_name}."

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
class ScanConfig:
    """Collect the values required to render a new scan template.

    Args:
        name (str): Snake-case scan name used for the file, class, and scan identifier.
        description (str): Module docstring description for the generated scan.
        summary (str): Class docstring summary for the generated scan.
        scan_type (str): Selected v4 scan type enum member name.
        template_arguments (list[ScanArgumentSpec]): Arguments rendered into the template.
    """

    name: str
    description: str
    summary: str
    scan_type: str
    template_arguments: list[ScanArgumentSpec]


class TemplateGenerator:
    """Render scan source code from local Jinja templates.

    Args:
        template_dir (Path): Directory containing the scan template files.
    """

    def __init__(self, template_dir: Path):
        self._environment = jinja2.Environment(
            loader=jinja2.FileSystemLoader(template_dir),
            extensions=[CopierFilters],
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render_scan(self, config: ScanConfig) -> str:
        """Render the v4 scan template for the provided configuration.

        Args:
            config (ScanConfig): User-selected scan configuration.

        Returns:
            str: Fully rendered Python source code for the generated scan.
        """
        template = self._environment.get_template("v4_scan.py.jinja")
        return (
            template.render(
                name=config.name,
                description=config.description,
                summary=config.summary,
                scan_type=config.scan_type,
                arguments=config.template_arguments,
            ).rstrip()
            + "\n"
        )


_BUILTIN_ARGUMENTS = (
    ScanArgumentSpec(
        name="exp_time",
        type="float",
        annotation_override='Annotated[float | None, ScanArgument(display_name="Exposure Time", description="Exposure time for the scan. Defaults to None.", units=Units.s, ge=0)]',
        default="None",
        doc="Exposure time for the scan. Defaults to None.",
        gui_group="Acquisition Parameters",
    ),
    ScanArgumentSpec(
        name="frames_per_trigger",
        type="int",
        annotation_override='Annotated[int | None, ScanArgument(display_name="Frames Per Trigger", description="Number of frames per trigger. Defaults to None.", ge=1)]',
        default="None",
        doc="Number of frames per trigger. Defaults to None.",
        gui_group="Acquisition Parameters",
    ),
    ScanArgumentSpec(
        name="settling_time",
        type="float",
        annotation_override='Annotated[float | None, ScanArgument(display_name="Settling Time", description="Settling time before the software trigger. Defaults to None.", units=Units.s, ge=0)]',
        default="None",
        doc="Settling time before the software trigger. Defaults to None.",
        gui_group="Acquisition Parameters",
    ),
    ScanArgumentSpec(
        name="settling_time_after_trigger",
        type="float",
        annotation_override='Annotated[float | None, ScanArgument(display_name="Settling Time After Trigger", description="Settling time after the software trigger. Defaults to None.", units=Units.s, ge=0)]',
        default="None",
        doc="Settling time after the software trigger. Defaults to None.",
        gui_group="Acquisition Parameters",
    ),
    ScanArgumentSpec(
        name="burst_at_each_point",
        type="int",
        annotation_override='Annotated[int | None, ScanArgument(display_name="Burst At Each Point", description="Number of bursts at each point. Defaults to None.", ge=1)]',
        default="None",
        doc="Number of bursts at each point. Defaults to None.",
        gui_group="Acquisition Parameters",
    ),
    ScanArgumentSpec(
        name="relative",
        type="bool",
        annotation_override='Annotated[bool | None, ScanArgument(display_name="Relative", description="Whether the positions are relative or absolute. Defaults to None.")]',
        default="None",
        doc="Whether the positions are relative or absolute. Defaults to None.",
        gui_group="Scan Parameters",
    ),
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
            f"{name} is not a valid name for a {target} (even after converting to {formatted_name}) - please enter something in snake_case"
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


def _prompt_for_builtin_arguments() -> list[ScanArgumentSpec]:
    """Prompt the user to select built-in scan arguments.

    Returns:
        list[ScanArgumentSpec]: Selected built-in argument specifications.

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
    return selected


def _prompt_for_custom_arguments() -> list[ScanArgumentSpec]:
    """Prompt the user for custom scan arguments.

    Returns:
        list[ScanArgumentSpec]: Custom argument specifications in prompt order.

    Raises:
        typer.Exit: If the user provides an invalid name, duplicate name, invalid unit,
            or cancels one of the interactive selectors.
    """
    arguments: list[ScanArgumentSpec] = []
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
        arg_type = questionary.select(
            "Select argument type", choices=list(_ARGUMENT_TYPES), style=_QUESTIONARY_STYLE
        ).ask()
        if arg_type is None:
            raise typer.Exit(code=1)
        arg_units = None
        if arg_type in {"float", "int", "bool"}:
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
            ScanArgumentSpec(name=arg_name, type=arg_type, doc=arg_description, units=arg_units)
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

    lines = [line for line in content.splitlines(keepends=True) if line.strip()]
    if import_line in lines:
        return

    lines.append(import_line)
    lines.sort()
    new_content = "".join(lines)
    init_file.parent.mkdir(parents=True, exist_ok=True)
    init_file.write_text(new_content, encoding="utf-8")


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
        repo = Path(plugin_repo_path())
        template_generator = TemplateGenerator(Path(__file__).with_name("templates"))
        scan_name = _normalize_identifier(name or typer.prompt("Scan name"), "scan")
        scans_dir = repo / repo.name / "scans"
        scan_file = scans_dir / f"{scan_name}.py"
        init_file = scans_dir / "__init__.py"
        if scan_file.exists():
            if not typer.confirm(f"Scan {scan_name} already exists. Override it?", default=False):
                raise typer.Exit(code=1)

        description = typer.prompt("Scan description", default="V4 scan implementation.")
        summary = typer.prompt("Scan summary", default="Describe the scan here.")
        scan_type = questionary.select(
            "Scan type", choices=list(_SCAN_TYPES), style=_QUESTIONARY_STYLE
        ).ask()
        if scan_type is None:
            raise typer.Exit(code=1)
        builtin_arguments = _prompt_for_builtin_arguments()
        custom_arguments = _prompt_for_custom_arguments()
        config = ScanConfig(
            name=scan_name,
            description=description,
            summary=summary,
            scan_type=scan_type,
            template_arguments=custom_arguments + builtin_arguments,
        )

        logger.info(f"Adding new scan {config.name}...")
        scan_file.parent.mkdir(parents=True, exist_ok=True)
        scan_file.write_text(template_generator.render_scan(config), encoding="utf-8")
        _ensure_scan_export(init_file, config.name)
        run_formatters(repo, [str(scan_file.relative_to(repo)), str(init_file.relative_to(repo))])
    except typer.Exit:
        raise
    except Exception:
        logger.error(traceback.format_exc())
        logger.error("exiting...")
        raise typer.Exit(code=1) from None

    logger.success(f"Added scan {config.name}!")
