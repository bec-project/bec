"""Resolve, validate, execute and export caller-supplied sequence documents."""

from __future__ import annotations

from typing import Literal

from ..components import Schedule
from .adapters import BECAdapter
from .catalog import ScanCatalog
from .description import describe
from .python_export import export_python
from .resolver import DefinitionLibrary, Resolved
from .runtime import Session
from .validation import validate


class Sequencer:
    """Validate and execute externally composed JSON sequences.

    Args:
        catalog (ScanCatalog): Catalog used for advertised scan validation.
        definitions (DefinitionLibrary | None): Supplied reusable definitions. Defaults to no
            definitions.
    """

    def __init__(self, catalog: ScanCatalog, definitions: DefinitionLibrary | None = None) -> None:
        """Initialize Sequencer parameters.

        Args:
            catalog (ScanCatalog): Catalog used for advertised scan validation.
            definitions (DefinitionLibrary | None): Supplied reusable definitions. Defaults to no
                definitions.
        """
        self.catalog = catalog
        self.definitions = definitions or DefinitionLibrary()

    def validate(
        self, schedule: Schedule | dict | str, adapter: BECAdapter | None = None
    ) -> Resolved:
        """Resolve all dependencies and validate the entire tree before execution.

        Args:
            schedule (Schedule | dict | str): Received sequence model, decoded JSON document or JSON
                text.
            adapter (BECAdapter | None): BEC execution adapter providing live proxy validation and
                client calls.

        Returns:
            Resolved: Validated tree with transitive definition references resolved.
        """
        schedule = (
            Schedule.model_validate_json(schedule)
            if isinstance(schedule, str)
            else Schedule.model_validate(schedule)
        )
        tree, _ = self.definitions.resolve(schedule.root)
        catalog = self.catalog if adapter is None else adapter.catalog()
        validate(tree, catalog, adapter)
        return tree

    def execute(self, schedule: Schedule | dict | str, adapter: BECAdapter) -> None:
        """Execute a validated sequence with native IPython calls and scoped statuses.

        Args:
            schedule (Schedule | dict | str): Received sequence model, decoded JSON document or JSON
                text.
            adapter (BECAdapter): BEC execution adapter providing live proxy validation and client
                calls.
        """
        tree = self.validate(schedule, adapter)
        with Session(adapter) as session:
            session.execute(tree)

    # pylint: disable=redefined-builtin
    def export(
        self, schedule: Schedule | dict | str, *, format: Literal["python", "description"]
    ) -> str:
        """Export deterministically using the same resolved semantics as execution.

        Args:
            schedule (Schedule | dict | str): Received sequence model, decoded JSON document or JSON
                text.
            format (Literal['python', 'description']): Requested export format: python or description.

        Returns:
            str: Deterministic source code or procedural description.
        """
        tree = self.validate(schedule)
        if format == "python":
            return export_python(tree, self.catalog.device_manager)
        if format == "description":
            return describe(tree)
        raise ValueError(f"Unsupported export format: {format}")
