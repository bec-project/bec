"""Resolve, validate, execute and export caller-supplied sequence documents."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from ..components import Schedule
from .catalog import ScanCatalog
from .description import describe
from .python_export import export_python
from .resolver import DefinitionLibrary, Resolved
from .runtime import Session
from .validation import validate

if TYPE_CHECKING:
    from bec_ipython_client import BECIPythonClient


class Sequencer:
    """Validate, execute and export sequences using a BEC IPython client.

    Args:
        client (BECIPythonClient): Client providing current scan metadata and device proxies.
        definitions (DefinitionLibrary | None): Supplied reusable definitions.
    """

    def __init__(
        self, client: BECIPythonClient, definitions: DefinitionLibrary | None = None
    ) -> None:
        """Bind the sequencer to the client's live scans and device manager.

        Args:
            client (BECIPythonClient): Client providing native scans and device proxies.
            definitions (DefinitionLibrary | None): Supplied reusable definitions.
        """
        self.client = client
        self._catalog = None
        self.definitions = definitions or DefinitionLibrary()

    @classmethod
    def from_catalog(
        cls, catalog: ScanCatalog, definitions: DefinitionLibrary | None = None
    ) -> Sequencer:
        """Create an offline sequencer for validation and inspection.

        Execution and Python exports containing Set nodes require a client.

        Args:
            catalog (ScanCatalog): Advertised metadata used for offline validation.
            definitions (DefinitionLibrary | None): Supplied reusable definitions.

        Returns:
            Sequencer: Offline sequencer using the supplied metadata.
        """
        sequencer = cls.__new__(cls)
        sequencer.client = None
        sequencer._catalog = catalog
        sequencer.definitions = definitions or DefinitionLibrary()
        return sequencer

    @property
    def catalog(self) -> ScanCatalog:
        """Return current client metadata, or the supplied offline catalog.

        Returns:
            ScanCatalog: Current catalog used for validation and inspection.
        """
        if self.client is not None:
            return ScanCatalog.from_client(self.client)
        return self._catalog

    def validate(self, schedule: Schedule | dict | str) -> Resolved:
        """Resolve and validate the entire tree before execution.

        Args:
            schedule (Schedule | dict | str): Sequence model, decoded document or JSON text.

        Returns:
            Resolved: Validated tree with transitive definition references resolved.
        """
        schedule = (
            Schedule.model_validate_json(schedule)
            if isinstance(schedule, str)
            else Schedule.model_validate(schedule)
        )
        tree, _ = self.definitions.resolve(schedule.root)
        device_manager = self.client.device_manager if self.client is not None else None
        validate(tree, self.catalog, device_manager)
        return tree

    def execute(self, schedule: Schedule | dict | str) -> None:
        """Execute a validated sequence using the bound client's native calls.

        Args:
            schedule (Schedule | dict | str): Sequence model, decoded document or JSON text.

        Raises:
            ValueError: If the sequencer was created without a client.
        """
        if self.client is None:
            raise ValueError("Sequence execution requires a live client")
        tree = self.validate(schedule)
        with Session(self.client) as session:
            session.execute(tree)

    # pylint: disable=redefined-builtin
    def export(
        self, schedule: Schedule | dict | str, *, format: Literal["python", "description"]
    ) -> str:
        """Export a validated sequence as native Python or a description.

        Args:
            schedule (Schedule | dict | str): Sequence model, decoded document or JSON text.
            format (Literal['python', 'description']): Requested export format.

        Returns:
            str: Deterministic source code or procedural description.
        """
        tree = self.validate(schedule)
        if format == "python":
            device_manager = self.client.device_manager if self.client is not None else None
            return export_python(tree, device_manager)
        if format == "description":
            return describe(tree)
        raise ValueError(f"Unsupported export format: {format}")
