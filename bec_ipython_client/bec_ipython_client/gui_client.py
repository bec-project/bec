"""Deferred GUI access for the interactive BEC client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from bec_widgets.cli.client_utils import BECGuiClient


class LazyBECGuiClient:
    """Import and create the GUI client on first use, independently of the shell namespace."""

    def __init__(self, gui_id: str | None = None) -> None:
        self._gui_id = gui_id
        self._client: BECGuiClient | None = None

    def get_client(self) -> BECGuiClient:
        """Return the GUI client, importing BEC Widgets when first requested.

        Returns:
            BECGuiClient: The initialized GUI client.

        Raises:
            ImportError: If BEC Widgets or one of its dependencies cannot be imported.
        """
        if self._client is None:
            try:
                # Import only when GUI access is requested.
                # pylint: disable=import-outside-toplevel
                from bec_widgets.cli.client_utils import BECGuiClient
            except ImportError as exc:
                raise ImportError(
                    "BEC Widgets is required to use the GUI. Install bec-widgets in the "
                    f"Python environment running this client. Import failed: {exc}"
                ) from exc

            client = BECGuiClient()
            if self._gui_id:
                client.connect_to_gui_server(self._gui_id)
            self._client = client
        return self._client

    def close(self) -> None:
        """Close an initialized GUI client without creating one during shutdown."""
        if self._client is not None:
            self._client.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.get_client(), name)

    def __dir__(self) -> list[str]:
        if self._client is None:
            return super().__dir__()
        return dir(self._client)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            setattr(self.get_client(), name, value)

    def __repr__(self) -> str:
        if self._client is None:
            return "LazyBECGuiClient(uninitialized)"
        return repr(self._client)
