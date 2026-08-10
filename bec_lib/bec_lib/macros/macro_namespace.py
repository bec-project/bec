from pathlib import Path

from IPython.lib import deepreload

from bec_lib.macros.config_model import MacroConfig, parse_macro_config
from bec_lib.plugin_helper import default_macro_config_path


class BecMacroNamespace:
    def __init__(self) -> None:
        self._config: MacroConfig | None = None
        self._in_builtins: set[str] = set()

    def load_default(self):
        self.load_config(default_macro_config_path())

    def load_config(self, config_path: Path):
        with open(config_path) as f:
            config = parse_macro_config(f)
            if self._config is not None:
                self._unload_macros()
            self._config = config
            self._load_macros()

    def _load_macros(self):
        """Populate the macro namespace from the owned config. Stash references to macros added
        to builtins for later removal."""

    def _unload_macros(self):
        """Destroy macro references in builtins, then clear the macro namespace."""
