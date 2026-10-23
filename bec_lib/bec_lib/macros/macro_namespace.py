import importlib
import importlib.util
import inspect
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Callable

from IPython.core.interactiveshell import InteractiveShell
from IPython.lib import deepreload

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.macros.config_model import (
    PLUGIN_MACROS_DIR_PLACEHOLDER,
    USER_MACROS_DIR_PLACEHOLDER,
    DirImportMacro,
    FileImportMacro,
    MacroConfig,
    MacroNsSpec,
    MacroRef,
    ModuleImportMacro,
    parse_macro_config,
)
from bec_lib.messages import MacroConfigMessage
from bec_lib.plugin_helper import default_macro_config_path, plugin_macros_dir
from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger

# Deep reloading these would replace live classes in a running session, so isinstance checks and
# existing object references would silently break. deepreload matches names exactly, so every
# already-imported submodule has to be listed.
_PROTECTED_PREFIXES = (
    "bec_lib",
    "bec_server",
    "bec_ipython_client",
    "bec_widgets",
    "ophyd",
    "ophyd_devices",
    "IPython",
    "numpy",
    "pydantic",
)


def _reload_excludes() -> tuple[str, ...]:
    """Modules which `deepreload` must not touch when reloading a macro module."""
    return (
        *sys.builtin_module_names,
        "sys",
        "os.path",
        "builtins",
        "__main__",
        *(name for name in list(sys.modules) if name.startswith(_PROTECTED_PREFIXES)),
    )


def _deep_reload(module: ModuleType) -> ModuleType:
    """Recursively reload a macro module and the modules it imports"""
    logger.info(f"Deep reloading macro module {module.__name__}")
    return deepreload.reload(module, exclude=_reload_excludes())


class BecMacroNamespace:
    def __init__(self, connector: RedisConnector, user_macros_dir: Path | None = None) -> None:
        """
        Args:
            connector (RedisConnector): connector used to share the macro config between clients
            user_macros_dir (Path | None): the local user macro directory which a config may refer
                to with the `<user_macros>` placeholder; usually from the service config
        """
        self._connector = connector
        self._user_macros_dir = user_macros_dir
        self._loaded = False
        self._loaded_names: set[str] = set()
        self._config: MacroConfig | None = None
        # pushed name -> macro, kept so that exactly these objects can be dropped again on reload
        self._in_interactive_client_global_namespace: dict[str, Callable[[Any], Any]] = {}
        self._interactive_namespace_ref: InteractiveShell | None = None
        # the stream only ever holds the current config, so reading it from the start hands a newly
        # started client the config the other clients are using
        self._connector.register(
            MessageEndpoints.macro_config(), cb=self._on_config_message, from_start=True
        )

    def shutdown(self):
        """Stop listening for macro config updates from other clients."""
        self._connector.unregister(MessageEndpoints.macro_config(), cb=self._on_config_message)

    def load_default(self):
        self.load_config(default_macro_config_path())

    def load_config(self, config_path: Path):
        """Load a macro config file, apply it, and publish it so that all other clients apply it too.

        Args:
            config_path (Path): path to the YAML macro config file
        """
        with open(config_path) as f:
            config = parse_macro_config(f)
        self._apply_config(config)
        self._connector.xadd(
            MessageEndpoints.macro_config(),
            {"data": MacroConfigMessage(config=config.model_dump_json())},
            max_size=1,
        )

    def _apply_config(self, config: MacroConfig):
        self._unload_macros()
        self._config = config
        self.reload()

    def _on_config_message(self, msg_dict: dict):
        """Apply a macro config published by another client, unless it is the one already loaded."""
        config = MacroConfig.model_validate_json(msg_dict["data"].config)
        if config == self._config:
            return
        logger.info("Received a new macro config, reloading macros.")
        try:
            self._apply_config(config)
        except Exception as e:
            logger.error(f"Failed to apply the received macro config: {e}")

    def reload(self):
        """Unload all macros and load them again from the owned config. Macro modules are deep
        reloaded, so changes made to them on disk are picked up. Globals in an attached interactive
        shell are dropped and pushed again."""
        self._unload_macros()
        self._load_macros()
        if self._interactive_namespace_ref is not None:
            self.populate_globals_in_ipython(self._interactive_namespace_ref)

    def populate_globals_in_ipython(self, shell: InteractiveShell):
        """Push the macros configured as `global_in_interactive_shell` into the user namespace of an
        IPython shell. Macros in a nested namespace are pushed under their own name, i.e.
        `alignment.align_x` becomes the global `align_x`.

        Args:
            shell (InteractiveShell): the shell to push the macros into. A reference is stored so
                that the globals can be dropped and pushed again on reload.
        """
        if self._config is None:
            raise ValueError("Macro config must be loaded before populating globals!")
        self._drop_globals_in_ipython()
        self._interactive_namespace_ref = shell
        self._in_interactive_client_global_namespace = {
            ref.rsplit(".", 1)[-1]: self._resolve_macro(ref)
            for ref in self._config.global_in_interactive_shell
        }
        shell.push(self._in_interactive_client_global_namespace)

    def _drop_globals_in_ipython(self):
        """Remove the macros which were pushed into the attached interactive shell, if any. Globals
        which the user has since rebound to something else are left alone."""
        if (
            self._interactive_namespace_ref is None
            or not self._in_interactive_client_global_namespace
        ):
            return
        self._interactive_namespace_ref.drop_by_id(self._in_interactive_client_global_namespace)
        self._in_interactive_client_global_namespace = {}

    def _resolve_macro(self, ref: str) -> Callable[[Any], Any]:
        """Look up a dotted macro reference, e.g. 'alignment.align_x', in this namespace.

        Args:
            ref (str): dotted path of the macro, relative to this namespace

        Returns:
            Callable[[Any], Any]: the loaded macro
        """
        macro = self
        for part in ref.split("."):
            macro = getattr(macro, part)
        return macro  # type: ignore # the config model validates that this is a macro, not a namespace

    def _load_macros(self):
        """Populate the macro namespace from the owned config."""
        if self._loaded:
            logger.warning("Macros are already loaded! Unload first to reload!")
            return
        if self._config is None:
            raise ValueError("Macro config must be loaded before attempting to load macros!")
        # If _add_macros... succeeds, they are added to this namespace all at once, so if _loaded is
        # True, _loaded_names are exactly the loaded macros. The temporary holder lives only for
        # this pass, so macros sharing a module all come from the same, once-reloaded, module object
        self._loaded_names = self._add_macros_to_namespace(self, self._config.macros, {})
        self._loaded = True

    def _add_macros_to_namespace(
        self, namespace: object, macros: MacroNsSpec, loaded_modules: dict[str, ModuleType]
    ) -> set[str]:
        """Load the given macros and set them on the namespace, returning the names which were set.
        Directory references are merged into the namespace, so these can differ from the keys."""
        loaded_macros: dict[str, Callable[[Any], Any] | SimpleNamespace] = {}
        for name, macro in macros.items():
            if isinstance(macro, DirImportMacro):
                new_macros = self._load_dir_macros(
                    self._resolve_dir(macro.reference), loaded_modules
                )
            else:
                new_macros = {name: self._load_macro_ref_or_spec(macro, loaded_modules)}
            for new_name in new_macros:
                if new_name in loaded_macros or hasattr(namespace, new_name):
                    raise ValueError(
                        f"'{new_name}' is not a valid macro or macro namespace name, or is defined more than once! Please update the macro config."
                    )
            loaded_macros.update(new_macros)
        for name, macro in loaded_macros.items():
            setattr(namespace, name, macro)
        return set(loaded_macros)

    def _resolve_dir(self, reference: str) -> Path | None:
        """Resolve a directory reference, replacing the placeholders with the directories of this
        client. Returns None, with a warning, if a placeholder cannot be resolved here."""
        if reference == USER_MACROS_DIR_PLACEHOLDER:
            if self._user_macros_dir is None:
                logger.warning(
                    f"No user macro directory is configured, skipping macros from {reference}"
                )
            return self._user_macros_dir
        if reference == PLUGIN_MACROS_DIR_PLACEHOLDER:
            try:
                return plugin_macros_dir()
            except ValueError as e:
                logger.warning(f"Skipping macros from {reference}: {e}")
                return None
        return Path(reference).expanduser().resolve()

    def _load_dir_macros(
        self, path: Path | None, loaded_modules: dict[str, ModuleType]
    ) -> dict[str, SimpleNamespace]:
        """Load a directory of macro files: each .py file becomes a namespace of its public
        functions, each subdirectory a nested namespace. Names which are not valid identifiers, or
        start with an underscore or a dot, are skipped."""
        if path is None:
            return {}
        if not path.is_dir():
            logger.warning(f"Macro directory {path} does not exist, skipping it")
            return {}
        macros: dict[str, SimpleNamespace] = {}
        for entry in sorted(path.iterdir()):
            name = entry.stem if entry.is_file() else entry.name
            if name.startswith(("_", ".")) or not (entry.is_dir() or entry.suffix == ".py"):
                continue
            if not name.isidentifier():
                logger.warning(f"Skipping {entry}: {name!r} is not a valid Python identifier")
                continue
            if entry.is_dir():
                macros[name] = SimpleNamespace(**self._load_dir_macros(entry, loaded_modules))
            else:
                macros[name] = self._load_file_macro(  # type: ignore # func_name is None
                    FileImportMacro(reference=entry, func_name=None), loaded_modules
                )
        return macros

    def _load_macro_ref_or_spec(
        self, macro: MacroRef | MacroNsSpec, loaded_modules: dict[str, ModuleType]
    ) -> Callable[[Any], Any] | SimpleNamespace:
        if isinstance(macro, dict):
            macro_ns = SimpleNamespace()
            self._add_macros_to_namespace(macro_ns, macro, loaded_modules)
            return macro_ns
        if isinstance(macro, ModuleImportMacro):
            return self._load_module_macro(macro, loaded_modules)
        return self._load_file_macro(macro, loaded_modules)

    def _load_module_macro(
        self, macro: ModuleImportMacro, loaded_modules: dict[str, ModuleType]
    ) -> Callable[[Any], Any] | SimpleNamespace:
        module = loaded_modules.get(macro.reference)
        if module is None:
            cached = sys.modules.get(macro.reference)
            module = (
                _deep_reload(cached)
                if cached is not None
                else importlib.import_module(macro.reference)
            )
            loaded_modules[macro.reference] = module
        return self._macro_from_module(module, macro.func_name)

    def _load_file_macro(
        self, macro: FileImportMacro, loaded_modules: dict[str, ModuleType]
    ) -> Callable[[Any], Any] | SimpleNamespace:
        path = macro.reference.expanduser().resolve()
        module = loaded_modules.get(str(path))
        if module is None:
            # no deepreload here: the file is executed afresh on every load anyway, and its
            # synthetic module name cannot be resolved by the import machinery deepreload relies on
            module_name = f"_bec_macros_{path.stem}"
            spec = importlib.util.spec_from_file_location(module_name, path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load a macro module from file {path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            loaded_modules[str(path)] = module
        return self._macro_from_module(module, macro.func_name)

    def _macro_from_module(
        self, module: ModuleType, func_name: str | None
    ) -> Callable[[Any], Any] | SimpleNamespace:
        if func_name is None:
            return SimpleNamespace(
                **{
                    name: func
                    for name, func in inspect.getmembers(module, inspect.isfunction)
                    if not name.startswith("_") and func.__module__ == module.__name__
                }
            )
        func = getattr(module, func_name, None)
        if not callable(func):
            raise ValueError(f"No callable {func_name!r} found in {module.__name__}!")
        return func

    def _unload_macros(self):
        """Destroy macro references in the interactive shell, then clear the macro namespace."""
        if not self._loaded:
            return
        if self._config is None:
            raise ValueError(
                "The loaded macro config seems to have been deleted while macros were loaded! This shouldn't be possible, please contact the BEC developers."
            )
        self._drop_globals_in_ipython()
        for macro_name in self._loaded_names:
            delattr(self, macro_name)
        self._loaded_names = set()
        self._loaded = False
