from importlib import import_module
from typing import Any

_LAZY_EXPORTS = {
    "lazy_import": ("bec_lib.utils.import_utils", "lazy_import"),
    "lazy_import_from": ("bec_lib.utils.import_utils", "lazy_import_from"),
    "scan_to_csv": ("bec_lib.utils.scan_utils", "scan_to_csv"),
    "scan_to_dict": ("bec_lib.utils.scan_utils", "scan_to_dict"),
    "threadlocked": ("bec_lib.utils.threading_utils", "threadlocked"),
    "user_access": ("bec_lib.utils.rpc_utils", "user_access"),
}

__all__ = sorted(_LAZY_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
