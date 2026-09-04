from collections.abc import Iterable
from importlib import import_module
from typing import Any, overload

from bec_lib.utils.proxy import Proxy


def lazy_import(module_name: str) -> Proxy:
    return Proxy(lambda: import_module(module_name), init_once=True)


@overload
def lazy_import_from(module_name: str, from_list: str) -> Proxy: ...


@overload
def lazy_import_from(module_name: str, from_list: Iterable[str]) -> tuple[Proxy, ...] | Proxy: ...


def lazy_import_from(module_name: str, from_list: str | Iterable[str]) -> tuple[Proxy, ...] | Proxy:
    names = (from_list,) if isinstance(from_list, str) else tuple(from_list)
    proxies = tuple(
        Proxy(lambda name=name: getattr(import_module(module_name), name), init_once=True)
        for name in names
    )
    if len(proxies) == 1:
        return proxies[0]
    return proxies


def isinstance_based_on_class_name(obj: Any, full_class_name: str) -> bool:
    """Return if object 'obj' is an instance of class named 'full_class_name'

    'full_class_name' must be a string like 'class_module.class_name', the corresponding class does not need to be imported at the caller module level
    """
    import inspect

    return full_class_name in [
        f"{klass.__module__}.{klass.__name__}" for klass in inspect.getmro(type(obj))
    ]
