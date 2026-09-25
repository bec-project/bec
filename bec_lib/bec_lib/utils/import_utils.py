import inspect
from importlib import import_module
from typing import Any

from bec_lib.utils.proxy import Proxy


def lazy_import(module_name):
    return Proxy(lambda: import_module(module_name), init_once=True)


def lazy_import_from(module_name, from_list):
    ret = (Proxy(lambda name=name: getattr(import_module(module_name), name)) for name in from_list)
    if len(from_list) == 1:
        return next(ret)
    else:
        return ret


def isinstance_based_on_class_name(obj: Any, full_class_name: str):
    """Return if object 'obj' is an instance of class named 'full_class_name'

    'full_class_name' must be a string like 'class_module.class_name', the corresponding class does not need to be imported at the caller module level
    """
    return full_class_name in [
        f"{klass.__module__}.{klass.__name__}" for klass in inspect.getmro(type(obj))
    ]
