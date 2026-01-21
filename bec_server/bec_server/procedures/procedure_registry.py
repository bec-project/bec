import inspect
from typing import Iterable

from bec_lib.messages import ProcedureExecutionMessage
from bec_server.procedures.builtin_procedures import (
    log_message_args_kwargs,
    run_macro,
    run_scan,
    run_script,
    sleep,
)
from bec_server.procedures.constants import BecProcedure

_BUILTIN_PROCEDURES: dict[str, BecProcedure] = {
    "_log_msg_args": log_message_args_kwargs,
    "run scan": run_scan,
    "sleep": sleep,
    "run_script": run_script,
    "run_macro": run_macro,
}

_PROCEDURE_REGISTRY: dict[str, BecProcedure] = {} | _BUILTIN_PROCEDURES


class ProcedureRegistryError(ValueError): ...


def available() -> Iterable[str]:
    return _PROCEDURE_REGISTRY.keys()


def check_builtin_procedure(msg: ProcedureExecutionMessage) -> bool:
    """Return true if the given msg references a builtin procedure"""
    return msg.identifier in available()


def callable_from_name(name: str) -> BecProcedure:
    if not is_registered(name):
        raise ProcedureRegistryError(f"No registered procedure {name}. Available: {available()}")
    return _PROCEDURE_REGISTRY[name]


def callable_from_execution_message(msg: ProcedureExecutionMessage) -> BecProcedure:
    """Get the function to execute for the given message"""
    return callable_from_name(msg.identifier)


def get_info(name: str) -> tuple[str, str | None]:
    proc_callable = callable_from_name(name)
    params = dict(inspect.signature(proc_callable).parameters)
    params.pop("bec", None)
    args = ", ".join(map(str, params.values()))
    return (f"({args})", proc_callable.__doc__)


def is_registered(identifier: str) -> bool:
    """Return true if there is a registered procedure with the given identifier"""
    return identifier in available()


def register(identifier: str, proc: BecProcedure):
    if not isinstance(proc, BecProcedure):
        raise ProcedureRegistryError(
            f"{proc} is not a valid procedure - see the BecProcedure protocol"
        )
    if is_registered(identifier):
        raise ProcedureRegistryError(f"Procedure {proc} is already registered")
    _PROCEDURE_REGISTRY[identifier] = proc
