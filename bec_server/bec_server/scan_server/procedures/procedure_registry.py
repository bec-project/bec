from typing import Any, Callable, Iterable, Iterator

from bec_lib.messages import ProcedureExecutionMessage
from bec_server.scan_server.procedures.builtin_procedures import (
    log_message_args_kwargs,
    run_scan,
    sleep,
)
from bec_server.scan_server.procedures.constants import BecProcedure

_BUILTIN_PROCEDURES: dict[str, BecProcedure] = {
    "log execution message args": log_message_args_kwargs,
    "run scan": run_scan,
    "sleep": sleep,
}

_PROCEDURE_REGISTRY: dict[str, BecProcedure] = {} | _BUILTIN_PROCEDURES


class ProcedureRegistryError(ValueError): ...


def available() -> Iterable[str]:
    return _PROCEDURE_REGISTRY.keys()


def check_builtin_procedure(msg: ProcedureExecutionMessage) -> bool:
    """Return true if the given msg references a builtin procedure"""
    return msg.identifier in available()


def callable_from_execution_message(msg: ProcedureExecutionMessage) -> BecProcedure:
    """Get the function to execute for the given message"""
    if not is_registered(msg.identifier):
        raise ProcedureRegistryError(
            f"No registered procedure {msg.identifier}. Available: {available()}"
        )
    return _PROCEDURE_REGISTRY[msg.identifier]


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
