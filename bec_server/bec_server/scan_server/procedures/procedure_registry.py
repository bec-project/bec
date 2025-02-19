from typing import Any, Callable, ParamSpec

from bec_lib.messages import ProcedureExecutionMessage
from bec_server.scan_server.procedures.builtin_procedures import log_message_args_kwargs, run_scan

P = ParamSpec("P")

BUILTIN_PROCEDURES: dict[str, Callable[..., None]] = {
    "log execution message args": log_message_args_kwargs,
    "run scan": run_scan,
}

PROCEDURE_LIST: dict[str, Callable[[Any], None]] = {} | BUILTIN_PROCEDURES


def check_builtin_procedure(msg: ProcedureExecutionMessage) -> bool:
    """Return true if the given msg references a builtin procedure"""
    return msg.identifier in BUILTIN_PROCEDURES.keys()


def callable_from_execution_message(msg: ProcedureExecutionMessage) -> Callable[..., None]:
    """Get the function to execute for the given message"""
    return PROCEDURE_LIST[msg.identifier]
