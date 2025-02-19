import inspect
from functools import partial

from bec_lib.client import BECClient
from bec_lib.messages import ProcedureExecutionMessage
from bec_server.scan_server.procedures.procedure_manager import ProcedureWorker
from bec_server.scan_server.procedures.procedure_registry import (
    callable_from_execution_message,
    check_builtin_procedure,
)


class InProcessProcedureWorker(ProcedureWorker):
    """A simple in-process procedure worker. Be careful with this, it should only run trusted code.
    Intended for built-in procedures like those to run a single scan."""

    def _setup_execution_environment(self):
        from bec_lib.logger import bec_logger

        self.logger = bec_logger.logger
        self.logger.info(f"In-process procedure worker for queue {self.key.endpoint} spinning up")
        self.bec_client = BECClient()
        self.bec_client.start()

    def _run_task(self, item: ProcedureExecutionMessage):
        if not isinstance(item, ProcedureExecutionMessage):
            self.logger.error(f"{item} is not a ProcedureExecutionMessage!")
            return
        if not check_builtin_procedure(item):
            self.logger.error(
                f"{item.identifier} is not a builtin procedure and should not be executed in-process!"
            )
            return
        args, kwargs = item.args_kwargs
        procedure_function = callable_from_execution_message(item)
        procedure_sig = inspect.signature(procedure_function)
        if (
            "bec" in procedure_sig.parameters
            and procedure_sig.parameters["bec"].annotation is BECClient
        ):
            procedure_function = partial(procedure_function, bec=self.bec_client)
        procedure_function(*args, **kwargs)

    def _kill_process(self):
        self.logger.info(
            f"In-process procedure worker for queue {self.key.endpoint} timed out after {self._lifetime_s} s, shutting down"
        )
