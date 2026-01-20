import inspect
import traceback
from functools import partial

from bec_ipython_client.main import BECIPythonClient
from bec_lib.client import BECClient
from bec_messages.messages import ProcedureExecutionMessage
from bec_lib.procedures.helper import BackendProcedureHelper
from bec_server.procedures.procedure_registry import (
    callable_from_execution_message,
    check_builtin_procedure,
)
from bec_server.procedures.worker_base import ProcedureWorker


class InProcessProcedureWorker(ProcedureWorker):
    """A simple in-process procedure worker. Be careful with this, it should only run trusted code.
    Intended for built-in procedures like those to run a single scan, or testing."""

    def _setup_execution_environment(self):
        from bec_lib.logger import bec_logger

        self.logger = bec_logger.logger
        self.logger.info(f"In-process procedure worker for queue {self.key.endpoint} spinning up")
        self.bec_client = BECIPythonClient()
        self.bec_client.start()
        self._helper = BackendProcedureHelper(self._conn)

    def _run_task(self, item: ProcedureExecutionMessage):
        if not isinstance(item, ProcedureExecutionMessage):
            self.logger.error(f"{item} is not a ProcedureExecutionMessage!")
            return
        self._helper.status_update(item.execution_id, "Started")
        if not check_builtin_procedure(item):
            self.logger.error(
                f"{item.identifier} is not a builtin procedure and should not be executed in-process!"
            )
            return
        try:
            args, kwargs = item.args_kwargs
            procedure_function = callable_from_execution_message(item)
            procedure_sig = inspect.signature(procedure_function)
            if "bec" in procedure_sig.parameters and procedure_sig.parameters["bec"].annotation in [
                BECClient,
                BECIPythonClient,
            ]:
                procedure_function = partial(procedure_function, bec=self.bec_client)
            procedure_function(*args, **kwargs)
        except Exception as e:
            self.logger.error(e)
            self._helper.status_update(item.execution_id, "Finished", traceback.format_exc())
        else:
            self._helper.status_update(item.execution_id, "Finished")

    def _kill_process(self):
        self.logger.info(
            f"In-process procedure worker for queue {self.key.endpoint} timed out after {self._lifetime_s} s, shutting down"
        )

    def abort_execution(self, execution_id: str):
        """Abort the execution with the given id"""
        ...  # No nice way to abort a running function, don't use this class for such things
