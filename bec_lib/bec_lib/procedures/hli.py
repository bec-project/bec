from typing import Any

from bec_lib.procedures.helper import FrontendProcedureHelper, ProcedureStatus
from bec_lib.redis_connector import RedisConnector


class ProcedureHli:
    def __init__(self, conn: RedisConnector) -> None:
        self._conn = conn
        self._helper = FrontendProcedureHelper(self._conn)

    def available_procedures(self):
        """Pretty-print a list of available procedures."""
        print(
            """
Available procedures:
---------------------
"""
        )

        for name, sig in self._helper.get.available_procedures().items():
            print(f"'{name}':\n    {sig}\n")

    def request_new(
        self,
        identifier: str,
        args_kwargs: tuple[tuple[Any, ...], dict[str, Any]] | None = None,
        queue: str | None = None,
    ):
        """Make a request for the given procedure to be executed

        Args:
            identifier (str): the identifier for the requested procedure
            args_kwargs (tuple[tuple, dict], optional): args and kwargs to be passed to the procedure
            queue (str, optional): the queue on which to execute the procedure

        returns:
            ProcedureStatus monitoring the status of the requested procedure.
        """
        return self._helper.request.procedure(identifier, args_kwargs, queue)

    def run_macro(
        self,
        macro_name: str,
        args_kwargs: tuple[tuple[Any, ...], dict[str, Any]] | None = None,
        queue: str | None = None,
    ) -> ProcedureStatus:
        """Make a request for the given procedure to be executed

        Args:
            macro_name (str): the name of the macro to execute as a procedure
            args_kwargs (tuple[tuple, dict], optional): args and kwargs to be passed to the procedure
            queue (str, optional): the queue on which to execute the procedure

        returns:
            ProcedureStatus monitoring the status of the requested procedure.
        """
        return self._helper.request.procedure("run_macro", ((macro_name, args_kwargs), {}), queue)
