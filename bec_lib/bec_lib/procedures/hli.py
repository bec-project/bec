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
Available procedures and their signatures:
------------------------------------------
"""
        )

        for name, (sig, doc) in self._helper.get.available_procedures().items():
            if not name.startswith("_"):
                print(f"'{name}':\n    {sig}")
                if doc:
                    print(f'    """{doc}"""\n\n')

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
        self, macro_name: str, *args, queue: str | None = None, **kwargs
    ) -> ProcedureStatus:
        """Make a request for the given procedure to be executed

        Args:
            macro_name (str): the name of the macro to execute as a procedure
        Keyword-only args:
            queue (str, optional): the queue on which to execute the procedure

        All other args and kwargs are passed directly to the macro. Since this function uses the
        "queue" keyword argument itself, it will not be passed to the macro, don't use a keyword
        argument named "queue" there.

        returns:
            ProcedureStatus monitoring the status of the requested procedure.
        """
        return self._helper.request.procedure(
            "run_macro", ((macro_name,), {"params": (args, kwargs)}), queue
        )
