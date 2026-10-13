"""Interpreter for native blocking IPython calls and scoped asynchronous statuses."""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from time import sleep
from typing import Any, Iterator

from pydantic import JsonValue

from ..components import ComponentCall, ForEach, Repeat, ScanCall, Sequence, Set, While
from .adapters import BECAdapter
from .evaluation import Scope, boolean, evaluate
from .resolver import Resolved, bind_parameters


class ExecutionError(RuntimeError):
    """Execution failure with stable node and call/iteration path.

    Args:
        path (tuple): Node and call/iteration path of the failure.
        message (str): Failure description.
    """

    def __init__(self, path: tuple, message: str) -> None:
        """Initialize ExecutionError parameters.

        Args:
            path (tuple): Node and call/iteration path used for contextual errors.
            message (str): Failure description associated with the execution path.
        """
        self.path = path
        super().__init__(f"Node {'/'.join(map(str, path))}: {message}")


class Session:
    """Execute a sequence with isolated scopes and tracked pending operations.

    Args:
        adapter (BECAdapter): BEC execution adapter providing live proxy validation and client
            calls.
    """

    def __init__(self, adapter: BECAdapter) -> None:
        """Initialize Session parameters.

        Args:
            adapter (BECAdapter): BEC execution adapter providing live proxy validation and client
                calls.
        """
        self.adapter = adapter
        self.scope = Scope()
        self.path: tuple = ()
        self.pending: deque[tuple[object, tuple]] = deque()

    def __enter__(self) -> Session:
        """Enter the execution session.

        Returns:
            Session: This execution session.
        """
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        """Drain outstanding operations on success or clean up on failure.

        Args:
            exc_type (type[BaseException] | None): Exception type leaving the session, or None on
                success.
            exc (BaseException | None): Exception leaving the session, or None on success.
            traceback (TracebackType | None): Exception traceback, or None on success.
        """
        try:
            if exc_type is None:
                while self.pending:
                    status, path = self.pending[0]
                    with self.node(path[-1], absolute=path):
                        self.wait(status)
                    self.pending.popleft()
        except BaseException:
            self._cancel_pending()
            raise
        if exc_type is not None:
            self._cancel_pending()

    def _cancel_pending(self) -> None:
        """Attempt supported cancellation of outstanding operations without masking failures."""
        for status, _path in self.pending:
            try:
                self.adapter.cancel(status)
            except Exception:
                # Cleanup must preserve the original failure. Device Status has no abort API.
                continue

    @contextmanager
    def node(self, node_id: str, *, absolute: tuple | None = None) -> Iterator[None]:
        """Attach node paths and contextual errors to interpreted nodes.

        Args:
            node_id (str): Definition-local identity of the component being executed.
            absolute (tuple | None): Optional full execution path overriding the nested path.

        Yields:
            None: Control while the component's execution path is active.
        """
        previous = self.path
        self.path = absolute if absolute is not None else previous + (node_id,)
        try:
            yield
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(self.path, str(exc)) from exc
        finally:
            self.path = previous

    @contextmanager
    def in_scope(self, scope: Scope, index: int | None = None) -> Iterator[None]:
        """Isolate component/loop bindings and preserve the iteration path.

        Args:
            scope (Scope): Lexical bindings used while visiting or evaluating this component.
            index (int | None): Optional loop iteration index added to the execution path.

        Yields:
            None: Control while the nested lexical scope is active.
        """
        previous_scope, previous_path = self.scope, self.path
        self.scope = scope
        if index is not None:
            self.path += (index,)
        try:
            yield
        finally:
            self.scope, self.path = previous_scope, previous_path

    def value(self, value: JsonValue) -> Any:
        """Resolve structured values in the current runtime scope.

        Args:
            value (JsonValue): Literal or structured expression to resolve in the current scope.

        Returns:
            Any: Data resolved in the current runtime scope.
        """
        return evaluate(value, self.scope, self.adapter.done)

    def wait(self, status: object) -> None:
        """Poll checked completion; native KeyboardInterrupt interrupts the wait.

        Args:
            status (object): Runtime asynchronous handle, or None for a completed blocking call.
        """
        while not self.adapter.done(status):
            sleep(0.02)

    # One branch per discriminated component, plus status bookkeeping.
    # pylint: disable=too-many-branches
    def execute(self, tree: Resolved) -> None:
        """Interpret a resolved tree directly; never compile or evaluate Python.

        Args:
            tree (Resolved): Resolved component tree.
        """
        node = tree.node
        with self.node(node.id):
            if isinstance(node, Sequence):
                for child in tree.children:
                    self.execute(child)
            elif isinstance(node, Repeat):
                for index in range(node.count):
                    with self.in_scope(self.scope.child(), index):
                        self.execute(tree.children[0])
            elif isinstance(node, ForEach):
                values = self.value(node.values)
                for index, value in enumerate(values):
                    with self.in_scope(self.scope.child(), index):
                        self.scope.variables[node.variable] = value
                        self.execute(tree.children[0])
            elif isinstance(node, While):
                index = 0
                while boolean(self.value(node.condition)):
                    with self.in_scope(self.scope.child(), index):
                        self.execute(tree.children[0])
                    index += 1
            elif isinstance(node, ComponentCall):
                params = bind_parameters(tree.definition.parameters, self.value(node.arguments))
                with self.in_scope(Scope(parameters=params)):
                    self.execute(tree.children[0])
            elif isinstance(node, Set):
                value = self.value(node.value)
                status = self.adapter.set(node.device, value, wait=node.wait)
                if not node.wait:
                    self.pending.append((status, self.path))
                if node.bind_status:
                    self.scope.statuses[node.bind_status] = status
            elif isinstance(node, ScanCall):
                args, kwargs = self.value(node.positional), self.value(node.arguments)
                self.adapter.scan(node.name, args, kwargs)
