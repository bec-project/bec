"""
This module contains the CallbackHandler class to handle callbacks.
The CallbackHandler class is used to register and run callbacks for different
event types. The CallbackRegister class is used to register callbacks
in a with statement.
"""

import builtins
import enum
import functools
import inspect
import threading
import traceback
import types
import weakref
from collections import deque
from collections.abc import Callable

import louie

from bec_lib.logger import bec_logger
from bec_lib.utils import threadlocked

logger = bec_logger.logger


def _rejection_reason(callback: Callable) -> str | None:
    """Why ``callback`` cannot be registered; None for bound methods, static methods and
    module-level functions."""
    if isinstance(callback, functools.partial):
        return "functools.partial objects are not supported"
    if inspect.ismethod(callback):
        return None
    if not isinstance(callback, types.FunctionType):
        return f"{type(callback).__name__} objects are not supported"
    if callback.__name__ == "<lambda>":
        return "lambdas are not supported"
    if "<locals>" in callback.__qualname__:
        return "functions defined inside another function are not supported"
    return None


class _StrongCallableRef:
    """Fallback ref wrapper for callables that cannot be safely weak-referenced."""

    def __init__(self, func: Callable) -> None:
        self._func = func

    def __call__(self) -> Callable:
        return self._func


class EventType(str, enum.Enum):
    """Event types

    SCAN_HISTORY_UPDATE: Update of the scan history, emits one ScanhistoryMessage for history_msg arg.
    SCAN_HISTORY_LOADED: Scan history loaded, emits a list of ScanhistoryMessages for history_msgs arg.
    """

    SCAN_SEGMENT = "scan_segment"
    SCAN_STATUS = "scan_status"
    NAMESPACE_UPDATE = "namespace_update"
    DEVICE_UPDATE = "device_update"
    SCAN_HISTORY_UPDATE = "scan_history_update"
    SCAN_HISTORY_LOADED = "scan_history_loaded"


class CallbackEntry:
    """Callback entry class to store callback information"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        id: int,
        event_type: EventType,
        func: Callable,
        sync: bool,
        *,
        on_dead: Callable[[int], None] | None = None,
    ) -> None:
        self.id = id
        self.event_type = event_type
        self.sync = sync
        self.queue = deque(maxlen=1000)
        self._lock = threading.RLock()
        self._on_delete = None
        self.func = self._make_ref(func, on_dead)

    def _make_ref(self, func: Callable, on_dead: Callable[[int], None] | None):
        if not inspect.ismethod(func):
            return _StrongCallableRef(func)
        callback_id, name, event_type = self.id, func.__name__, self.event_type

        def on_delete(_ref):
            logger.info(f"Callback {name} for {event_type} was garbage collected and removed.")
            if on_dead is not None:
                on_dead(callback_id)

        try:
            ref = louie.saferef.safe_ref(func, on_delete=on_delete)
        except TypeError:
            return _StrongCallableRef(func)
        self._on_delete = on_delete
        return ref

    def release(self) -> None:
        """Detach the collection notification; louie keeps it alive as long as the owner lives."""
        methods = getattr(self.func, "deletion_methods", [])
        if self._on_delete in methods:
            methods.remove(self._on_delete)

    def _resolve_func(self) -> Callable | None:
        return None if self.func is None else self.func()

    @threadlocked
    def run(self, *args, **kwargs) -> None:
        """Run the callback function. If sync is True, the callback is run immediately. Otherwise, the callback is added to a queue and executed in the next poll."""
        if not self.sync:
            self._run_cb(*args, **kwargs)
            return
        self.queue.append((args, kwargs))

    def _run_cb(self, *args, **kwargs) -> None:
        """Run the callback function in a safe way."""
        try:
            func = self._resolve_func()
            if func is not None:
                func(*args, **kwargs)
        except Exception:
            content = traceback.format_exc()
            logger.warning(f"Failed to run callback function: {content}")

    def __str__(self) -> str:
        func = self._resolve_func()
        func_name = getattr(func, "__name__", type(func).__name__) if func is not None else "<dead>"
        return f"<CallbackEntry>: (event_type: {self.event_type}, function: {func_name}, sync: {self.sync}, pending events: {self.num_pending_events})"

    @property
    def num_pending_events(self):
        """number of pending events"""
        return len(self.queue)

    def is_alive(self) -> bool:
        """Check if the callback function is still alive"""
        return self._resolve_func() is not None

    @threadlocked
    def poll(self) -> None:
        """Run callback.

        Raises:
            RuntimeError: Raises if attempt is made to run async callbacks manually.
        """
        if not self.sync:
            raise RuntimeError("Cannot poll on an async callback.")
        args, kwargs = self.queue.popleft()
        self._run_cb(*args, **kwargs)


class CallbackHandler:
    """Callback handler class"""

    def __init__(self) -> None:
        self.callbacks = {}
        self.id_counter = 0
        self._lock = threading.RLock()
        self._dead_ids = deque()
        self._on_dead = functools.partial(self._notify_dead, weakref.ref(self))

    @threadlocked
    def register(self, event_type: str, callback: Callable, sync=False) -> int:
        """Register a callback to an event type

        Only bound methods, static methods and module-level functions are accepted; anything else
        (lambdas, local functions, functools.partial, callable objects, builtins) is rejected with
        an error log. Bound methods are referenced weakly and removed once their owner is garbage
        collected.

        Args:
            event_type (str): Event type
            callback (Callable): Callback function
            sync (bool, optional): Synchronous or async callback. Defaults to False.

        Returns:
            int: Callback id, or -1 if the callback was rejected
        """
        event_type = EventType(event_type)
        reason = _rejection_reason(callback)
        if reason is not None:
            logger.error(
                f"Callback {callback!r} for {event_type.value} was not registered: {reason}."
            )
            return -1
        callback_id = self.new_id()
        self.callbacks[callback_id] = CallbackEntry(
            callback_id, event_type, callback, sync, on_dead=self._on_dead
        )
        return callback_id

    @threadlocked
    def register_many(self, event_type: str, callbacks: list[Callable], sync=False) -> list[int]:
        """Register multiple callbacks to an event type

        Args:
            event_type (str): Event type
            callbacks (list[Callable]): List of callback functions
            sync (bool, optional): Synchronous or async callback. Defaults to False.

        Returns:
            list: List of caallback ids
        """

        if not isinstance(callbacks, list):
            callbacks = [callbacks]
        ids = []
        for cbk in callbacks:
            if cbk is None:
                ids.append(-1)
            else:
                ids.append(self.register(event_type, cbk, sync))
        return ids

    @threadlocked
    def remove(self, id: int) -> int:
        """Remove a registered callback by its id

        Args:
            id (int): Callback id

        Returns:
            int: Returns the id of the removed callback. -1 if it failed.
        """
        try:
            entry = self.callbacks.pop(id)
        except KeyError:
            return -1
        entry.release()
        return id

    def new_id(self):
        """Generate a new callback id"""
        self.id_counter += 1
        return self.id_counter

    @staticmethod
    def _notify_dead(handler_ref: weakref.ref, callback_id: int) -> None:
        handler = handler_ref()
        if handler is not None:
            handler._mark_dead(callback_id)  # pylint: disable=protected-access

    def _mark_dead(self, callback_id: int) -> None:
        # runs inside a weakref finalizer on an arbitrary thread: never block on the lock
        self._dead_ids.append(callback_id)
        if not self._lock.acquire(blocking=False):  # pylint: disable=consider-using-with
            return
        try:
            self._remove_dead_callbacks()
        finally:
            self._lock.release()

    def _remove_dead_callbacks(self) -> None:
        while self._dead_ids:
            self.callbacks.pop(self._dead_ids.popleft(), None)
        # louie shares one reference per bound method and keeps only the latest on_delete
        for callback_id, cb in list(self.callbacks.items()):
            if not cb.is_alive():
                self.callbacks.pop(callback_id)
                logger.info(
                    f"Callback {callback_id} for {cb.event_type} was garbage collected and removed."
                )

    @threadlocked
    def run(self, event_type: str, *args, **kwargs):
        """Run all callbacks for a given event type"""
        self._remove_dead_callbacks()
        for cb in list(self.callbacks.values()):
            if event_type != cb.event_type:
                continue
            cb.run(*args, **kwargs)

    @threadlocked
    def poll(self):
        """Run all pending callbacks"""
        self._remove_dead_callbacks()
        for callback in list(self.callbacks.values()):
            if not callback.sync:
                continue
            while callback.num_pending_events:
                callback.poll()


class CallbackRegister:
    def __init__(self, event_type, callbacks, sync=False, callback_handler=None) -> None:
        """Callback register class to register callbacks in a with statement

        Args:
            callback_handler (CallbackHandler): Callback handler
        """
        if not callback_handler:
            bec = builtins.__dict__.get("bec")
            self.callback_handler = bec.callbacks
        else:
            self.callback_handler = callback_handler
        self.event_type = event_type
        if not isinstance(callbacks, list):
            callbacks = [callbacks]
        self.callbacks = callbacks
        self.sync = sync
        self.callback_ids = []

    def __enter__(self):
        for callback in self.callbacks:
            if callback is None:
                continue
            self.callback_ids.append(
                self.callback_handler.register(self.event_type, callback, sync=self.sync)
            )
        return self

    def __exit__(self, *exc):
        for cb_id in self.callback_ids:
            self.callback_handler.remove(cb_id)
