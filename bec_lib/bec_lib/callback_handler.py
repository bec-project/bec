"""
This module contains the CallbackHandler class to handle callbacks.
The CallbackHandler class is used to register and run callbacks for different
event types. The CallbackRegister class is used to register callbacks
in a with statement.
"""

import builtins
import enum
import threading
import traceback
from collections import deque
from collections.abc import Callable

from bec_lib.logger import bec_logger
from bec_lib.utils import threadlocked

logger = bec_logger.logger


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

    def __init__(self, id: int, event_type: EventType, func: Callable, sync: bool) -> None:
        self.id = id
        self.func = func
        self.event_type = event_type
        self.sync = sync
        self.queue = deque(maxlen=1000)
        self._lock = threading.RLock()

    @threadlocked
    def run(self, *args, **kwargs) -> None:
        """Run the callback function. If sync is True, the callback is run immediately. Otherwise, the callback is added to a queue and exectued in the next poll."""
        if not self.sync:
            self._run_cb(*args, **kwargs)
            return
        self.queue.append((args, kwargs))

    def _run_cb(self, *args, **kwargs) -> None:
        """Run the callback function in a safe way."""
        try:
            self.func(*args, **kwargs)
        except Exception:
            content = traceback.format_exc()
            logger.warning(f"Failed to run callback function: {content}")

    def __str__(self) -> str:
        return f"<CallbackEntry>: (event_type: {self.event_type}, function: {self.func.__name__}, sync: {self.sync}, pending events: {self.num_pending_events})"

    @property
    def num_pending_events(self):
        """number of pending events"""
        return len(self.queue)

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

    @threadlocked
    def register(self, event_type: str, callback: Callable, sync=False) -> int:
        """Register a callback to an event type

        Args:
            event_type (str): Event type
            callback (Callable): Callback function
            sync (bool, optional): Synchronous or async callback. Defaults to False.

        Returns:
            int: Callback id
        """
        event_type = EventType(event_type)
        callback_id = self.new_id()
        self.callbacks[callback_id] = CallbackEntry(callback_id, event_type, callback, sync)
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
            if cbk:
                ids.append(self.register(event_type, cbk, sync))
            else:
                ids.append(-1)
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
            self.callbacks.pop(id)
            return id
        except KeyError:
            return -1

    def new_id(self):
        """Generate a new callback id"""
        self.id_counter += 1
        return self.id_counter

    @threadlocked
    def run(self, event_type: str, *args, **kwargs):
        """Run all callbacks for a given event type"""
        for cb in self.callbacks.values():
            if event_type != cb.event_type:
                continue
            cb.run(*args, **kwargs)

    @threadlocked
    def poll(self):
        """Run all pending callbacks"""
        for callback in self.callbacks.values():
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
            if not callback:
                continue
            self.callback_ids.append(
                self.callback_handler.register(self.event_type, callback, sync=self.sync)
            )
        return self

    def __exit__(self, *exc):
        for cb_id in self.callback_ids:
            self.callback_handler.remove(cb_id)
