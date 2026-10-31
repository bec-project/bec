"""Test-only asyncio HAL. Imports neither ophyd nor Bluesky.

This fixture is intentionally not production hardware support. It exercises the
BEC-owned contract using opaque native objects, real asyncio tasks and pushed
messages, including stop while acquisition awaits a transport event.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import replace
from typing import Any

from bec_lib import messages
from bec_lib.device_backend import (
    Capability,
    Completion,
    ConfigurationResult,
    DeviceEvent,
    DeviceHandler,
    DeviceIdentity,
    EventKind,
    MessageChannel,
    Operation,
    OperationMetadata,
    ReadCategory,
    RPCResult,
)

_async_owner = threading.local()


class Interrupted(RuntimeError):
    """A test acquisition was interrupted, after the stop attempt settled."""


class Native:
    """Opaque hardware object; intentionally has no server-facing HAL methods."""

    __slots__ = ()


class NoDiagnostics:
    stopped = True

    def start(self) -> None:
        pass

    def request_stop(self) -> None:
        pass

    def join(self, timeout: float | None = None) -> None:
        pass


class PushSubscription:
    def __init__(self, owner, callback, context=None):
        self.owner = owner
        self.callback = callback
        self.context = context
        self.accepting = True
        self.lock = threading.RLock()
        self.pending = set()
        self.errors = []

    def emit(self, event):
        with self.lock:
            if not self.accepting:
                return
            event_context = (
                self.context()
                if self.context is not None and event.kind == EventKind.MESSAGE
                else event.context
            )
            snapshot = replace(
                event,
                context=event_context,
                message=event.message.model_copy(deep=True) if event.message is not None else None,
                signal_metadata=copy.deepcopy(event.signal_metadata),
            )
            future = self.owner.provider.events.submit(self.callback, snapshot)
            self.pending.add(future)

        def finished(done):
            with self.lock:
                self.pending.discard(done)
                if done.exception() is not None:
                    self.errors.append(done.exception())

        future.add_done_callback(finished)

    def close(self):
        with self.lock:
            self.accepting = False
            pending = tuple(self.pending)
        if pending:
            _, unfinished = wait(pending, timeout=2)
            if unfinished:
                raise TimeoutError("Async fixture event drain timed out")
        if self in self.owner.subscriptions:
            self.owner.subscriptions.remove(self)
        if self.errors:
            raise self.errors[0]


class AsyncHandler:
    """Bound facade; only this implementation knows how to use its owner loop."""

    def __init__(self, name, provider):
        self.provider = provider
        self.name = name
        self.constructor_thread = threading.get_ident()
        self.thread_ids = set()
        self.value = 0
        self.exposure = 0.1
        self.timestamp = time.time()
        self._connected = False
        self._staged = False
        self._closed = False
        self._closing = False
        self.generation = 0
        self.admission = asyncio.Event()
        self.admission.set()
        self.transport = asyncio.Event()
        self.stop_ack = asyncio.Event()
        self.stop_ack.set()
        self.stop_done = asyncio.Event()
        self.stop_done.set()
        self.started = threading.Event()
        self.stop_started = threading.Event()
        self.hardware_running = False
        self.stop_confirmed = False
        self.stopping = False
        self.trigger_timeout = None
        self.stop_timeout = 1
        self.fail_acquisition = False
        self.fail_stop = False
        self.tasks = set()
        self.subscriptions = []
        self.peer = None
        self.native_calls = 0

    @property
    def identity(self):
        return DeviceIdentity(self.name, name=self.name)

    @property
    def capabilities(self):
        return frozenset(
            (
                Capability.STAGE,
                Capability.SET,
                Capability.STOP,
                Capability.PRE_SCAN,
                Capability.COMPLETE,
                Capability.KICKOFF,
            )
        )

    @property
    def connected(self):
        return self._connected and not self._closed

    @property
    def staged(self):
        return self._staged

    @property
    def read_category(self):
        return ReadCategory.NORMAL

    def resolve(self, path):
        if not path:
            return self
        if path in ("value", "exposure"):
            return AsyncTarget(self, path)
        raise AttributeError(path)

    def connect(self, *, all_signals=False, timeout=5):
        self.provider.run(lambda: setattr(self, "_connected", True), timeout)

    def on_connected(self):
        pass

    def describe(self, *, connected=True):
        description = {self.name: {"source": "test:async", "dtype": "number", "shape": []}}
        return {
            "device_attr_name": "",
            "device_base_class": "positioner",
            "device_class": "AsyncFixture",
            "ownership_mode": "claimable",
            "read_access": True,
            "write_access": True,
            "signals": {},
            "hints": {"fields": [self.name]} if connected else {},
            "describe": description if connected else {},
            "describe_configuration": {},
            "sub_devices": [],
            "custom_user_access": {},
        }

    def read(self):
        return self.provider.run(
            lambda: {self.name: {"value": self.value, "timestamp": self.timestamp}}
        )

    def read_configuration(self):
        return self.provider.run(
            lambda: {f"{self.name}_exposure": {"value": self.exposure, "timestamp": self.timestamp}}
        )

    def limits(self):
        return None

    def configure(self, values):
        self.provider.run(lambda: [setattr(self, key, value) for key, value in values.items()])
        return ConfigurationResult()

    def _emit(self, event):
        for subscription in tuple(self.subscriptions):
            subscription.emit(event)

    def transport_update(self, value):
        """A native pushed transport callback, marshalled to its owning loop."""

        def changed():
            self.value = value
            self.timestamp = time.time()
            self._emit(DeviceEvent(self.identity, EventKind.READBACK))

        self.provider.loop.call_soon_threadsafe(changed)

    def publish_file(self, filename):
        def publish():
            message = messages.FileMessage(
                file_path=filename, done=True, successful=True, device_name=self.name
            )
            self._emit(
                DeviceEvent(
                    DeviceIdentity(self.name, "file", f"{self.name}_file"),
                    EventKind.MESSAGE,
                    message=message,
                    channel=MessageChannel.FILE,
                )
            )

        self.provider.run(publish)

    def _submit(self, function, *, control=False):
        if self._closed or (self._closing and not control):
            raise RuntimeError("Async fixture is closing")
        operation = Operation(dispatch=self.provider.dispatch)
        generation = self.generation

        async def execute():
            task = asyncio.current_task()
            admitted = control
            if not control:
                self.tasks.add(task)
            try:
                if not control:
                    if generation != self.generation:
                        raise Interrupted("Cancelled before admission")
                    await self.admission.wait()
                    if generation != self.generation:
                        raise Interrupted("Cancelled before admission")
                admitted = True
                self.thread_ids.add(threading.get_ident())
                result = await function()
                operation.set_result(result)
            except asyncio.CancelledError:
                await self.stop_done.wait()
                operation.set_exception(
                    Interrupted(
                        "Acquisition interrupted" if admitted else "Cancelled before admission"
                    )
                )
            except Exception as error:
                operation.set_exception(error)
            finally:
                self.tasks.discard(task)

        future = asyncio.run_coroutine_threadsafe(execute(), self.provider.loop)
        self.provider.submissions.add(future)
        future.add_done_callback(self.provider.submissions.discard)
        return operation

    async def _acquire(self):
        self.native_calls += 1
        self.started.set()
        self.hardware_running = True
        try:
            if self.peer is not None:
                await self._observe(self.peer.set(7))
            if self.trigger_timeout is None:
                await self.transport.wait()
            else:
                await asyncio.wait_for(self.transport.wait(), self.trigger_timeout)
            if self.fail_acquisition:
                raise ValueError("Async native acquisition failed")
            self.value += 1
            self.timestamp = time.time()
            self._emit(DeviceEvent(self.identity, EventKind.READBACK))
        finally:
            # Stop owns shutdown during interruption; normal completion/error
            # performs its own acknowledged fake hardware cleanup.
            if not self.stopping:
                self.hardware_running = False

    async def _observe(self, operation):
        loop = asyncio.get_running_loop()
        result = loop.create_future()

        def finished(completed):
            def settle():
                if result.done():
                    return
                error = completed.exception(timeout=0)
                if error is None:
                    result.set_result(completed.result(timeout=0))
                else:
                    result.set_exception(error)

            loop.call_soon_threadsafe(settle)

        operation.add_callback(finished)
        return await result

    def trigger(self):
        return self._submit(self._acquire)

    def set(self, value):
        async def move():
            await asyncio.sleep(0)
            self.value = value
            self.timestamp = time.time()
            self._emit(DeviceEvent(self.identity, EventKind.READBACK))

        return self._submit(move)

    def stage(self):
        async def stage():
            self._staged = True

        return self._submit(stage)

    def unstage(self):
        async def unstage():
            self._staged = False

        return self._submit(unstage)

    def pre_scan(self):
        return None

    def kickoff(self, configure, metadata):
        self.configure(configure)
        return Operation.finished(None)

    def complete(self):
        return Operation.finished(None)

    def stop(self):
        async def stop():
            self.generation += 1
            self.stopping = True
            self.stop_done.clear()
            self.stop_started.set()
            self.stop_confirmed = False
            current = asyncio.current_task()
            pending = [task for task in self.tasks if task is not current]
            error = None
            try:
                await asyncio.wait_for(self.stop_ack.wait(), self.stop_timeout)
                if self.fail_stop:
                    raise RuntimeError("Async stop failed")
                self.hardware_running = False
                self.stop_confirmed = True
            except Exception as failure:
                error = failure
            finally:
                self.stop_done.set()
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                self.stopping = False
            if error is not None:
                raise error

        return self._submit(stop, control=True)

    def subscribe(self, callback, *, context=None):
        if self._closing or self._closed:
            raise RuntimeError("Async fixture is closing")
        subscription = PushSubscription(self, callback, context=context)
        self.subscriptions.append(subscription)
        return subscription

    def flush_events(self, timeout=10):
        if getattr(
            _async_owner, "provider", None
        ) is not None or threading.current_thread().name.startswith("AsyncProofEvents"):
            raise RuntimeError("Cannot synchronously wait on an owner or event thread")
        self.provider.events.submit(lambda: None).result(timeout)
        errors = []
        for subscription in self.subscriptions:
            with subscription.lock:
                errors.extend(subscription.errors)
                subscription.errors.clear()
        if errors:
            raise errors[0]

    def subscribe_readback(self, callback):
        return self.subscribe(callback)

    def runtime_info(self):
        return {"file": messages.SignalInfo(role="file_event")}

    def rpc(self, path, args, kwargs, *, rid=None):
        if path.endswith(".set"):
            target = self.resolve(path.rsplit(".", 1)[0])
            completion = target.set(args[0])
            return RPCResult(
                completion=completion,
                status=OperationMetadata(None),
                target_path=target.identity.path,
                refresh=True,
            )
        if path == "set":
            return RPCResult(
                completion=self.set(args[0]), status=OperationMetadata(None), refresh=True
            )
        if path == "trigger":
            return RPCResult(completion=self.trigger(), status=OperationMetadata(None))
        if path == "value":
            return RPCResult(value=self.provider.run(lambda: self.value))
        raise AttributeError(path)

    def set_property(self, path, value):
        if path != "exposure":
            raise AttributeError(path)
        self.provider.run(lambda: setattr(self, path, value))

    def close(self):
        if self._closed:
            return
        self._closing = True
        self.stop().result(timeout=2)
        self.provider.run(self.admission.set)
        for subscription in tuple(self.subscriptions):
            subscription.close()
        self._closed = True
        self._connected = False


class AsyncTarget:
    """Child handler delegates device ownership but retains requested target identity."""

    def __init__(self, root, path):
        self.root, self.path = root, path

    @property
    def identity(self):
        return DeviceIdentity(self.root.name, self.path, f"{self.root.name}_{self.path}")

    @property
    def read_category(self):
        return ReadCategory.CONFIGURATION if self.path == "exposure" else ReadCategory.NORMAL

    def __getattr__(self, name):
        return getattr(self.root, name)

    def set(self, value):
        if self.path == "value":
            return self.root.set(value)

        async def configure():
            self.root.exposure = value

        return self.root._submit(configure)


class AsyncProvider:
    """Test-only provider with a persistent loop and independently runnable stop."""

    def __init__(self):
        self.events = ThreadPoolExecutor(max_workers=1, thread_name_prefix="AsyncProofEvents")
        self.callbacks = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="AsyncProofCompletion"
        )
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.handlers = {}
        self.submissions = set()
        self.closed = False
        self.callbacks_closed = False
        self.dispatch_lock = threading.Lock()
        self.callback_futures = set()
        self.callback_errors = []
        self.thread = threading.Thread(target=self._run, name="AsyncProofOwner", daemon=True)
        self.thread.start()
        assert self.ready.wait(2)

    def _run(self):
        _async_owner.provider = self
        asyncio.set_event_loop(self.loop)
        self.ready.set()
        try:
            self.loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            if pending:
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self.loop.close()

    def run(self, function, timeout=2):
        if self.closed:
            raise RuntimeError("Async fixture provider is closed")
        if getattr(_async_owner, "provider", None) is not None:
            raise RuntimeError("Cannot synchronously wait on the async owner")

        async def invoke():
            result = function()
            return await result if inspect.isawaitable(result) else result

        return asyncio.run_coroutine_threadsafe(invoke(), self.loop).result(timeout)

    def dispatch(self, callback):
        with self.dispatch_lock:
            if self.callbacks_closed:
                future = None
            else:
                future = self.callbacks.submit(callback)
                self.callback_futures.add(future)
        if future is not None:

            def finished(done):
                with self.dispatch_lock:
                    self.callback_futures.discard(done)
                    if done.exception() is not None:
                        self.callback_errors.append(done.exception())

            future.add_done_callback(finished)
            return
        callback()  # Terminal late registration runs on the observer's caller.

    def load_class(self, specification):
        if specification != "proof.AsyncDevice":
            raise ValueError(specification)
        return Native

    def construct(self, device_class, config, context=None):
        config = config.copy()
        name = config.pop("name")

        def create():
            native = device_class()
            self.handlers[native] = AsyncHandler(name, self)
            return native

        return self.run(create), config

    def wrap(self, device):
        return self.handlers[device]

    def requires_dependencies(self, device_class):
        return False

    def register_proxy(self, proxy, devices):
        raise NotImplementedError("The async proof has no legacy proxies")

    def callback_monitor(self):
        return NoDiagnostics()

    def error_device(self, error):
        return None

    def close(self):
        if self.closed:
            return
        for handler in tuple(self.handlers.values()):
            handler.close()
        if self.submissions:
            _, unfinished = wait(tuple(self.submissions), timeout=2)
            if unfinished:
                raise TimeoutError("Async proof submissions did not drain")
        with self.dispatch_lock:
            self.callbacks_closed = True
            pending = tuple(self.callback_futures)
        if pending:
            _, unfinished = wait(pending, timeout=2)
            if unfinished:
                raise TimeoutError("Async proof completion callbacks did not drain")
        self.events.shutdown(wait=True)
        self.callbacks.shutdown(wait=True)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(2)
        if self.thread.is_alive():
            raise TimeoutError("Async proof loop did not close")
        self.closed = True
        if self.callback_errors:
            raise self.callback_errors[0]
