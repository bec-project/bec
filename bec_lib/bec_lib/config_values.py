from threading import Event
from typing import Any, Callable, Generic, TypeVar
from weakref import ReferenceType

from louie.saferef import BoundMethodWeakref, safe_ref

from bec_lib.endpoints import EndpointInfo, MessageOp
from bec_lib.logger import bec_logger
from bec_lib.messages import ManagedConfigMessage
from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger
ValueT = TypeVar("ValueT")


class RedisConfigValue(property, Generic[ValueT]):
    def __init__(
        self,
        connector: RedisConnector,
        endpoint: EndpointInfo[type[ManagedConfigMessage[ValueT]]],
        wait_for_writes: bool = True,
    ) -> None:
        """A config value bound to a value in Redis, which uses the ManagedConfigMessage and an associated endpoint,
        and which can be subscribed to."""

        if endpoint.message_op != MessageOp.STREAM or not issubclass(
            endpoint.message_type, ManagedConfigMessage
        ):
            raise TypeError(
                "RedisConfigManager needs a STREAM endpoint with a message type which is a subclass of ManagedConfigMessage"
            )
        self._ep = endpoint
        self._connector = connector
        self._cbs: set[ReferenceType[Callable[[ValueT]]] | BoundMethodWeakref] = set()
        self._config = self._fetch()
        self._connector.register(self._ep, cb=self._update_cb)
        self._writing_wait_event = Event()
        self._writing_wait_event.set()
        self._wait_for_writes = wait_for_writes

    def __del__(self):
        if hasattr(self, "_connector"):
            self.unregister_all()

    def __bool__(self):
        raise ValueError(f"Maybe you meant to check {self}.value?")

    def _fetch(self) -> ManagedConfigMessage[ValueT]:
        existing = self._connector.xread(self._ep, from_start=True)
        if existing is None or existing == []:
            logger.warning(
                f"No value found in redis for managed config var {self._ep.endpoint}, resetting to default."
            )
            config = self._ep.message_type()  # type: ignore # concrete classes must have a default
            self._write(config)
            return config
        return existing[-1]["config"]

    def _write(self, updated: ManagedConfigMessage[ValueT], wait: bool = True):
        if wait:
            self._writing_wait_event.clear()
        self._connector.xadd(self._ep, {"config": updated}, max_size=1)
        self._writing_wait_event.wait(timeout=2)
        if not self._writing_wait_event.is_set():
            logger.error(
                f"Timed out waiting for config variable {self._ep.endpoint} to return from Redis"
            )

    def _update_cb(self, msg_dict: dict):
        try:
            self._config = msg_dict["config"]
            for cb_ref in list(self._cbs):
                if cb := cb_ref():
                    try:
                        cb(self._config.value)
                    except Exception as e:
                        logger.error(f"Exception in managed config value callback {cb}: {e}")
                else:
                    self._cbs.discard(cb_ref)
        finally:
            self._writing_wait_event.set()

    @property
    def value(self) -> ValueT:
        return self._config.value

    @value.setter
    def value(self, value: ValueT):
        self._write(self._ep.message_type(value=value), self._wait_for_writes)

    def subscribe(self, cb: Callable[[ValueT], Any]):
        self._cbs.add(safe_ref(cb))

    def unsubscribe(self, cb: Callable[[ValueT], Any]):
        self._cbs.discard(safe_ref(cb))

    def unregister_all(self):
        self._connector.unregister(self._ep, cb=self._update_cb)
