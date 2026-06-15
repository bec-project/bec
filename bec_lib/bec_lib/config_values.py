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
        self, connector: RedisConnector, endpoint: EndpointInfo[type[ManagedConfigMessage[ValueT]]]
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

    def __del__(self):
        self.unregister_all()

    def __bool__(self):
        raise ValueError(f"Maybe you meant to check {self}.value?")

    def _fetch(self) -> ManagedConfigMessage[ValueT]:
        existing = self._connector.xread(self._ep, id="+", count=1)
        if existing is None:
            config = self._ep.message_type()  # type: ignore # concrete classes must have a default
            self._write(config)
            return config
        return existing[0]["config"]

    def _write(self, updated: ManagedConfigMessage[ValueT]):
        self._connector.xadd(self._ep, {"config": updated}, max_size=1)

    def _update_cb(self, msg_dict: dict):
        self._config = msg_dict["config"]
        for cb_ref in self._cbs:
            if cb := cb_ref():
                try:
                    cb(self._config.value)
                except Exception as e:
                    logger.error(f"Exception in managed config value callback {cb}: {e}")
            else:
                self._cbs.discard(cb_ref)

    @property
    def value(self) -> ValueT:
        return self._config.value

    @value.setter
    def value(self, value: ValueT):
        self._write(self._ep.message_type(value=value))

    def subscribe(self, cb: Callable[[ValueT], Any]):
        self._cbs.add(safe_ref(cb))

    def unsubscribe(self, cb: Callable[[ValueT], Any]):
        self._cbs.discard(safe_ref(cb))

    def unregister_all(self):
        self._connector.unregister(self._ep, cb=self._update_cb)
