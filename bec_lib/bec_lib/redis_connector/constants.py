from typing import ParamSpec, TypedDict, TypeVar

from bec_lib.messages import BECMessage

P = ParamSpec("P")
_BecMsgT = TypeVar("_BecMsgT", bound=BECMessage)


class PubSubMessage(TypedDict):
    channel: bytes
    data: bytes
    pattern: bytes | None


class IncompatibleMessageForEndpoint(TypeError): ...


class IncompatibleRedisOperation(TypeError): ...


class InvalidItemForOperation(ValueError): ...


class WrongArguments(ValueError): ...
