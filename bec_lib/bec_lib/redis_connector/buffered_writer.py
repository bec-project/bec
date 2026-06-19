from collections import defaultdict, deque
from dataclasses import dataclass
from time import monotonic_ns
from typing import Callable

from bec_lib.messages import BECMessage

MessageFactory = Callable[[], BECMessage]
WriteCommand = Callable[[str, BECMessage], None]
BufferEntry = tuple[str, WriteCommand]

# Notes:
#  - Should it just be message creation and kwargs, then generic over MessageT ?
#  - or kwargs | callable[[], kwargs]
#


@dataclass
class WriteBuffer:
    message_factories: deque[MessageFactory]
    last_called_time: float


class BufferedWriter:
    def __init__(self):
        self._write_buffers: dict[BufferEntry, WriteBuffer] = defaultdict(
            lambda: WriteBuffer(deque(), 0)
        )
        self._cooldown_ns = 100_000_000  # 0.1 s

    def write(self, endpoint: str, command: WriteCommand, message_factory: MessageFactory):
        entry = self._write_buffers[(endpoint, command)]
        try:
            if monotonic_ns() - entry.last_called_time > self._cooldown_ns:
                command(endpoint, message_factory())
                return
            entry.message_factories.append(message_factory)
        finally:
            entry.last_called_time = monotonic_ns()
