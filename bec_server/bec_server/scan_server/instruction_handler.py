from __future__ import annotations

import collections
import threading
from typing import TYPE_CHECKING, Callable

from bec_lib.endpoints import MessageEndpoints

if TYPE_CHECKING:
    from bec_lib import messages
    from bec_lib.redis_connector import RedisConnector


class InstructionHandler:
    """
    Class to handle device instructions. This class is responsible for storing device instructions and calling any
    callbacks that are waiting for a specific instruction. The class also registers a callback for the
    device_instructions_response endpoint to add instructions to the storage.
    """

    def __init__(self, connector: RedisConnector):
        """
        Args:
            connector(RedisConnector): The connector to use for registering the device_instructions_response callback
        """
        self._connector = connector
        self._max_history = 50000
        self._instruction_storage: dict[str, messages.DeviceInstructionResponse] = {}
        self._callback_storage = collections.defaultdict(lambda: [])
        self._lock = threading.Lock()
        self._connector.register(
            MessageEndpoints.device_instructions_response(),
            cb=self._device_instructions_callback,
            parent=self,
        )

    @staticmethod
    def _device_instructions_callback(msg, parent):
        # pylint: disable=protected-access
        with parent._lock:
            parent.add_instruction(msg.value)

    def add_instruction(self, instruction: messages.DeviceInstructionResponse) -> None:
        """
        Add an instruction to the instruction storage and call any callbacks that are waiting for this instruction.
        If the instruction is completed or errored, remove the instruction from the storage.

        Args:
            instruction(messages.DeviceInstructionResponse): The instruction to add to the storage
        """

        if len(self._instruction_storage) > self._max_history:
            item_diid = next(reversed(self._instruction_storage))
            self._instruction_storage.pop(item_diid)
            self._callback_storage.pop(item_diid)

        self._instruction_storage[instruction.instruction_id] = instruction

        if instruction.instruction_id in self._callback_storage:
            for callback in self._callback_storage[instruction.instruction_id]:
                self._run_callback(callback, instruction)

        # Since we always create the status objects before submitting the instruction, we can safely remove the callback
        # once the status is completed or errored, whilst ensuring that the status object was updated properly
        if instruction.status in ["completed", "error"]:
            self._callback_storage.pop(instruction.instruction_id, None)
            self._instruction_storage.pop(instruction.instruction_id, None)

    @staticmethod
    def _run_callback(
        callback: Callable[[messages.DeviceInstructionResponse], None],
        instruction: messages.DeviceInstructionResponse,
    ) -> None:
        try:
            callback(instruction)
        except Exception as e:
            print(f"Error in callback for instruction {instruction.instruction_id}: {e}")

    def register_callback(
        self, instruction_id: str, callback: Callable[[messages.DeviceInstructionResponse], None]
    ) -> None:
        """
        Register a callback for a specific instruction. If the instruction is already in the storage, call the callback
        immediately.

        Args:
            instruction_id(str): The instruction id to register the callback for
            callback(Callable[[messages.DeviceInstructionResponse], None]): The callback to call when the instruction is
                received. The instruction is passed as the only argument.
        """

        if instruction_id in self._instruction_storage:
            self._run_callback(callback, self._instruction_storage[instruction_id])
            return

        with self._lock:
            self._callback_storage[instruction_id].append(callback)
