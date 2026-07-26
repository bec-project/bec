"""
Scan stubs are commands that can be used to control devices during a scan. They typically yield device messages that are
consumed by the scan worker and potentially forwarded to the device server.
"""

from __future__ import annotations

import concurrent
import concurrent.futures
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

import numpy as np

from bec_lib import messages
from bec_lib.logger import bec_logger

from .errors import DeviceInstructionError

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.scan_server.instruction_handler import InstructionHandler


logger = bec_logger.logger


class ScanStubStatus:
    """
    Status object that can be used to wait for the completion of a device instruction.
    """

    def __init__(
        self,
        instruction_handler: InstructionHandler,
        device_instr_id: str = None,
        done: bool = False,
        shutdown_event: threading.Event = None,
        registry: dict = None,
        is_container: bool = False,
        name: str | None = None,
    ) -> None:
        """
        Initialize the status object.

        Args:
            instruction_handler (InstructionHandler): Instruction handler.
            device_instr_id (str): Device instruction ID.
            done (bool, optional): Flag that indicates if the status object is done. Defaults to False.
            shutdown_event (threading.Event, optional): Shutdown event. Defaults to None.
            registry (dict, optional): Registry for status objects. Defaults to None.
            is_container (bool, optional): Flag that indicates if the status object is a container. Defaults to False.
            name (str, optional): Name of the status object. Defaults to None.

        """
        self._name = name
        self._instruction_handler = instruction_handler
        self._device_instr_id = (
            device_instr_id if device_instr_id is not None else str(uuid.uuid4())
        )
        self._shutdown_event = shutdown_event if shutdown_event is not None else threading.Event()
        self._registry = registry if registry is not None else {}
        self._sub_status_objects: list[ScanStubStatus] = []
        self._done = done
        self._done_checked = False
        self.value = None
        self.message = None
        self._result_is_status: bool | None = None
        self._future = concurrent.futures.Future()
        self._is_container = is_container
        if is_container:
            self.set_done()
        else:
            self._instruction_handler.register_callback(self._device_instr_id, self._update_future)

    @property
    def done(self) -> bool:
        """
        Get the done flag.

        Returns:
            bool: Done flag
        """
        if self._shutdown_event.is_set():
            self.set_done_checked()
            return True
        self.set_done_checked()
        sub_status_done = self._get_sub_status_done()
        return self._done and sub_status_done

    @done.setter
    def done(self, value: bool):
        self._done = value

    def set_done_checked(self):
        """
        Manually set the done checked flag to avoid creating warnings for unchecked status objects.
        """
        self._done_checked = True
        for st in self._sub_status_objects:
            st._done_checked = True

    def add_status(self, status: ScanStubStatus):
        """
        Add a status object to the current status object.
        This can be used to wait for the completion of multiple status objects.

        Args:
            status (ScanStubStatus): Status object

        """

        self._sub_status_objects.append(status)

    def _update_future(self, message: messages.DeviceInstructionResponse = None):
        self.message = message
        if self.message.result_is_status is not None:
            self._result_is_status = self.message.result_is_status

        if message.status == "completed":
            self.set_done(message.result)
        elif message.status == "error":
            self.set_failed(message.error_info)
        else:
            self.set_running()

    def set_done(self, result=None):
        """
        Set the status object to done.

        Args:
            result (any, optional): Result of the operation. Defaults to None.
        """
        self.done = True
        self._future.set_result(result)

    def set_failed(self, error_info: messages.ErrorInfo):
        """
        Set the status object to failed.

        Args:
            error_info (messages.ErrorInfo, optional): Error information. Defaults to None.
        """
        self.done = True
        exc = DeviceInstructionError(error_info)
        self._future.set_exception(exc)

    def set_running(self):
        """
        Set the status object to running.
        """
        if self._future.running():
            return
        self.done = False
        self._future.set_running_or_notify_cancel()

    @property
    def result(self) -> Any:
        """
        Get the result of the operation.
        If the status object is a container of multiple status objects, the result will be a list of the results of the sub status objects.
        If the status object is not done, the result will be None.

        Returns:
            Any: Result of the operation, or list of results of the sub status objects.
        """

        if not self._done or not self._get_sub_status_done():
            return None

        if self._sub_status_objects:
            out = []
            for st in self._sub_status_objects:
                out.append(st._future.result())
            if not self._is_container:
                out.append(self._future.result())
            return out
        return self._future.result()

    def _get_sub_status_done(self) -> bool:
        return (
            all(st._done for st in self._sub_status_objects) if self._sub_status_objects else True
        )

    @staticmethod
    def _raise_if_failed(obj: concurrent.futures.Future) -> None:
        if obj.exception() is not None:
            raise obj.exception()

    def wait(
        self,
        min_wait: float | None = None,
        timeout: float = np.inf,
        logger_wait=5,
        resolve_on_known_type: bool = False,
    ) -> ScanStubStatus:
        """
        Wait for the completion of the status object.

        Args:
            min_wait (float, optional): Minimum wait time in seconds. Defaults to None.
            timeout (float, optional): Timeout in seconds. Defaults to None.
            logger_wait (int, optional): Time in seconds before logging the remaining status objects. Defaults to 5.
            resolve_on_known_type (bool, optional): Whether to exit early once the return type of the rpc method is known.
                It is used to discriminate status objects from normal return values. It is mostly for internal use and
                should be used with caution. Defaults to False.

        Raises:
            TimeoutError: Raised if the timeout is reached.
            DeviceInstructionError: Raised if the instruction failed.
            ValueError: Raised if resolve_on_known_type is True but the status object has sub status objects.

        Returns:
            ScanStubStatus: Status object
        """
        if resolve_on_known_type and self._sub_status_objects:
            # Something is wrong if we have multiple status objects and the caller expects to resolve to a single type.
            raise ValueError(
                "resolve_on_known_type is not supported for status objects with sub status objects."
            )

        self._registry.pop(self._device_instr_id, None)
        for st in self._sub_status_objects:
            self._registry.pop(st._device_instr_id, None)

        if min_wait is not None:
            time.sleep(min_wait)

        if self._done and self._get_sub_status_done():
            self._done_checked = True
            self._raise_if_failed(self._future)
            for st in self._sub_status_objects:
                st._done_checked = True
                self._raise_if_failed(st._future)
            return self

        # pylint: disable=protected-access
        futures = [st._future for st in self._sub_status_objects]
        futures.append(self._future)

        increment = 0.5
        wait_time = 0

        while not all(e.done() for e in futures):
            if resolve_on_known_type and self._result_is_status is not None:
                break
            done, _ = concurrent.futures.wait(
                futures, timeout=increment, return_when=concurrent.futures.FIRST_EXCEPTION
            )
            for future in done:
                self._raise_if_failed(future)
            wait_time += increment
            if wait_time >= timeout:
                raise TimeoutError("The wait operation timed out.")
            if self._shutdown_event.is_set():
                break
            if wait_time > logger_wait:
                objs = []
                objs.extend([str(st) for st in self._sub_status_objects if not st.done])
                objs.append(str(self))
                logger.info(f"Waiting for the completion of the following status objects: {objs}")

        return self

    def __repr__(self):
        name = f"{self._name}, " if self._name else ""
        if self.message:
            instr = self.message.instruction.action
            devices = self.message.instruction.device
            return f"ScanStubStatus({name}{self._device_instr_id}, action={instr}, devices={devices}, done={self._done})"
        return f"ScanStubStatus({name}{self._device_instr_id}, done={self._done})"
