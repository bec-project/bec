"""Initialize and supervise scan execution and its supporting services."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from bec_lib import messages
from bec_lib.alarm_handler import Alarms
from bec_lib.bec_service import BECService
from bec_lib.connector import MessageObject
from bec_lib.devicemanager import DeviceManagerBase as DeviceManager
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.scan_number_container import ScanNumberContainer
from bec_lib.service_config import ServiceConfig
from bec_server.actors.builtin_actor_manager import BuiltinActorManager
from bec_server.actors.manager import ActorManager
from bec_server.procedures.container_utils import podman_available
from bec_server.procedures.container_worker import ContainerProcedureWorker
from bec_server.procedures.manager import ProcedureManager, RedisCredentials
from bec_server.procedures.subprocess_worker import SubProcessWorker

from .beamline_state_manager import BeamlineStateManager
from .device_lock_registry import DeviceLockRegistry
from .scan_assembler import ScanAssembler
from .scan_guard import ScanGuard
from .scan_manager import ScanManager
from .scan_queue import QueueManager

if TYPE_CHECKING:
    from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger


class ScanServer(BECService):
    """Coordinate scan requests, execution, and supporting services."""

    def __init__(self, config: ServiceConfig, connector_cls: type[RedisConnector]) -> None:
        """Initialize scan services, request handling, and supporting managers.

        Args:
            config (ServiceConfig): Configuration of the scan service.
            connector_cls (type[RedisConnector]): Redis connector class used by the service.
        """
        super().__init__(config, connector_cls, unique_service=True)
        self.device_lock_registry = DeviceLockRegistry()
        self._start_scan_manager()
        self._start_device_manager()
        self._start_queue_manager()
        self._start_scan_guard()
        self._start_scan_assembler()
        self._start_alarm_handler()
        self._reset_scan_number()
        self.queue_manager.set_number_baseline(self.scan_number)
        self._start_procedure_manager(
            use_subprocess_proc_worker=config.model.procedures.use_subprocess_worker
        )
        self._start_actor_managers()
        self.beamline_states = None
        self._start_beamline_state_manager()
        self.status = messages.BECStatus.RUNNING

    @property
    def scan_number(self) -> int:
        """Read the shared scan counter.

        Returns:
            int: Current shared scan counter value.
        """
        return self.scan_number_container.scan_number

    @scan_number.setter
    def scan_number(self, val: int) -> None:
        """Set the shared scan counter.

        Args:
            val (int): New counter value.
        """
        self.scan_number_container.scan_number = val

    @property
    def dataset_number(self) -> int:
        """Read the shared dataset counter.

        Returns:
            int: Current shared dataset counter value.
        """
        return self.scan_number_container.dataset_number

    @dataset_number.setter
    def dataset_number(self, val: int) -> None:
        """Set the shared dataset counter.

        Args:
            val (int): New counter value.
        """
        self.scan_number_container.dataset_number = val

    def shutdown(self, per_thread_timeout_s: float | None = None) -> None:
        """Shutdown the scan server.

        Args:
            per_thread_timeout_s (float | None): Unused compatibility parameter.
        """
        self.builtin_actor_manager.shutdown()
        self.actor_manager.shutdown()
        self.proc_manager.shutdown()
        self.scan_guard.shutdown()
        self.queue_manager.shutdown()
        self.device_manager.shutdown()

    #############################################
    ############### Helper Methods ##############
    #############################################

    def _start_device_manager(self) -> None:
        """Wait for the device server and initialize its proxy device manager."""
        self.wait_for_service("DeviceServer")
        self.device_manager = DeviceManager(self)
        self.device_manager.initialize([self.bootstrap_server])

    def _start_scan_manager(self) -> None:
        """Initialize the scan definition manager."""
        self.scan_manager = ScanManager(parent=self)

    def _start_queue_manager(self) -> None:
        """Initialize the queue manager and create the primary queue."""
        self.queue_manager = QueueManager(parent=self)
        self.queue_manager.add_queue("primary")

    def _start_scan_assembler(self) -> None:
        """Initialize the scan request assembler."""
        self.scan_assembler = ScanAssembler(parent=self)

    def _start_scan_guard(self) -> None:
        """Register scan request validation and admission callbacks."""
        self.scan_guard = ScanGuard(parent=self)

    def _start_beamline_state_manager(self) -> None:
        """Initialize beamline state monitoring."""
        self.beamline_states = BeamlineStateManager(self.connector, self.device_manager)

    def _start_alarm_handler(self) -> None:
        """Register the scan alarm callback."""
        self.connector.register(MessageEndpoints.alarm(), cb=self._alarm_callback)

    def _reset_scan_number(self) -> None:
        """Initialize the shared scan and dataset counters."""
        self.scan_number_container = ScanNumberContainer(self.connector)
        if self.connector.get(MessageEndpoints.scan_number()) is None:
            self.scan_number = 0
        if self.connector.get(MessageEndpoints.dataset_number()) is None:
            self.dataset_number = 0

    def _start_procedure_manager(self, use_subprocess_proc_worker: bool = False) -> None:
        """Initialize the configured procedure execution backend.

        Args:
            use_subprocess_proc_worker (bool): Whether to execute procedures in subprocesses.
        """
        procedure_worker = (
            SubProcessWorker
            if (use_subprocess_proc_worker or not podman_available())
            else ContainerProcedureWorker
        )
        self.proc_manager = ProcedureManager(
            self.bootstrap_server,
            procedure_worker,
            redis_credentials=cast(RedisCredentials, self.acl.current_credentials()),
        )

    def _start_actor_managers(self) -> None:
        """Initialize built-in and user-defined actors."""
        self.actor_manager = ActorManager(self.bootstrap_server)
        self.builtin_actor_manager = BuiltinActorManager(self.bootstrap_server)

    def _alarm_callback(self, msg: MessageObject[messages.AlarmMessage]) -> None:
        """Forward scan-stopping alarms to the queue coordinator.

        Args:
            msg (MessageObject[messages.AlarmMessage]): Request or event message to handle.
        """
        alarm = cast(messages.AlarmMessage, msg.value)
        queue = alarm.metadata.get("queue", "primary")
        if Alarms(alarm.content["severity"]) == Alarms.MAJOR:
            logger.info(f"Received alarm: {alarm}")
            scan_id = alarm.metadata.get("scan_id")
            if alarm.metadata.get("request_rejected"):
                return
            self.queue_manager.post_abort(
                scan_id=scan_id, queue=queue, exit_info=("aborted", "alarm")
            )
