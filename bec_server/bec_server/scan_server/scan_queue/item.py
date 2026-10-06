"""Assembly, state, and history for one queued scan request."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Literal

from bec_lib import messages

from .types import ExitInfoType, InstructionQueueStatus

if TYPE_CHECKING:
    from ..direct_scan_worker import ScanControl
    from ..scan_assembler import ScanAssembler
    from ..scans.scan_base import ScanBase
    from .queue import ScanQueue


class DirectInstructionQueueItem:
    """Own one request's execution state on the manager coordinator thread."""

    def __init__(self, parent: ScanQueue, assembler: ScanAssembler) -> None:
        """Initialize request assembly and lifecycle tracking.

        Args:
            parent (ScanQueue): Named queue that owns this request item.
            assembler (ScanAssembler): Assembler used to construct the scan from its request.
        """
        self.parent = parent
        self.assembler = assembler
        self.control: ScanControl | None = None
        self.exit_info: ExitInfoType | None = None
        self.queue_id = str(uuid.uuid4())
        self._scan_id: str | None = str(uuid.uuid4())

        self._status = InstructionQueueStatus.PENDING
        self._run_on_exception_hook: bool | None = None

        self.active_scan: ScanBase | None = None
        self.scan: ScanBase | None = None
        self.request: messages.ScanQueueMessage | None = None
        self.assigned_scan_number: int | None = None
        self.assigned_dataset_number: int | None = None
        self.reason: Literal["user", "alarm", "restart"] | None = None

    #############################################
    ############## Item Management ##############
    #############################################

    @property
    def scans(self) -> list[ScanBase]:
        """Compatibility view of the submitted main scan."""
        return [self.scan] if self.scan is not None else []

    @property
    def scan_msgs(self) -> list[messages.ScanQueueMessage]:
        """Compatibility view of the submitted main request."""
        return [self.request] if self.request is not None else []

    def assign_numbers(self, scan_number: int | None, dataset_number: int | None) -> None:
        """Assign the submitted scan's public scan and dataset numbers.

        Args:
            scan_number (int | None): Public scan number reserved for the item.
            dataset_number (int | None): Dataset number reserved for the item.
        """
        self.assigned_scan_number = scan_number
        self.assigned_dataset_number = dataset_number
        if self.scan is not None:
            self.scan.scan_info.scan_number = scan_number
            self.scan.scan_info.dataset_number = dataset_number

    @property
    def status(self) -> InstructionQueueStatus:
        """Read the instruction queue state.

        Returns:
            InstructionQueueStatus: Current queue state.
        """
        return self._status

    @status.setter
    def status(self, val: InstructionQueueStatus) -> None:
        """Record item state; instruction methods apply execution controls explicitly.

        Args:
            val (InstructionQueueStatus): New queue state.
        """
        self._status = val

    @property
    def scan_id(self) -> list[str | None]:
        """Read the scan identifiers attached to this item.

        Returns:
            list[str | None]: Identifiers of the scans attached to the queue item.
        """
        return [self._scan_id if self.is_scan[0] else None] if self.scan is not None else []

    @property
    def is_scan(self) -> list[bool]:
        """Identify which attached requests represent scans.

        Returns:
            list[bool]: Whether each attached request represents a scan.
        """
        return [self.scan.scan_info.scan_type is not None] if self.scan is not None else []

    @property
    def scan_number(self) -> list[int | None]:
        """Read the assigned or estimated scan numbers.

        Returns:
            list[int | None]: Assigned or estimated numbers of the attached scans.
        """
        return [self._get_scan_number()] if self.scan is not None else []

    @property
    def run_on_exception_hook(self) -> bool:
        """Read the scan exception cleanup policy.

        Returns:
            bool: Whether the scan exception cleanup hook is enabled.
        """
        if self._run_on_exception_hook is not None:
            return self._run_on_exception_hook
        if self.active_scan is not None:
            return bool(self.active_scan.scan_info.run_on_exception_hook)
        return False

    @run_on_exception_hook.setter
    def run_on_exception_hook(self, val: bool) -> None:
        """Set the scan exception cleanup policy.

        Args:
            val (bool): New cleanup policy.
        """
        self._run_on_exception_hook = val
        if self.control is not None:
            self.control.set_cleanup_enabled(val)

    def append_scan_request(self, msg: messages.ScanQueueMessage) -> None:
        """Assemble one scan request and attach it to this queue item.

        Args:
            msg (messages.ScanQueueMessage): the scan queue message containing the scan information

        Raises:
            RuntimeError: The item already contains a scan request.
        """
        if self.scan is not None:
            raise RuntimeError("A queue item can contain only one scan request")
        scan_cls = self.assembler.scan_manager.scan_dict[msg.scan_type]
        scan_id = self._scan_id if getattr(scan_cls, "is_scan", True) else None
        scan = self.assembler.assemble_scan(msg, scan_id=scan_id)
        self.scan = scan
        self.request = msg
        self._scan_id = scan.scan_info.scan_id
        scan.scan_info.metadata["queue_id"] = self.queue_id

    #############################################
    ############ Instruction Methods ############
    #############################################

    def set_active(self) -> None:
        """Change the instruction queue status to RUNNING."""
        if self.status == InstructionQueueStatus.PENDING:
            self.status = InstructionQueueStatus.RUNNING

    @property
    def requested_action(self) -> Literal["pause", "abort", "halt"] | None:
        """Read the execution request from the existing worker control channel."""
        return self.control.requested_action if self.control is not None else None

    def stop(self) -> bool:
        """Request interruption without claiming that execution has finished.

        Returns:
            bool: Whether a new stop or halt escalation was requested.
        """
        return self.control.stop(self.exit_info) if self.control is not None else False

    def pause(self) -> None:
        """Request pause; the worker acknowledges its actual state at a checkpoint."""
        if self.control is not None:
            self.control.request_pause()

    def resume(self) -> None:
        """Withdraw a pause request without reviving an interrupted acquisition."""
        if self.control is not None:
            self.control.resume()
        elif self.status == InstructionQueueStatus.PAUSED:
            self.status = InstructionQueueStatus.PENDING
        if self.status == InstructionQueueStatus.DEFERRED_PAUSE:
            self.status = InstructionQueueStatus.RUNNING

    #############################################
    ############### Helper Methods ##############
    #############################################

    def describe(self) -> messages.QueueInfoEntry:
        """Description of the instruction queue.

        Returns:
            messages.QueueInfoEntry: Status message describing the request and its scan.
        """
        return self._describe(None)

    def describe_at_offset(self, offset: int) -> messages.QueueInfoEntry:
        """Describe this item using its precomputed queue-relative scan-number offset.

        Args:
            offset (int): Offset of this public scan item within the queue.

        Returns:
            messages.QueueInfoEntry: Request description with assigned or estimated scan numbers.
        """
        return self._describe(offset)

    def describe_history(self) -> messages.ScanQueueHistoryMessage:
        """Build a detached terminal history entry for publication."""
        return messages.ScanQueueHistoryMessage(
            status=self.status.name, queue_id=self.queue_id, info=self.describe()
        ).model_copy(deep=True)

    def _describe(self, offset: int | None) -> messages.QueueInfoEntry:
        """Build a request description using an optional precomputed number offset.

        Args:
            offset (int | None): Queue-relative number offset, or None to calculate it on demand.

        Returns:
            messages.QueueInfoEntry: Request and scan status description.
        """
        request_blocks = self._describe_scans(offset)
        reason = self.reason
        if self.exit_info is not None:
            _, exit_reason = self.exit_info
            reason = reason or exit_reason
        return messages.QueueInfoEntry(
            queue_id=self.queue_id,
            scan_id=self.scan_id,
            is_scan=self.is_scan,
            request_blocks=request_blocks,
            scan_number=[self._get_scan_number(offset)] if self.scan is not None else [],
            status=self.status.name,
            active_request_block=self._describe_active_scan(offset),
            reason=reason,
        )

    def _describe_active_scan(self, offset: int | None) -> messages.RequestBlock | None:
        """Describe active execution through the original user-facing scan request."""
        if self.active_scan is None or self.active_scan not in self.scans:
            return None
        return self._get_request_block_message(self.active_scan, self.request, offset)

    def _describe_scans(self, offset: int | None) -> list[messages.RequestBlock]:
        """Describe the submitted request without exposing internal subscans."""
        if self.scan is None or self.request is None:
            return []
        return [self._get_request_block_message(self.scan, self.request, offset)]

    def _get_request_block_message(
        self, scan: ScanBase, msg: messages.ScanQueueMessage, offset: int | None = None
    ) -> messages.RequestBlock:
        """Get the request block message for a given scan and scan queue message.

        Args:
            scan (ScanBase): the scan for which to get the request block message
            msg (messages.ScanQueueMessage): the scan queue message containing the scan information

            offset (int | None): Precomputed queue offset, or None to calculate it on demand.

        Returns:
            messages.RequestBlock: Request block describing the scan and its device locks.
        """
        return messages.RequestBlock(
            msg=msg,
            RID=msg.metadata["RID"],
            readout_priority=scan.scan_info.readout_priority_modification,
            is_scan=scan.scan_info.scan_type is not None,
            scan_number=self._get_scan_number(offset),
            scan_id=self.scan_id[0],
            report_instructions=scan.scan_info.scan_report_instructions,
            owned_device_locks=scan.actions.get_owned_device_locks(),
            pending_device_locks=scan.actions.get_pending_device_locks(),
        )

    @property
    def _scan_server_scan_number(self) -> int:
        """Read the last scan number acknowledged by the coordinator.

        Returns:
            int: Last scan number acknowledged by the coordinator.
        """
        return self.parent.queue_manager._last_scan_number

    def _get_scan_number(self, offset: int | None = None) -> int | None:
        """Return an assigned scan number or estimate its queue-relative number.

        Args:
            offset (int | None): Precomputed queue offset, or None to calculate it on demand.

        Returns:
            int | None: Assigned or estimated scan number, or None for non-scan requests.
        """
        if not self.is_scan or not self.is_scan[0]:
            return None
        if self.assigned_scan_number is not None:
            return self.assigned_scan_number
        if self.scan.scan_info.scan_number is not None:
            return self.scan.scan_info.scan_number
        return self._scan_server_scan_number + (
            self.parent.scan_offset(self) if offset is None else offset
        )
