"""Assembly, state, and history for one queued scan request."""

from __future__ import annotations

import threading
import uuid
from typing import TYPE_CHECKING, Literal

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

from .operations import coordinated
from .types import ExitInfoType, InstructionQueueStatus

if TYPE_CHECKING:
    from ..direct_scan_worker import ScanControl
    from ..scan_assembler import ScanAssembler
    from ..scans.scan_base import ScanBase
    from .queue import ScanQueue

logger = bec_logger.logger


class DirectInstructionQueueItem:
    """Assemble and track one queued scan request."""

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
        self._scan_id = str(uuid.uuid4())

        self._status = InstructionQueueStatus.PENDING
        self._run_on_exception_hook: bool | None = None

        self.active_scan: ScanBase | None = None
        self.scans: list[ScanBase] = []
        self.scan_msgs: list[messages.ScanQueueMessage] = []
        self.reason: Literal["user", "alarm", "restart"] | None = None

    @property
    @coordinated
    def status(self) -> InstructionQueueStatus:
        """Read the instruction queue state.

        Returns:
            InstructionQueueStatus: Current queue state.
        """
        return self._status

    @status.setter
    @coordinated
    def status(self, val: InstructionQueueStatus) -> None:
        """Set the instruction queue state.

        Args:
            val (InstructionQueueStatus): New queue state.
        """
        logger.debug(
            f"Setting status of direct instruction queue {self.parent.queue_name} to {val.name} from thread {threading.current_thread().name}"
        )
        self._status = val
        if self.control is not None:
            if val == InstructionQueueStatus.STOPPED:
                self.control.stop(self.exit_info)
            elif val in (InstructionQueueStatus.RUNNING, InstructionQueueStatus.PAUSED):
                self.control.set_status(val)
        if val == InstructionQueueStatus.STOPPED:
            self.stop()
        self.parent.queue_manager.send_queue_status()

    @property
    @coordinated
    def scan_id(self) -> list[str | None]:
        """Read the scan identifiers attached to this item.

        Returns:
            list[str | None]: Identifiers of the scans attached to the queue item.
        """
        return [scan.scan_info.scan_id for scan in self.scans]

    @property
    @coordinated
    def is_scan(self) -> list[bool]:
        """Identify which attached requests represent scans.

        Returns:
            list[bool]: Whether each attached request represents a scan.
        """
        return [scan.scan_info.scan_type is not None for scan in self.scans]

    @property
    @coordinated
    def scan_number(self) -> list[int | None]:
        """Read the assigned or estimated scan numbers.

        Returns:
            list[int | None]: Assigned or estimated numbers of the attached scans.
        """
        return [self._get_scan_number(scan) for scan in self.scans]

    @coordinated
    def append_scan_request(self, msg: messages.ScanQueueMessage) -> None:
        """Assemble one scan request and attach it to this queue item.

        Args:
            msg (messages.ScanQueueMessage): the scan queue message containing the scan information

        Raises:
            RuntimeError: The item already contains a scan request.
        """
        if self.scans:
            raise RuntimeError("A queue item can contain only one scan request")
        scan_cls = self.assembler.scan_manager.scan_dict[msg.scan_type]
        scan_id = self._scan_id if getattr(scan_cls, "is_scan", True) else None
        scan = self.assembler.assemble_scan(msg, scan_id=scan_id)
        self.scans.append(scan)
        self.scan_msgs.append(msg)

    @coordinated
    def set_active(self) -> None:
        """Change the instruction queue status to RUNNING."""
        if self.status == InstructionQueueStatus.PENDING:
            self.status = InstructionQueueStatus.RUNNING

    @property
    @coordinated
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
    @coordinated
    def run_on_exception_hook(self, val: bool) -> None:
        """Set the scan exception cleanup policy.

        Args:
            val (bool): New cleanup policy.
        """
        self._run_on_exception_hook = val
        if self.control is not None:
            self.control.set_cleanup_enabled(val)

    @coordinated
    def describe(self) -> messages.QueueInfoEntry:
        """Description of the instruction queue.

        Returns:
            messages.QueueInfoEntry: Status message describing the request and its scan.
        """
        return self._describe(None)

    @coordinated
    def describe_at_offset(self, offset: int) -> messages.QueueInfoEntry:
        """Describe this item using its precomputed queue-relative scan-number offset.

        Args:
            offset (int): Offset computed with the same rules as scan_ids_head.

        Returns:
            messages.QueueInfoEntry: Request description with assigned or estimated scan numbers.
        """
        return self._describe(offset)

    @coordinated
    def describe_active_scan(self) -> messages.RequestBlock | None:
        """Description of the active scan.

        Returns:
            messages.RequestBlock | None: Description of the active scan, or None if none is
                selected.
        """
        return self._describe_active_scan(None)

    @coordinated
    def describe_scans(self) -> list[messages.RequestBlock]:
        """Description of the scans in the instruction queue item.

        Returns:
            list[messages.RequestBlock]: Descriptions of the scans attached to the request.
        """
        return self._describe_scans(None)

    @coordinated
    def scan_ids_head(self, target_scan: ScanBase) -> int:
        """Calculate the scan-number offset for a scan within the current queue.

        Args:
            target_scan (ScanBase): Scan whose position in the queue determines its number offset.

        Returns:
            int: One-based scan-number offset within this queue.
        """
        offset = 1
        for queue in self.parent.queue:
            if queue.status in [InstructionQueueStatus.COMPLETED, InstructionQueueStatus.RUNNING]:
                continue
            if queue.queue_id != self.queue_id:
                offset += sum(bool(scan_id) for scan_id in queue.scan_id)
                continue
            for scan in queue.scans:
                if scan is target_scan:
                    return offset
                if scan.scan_info.scan_id:
                    offset += 1
            return offset
        return offset

    @coordinated
    def move_to_next_scan(self) -> ScanBase:
        """Move to the next scan in the instruction queue item.

        Returns:
            ScanBase: Scan selected for execution.

        Raises:
            StopIteration: No further scan is available in the item.
        """
        if self.active_scan is None:
            if len(self.scans) > 0:
                scan = self.scans[0]
                self._set_scan_as_active(scan)
                return scan
            raise StopIteration("No active scan and no scans in the queue.")
        current_index = self.scans.index(self.active_scan)
        if current_index + 1 < len(self.scans):
            scan = self.scans[current_index + 1]
            self._set_scan_as_active(scan)
            return scan
        raise StopIteration("No more scans in the queue.")

    @coordinated
    def append_to_queue_history(self) -> None:
        """Append a new queue item to the redis history buffer."""
        msg = messages.ScanQueueHistoryMessage(
            status=self.status.name, queue_id=self.queue_id, info=self.describe()
        )
        self.parent.queue_manager._publisher.post(
            self.parent.queue_manager.connector.lpush,
            MessageEndpoints.scan_queue_history(),
            msg.model_copy(deep=True),
            max_size=100,
        )

    @coordinated
    def stop(self) -> None:
        """Stop the instruction queue item and all active scans."""
        for scan in self.scans:
            scan._shutdown_event.set()

    @coordinated
    def abort(self) -> None:
        """Discard the active scan and attached request data."""
        self.active_scan = None
        self.scans = []
        self.scan_msgs = []

    #############################################
    ############### Helper Methods ##############
    #############################################

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
            scan_number=[self._get_scan_number(scan, offset) for scan in self.scans],
            status=self.status.name,
            active_request_block=self._describe_active_scan(offset),
            reason=reason,
        )

    def _describe_active_scan(self, offset: int | None) -> messages.RequestBlock | None:
        """Describe the selected scan using the supplied offset when available.

        Args:
            offset (int | None): Queue-relative number offset, or None to calculate it on demand.

        Returns:
            messages.RequestBlock | None: Active scan description, or None if none is selected.
        """
        if self.active_scan is None:
            return None
        if self.active_scan not in self.scans:
            return None
        msg = self.scan_msgs[self.scans.index(self.active_scan)]
        return self._get_request_block_message(self.active_scan, msg, offset)

    def _describe_scans(self, offset: int | None) -> list[messages.RequestBlock]:
        """Describe attached scans using the supplied offset when available.

        Args:
            offset (int | None): Queue-relative number offset, or None to calculate it on demand.

        Returns:
            list[messages.RequestBlock]: Attached scan descriptions in request order.
        """
        return [
            self._get_request_block_message(scan, msg, offset)
            for scan, msg in zip(self.scans, self.scan_msgs)
        ]

    @coordinated
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
            scan_number=self._get_scan_number(scan, offset),
            scan_id=scan.scan_info.scan_id,
            report_instructions=scan.scan_info.scan_report_instructions,
            owned_device_locks=scan.actions.get_owned_device_locks(),
            pending_device_locks=scan.actions.get_pending_device_locks(),
        )

    @property
    @coordinated
    def _scan_server_scan_number(self) -> int:
        """Read the last scan number acknowledged by the coordinator.

        Returns:
            int: Last scan number acknowledged by the coordinator.
        """
        return self.parent.queue_manager._last_scan_number

    @coordinated
    def _get_scan_number(self, scan: ScanBase, offset: int | None = None) -> int | None:
        """Return an assigned scan number or estimate its queue-relative number.

        Args:
            scan (ScanBase): Scan instance to execute or describe.

            offset (int | None): Precomputed queue offset, or None to calculate it on demand.

        Returns:
            int | None: Assigned or estimated scan number, or None for non-scan requests.
        """
        if not scan.is_scan:
            return None
        if scan.scan_info.scan_number is not None:
            # We've already assigned a scan number to this scan, return it
            return scan.scan_info.scan_number
        return self._scan_server_scan_number + (
            self.scan_ids_head(scan) if offset is None else offset
        )

    @coordinated
    def _set_scan_as_active(self, scan: ScanBase) -> None:
        """Set a given scan as the active scan.

        Args:
            scan (ScanBase): Scan instance to execute or describe.
        """
        self.active_scan = scan
