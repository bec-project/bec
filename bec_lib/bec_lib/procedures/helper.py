from enum import Enum
from threading import Event
from typing import Any, Literal

from bec_lib.connector import MessageObject
from bec_lib.endpoints import EndpointInfo
from bec_lib.endpoints import MessageEndpoints as ME
from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage
from bec_lib.messages import ProcedureAbortMessage as AbrtMsg
from bec_lib.messages import ProcedureClearUnhandledMessage as ClrMsg
from bec_lib.messages import ProcedureExecutionMessage as ExecMsg
from bec_lib.messages import ProcedureQNotifMessage as QNotifMsg
from bec_lib.messages import ProcedureRequestMessage as ReqMsg
from bec_lib.messages import ProcedureStatusUpdate as StatUpd
from bec_lib.messages import RequestResponseMessage as RespMsg
from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger


class ProcedureState(Enum):
    REQUESTED = "Requested"
    REJECTED = "Rejected"
    SCHEDULED = "Scheduled"
    RUNNING = "Running"
    SUCCESS = "Success"
    FAILED = "Failed"


class ProcedureStatus:
    def __init__(self, conn: RedisConnector, proc_id, identifier: str) -> None:
        self._conn = conn
        self._id = proc_id
        self._identifier = identifier
        self._helper = FrontendProcedureHelper(self._conn, monitor_responses=False)
        self._state: ProcedureState = ProcedureState.REQUESTED
        self._done: Event = Event()
        self._error: str | None = None

    @property
    def done(self):
        return self._done.is_set()

    def wait(self, timeout_s: float | None = None, raise_on_failure: bool = False):
        """Wait for the procedure to be finished."""
        try:
            self._done.wait(timeout=timeout_s)
        except KeyboardInterrupt:
            logger.error(
                "Cancelled waiting. To cancel the procedure please call .cancel() on this status."
            )
        if raise_on_failure and self.done and self._state == ProcedureState.FAILED:
            raise RuntimeError(str(self._error))

    def cancel(self):
        """Request for the procedure to be cancelled."""
        if self.state not in [
            ProcedureState.REQUESTED,
            ProcedureState.RUNNING,
            ProcedureState.SCHEDULED,
        ]:
            raise ValueError(f"A procedure which is already {self.state} cannot be cancelled!")
        self._helper.request.abort_execution(self._id)

    def set_error(self, error: str):
        if self.state not in [ProcedureState.FAILED, ProcedureState.REJECTED]:
            raise ValueError("Cannot set error on an unfinished status.")
        self._error = error

    def raise_for_status(self):
        if self.state in [ProcedureState.FAILED, ProcedureState.REJECTED]:
            raise RuntimeError(self._error)

    @property
    def error(self):
        return self._error

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state: ProcedureState):
        # A rejected or finished status can no longer be updated
        if self._state in [ProcedureState.REJECTED, ProcedureState.FAILED, ProcedureState.SUCCESS]:
            return
        # The request can be accepted or denied
        if self._state is ProcedureState.REQUESTED:
            if state not in [ProcedureState.REJECTED, ProcedureState.SCHEDULED]:
                raise ValueError(f"Improper transition from {self._state} to {state}")
            self._state = state
            if state == ProcedureState.REJECTED:
                self._done.set()
                return
            if state == ProcedureState.SCHEDULED:
                return
        # A scheduled procedure can be started
        if self._state == ProcedureState.SCHEDULED:
            if state == ProcedureState.RUNNING:
                self._state = state
                return
        # A scheduled or running procedure can be set to finished
        if self._state in [ProcedureState.RUNNING, ProcedureState.SCHEDULED]:
            if state in [ProcedureState.FAILED, ProcedureState.SUCCESS]:
                self._state = state
                self._done.set()
                return
            else:
                raise ValueError(f"Improper transition from {self._state} to {state}")

    def __repr__(self) -> str:
        msg = f"<ProcedureStatus for '{self._identifier}', state: '{self.state.value}'>"
        if self._error is not None:
            msg += f"\nERROR:\n{self._error}"
        return msg


class _HelperBase:
    def __init__(self, conn: RedisConnector, monitor_responses: bool = True) -> None:
        self._monitor_responses = monitor_responses
        self._conn = conn
        self._callback_ids: dict[str, ProcedureStatus] = {}
        if self._monitor_responses:
            self._conn.register(ME.procedure_request_response(), cb=self._request_cb)
            self._conn.register(ME.procedure_status_update(), cb=self._update_cb)

    def _request_cb(self, msg: MessageObject):
        if not self._monitor_responses:
            return
        msg_: RespMsg = msg.value  # type: ignore
        if not isinstance(msg_.message, dict):
            raise ValueError(
                f"Malformed request response message: message should be a dict, got {msg_.message}"
            )
        if (_id := msg_.message.get("execution_id", "_")) in self._callback_ids:
            if msg_.accepted:
                self._callback_ids[_id].state = ProcedureState.SCHEDULED
            else:
                self._callback_ids[_id].state = ProcedureState.REJECTED
                self._callback_ids[_id].set_error(msg_.message.get("message", "Unknown error."))
                del self._callback_ids[_id]
            logger.debug(f"Updated status for procedure execution {_id}")

    def _update_cb(self, msg: MessageObject):
        if not self._monitor_responses:
            return
        msg_: StatUpd = msg.value  # type: ignore
        if msg_.execution_id in self._callback_ids:
            if msg_.action == "Started":
                self._callback_ids[msg_.execution_id].state = ProcedureState.RUNNING
            elif msg_.action == "Aborted":
                self._callback_ids[msg_.execution_id].state = ProcedureState.FAILED
                self._callback_ids[msg_.execution_id].set_error("Aborted by user.")
                del self._callback_ids[msg_.execution_id]
            elif msg_.action == "Finished":
                if msg_.error is not None:
                    self._callback_ids[msg_.execution_id].state = ProcedureState.FAILED
                    self._callback_ids[msg_.execution_id].set_error(msg_.error)
                else:
                    self._callback_ids[msg_.execution_id].state = ProcedureState.SUCCESS
                del self._callback_ids[msg_.execution_id]
            logger.debug(f"Updated status for procedure execution {msg_.execution_id}")

    def _set_callbacks(self, exec_id: str, status: ProcedureStatus):
        if not self._monitor_responses:
            return
        self._callback_ids[exec_id] = status


class _Request(_HelperBase):

    def __init__(self, conn: RedisConnector, monitor_responses: bool = True) -> None:
        super().__init__(conn, monitor_responses)

    def _xadd(self, ep: EndpointInfo, msg: BECMessage):
        self._conn.xadd(ep, msg.model_dump(), max_size=1000, expire=3600)

    def procedure(
        self,
        identifier: str,
        args_kwargs: tuple[tuple[Any, ...], dict[str, Any]] | None = None,
        queue: str | None = None,
    ):
        msg = ReqMsg(identifier=identifier, args_kwargs=args_kwargs, queue=queue)
        return self._procedure(msg)

    def _procedure(self, msg):
        st = ProcedureStatus(self._conn, msg.execution_id, msg.identifier)
        self._set_callbacks(msg.execution_id, st)
        self._xadd(ME.procedure_request(), msg)
        return st

    def abort_execution(self, execution_id: str):
        """Send a message requesting an abort of execution_id"""
        return self._xadd(ME.procedure_abort(), AbrtMsg(execution_id=execution_id))

    def abort_queue(self, queue: str):
        """Send a message requesting an abort of execution_id"""
        return self._xadd(ME.procedure_abort(), AbrtMsg(queue=queue))

    def abort_all(self):
        """Send a message requesting an abort of execution_id"""
        return self._xadd(ME.procedure_abort(), AbrtMsg(abort_all=True))

    def clear_unhandled_execution(self, execution_id: str):
        """Send a message requesting an abort of execution_id"""
        return self._xadd(ME.procedure_clear_unhandled(), ClrMsg(execution_id=execution_id))

    def clear_unhandled_queue(self, queue: str):
        """Send a message requesting an abort of execution_id"""
        return self._xadd(ME.procedure_clear_unhandled(), ClrMsg(queue=queue))

    def clear_all_unhandled(self):
        """Send a message requesting an abort of execution_id"""
        return self._xadd(ME.procedure_clear_unhandled(), ClrMsg(abort_all=True))


class _Get(_HelperBase):
    def __init__(self, conn: RedisConnector, monitor_responses: bool = False) -> None:
        super().__init__(conn, monitor_responses)

    def available_procedures(self) -> dict[str, str]:
        if (data := self._conn.get(ME.available_procedures())) is None:
            return {}
        return data.resource

    def running_procedures(self) -> set[ExecMsg]:
        """Get all the running procedures"""
        return self._conn.get_set_members(ME.active_procedure_executions())

    def exec_queue(self, queue: str) -> list[ExecMsg]:
        """Get all the ProcedureExecutionMessages from a given execution queue"""
        return self._conn.lrange(ME.procedure_execution(queue), 0, -1)

    def unhandled_queue(self, queue: str) -> list[ExecMsg]:
        """Get all the ProcedureExecutionMessages from a given unhandled execution queue"""
        return self._conn.lrange(ME.unhandled_procedure_execution(queue), 0, -1)

    def active_and_pending_queue_names(self) -> list[str]:
        """Get the names of all pending queues and queues of currently running procedures"""
        return list(set(self.queue_names()) | set(self.active_queue_names()))

    def active_queue_names(self) -> list[str]:
        """Get the names of all queues of currently running procedures"""
        return list({msg.queue for msg in self.running_procedures()})

    def queue_names(self, queue_type: Literal["execution", "unhandled"] = "execution") -> list[str]:
        """Get the names of queues currently containing pending ProcedureExecutionMessages

        Args:
            queue_type (Literal["execution", "unhandled"]): Type of queue, default "execution" for currently active executions, "unhandled" for aborted executions
        """
        ep = (
            ME.procedure_execution
            if queue_type == "execution"
            else ME.unhandled_procedure_execution
        )
        raw: list[str] = [s.decode() for s in self._conn.keys(ep("*"))]
        return [s.split("/")[-1] for s in raw]

    def log_queue_names(self) -> list[str]:
        """Get the names of queues currently containing logs from procedures."""
        raw: list[str] = [s.decode() for s in self._conn.keys(ME.procedure_logs("*"))]
        return [s.split("/")[-1] for s in raw]


class FrontendProcedureHelper(_HelperBase):

    def __init__(self, conn: RedisConnector, monitor_responses: bool = True) -> None:
        super().__init__(conn, monitor_responses)
        self.request = _Request(conn)
        self.get = _Get(conn)


class _BackendHelperBase(_HelperBase):
    def __init__(self, conn: RedisConnector, parent: "BackendProcedureHelper") -> None:
        self._conn = conn
        self._parent = parent


class _Push(_BackendHelperBase):
    def exec(self, queue: str, msg: ExecMsg):
        """Push execution message `msg` to execution queue `queue`"""
        self._conn.rpush(ME.procedure_execution(queue), msg)
        self._parent.notify_watchers(queue, "execution")

    def unhandled(self, queue: str, msg: ExecMsg):
        """Push execution message `msg` to unhandled execution queue `queue`"""
        self._conn.rpush(ME.unhandled_procedure_execution(queue), msg)
        self._parent.notify_watchers(queue, "unhandled")


class _Clear(_BackendHelperBase):
    def all_unhandled(self):
        """Remove all unhandled execution queues"""
        for queue in self._parent.get.queue_names("unhandled"):
            self.unhandled_queue(queue)

    def unhandled_queue(self, queue: str):
        """Remove an unhandled execution queue"""
        self._conn.delete(ME.unhandled_procedure_execution(queue))
        self._parent.notify_watchers(queue, "unhandled")

    def unhandled_execution(self, execution_id: str):
        """Remove a ProcedureExecutionMessage from its unhandled queue by its execution ID"""
        for queue in self._parent.get.queue_names("unhandled"):
            for msg in self._parent.get.unhandled_queue(queue):
                if msg.execution_id == execution_id:
                    if self._conn.lrem(ME.unhandled_procedure_execution(msg.queue), 0, msg) > 0:
                        logger.debug(f"Removed execution {msg} from queue.")
                        self._parent.notify_watchers(queue, "unhandled")
                        return
        logger.debug(f"Execution {execution_id} not found in any unhandled queue.")


class _Move(_BackendHelperBase):
    def all_active_to_unhandled(self):
        """Move all messages in the active executions set to unhandled"""
        for msg in self._parent.get.running_procedures():
            self._parent.push.unhandled(msg.queue, msg)
        self._conn.delete(ME.active_procedure_executions())

    def execution_queue_to_unhandled(self, queue: str):
        """Move all messages from execution queue to unhandled execution queue of the same name"""
        for msg in self._parent.get.exec_queue(queue):
            self._parent.push.unhandled(queue, msg)
        self._conn.delete(ME.procedure_execution(queue))

    def all_execution_queues_to_unhandled(self):
        """Move all messages from all execution queues to unhandled execution queues of the same name"""
        for queue in self._parent.get.queue_names():
            self.execution_queue_to_unhandled(queue)


class _RemoveFromActive(_BackendHelperBase):
    def by_exec_id(self, execution_id: str):
        """Remove a message from the set of currently active executions"""
        for msg in self._conn.get_set_members(ME.active_procedure_executions()):
            if msg.execution_id == execution_id:
                self._conn.remove_from_set(ME.active_procedure_executions(), msg)
                logger.debug(f"removed active procedure {execution_id}")
                return
        logger.debug(f"No active procedure {execution_id} to remove")

    def by_queue(self, queue: str):
        """Remove a message from the set of currently active executions"""
        removed = False
        for msg in self._conn.get_set_members(ME.active_procedure_executions()):
            if msg.queue == queue:
                self._conn.remove_from_set(ME.active_procedure_executions(), msg)
                logger.debug(f"removed active procedure {msg} with queue {queue}")
        if removed:
            return
        logger.debug(f"No active procedure with queue {queue} to remove")


class BackendProcedureHelper(FrontendProcedureHelper):
    def __init__(self, conn: RedisConnector, monitor_responses: bool = True) -> None:
        super().__init__(conn, monitor_responses)
        self.push = _Push(conn, self)
        self.clear = _Clear(conn, self)
        self.move = _Move(conn, self)
        self.remove_from_active = _RemoveFromActive(conn, self)
        self._stat_ep = ME.procedure_status_update()

    def status_update(
        self, id: str, action: Literal["Started", "Aborted", "Finished"], error: str | None = None
    ):
        self._conn.send(self._stat_ep, StatUpd(execution_id=id, action=action, error=error))

    def notify_watchers(self, queue: str, queue_type: Literal["execution", "unhandled"]):
        return self._conn.send(
            ME.procedure_queue_notif(), QNotifMsg(queue_name=queue, queue_type=queue_type)
        )

    def notify_all(self, queue_type: Literal["execution", "unhandled"]):
        for queue in self.get.queue_names(queue_type):
            self.notify_watchers(queue, queue_type)
