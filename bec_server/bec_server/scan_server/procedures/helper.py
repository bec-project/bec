from typing import Literal

from bec_lib.endpoints import EndpointInfo
from bec_lib.endpoints import MessageEndpoints as ME
from bec_lib.logger import bec_logger
from bec_lib.messages import BECMessage
from bec_lib.messages import ProcedureAbortMessage as AbrtMsg
from bec_lib.messages import ProcedureClearUnhandledMessage as ClrMsg
from bec_lib.messages import ProcedureExecutionMessage as ExecMsg
from bec_lib.messages import ProcedureQNotifMessage as QNotifMsg
from bec_lib.messages import ProcedureRequestMessage as ReqMsg
from bec_lib.redis_connector import RedisConnector

logger = bec_logger.logger


class _HelperBase:
    def __init__(self, conn: RedisConnector) -> None:
        self._conn = conn


class _Request(_HelperBase):
    def _xadd(self, ep: EndpointInfo, msg: BECMessage):
        self._conn.xadd(ep, msg.model_dump())

    def procedure(self, msg: ReqMsg):
        self._xadd(ME.procedure_request(), msg)

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


class FrontendProcedureHelper(_HelperBase):

    def __init__(self, conn: RedisConnector) -> None:
        super().__init__(conn)
        self.request = _Request(conn)
        self.get = _Get(conn)


class _BackenHelperBase(_HelperBase):
    def __init__(self, conn: RedisConnector, parent: "BackendProcedureHelper") -> None:
        self._conn = conn
        self._parent = parent


class _Push(_BackenHelperBase):
    def exec(self, queue: str, msg: ExecMsg):
        """Push execution message `msg` to execution queue `queue`"""
        self._conn.rpush(ME.procedure_execution(queue), msg)
        self._parent.notify_watchers(queue, "execution")

    def unhandled(self, queue: str, msg: ExecMsg):
        """Push execution message `msg` to unhandled execution queue `queue`"""
        self._conn.rpush(ME.unhandled_procedure_execution(queue), msg)
        self._parent.notify_watchers(queue, "unhandled")


class _Clear(_BackenHelperBase):
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


class _Move(_BackenHelperBase):
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


class _RemoveFromActive(_BackenHelperBase):
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
    def __init__(self, conn: RedisConnector) -> None:
        super().__init__(conn)
        self.push = _Push(conn, self)
        self.clear = _Clear(conn, self)
        self.move = _Move(conn, self)
        self.remove_from_active = _RemoveFromActive(conn, self)

    def notify_watchers(self, queue: str, queue_type: Literal["execution", "unhandled"]):
        return self._conn.send(
            ME.procedure_queue_notif(), QNotifMsg(queue_name=queue, queue_type=queue_type)
        )

    def notify_all(self, queue_type: Literal["execution", "unhandled"]):
        for queue in self.get.queue_names(queue_type):
            self.notify_watchers(queue, queue_type)
