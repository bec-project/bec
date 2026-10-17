import os
import subprocess
import sys
from signal import SIGINT

from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_server.procedures.oop_worker_base import PROCESS_TIMEOUT, OutOfProcessWorkerBase, main

logger = bec_logger.logger


class SubProcessWorker(OutOfProcessWorkerBase):
    """Run procedures in a subprocess, streaming stdout and stderr to Redis."""

    WORKER_FILE = __file__

    def _setup_execution_environment(self):
        # Match the UTF-8 decoder in the parent even when the host uses another encoding.
        env: dict[str, str] = {"PYTHONIOENCODING": "utf-8"} | self._worker_environment()
        first_start = not hasattr(self, "_output_logs")
        output_conn = RedisConnector(
            [self._redis_server], name=f"ProcedureWorker-{self._queue} output"
        )
        try:
            if self._control_credentials is not None:
                output_conn.authenticate(
                    username=self._control_credentials["username"] or "default",
                    password=self._control_credentials["token"],
                )
            if first_start and self._worker_id is None:
                output_conn.delete(MessageEndpoints.procedure_logs(self._queue))
            self._process = subprocess.Popen(
                [sys.executable, "-u", self.WORKER_FILE],
                stdin=subprocess.PIPE if self._worker_id is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE if self._worker_id is not None else subprocess.STDOUT,
                bufsize=0,
                env=os.environ | env | self._additional_env,
            )
        except BaseException:
            output_conn.shutdown()
            raise
        self._ending = False
        self._start_output_capture(output_conn)
        logger.success("Subprocess Procedure Worker started.")
        self._ending: bool = False

    def _ending_or_ended(self):
        return self._ending or (self._process.poll() is not None)

    def _kill_process(self):
        """Attempts to end the process gently by first sending SIGINT, which is handled by the BECIpythonClient to
        gracefully shut down, and end any scans it has started. See:
        bec_ipython_client/main.py#L125 and bec_ipython_client/signals.py#L103
        If the process fails to shut down naturally, kills it unceremoniously. This will leave the running scan running
        on the ScanServer.
        """
        if not self._ending_or_ended():
            self._ending = True
            self._process.send_signal(SIGINT)
            self._process.send_signal(SIGINT)
            try:
                self._process.wait(PROCESS_TIMEOUT)
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"Subprocess Procedure Worker failed to exit in {PROCESS_TIMEOUT} s. Killing."
                )
                self._process.kill()
                self._process.wait(PROCESS_TIMEOUT)

    def abort_execution(self, execution_id: str):
        """Abort the execution with the given id. Has no effect if the given ID is not the current job"""
        if execution_id == self._current_execution_id:
            self._kill_process()
            self._helper.remove_from_active.by_exec_id(execution_id)
            logger.info(
                f"Aborting execution {execution_id}, restarting worker for queue: {self._queue}"
            )
            self._join_output()
            self._setup_execution_environment()

    def logs(self) -> list[str]:
        """Return captured output without consuming it or waiting for a live process."""
        if self._process.poll() is not None:
            self._join_output()
        with self._output_lock:
            return "".join(self._output_logs).splitlines()


if __name__ == "__main__":
    main(output_to_redis=False)
