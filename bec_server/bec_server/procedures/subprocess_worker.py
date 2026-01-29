import os
import subprocess
import sys
import time
from signal import SIGINT

from bec_lib.logger import bec_logger
from bec_server.procedures.oop_worker_base import PROCESS_TIMEOUT, OutOfProcessWorkerBase, main

logger = bec_logger.logger


class SubProcessWorker(OutOfProcessWorkerBase):
    """A worker which runs scripts in a container with a full BEC environment,
    mounted from the filesystem, and only access to Redis"""

    def _setup_execution_environment(self):
        env: dict[str, str] = self._worker_environment()  # type: ignore
        env.update({"redis_server": self._redis_server})
        self._process = subprocess.Popen(
            [sys.executable, __file__],
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1000,
            env=os.environ | env,
        )
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
            try:
                self._process.wait(PROCESS_TIMEOUT)
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"Subprocess Procedure Worker failed to exit in {PROCESS_TIMEOUT} s. Killing."
                )
                self._process.kill()

    def abort_execution(self, execution_id: str):
        """Abort the execution with the given id. Has no effect if the given ID is not the current job"""
        if execution_id == self._current_execution_id:
            self._kill_process()
            self._helper.remove_from_active.by_exec_id(execution_id)
            logger.info(
                f"Aborting execution {execution_id}, restarting worker for queue: {self._queue}"
            )
            self._process.communicate(
                timeout=PROCESS_TIMEOUT
            )  # make sure process ended in kill process and flush pipes
            self._setup_execution_environment()

    def logs(self):
        if self._process.stdout is None:
            return []
        return list(self._process.stdout)


if __name__ == "__main__":
    main()
