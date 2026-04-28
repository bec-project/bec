import importlib
import os
from contextlib import redirect_stdout
from inspect import isclass

from bec_lib.client import BECClient
from bec_lib.logger import LogLevel, bec_logger
from bec_lib.messages import ProcedureWorkerStatus
from bec_lib.redis_connector import RedisConnector
from bec_server.actors.actor import ActorBase
from bec_server.procedures.oop_worker_base import RedisOutputDiverter, get_env, push_status, setup
from bec_server.procedures.subprocess_worker import SubProcessWorker

logger = bec_logger.logger


def actor_procedure(actor_module: str, actor_class_name: str, exec_id: str, bec: BECClient):
    try:
        mod = importlib.import_module(actor_module)
        actor_class = getattr(mod, actor_class_name)
    except ImportError:
        logger.error(f"Module '{actor_module}' not found! Exiting.")
        return
    except AttributeError:
        logger.error(
            f"Module '{actor_module}' does not contain {actor_class_name}! Available classes in module: {list(filter(isclass, mod.__dict__.values()))}."
        )
        return
    if not issubclass(actor_class, ActorBase):
        logger.error(f"{actor_class_name} is not a valid Actor! Exiting.")
        return

    actor = actor_class(bec, f"{actor_module}.{actor_class_name}", exec_id)
    logger.success(f"Calling .run for actor {exec_id}")
    actor.run()


class ActorProcedureWorker(SubProcessWorker):
    WORKER_FILE = __file__

    def _worker_environment(self):
        return super()._worker_environment() | {"client_class": "BECClient"}


def get_actor_env():
    return {
        "actor_module": os.environ["actor_module"],
        "actor_class_name": os.environ["actor_class_name"],
        "exec_id": os.environ["actor_exec_id"],
    }


def main():
    env, helper, client, conn = setup(get_env())
    actor_env = get_actor_env()
    logger_connector = RedisConnector(env["redis_server"])
    output_diverter = RedisOutputDiverter(logger_connector, env["queue"])
    with redirect_stdout(output_diverter):
        logger.add(
            output_diverter,
            level=LogLevel.SUCCESS,
            format=bec_logger.formatting(is_container=True),
            filter=bec_logger.filter(),
        )
        logger.success(f"Starting ActorProcedureWorker with env: {actor_env}")
    push_status(conn, env["queue"], ProcedureWorkerStatus.IDLE)
    actor_procedure(bec=client, **actor_env)
    conn.shutdown()
    logger_connector.shutdown()


if __name__ == "__main__":
    """Replaces the main contents of Worker.work() - should be called as the container entrypoint or command"""
    main()
