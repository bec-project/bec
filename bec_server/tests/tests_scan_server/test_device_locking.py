from typing import Callable

import pytest

from bec_lib import messages
from bec_server.scan_server.scan_queue import QueueManager


@pytest.fixture
def qm_with_3_qs_and_lock_man(queuemanager_mock: Callable[..., QueueManager]):
    queue_manager = queuemanager_mock(["1", "2", "3"])
    yield queue_manager, queue_manager.parent.device_locks


def test_queuemanager_add_to_queue_restarts_queue_if_worker_is_dead(qm_with_3_qs_and_lock_man):
    queue_manager, locks = qm_with_3_qs_and_lock_man
    msg = messages.ScanQueueMessage(
        scan_type="mv",
        parameter={"args": {"samx": (1,)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )

    queue_manager.add_to_queue(scan_queue="1", msg=msg)

    ...
