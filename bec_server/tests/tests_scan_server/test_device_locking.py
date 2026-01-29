from typing import Callable

import pytest
from bec_server.scan_server.scan_queue import QueueManager

from bec_lib import messages


@pytest.fixture
def qm_with_3_qs_and_lock_man(queuemanager_mock: Callable[..., QueueManager]):
    queue_manager = queuemanager_mock(["1", "2", "3"])
    yield queue_manager, queue_manager.parent.device_locks


def _linescan_msg(dev: str, start: float, stop: float):
    return messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={"args": {dev: (start, stop)}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )


def test_devices_from_instance(queuemanager_mock):
    q_manager = queuemanager_mock()
    assembler = q_manager.parent.scan_assembler
    scan_instance = assembler.assemble_device_instructions(_linescan_msg("samx", -1, 1), "test")
    device_access = scan_instance.instance_device_access()
    assert device_access.device_locking == set(("samx",))


def test_queuemanager_add_to_queue_restarts_queue_if_worker_is_dead(qm_with_3_qs_and_lock_man):
    queue_manager, locks = qm_with_3_qs_and_lock_man
    msg = _linescan_msg("samx", -5, 5)

    queue_manager.add_to_queue(scan_queue="1", msg=msg)

    ...
