from typing import Callable

import pytest

from bec_lib import messages
from bec_lib.tests.utils import wait_until
from bec_server.scan_server.scan_queue import QueueManager


@pytest.fixture
def qm_with_3_qs_and_lock_man(queuemanager_mock: Callable[..., QueueManager]):
    queue_manager = queuemanager_mock(["1", "2", "3"])
    yield queue_manager, queue_manager.parent.device_locks


def _linescan_msg(*args: tuple[str, float, float]):
    return messages.ScanQueueMessage(
        scan_type="line_scan",
        parameter={"args": {d: (a, b) for (d, a, b) in args}, "kwargs": {}},
        queue="primary",
        metadata={"RID": "something"},
    )


@pytest.mark.parametrize(
    ["msg", "devices"],
    [
        (_linescan_msg(("samx", -1, 1)), ("samx",)),
        (_linescan_msg(("samx", -1, 1), ("samy", -1, 1)), ("samx", "samy")),
        (_linescan_msg(("a", -1, 1), ("b", -1, 1), ("c", -1, 1)), ("a", "b", "c")),
    ],
)
def test_devices_from_instance(queuemanager_mock, msg, devices):
    q_manager = queuemanager_mock()
    assembler = q_manager.parent.scan_assembler
    scan_instance = assembler.assemble_device_instructions(msg, "test")
    device_access = scan_instance.instance_device_access()
    assert device_access.device_locking == set(devices)


def test_scan_worker_locks_devices_single(qm_with_3_qs_and_lock_man):
    queue_manager, locks = qm_with_3_qs_and_lock_man
    msg = _linescan_msg(("samx", -5, 5))
    queue_manager.add_to_queue(scan_queue="1", msg=msg)
    wait_until(lambda: locks._locks != {}, timeout_s=1)
