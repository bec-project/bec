import time

import fakeredis
import h5py
import pytest

from bec_lib import bec_logger, messages
from bec_lib.client import BECClient
from bec_lib.redis_connector import RedisConnector

# overwrite threads_check fixture from bec_lib,
# to have it in autouse


@pytest.fixture(autouse=True)
def threads_check(threads_check):
    yield
    bec_logger.logger.remove()


@pytest.fixture(autouse=True)
def bec_client_singleton_reset():
    """Reset the BECClient singleton before and after each test."""
    # pylint: disable=protected-access
    BECClient._reset_singleton()
    yield
    BECClient._reset_singleton()


def fake_redis_server(host, port, **kwargs):
    redis = fakeredis.FakeRedis()
    return redis


@pytest.fixture
def connected_connector():
    RedisConnector.RETRY_ON_TIMEOUT = 0
    connector = RedisConnector("localhost:1", redis_cls=fake_redis_server)
    connector._redis_conn.flushall()
    try:
        yield connector
    finally:
        connector.shutdown()


@pytest.fixture
def scan_queue_status_msg():
    yield messages.ScanQueueStatusMessage(
        queue={
            "primary": messages.ScanQueueStatus(
                info=[
                    messages.QueueInfoEntry(
                        queue_id="7c15c9a2-71d4-4f2a-91a7-c4a63088fa38",
                        scan_id=["bfa582aa-f9cd-4258-ab5d-3e5d54d3dde5"],
                        is_scan=[True],
                        request_blocks=[
                            messages.RequestBlock(
                                msg=messages.ScanQueueMessage(
                                    scan_type="fermat_scan",
                                    parameter={
                                        "args": {"samx": [-2, 2], "samy": [-2, 2]},
                                        "kwargs": {"step": 1.5, "exp_time": 0.02, "relative": True},
                                    },
                                    metadata={"RID": "cd8fc68f-fe65-4031-9a37-e0e7ba9df542"},
                                    queue="primary",
                                ),
                                RID="cd8fc68f-fe65-4031-9a37-e0e7ba9df542",
                                scan_motors=["samx", "samy"],
                                is_scan=True,
                                scan_number=25,
                                scan_id="bfa582aa-f9cd-4258-ab5d-3e5d54d3dde5",
                                readout_priority={
                                    "monitored": ["bpm4i", "samx", "samy"],
                                    "async": ["waveform"],
                                },
                            )
                        ],
                        scan_number=[25],
                        status="RUNNING",
                        active_request_block=None,
                    )
                ],
                status="RUNNING",
            )
        }
    )


@pytest.fixture
def mock_file(tmpdir):
    """
    Create a mock hdf5 file.
    """
    file_path = tmpdir / "test.h5"
    readout_groups = {
        "baseline": ["samz"],
        "monitored": ["samx", "bpm4i", "samy"],
        "async": ["waveform"],
    }
    with h5py.File(file_path, "w") as f:
        metadata = f.create_group("entry/collection/metadata")
        metadata.create_dataset("sample_name", data="test_sample")
        metadata_bec = f.create_group("entry/collection/metadata/bec")
        metadata_bec.create_dataset("scan_id", data="scan_id_1")
        for group, devices in readout_groups.items():
            readout_group = f.create_group(f"entry/collection/readout_groups/{group}")

            for device in devices:
                dev_group = f.create_group(f"entry/collection/devices/{device}/{device}")

                data = [1, 2, 3] if group in ["monitored", "async"] else 1

                for signal in ["value", "timestamp"]:
                    dev_group.create_dataset(signal, data=data)

                if device in ["samx", "samy", "samz"]:
                    dev_group = f.create_group(
                        f"entry/collection/devices/{device}/{device}_setpoint"
                    )
                    for signal in ["value", "timestamp"]:
                        dev_group.create_dataset(signal, data=data)

                # create a link from the readout group to the device
                readout_group[device] = h5py.SoftLink(f"/entry/collection/devices/{device}")

    return file_path


@pytest.fixture
def file_history_messages(mock_file):
    return [
        messages.ScanHistoryMessage(
            scan_id="scan_id_1",
            scan_number=1,
            dataset_number=1,
            file_path=str(mock_file),
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
        messages.ScanHistoryMessage(
            scan_id="scan_id_2",
            scan_number=2,
            dataset_number=2,
            file_path=str(mock_file),
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
        messages.ScanHistoryMessage(
            scan_id="scan_id_3",
            scan_number=3,
            dataset_number=2,
            file_path=str(mock_file),
            exit_status="closed",
            start_time=time.time(),
            end_time=time.time(),
            scan_name="line_scan",
            num_points=10,
        ),
    ]
