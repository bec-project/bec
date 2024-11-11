import pytest

from bec_lib import messages
from bec_lib.live_scan_data import LiveScanData


@pytest.fixture
def scan_data():
    scan_data = LiveScanData()
    for ii in range(10):
        msg = messages.ScanMessage(
            point_id=ii,
            scan_id="scan_id",
            data={
                "samx": {
                    "setpoint": {"value": ii, "timestamp": ii},
                    "samx": {"value": ii, "timestamp": ii},
                }
            },
        )
        scan_data.set(ii, msg)
    yield scan_data


def test_scan_data_provides_BECMessages(scan_data):
    assert scan_data[0] == messages.ScanMessage(
        point_id=0,
        scan_id="scan_id",
        data={
            "samx": {"setpoint": {"value": 0, "timestamp": 0}, "samx": {"value": 0, "timestamp": 0}}
        },
    )
    assert scan_data[1] == messages.ScanMessage(
        point_id=1,
        scan_id="scan_id",
        data={
            "samx": {"setpoint": {"value": 1, "timestamp": 1}, "samx": {"value": 1, "timestamp": 1}}
        },
    )


def test_scan_data_signals_single_val(scan_data):
    assert scan_data.samx.setpoint.val[0] == 0
    assert scan_data.samx.setpoint.timestamps[0] == 0
    assert scan_data.samx.samx.val[0] == 0
    assert scan_data.samx.samx.timestamps[0] == 0
    assert scan_data.samx.samx.get("val")[0] == 0

    assert scan_data.samx.setpoint[0] == {"value": 0, "timestamp": 0}
    assert scan_data["samx"]["setpoint"][0] == {"value": 0, "timestamp": 0}

    assert scan_data.samx.setpoint.get(0) == {"value": 0, "timestamp": 0}
    assert scan_data["samx"].get("setpoint").get(0) == {"value": 0, "timestamp": 0}


def test_scan_data_signals_list_val(scan_data):
    assert scan_data["samx"]["setpoint"] == {ii: {"value": ii, "timestamp": ii} for ii in range(10)}
    assert scan_data.samx.setpoint == {ii: {"value": ii, "timestamp": ii} for ii in range(10)}
    assert scan_data.samx.setpoint.timestamps == [ii for ii in range(10)]

    assert scan_data["samx"]["setpoint"]["val"] == [ii for ii in range(10)]
    assert scan_data["samx"]["setpoint"]["timestamp"] == [ii for ii in range(10)]
    assert scan_data["samx"]["setpoint"].get("val") == [ii for ii in range(10)]
    assert scan_data["samx"]["setpoint"].get("timestamp") == [ii for ii in range(10)]


def test_scan_data_device_data(scan_data):
    assert scan_data["samx"] == {
        "setpoint": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
        "samx": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
    }
    assert scan_data.samx == {
        "setpoint": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
        "samx": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
    }
    assert scan_data.samx[0] == {
        "setpoint": {"value": 0, "timestamp": 0},
        "samx": {"value": 0, "timestamp": 0},
    }
    assert scan_data.get("samx") == {
        "setpoint": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
        "samx": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
    }
    assert scan_data.samx.get(0) == {
        "setpoint": {"value": 0, "timestamp": 0},
        "samx": {"value": 0, "timestamp": 0},
    }


def test_scan_data_device_data_dict_operations(scan_data):
    assert scan_data.samx.keys() == {"setpoint": 0, "samx": 0}.keys()
    assert list(scan_data.samx.values()) == [
        {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
        {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
    ]
    assert dict(scan_data.samx.items()) == dict(
        {
            "setpoint": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
            "samx": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
        }.items()
    )

    assert len(scan_data.samx.setpoint) == 10


def test_scan_data_signal_dict_operations(scan_data):
    assert (
        scan_data.samx.setpoint.keys()
        == {ii: {"value": ii, "timestamp": ii} for ii in range(10)}.keys()
    )

    assert list(scan_data.samx.setpoint.values()) == list(
        {ii: {"value": ii, "timestamp": ii} for ii in range(10)}.values()
    )

    assert dict(scan_data.samx.setpoint.items()) == dict(
        {ii: {"value": ii, "timestamp": ii} for ii in range(10)}.items()
    )

    assert len(scan_data.samx.setpoint) == 10


def test_scan_data_dict_operations(scan_data):
    assert scan_data.keys() == {"samx": 0}.keys()

    assert list(scan_data.values()) == [
        {
            "setpoint": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
            "samx": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
        }
    ]
    assert dict(scan_data.items()) == dict(
        {
            "samx": {
                "setpoint": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
                "samx": {ii: {"value": ii, "timestamp": ii} for ii in range(10)},
            }
        }.items()
    )

    assert "samx" in scan_data
    assert "not_a_device" not in scan_data

    assert len(scan_data) == 10
