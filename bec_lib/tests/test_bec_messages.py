import numpy as np
import pydantic
import pytest

from bec_lib import messages
from bec_lib.serialization import MsgpackSerialization


@pytest.mark.parametrize("version", [1.0, 1.1, 1.2, None])
def test_bec_message_msgpack_serialization_version(version):
    msg = messages.DeviceInstructionMessage(
        device="samx", action="set", parameter={"set": 0.5}, metadata={"RID": "1234"}
    )
    if version is not None and version < 1.2:
        with pytest.raises(RuntimeError) as exception:
            MsgpackSerialization.dumps(msg, version=version)
        assert "Unsupported BECMessage version" in str(exception.value)
    else:
        res = MsgpackSerialization.dumps(msg)
        print(res)
        v12res = b'\xc7z\x01BECMSG_1.2_34_67_EOH_{"msg_type": "device_instruction"}\x84\xa8metadata\x81\xa3RID\xa41234\xa6device\xa4samx\xa6action\xa3set\xa9parameter\x81\xa3set\xcb?\xe0\x00\x00\x00\x00\x00\x00'
        assert res == v12res
        res_loaded = MsgpackSerialization.loads(res)
        assert res_loaded == msg


@pytest.mark.parametrize("version", [1.2, None])
def test_bec_message_serialization_numpy_ndarray(version):
    msg = messages.DeviceMessage(
        signals={"samx": {"value": np.random.rand(20).astype(np.float32)}}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    print(res)
    res_loaded = MsgpackSerialization.loads(res)
    np.testing.assert_equal(res_loaded.content, msg.content)
    assert res_loaded == msg


def test_bundled_message():
    sub_msg = messages.DeviceMessage(signals={"samx": {"value": 5.2}}, metadata={"RID": "1234"})
    msg = messages.BundleMessage()
    msg.append(sub_msg)
    msg.append(sub_msg)
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == [sub_msg, sub_msg]


def test_ScanQueueModificationMessage():
    msg = messages.ScanQueueModificationMessage(
        scan_id="1234", action="halt", parameter={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ScanQueueModificationMessage_with_wrong_action_returns_None():
    with pytest.raises(pydantic.ValidationError):
        messages.ScanQueueModificationMessage(
            scan_id="1234", action="wrong_action", parameter={"RID": "1234"}
        )


def test_ScanQueueStatusMessage_must_include_primary_queue():
    with pytest.raises(pydantic.ValidationError):
        messages.ScanQueueStatusMessage(queue={}, metadata={"RID": "1234"})


def test_ScanQueueStatusMessage_loads_successfully():
    msg = messages.ScanQueueStatusMessage(queue={"primary": {}}, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DeviceMessage_loads_successfully():
    msg = messages.DeviceMessage(signals={"samx": {"value": 5.2}}, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DeviceMessage_must_include_signals_as_dict():
    with pytest.raises(pydantic.ValidationError):
        messages.DeviceMessage(signals="wrong_signals", metadata={"RID": "1234"})


def test_ClientInfoMessage():
    msg = messages.ClientInfoMessage(
        message="test", show_asap=True, RID="1234", metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ClientInfoMessage_raises():
    with pytest.raises(pydantic.ValidationError):
        messages.ClientInfoMessage(
            message="test",
            source="abc",
            show_asap=True,
            RID="1234",
            metadata={"RID": "1234", "wrong": "wrong"},
        )


def test_DeviceRPCMessage():
    msg = messages.DeviceRPCMessage(
        device="samx", return_val=1, out="done", success=True, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DeviceStatusMessage():
    msg = messages.DeviceStatusMessage(device="samx", status=0, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DeviceReqStatusMessage():
    msg = messages.DeviceReqStatusMessage(device="samx", success=True, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DeviceInfoMessage():
    msg = messages.DeviceInfoMessage(
        device="samx", info={"version": "1.0"}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ScanMessage():
    msg = messages.ScanMessage(
        point_id=1, scan_id="scan_id", data={"value": 3}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ScanBaselineMessage():
    msg = messages.ScanBaselineMessage(
        scan_id="scan_id", data={"value": 3}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


@pytest.mark.parametrize(
    "action,valid",
    [("add", True), ("set", True), ("update", True), ("reload", True), ("wrong_action", False)],
)
def test_DeviceConfigMessage(action, valid):
    if valid:
        msg = messages.DeviceConfigMessage(
            action=action, config={"device": "samx"}, metadata={"RID": "1234"}
        )
        res = MsgpackSerialization.dumps(msg)
        res_loaded = MsgpackSerialization.loads(res)
        assert res_loaded == msg
    else:
        with pytest.raises(pydantic.ValidationError):
            messages.DeviceConfigMessage(
                action=action, config={"device": "samx"}, metadata={"RID": "1234"}
            )


def test_LogMessage():
    msg = messages.LogMessage(
        log_type="error", log_msg="An error occurred", metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_AlarmMessage():
    msg = messages.AlarmMessage(
        severity=2,
        alarm_type="major",
        source={"system": "samx"},
        msg="An error occurred",
        metadata={"RID": "1234"},
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_StatusMessage():
    msg = messages.StatusMessage(
        name="system",
        status=messages.BECStatus.RUNNING,
        info={"version": "1.0"},
        metadata={"RID": "1234"},
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_FileMessage():
    msg = messages.FileMessage(
        device_name="samx",
        file_path="/path/to/file",
        done=True,
        successful=True,
        hinted_h5_entries={"data": "entry/data"},
        metadata={"RID": "1234"},
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_VariableMessage():
    msg = messages.VariableMessage(value="value", metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ObserverMessage():
    msg = messages.ObserverMessage(observer=[{"name": "observer1"}], metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ServiceMetricMessage():
    msg = messages.ServiceMetricMessage(
        name="service1", metrics={"metric1": 1}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ProcessedDataMessage():
    msg = messages.ProcessedDataMessage(data={"samx": {"value": 5.2}}, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DAPConfigMessage():
    msg = messages.DAPConfigMessage(config={"val": "val"}, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_AvailableResourceMessage():
    msg = messages.AvailableResourceMessage(
        resource={"resource": "available"}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ProgressMessage():
    msg = messages.ProgressMessage(value=0.5, max_value=10, done=False, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_GUIConfigMessage():
    msg = messages.GUIConfigMessage(config={"config": "value"}, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ScanQueueHistoryMessage():
    msg = messages.ScanQueueHistoryMessage(
        status="running", queue_id="queue_id", info={"val": "val"}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DAPResponseMessage():
    msg = messages.DAPResponseMessage(success=True, data=({}, None), metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DAPResponseMessage_accepts_None():
    msg = messages.DAPResponseMessage(success=True, data=None, metadata={"RID": "1234"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DAPRequestMessage():
    msg = messages.DAPRequestMessage(
        dap_cls="dap_cls",
        dap_type="continuous",
        config={"config": "value"},
        metadata={"RID": "1234"},
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_wrong_DAPRequestMessage():
    with pytest.raises(pydantic.ValidationError):
        messages.DAPRequestMessage(
            dap_cls="dap_cls",
            dap_type="error",
            config={"config": "value"},
            metadata={"RID": "1234"},
        )


def test_FileContentMessage():
    msg = messages.FileContentMessage(
        file_path="/path/to/file", data={}, scan_info={}, metadata={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_CredentialsMessage():
    msg = messages.CredentialsMessage(credentials={"username": "user", "password": "pass"})
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_DeviceInstructionMessage():
    msg = messages.DeviceInstructionMessage(
        device="samx", action="set", parameter={"set": 0.5}, metadata=None
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg
    assert res_loaded.metadata == {}


def test_DeviceMonitor2DMessage():
    # Test 2D data
    msg = messages.DeviceMonitor2DMessage(
        device="eiger", data=np.random.rand(2, 100), metadata=None
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg
    assert res_loaded.metadata == {}
    # Test rgb image, i.e. image with 3 channels
    msg = messages.DeviceMonitor2DMessage(device="eiger", data=np.random.rand(3, 3), metadata=None)
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg
    assert res_loaded.metadata == {}
    # no float
    with pytest.raises(pydantic.ValidationError):
        messages.DeviceMonitor2DMessage(device="eiger", data=0.0, metadata={"RID": "1234"})
    # no 1D array
    with pytest.raises(pydantic.ValidationError):
        messages.DeviceMonitor2DMessage(
            device="eiger", data=np.random.rand(100), metadata={"RID": "1234"}
        )


def test_DeviceMonitor1DMessage():
    # Test 2D data
    msg = messages.DeviceMonitor1DMessage(device="eiger", data=np.random.rand(100), metadata=None)
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg
    assert res_loaded.metadata == {}
    # no float
    with pytest.raises(pydantic.ValidationError):
        messages.DeviceMonitor1DMessage(device="eiger", data=0.0, metadata={"RID": "1234"})

    # no 2xN array
    with pytest.raises(pydantic.ValidationError):
        messages.DeviceMonitor1DMessage(
            device="eiger", data=np.random.rand(2, 3), metadata={"RID": "1234"}
        )
