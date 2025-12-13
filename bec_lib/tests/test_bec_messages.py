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
        res_expected = b"\x81\xad__bec_codec__\x83\xacencoder_name\xaaBECMessage\xa9type_name\xb8DeviceInstructionMessage\xa4data\x84\xa8metadata\x81\xa3RID\xa41234\xa6device\xa4samx\xa6action\xa3set\xa9parameter\x81\xa3set\xcb?\xe0\x00\x00\x00\x00\x00\x00"
        assert res == res_expected
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


def test_device_message_with_async_update():
    msg = messages.DeviceMessage(
        signals={"samx": {"value": 5.2}},
        metadata={
            "RID": "1234",
            "async_update": messages.DeviceAsyncUpdate(
                type="add", max_shape=[None, 1024, 1024]
            ).model_dump(),
        },
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)

    assert res_loaded == msg


def test_device_message_with_invalid_async_update():
    with pytest.raises(pydantic.ValidationError):
        messages.DeviceMessage(
            signals={"samx": {"value": 5.2}},
            metadata={"RID": "1234", "async_update": {"type": "wrong"}},
        )


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
    msg = messages.ScanQueueStatusMessage(
        queue={"primary": messages.ScanQueueStatus(info=[], status="RUNNING")},
        metadata={"RID": "1234"},
    )
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
    msg = messages.DeviceReqStatusMessage(device="samx", success=True, request_id="1234")
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
        info=messages.ErrorInfo(
            error_message="This is an alarm message.",
            compact_error_message="Alarm content",
            exception_type="AlarmType",
            device="AlarmDevice",
        ),
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


def test_ProcedureWorkerStatusMessage():
    msg = messages.ProcedureWorkerStatusMessage(
        worker_queue="background tasks",
        status=messages.ProcedureWorkerStatus.IDLE,
        metadata={"RID": "1234"},
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ProcedureWorkerStatusMessage_validation():
    with pytest.raises(pydantic.ValidationError) as e:
        messages.ProcedureWorkerStatusMessage(
            worker_queue="background tasks",
            status=messages.ProcedureWorkerStatus.RUNNING,
            metadata={"RID": "1234"},
        )
    assert e.match("Adding an execution ID is mandatory")
    with pytest.raises(pydantic.ValidationError) as e:
        messages.ProcedureWorkerStatusMessage(
            worker_queue="background tasks",
            status=messages.ProcedureWorkerStatus.IDLE,
            metadata={"RID": "1234"},
            current_execution_id="test",
        )
    assert e.match("Adding an execution ID is only valid")


def test_ProcedureAbortMessage_validation():
    with pytest.raises(pydantic.ValidationError) as e:
        messages.ProcedureAbortMessage(queue="test", execution_id="test")
    assert e.match("only supply one argument")
    messages.ProcedureAbortMessage(queue="test")


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
        status="running",
        queue_id="queue_id",
        info=messages.QueueInfoEntry(
            queue_id="queue_i",
            scan_id=["scan_id", None],
            is_scan=[True, False],
            request_blocks=[],
            scan_number=[1, None],
            status="RUNNING",
        ),
        metadata={"RID": "1234"},
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
    msg = messages.DeviceInstructionMessage(device="samx", action="set", parameter={"set": 0.5})
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


def test_GUIRegistryStateMessage():
    msg = messages.GUIRegistryStateMessage(
        state={
            "my_dock_area": {
                "gui_id": "test_id",
                "name": "test_name",
                "config": {},
                "widget_class": "test_class",
                "__rpc__": True,
            }
        }
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg
    assert res_loaded.metadata == {}

    with pytest.raises(pydantic.ValidationError):
        messages.GUIRegistryStateMessage(
            state={
                "my_dock_area": {
                    "gui_id": "test_id",
                    "name": "test_name",
                    "config": 2,
                    "widget_class": "test_class",
                }
            }
        )


class TestDeviceAsyncUpdate:
    """Tests for DeviceAsyncUpdate model validation"""

    def test_valid_add_type_with_1d_max_shape(self):
        """Test add type with 1D unlimited max_shape"""
        update = messages.DeviceAsyncUpdate(type="add", max_shape=[None])
        assert update.type == "add"
        assert update.max_shape == [None]
        assert update.index is None

    def test_valid_add_type_with_multi_dimensional_max_shape(self):
        """Test add type with multi-dimensional max_shape"""
        update = messages.DeviceAsyncUpdate(type="add", max_shape=[None, 1024, 1024])
        assert update.type == "add"
        assert update.max_shape == [None, 1024, 1024]

    def test_valid_add_type_with_fixed_shape(self):
        """Test add type with fully fixed max_shape"""
        update = messages.DeviceAsyncUpdate(type="add", max_shape=[100, 200])
        assert update.type == "add"
        assert update.max_shape == [100, 200]

    def test_valid_add_slice_type_with_index(self):
        """Test add_slice type with required index and 2D max_shape"""
        update = messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None, 1024], index=5)
        assert update.type == "add_slice"
        assert update.max_shape == [None, 1024]
        assert update.index == 5

    def test_valid_add_slice_type_with_1d_max_shape(self):
        """Test add_slice type with 1D max_shape"""
        update = messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None], index=0)
        assert update.type == "add_slice"
        assert update.max_shape == [None]
        assert update.index == 0

    def test_valid_add_slice_type_with_variable_size(self):
        """Test add_slice type with variable size in second dimension"""
        update = messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None, None], index=3)
        assert update.type == "add_slice"
        assert update.max_shape == [None, None]
        assert update.index == 3

    def test_valid_replace_type_without_max_shape(self):
        """Test replace type without max_shape or index"""
        update = messages.DeviceAsyncUpdate(type="replace")
        assert update.type == "replace"
        assert update.max_shape is None
        assert update.index is None

    def test_invalid_add_type_missing_max_shape(self):
        """Test that add type requires max_shape"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add")
        assert "max_shape is required" in str(exc_info.value)

    def test_invalid_add_slice_type_missing_max_shape(self):
        """Test that add_slice type requires max_shape"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add_slice", index=0)
        assert "max_shape is required" in str(exc_info.value)

    def test_invalid_add_slice_type_missing_index(self):
        """Test that add_slice type requires index"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None, 1024])
        assert "index is required" in str(exc_info.value)

    def test_invalid_add_slice_type_with_3d_max_shape(self):
        """Test that add_slice type cannot have more than 2D max_shape"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None, 1024, 1024], index=0)
        assert "cannot exceed two dimensions" in str(exc_info.value)

    def test_invalid_max_shape_none_in_middle(self):
        """Test that None values cannot appear in the middle of max_shape"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=[1024, None])
        assert "None values must only appear at the beginning" in str(exc_info.value)

    def test_invalid_max_shape_none_after_integer(self):
        """Test that None cannot appear after an integer in max_shape"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=[1024, None, 512])
        assert "None values must only appear at the beginning" in str(exc_info.value)

    def test_invalid_max_shape_mixed_none_positions(self):
        """Test that None values must be consecutive at the beginning"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=[None, 1024, None])
        assert "None values must only appear at the beginning" in str(exc_info.value)

    def test_valid_max_shape_multiple_none_at_beginning(self):
        """Test that multiple None values at the beginning are valid"""
        update = messages.DeviceAsyncUpdate(type="add", max_shape=[None, None, 1024])
        assert update.max_shape == [None, None, 1024]

    def test_invalid_type(self):
        """Test that invalid type is rejected"""
        with pytest.raises(pydantic.ValidationError):
            messages.DeviceAsyncUpdate(type="invalid_type", max_shape=[None])

    @pytest.mark.parametrize(
        "max_shape",
        [[None], [None, 100], [None, None], [None, None, 100], [100], [100, 200], [100, 200, 300]],
    )
    def test_valid_max_shape_patterns_for_add(self, max_shape):
        """Test various valid max_shape patterns for add type"""
        update = messages.DeviceAsyncUpdate(type="add", max_shape=max_shape)
        assert update.max_shape == max_shape

    @pytest.mark.parametrize(
        "max_shape,index",
        [([None], 0), ([None, 100], 5), ([None, None], 10), ([100], 2), ([100, 200], 15)],
    )
    def test_valid_max_shape_patterns_for_add_slice(self, max_shape, index):
        """Test various valid max_shape patterns for add_slice type"""
        update = messages.DeviceAsyncUpdate(type="add_slice", max_shape=max_shape, index=index)
        assert update.max_shape == max_shape
        assert update.index == index

    @pytest.mark.parametrize(
        "max_shape", [[100, None], [None, 100, None], [100, None, 200], [100, 200, None]]
    )
    def test_invalid_max_shape_patterns(self, max_shape):
        """Test various invalid max_shape patterns with None not at beginning"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=max_shape)
        assert "None values must only appear at the beginning" in str(exc_info.value)

    def test_invalid_all_none_max_2_dimensions(self):
        """Test that when all dimensions are None, maximum is 2 dimensions"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=[None, None, None])
        assert "when all dimensions are None" in str(exc_info.value)
        assert "maximum number of dimensions is 2" in str(exc_info.value)

    def test_valid_all_none_2_dimensions(self):
        """Test that [None, None] is valid"""
        update = messages.DeviceAsyncUpdate(type="add", max_shape=[None, None])
        assert update.max_shape == [None, None]

    def test_invalid_empty_max_shape_for_add(self):
        """Test that empty max_shape is rejected for add type"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=[])
        assert "max_shape is required and cannot be empty" in str(exc_info.value)

    def test_invalid_empty_max_shape_for_add_slice(self):
        """Test that empty max_shape is rejected for add_slice type"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add_slice", max_shape=[], index=0)
        assert "max_shape is required and cannot be empty" in str(exc_info.value)

    def test_invalid_negative_max_shape(self):
        """Test that negative values in max_shape are rejected"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=[None, -100])
        assert "all non-None dimensions must be positive integers" in str(exc_info.value)

    @pytest.mark.parametrize("bad_value", [-1024, -1, 0])
    def test_invalid_max_shape_non_positive_values(self, bad_value):
        """Test that non-positive integer values in max_shape are rejected"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add", max_shape=[None, bad_value])
        assert "all non-None dimensions must be positive integers" in str(exc_info.value)

    def test_invalid_add_slice_negative_index(self):
        """Test that negative index less than -1 is rejected for add_slice"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None, 1024], index=-2)
        assert "index must be an integer >= -1" in str(exc_info.value)

    @pytest.mark.parametrize("index", [0, 1, 10, 100, -1])
    def test_valid_add_slice_various_indices(self, index):
        """Test various valid index values for add_slice type"""
        update = messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None, 1024], index=index)
        assert update.index == index
