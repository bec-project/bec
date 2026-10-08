import getpass
import json
import warnings

import numpy as np
import pydantic
import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints, MessageOp
from bec_lib.messaging_services import NotificationMessageObject
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


def test_device_async_signal_index_message():
    msg = messages.DeviceAsyncSignalIndexMessage(
        scan_id="scan-1",
        device="waveform",
        signal="waveform_data",
        shapes={"waveform_a": [2], "waveform_b": []},
        indices={"waveform_a": 0, "waveform_b": 3},
        async_update=messages.DeviceAsyncUpdate(type="add", max_shape=[None]),
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)

    assert res_loaded == msg


def test_signal_info_defaults_and_roundtrip():
    info = messages.SignalInfo()
    assert info.signals is None
    assert info.signal_metadata is None
    assert info.use_alias is False
    assert info.correlation_group is None
    assert info.transports is None

    configured = messages.SignalInfo(
        data_type="processed",
        saved=False,
        ndim=2,
        scope="continuous",
        role="preview",
        signals=[("image", 1)],
        signal_metadata={"units": "counts"},
        correlation_group="monitored",
        use_alias=True,
    )
    assert messages.SignalInfo.model_validate(configured.model_dump()) == configured


@pytest.mark.parametrize(
    "transports",
    [
        None,
        [],
        [messages.RedisSignalTransport.from_endpoint_info(MessageEndpoints.scan_status())],
        [
            messages.RedisSignalTransport.from_endpoint_info(
                MessageEndpoints.device_preview("detector", "preview")
            ).model_dump(mode="json"),
            {
                "transport": "zmq",
                "address": "tcp://detector-host:5555",
                "topic": "detector/preview",
                "encoder": "bec-v1",
            },
        ],
    ],
)
def test_signal_info_transports_roundtrip(transports):
    info = messages.SignalInfo(transports=transports)
    assert messages.SignalInfo.model_validate_json(info.model_dump_json()) == info
    msg = messages.DeviceInfoMessage(device="detector", info=info.model_dump())
    loaded = MsgpackSerialization.loads(MsgpackSerialization.dumps(msg))
    assert messages.SignalInfo.model_validate(loaded.info) == info
    if transports:
        assert isinstance(info.transports[0], messages.RedisSignalTransport)
        if len(transports) > 1:
            assert isinstance(info.transports[1], messages.ZMQSignalTransport)


@pytest.mark.parametrize(
    "transport",
    [
        {"transport": "unknown"},
        {"transport": "redis"},
        {"transport": "redis", "endpoint": ""},
        {"transport": "redis", "endpoint": "preview", "message_op": MessageOp.STREAM},
        {"transport": "redis", "endpoint": "preview", "message_type": "DevicePreviewMessage"},
        {
            "transport": "redis",
            "endpoint": "preview",
            "message_type": "DevicePreviewMessage",
            "message_op": "invalid",
        },
        {"transport": "zmq", "address": "tcp://localhost:5555", "topic": "preview"},
        {"transport": "zmq", "topic": "preview", "encoder": "bec-v1"},
        {"transport": "zmq", "address": "", "topic": "preview", "encoder": "bec-v1"},
        {"transport": "zmq", "address": "tcp://localhost:5555", "topic": "", "encoder": ""},
    ],
)
def test_signal_info_transports_reject_invalid_descriptors(transport):
    with pytest.raises(pydantic.ValidationError):
        messages.SignalInfo(transports=[transport])
    info = messages.SignalInfo()
    with pytest.raises(pydantic.ValidationError):
        info.transports = [transport]


@pytest.mark.parametrize(
    "endpoint_info",
    [
        MessageEndpoints.device_preview("detector", "preview"),
        MessageEndpoints.device_readback("samx"),
        MessageEndpoints.scan_status(),
    ],
)
def test_redis_signal_transport_from_endpoint_info(endpoint_info):
    transport = messages.RedisSignalTransport.from_endpoint_info(endpoint_info)
    assert transport.transport == "redis"
    assert transport.endpoint == endpoint_info.endpoint
    assert transport.message_type == endpoint_info.message_type.__name__
    assert transport.message_op == endpoint_info.message_op
    restored = messages.RedisSignalTransport.model_validate_json(transport.model_dump_json())
    assert restored == transport
    assert restored.message_op is endpoint_info.message_op
    info = messages.SignalInfo(transports=[transport])
    assert messages.SignalInfo.model_validate_json(info.model_dump_json()) == info
    with pytest.raises(pydantic.ValidationError):
        transport.endpoint = ""
    with pytest.raises(pydantic.ValidationError):
        transport.message_op = "invalid"


def test_zmq_signal_transport_assignment():
    zmq = messages.ZMQSignalTransport(address="ipc:///tmp/bec-preview", topic="", encoder="bec-v1")
    assert zmq.transport == "zmq"
    with pytest.raises(pydantic.ValidationError):
        zmq.encoder = ""


def test_zmq_signal_transport_defaults():
    transport = messages.ZMQSignalTransport(address="tcp://localhost:5555", encoder="bec-v1")
    assert transport.socket_type == "sub"
    assert transport.connection_mode == "connect"
    assert transport.topic is None


@pytest.mark.parametrize("socket_type", ["sub", "pull"])
@pytest.mark.parametrize("connection_mode", ["connect", "bind"])
def test_zmq_signal_transport_modes_roundtrip(socket_type, connection_mode):
    transport = messages.ZMQSignalTransport(
        address="tcp://localhost:5555",
        socket_type=socket_type,
        connection_mode=connection_mode,
        encoder="dectris-stream-v2" if socket_type == "pull" else "array-1.0",
    )
    info = messages.SignalInfo(transports=[transport])
    assert messages.SignalInfo.model_validate_json(info.model_dump_json()) == info
    msg = messages.DeviceInfoMessage(device="detector", info=info.model_dump())
    loaded = MsgpackSerialization.loads(MsgpackSerialization.dumps(msg))
    assert messages.SignalInfo.model_validate(loaded.info) == info


@pytest.mark.parametrize("topic", [None, "", "camera/color"])
def test_zmq_signal_transport_sub_topics(topic):
    transport = messages.ZMQSignalTransport(
        address="tcp://localhost:5555", topic=topic, encoder="camera-stream-v1"
    )
    assert transport.topic == topic
    assert messages.ZMQSignalTransport.model_validate_json(transport.model_dump_json()) == transport


@pytest.mark.parametrize(
    "invalid_fields",
    [
        {"socket_type": "pub"},
        {"connection_mode": "listen"},
        {"socket_type": "pull", "topic": "camera/color"},
        {"socket_type": "pull", "topic": ""},
    ],
)
def test_zmq_signal_transport_rejects_invalid_modes(invalid_fields):
    with pytest.raises(pydantic.ValidationError):
        messages.SignalInfo(
            transports=[
                {
                    "transport": "zmq",
                    "address": "tcp://localhost:5555",
                    "encoder": "bec-v1",
                    **invalid_fields,
                }
            ]
        )


def test_zmq_signal_transport_topic_assignment():
    transport = messages.ZMQSignalTransport(
        address="tcp://localhost:5555", topic="preview", encoder="bec-v1"
    )
    with pytest.raises(pydantic.ValidationError):
        transport.socket_type = "pull"
    assert transport.socket_type == "sub"
    transport.topic = None
    transport.socket_type = "pull"
    with pytest.raises(pydantic.ValidationError):
        transport.topic = "preview"
    assert transport.topic is None
    transport.connection_mode = "bind"
    with pytest.raises(pydantic.ValidationError):
        transport.connection_mode = "listen"
    assert transport.connection_mode == "bind"


@pytest.mark.parametrize("group", [None, "baseline", "monitored", "fly-scan"])
def test_signal_info_correlation_group_legacy_compatibility(group):
    info = messages.SignalInfo(correlation_group=group)
    assert info.correlation_group == group
    with pytest.warns(DeprecationWarning, match="use correlation_group instead"):
        legacy = messages.SignalInfo(acquisition_group=group)
    assert legacy == info
    with pytest.warns(DeprecationWarning, match="use correlation_group instead"):
        assert info.acquisition_group == group
    payload = info.model_dump()
    assert payload["correlation_group"] == group
    assert "acquisition_group" not in payload
    assert legacy.model_dump() == payload
    assert messages.SignalInfo.model_validate(payload) == info


def test_signal_info_correlation_group_assignment():
    info = messages.SignalInfo(correlation_group="baseline")
    with pytest.warns(DeprecationWarning, match="use correlation_group instead"):
        info.acquisition_group = "monitored"
    assert info.correlation_group == "monitored"
    info.correlation_group = "fly-scan"
    with pytest.warns(DeprecationWarning, match="use correlation_group instead"):
        assert info.acquisition_group == "fly-scan"
    with pytest.raises(pydantic.ValidationError):
        info.correlation_group = 42
    with pytest.warns(DeprecationWarning, match="use correlation_group instead"):
        with pytest.raises(pydantic.ValidationError):
            info.acquisition_group = 42
    assert info.correlation_group == "fly-scan"
    with pytest.warns(DeprecationWarning, match="use correlation_group instead"):
        info.acquisition_group = None
    assert info.correlation_group is None


def test_signal_info_correlation_group_takes_precedence():
    info = messages.SignalInfo(correlation_group=None, acquisition_group="legacy")
    assert info.correlation_group is None
    assert "acquisition_group" not in info.model_dump()


def test_signal_info_acquisition_group_schema_is_deprecated():
    properties = messages.SignalInfo.model_json_schema(mode="validation")["properties"]
    assert properties["acquisition_group"]["deprecated"] is True
    assert not properties["correlation_group"].get("deprecated", False)
    serialized = messages.SignalInfo.model_json_schema(mode="serialization")["properties"]
    assert "acquisition_group" not in serialized


def test_signal_info_serialization_excludes_deprecated_alias_without_warnings():
    info = messages.SignalInfo(correlation_group="monitored")
    msg = messages.ScanDeviceInfoMessage(
        scan_id="scan-1",
        devices={"detector": messages.DeviceRuntimeInfo(signal_info={"preview": info})},
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        payload = info.model_dump()
        assert json.loads(info.model_dump_json()) == payload
        assert "acquisition_group" not in payload
        nested = msg.model_dump()["devices"]["detector"]["signal_info"]["preview"]
        assert nested == payload
        assert MsgpackSerialization.loads(MsgpackSerialization.dumps(msg)) == msg
    assert not caught


def test_signal_info_rejects_invalid_dimensions():
    with pytest.raises(pydantic.ValidationError):
        messages.SignalInfo(ndim=3)


def test_signal_info_rejects_invalid_dimension_assignment():
    info = messages.SignalInfo(ndim=1)
    info.ndim = 2

    with pytest.raises(pydantic.ValidationError):
        info.ndim = 3

    assert info.ndim == 2


def test_scan_device_info_message_roundtrip():
    msg = messages.ScanDeviceInfoMessage(
        scan_id="scan-1",
        devices={
            "eiger": messages.DeviceRuntimeInfo(
                signal_info={
                    "preview": messages.SignalInfo(
                        data_type="processed",
                        saved=False,
                        ndim=2,
                        scope="continuous",
                        role="preview",
                        rpc_access=True,
                        signals=[("image", 1)],
                        signal_metadata={"units": "counts"},
                        correlation_group="monitored",
                        use_alias=True,
                    )
                },
                disabled_signals=["raw_image", "sub.diagnostic"],
            ),
            "samx": messages.DeviceRuntimeInfo(),
        },
        metadata={"RID": "rid-1"},
    )

    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)

    assert res_loaded == msg
    assert res_loaded.devices["eiger"].signal_info["preview"].ndim == 2
    assert res_loaded.devices["eiger"].signal_info["preview"].role == "preview"
    assert res_loaded.devices["eiger"].disabled_signals == ["raw_image", "sub.diagnostic"]


def test_device_runtime_info_defaults_are_independent():
    detector = messages.DeviceRuntimeInfo()
    motor = messages.DeviceRuntimeInfo()
    detector.signal_info["preview"] = messages.SignalInfo(role="preview")
    detector.disabled_signals.append("raw_image")
    assert motor.signal_info == {}
    assert motor.disabled_signals == []


def test_scan_device_info_validates_nested_signal_info():
    with pytest.raises(pydantic.ValidationError):
        messages.ScanDeviceInfoMessage(
            scan_id="scan-1", devices={"eiger": {"signal_info": {"preview": {"ndim": 3}}}}
        )


def test_scan_device_info_endpoint_contract():
    endpoint = MessageEndpoints.scan_device_info()

    assert endpoint.endpoint == "info/scan_device_info"
    assert endpoint.message_type is messages.ScanDeviceInfoMessage
    assert endpoint.message_op == MessageOp.STREAM


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
        request_id="1234", action="halt", parameter={"RID": "1234"}
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_ScanQueueModificationMessage_with_wrong_action_returns_None():
    with pytest.raises(pydantic.ValidationError):
        messages.ScanQueueModificationMessage(
            request_id="1234", action="wrong_action", parameter={"RID": "1234"}
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


def test_NotificationMessage():
    notification = (
        NotificationMessageObject()
        .add_text("Scan started", bold=True, color="red")
        .add_tags(["beamline", "scan"])
    )
    msg = messages.NotificationMessage(event="new_scan", message=notification._content)
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


def test_NotificationConfigMessage():
    msg = messages.NotificationConfigMessage(
        routes={
            "new_scan": [
                messages.NotificationServiceTarget(service_name="scilog", scope="logbook")
            ],
            "alarm": [
                messages.NotificationServiceTarget(
                    service_name="signal", scope=["+41791234567", "+41797654321"]
                )
            ],
        }
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg


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


def test_StorageCopyRequestMessage():
    msg = messages.StorageCopyRequestMessage(
        source_file="/tmp/source.h5",
        scope="flomni_alignment",
        subdir="results/nested",
        metadata={"RID": "1234"},
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


@pytest.mark.parametrize("action", ["close_scan_group", "open_scan_def", "close_scan_def"])
def test_DeviceInstructionMessage_rejects_removed_scan_actions(action):
    with pytest.raises(pydantic.ValidationError):
        messages.DeviceInstructionMessage(device=None, action=action, parameter={})


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

    @pytest.mark.parametrize("max_shape", [[None], [100]])
    def test_invalid_add_slice_type_with_1d_max_shape(self, max_shape):
        """Test that add_slice rejects one-dimensional target datasets."""
        with pytest.raises(pydantic.ValidationError, match="must have exactly two dimensions"):
            messages.DeviceAsyncUpdate(type="add_slice", max_shape=max_shape, index=0)

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
        assert "must have exactly two dimensions" in str(exc_info.value)

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
        "max_shape,index", [([None, 100], 5), ([None, None], 10), ([100, 200], 15)]
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

    @pytest.mark.parametrize("index", [-1, -2])
    @pytest.mark.parametrize("max_shape", [[None, 1024], [None, None], [100, 1024]])
    def test_invalid_add_slice_negative_index(self, index, max_shape):
        """Test that all negative row indices are rejected for add_slice"""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            messages.DeviceAsyncUpdate(type="add_slice", max_shape=max_shape, index=index)
        assert "index must be an integer >= 0" in str(exc_info.value)

    @pytest.mark.parametrize("max_shape", [[4, 4], [4, 1024]])
    @pytest.mark.parametrize("index", [4, 5])
    def test_invalid_add_slice_index_exceeds_row_limit(self, max_shape, index):
        """Reject indices at or beyond the finite first-axis limit."""
        with pytest.raises(
            pydantic.ValidationError, match=r"index must be smaller than max_shape\[0\]"
        ):
            messages.DeviceAsyncUpdate(type="add_slice", max_shape=max_shape, index=index)

    @pytest.mark.parametrize("max_shape", [[4, 4], [4, 1024]])
    @pytest.mark.parametrize("index", [0, 3])
    def test_valid_add_slice_index_within_row_limit(self, max_shape, index):
        """Both the first and last row within the finite limit are valid."""
        update = messages.DeviceAsyncUpdate(type="add_slice", max_shape=max_shape, index=index)
        assert update.index == index

    @pytest.mark.parametrize("update_type", ["add", "replace"])
    def test_other_update_types_ignore_index_row_limit(self, update_type):
        """The row-index bound applies only to add_slice."""
        update = messages.DeviceAsyncUpdate(type=update_type, max_shape=[4, 1024], index=4)
        assert update.index == 4

    @pytest.mark.parametrize("index", [0, 1, 10, 100])
    def test_valid_add_slice_various_indices(self, index):
        """Test various valid index values for add_slice type"""
        update = messages.DeviceAsyncUpdate(type="add_slice", max_shape=[None, 1024], index=index)
        assert update.index == index


def test_dynamic_metric_message():
    message = messages.DynamicMetricMessage.from_dict(
        {
            "m1": 5,
            "m2": 5.5,
            "m3": {"value": "test", "possible_values": ["prod", "test"]},
            "m4": True,
        }
    )
    assert isinstance(message.metrics["-m1"], messages._IntDynamicMetricValue)
    assert isinstance(message.metrics["-m2"], messages._FloatDynamicMetricValue)
    assert isinstance(message.metrics["-m3"], messages._StrDynamicMetricValue)
    assert isinstance(message.metrics["-m4"], messages._BoolDynamicMetricValue)


def test_feedback_message():
    msg = messages.FeedbackMessage(
        feedback="This is a test feedback.",
        rating=4,
        feedback_type="feature_request",
        contact="user@example.com",
    )
    res = MsgpackSerialization.dumps(msg)
    res_loaded = MsgpackSerialization.loads(res)
    assert res_loaded == msg
    assert res_loaded.username == getpass.getuser()
    assert res_loaded.versions == messages.ServiceVersions._get_version_numbers()
