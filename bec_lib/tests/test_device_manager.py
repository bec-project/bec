# pylint: skip-file
import os
from collections import defaultdict
from unittest import mock

import pydantic
import pytest
import typeguard

import bec_lib
from bec_lib import messages
from bec_lib.connector import MessageObject
from bec_lib.devicemanager import DeviceConfigError

dir_path = os.path.dirname(bec_lib.__file__)


def test_device_manager_initialize(device_manager):
    with mock.patch.object(device_manager, "_get_config") as get_config:
        device_manager.initialize("")
        get_config.assert_called_once()


@pytest.mark.parametrize(
    "msg",
    [
        (messages.DeviceConfigMessage(action="update", config={"samx": {}})),
        (messages.DeviceConfigMessage(action="add", config={"samx": {}})),
        (messages.DeviceConfigMessage(action="remove", config={"samx": {}})),
    ],
)
def test_parse_config_request(device_manager, msg):
    with mock.patch.object(device_manager, "_add_device") as add_device:
        with mock.patch.object(device_manager, "_get_device_info") as get_device_info:
            device_manager.parse_config_message(msg)
            if msg.action == "add":
                get_device_info.assert_called_once()
                add_device.assert_called_once()


def test_config_request_update(dm_with_devices):
    device_manager = dm_with_devices
    msg = messages.DeviceConfigMessage(action="update", config={"samx": {}})
    with mock.patch.object(device_manager, "_add_device") as add_device:
        device_manager.parse_config_message(msg)

    msg = messages.DeviceConfigMessage(
        action="update", config={"samx": {"deviceConfig": {"tolerance": 1}}}
    )
    device_manager.parse_config_message(msg)
    assert device_manager.devices.samx._config["deviceConfig"]["tolerance"] == 1

    msg = messages.DeviceConfigMessage(action="update", config={"samx": {"enabled": False}})
    device_manager.parse_config_message(msg)
    assert device_manager.devices.samx._config["enabled"] is False


def test_config_request_reload(dm_with_devices):
    device_manager = dm_with_devices

    msg = messages.DeviceConfigMessage(action="reload", config=None)
    with mock.patch.object(device_manager, "_get_config") as get_config:
        device_manager.parse_config_message(msg)
        assert len(device_manager.devices) == 0
        get_config.assert_called_once()


@pytest.mark.parametrize(
    "msg,raised",
    [
        ({"action": "add", "config": {}}, True),
        ({"action": "remove", "config": {}}, True),
        ({"action": "reload", "config": {}}, False),
        ({"action": "add", "config": {"new_device": {}}}, False),
    ],
)
def test_check_request_validity(device_manager, msg, raised):
    if raised:
        with pytest.raises((DeviceConfigError, pydantic.ValidationError)):
            msg_in = messages.DeviceConfigMessage(**msg)
            device_manager.check_request_validity(msg_in)
    else:
        msg_in = messages.DeviceConfigMessage(**msg)
        device_manager.check_request_validity(msg_in)


def test_get_config_calls_load(device_manager):
    with mock.patch.object(
        device_manager, "_get_redis_device_config", return_value={"devices": [{}]}
    ) as get_redis_config:
        with mock.patch.object(device_manager, "_load_session") as load_session:
            device_manager._get_config()
            get_redis_config.assert_called_once()
            load_session.assert_called_once()


def test_get_redis_device_config(device_manager):
    with mock.patch.object(device_manager, "connector") as connector:
        connector.get.return_value = messages.AvailableResourceMessage(resource={"devices": [{}]})
        assert device_manager._get_redis_device_config() == {"devices": [{}]}


def test_get_devices_with_tags(test_config_yaml, dm_with_devices):
    config_content = test_config_yaml
    device_manager = dm_with_devices

    available_tags = defaultdict(lambda: [])
    for dev_name, dev in config_content.items():
        tags = dev.get("deviceTags")
        if tags is None:
            continue
        for tag in tags:
            available_tags[tag].append(dev_name)

    for tag, devices in available_tags.items():
        dev_list = device_manager.devices.get_devices_with_tags(tag)
        dev_names = {dev.name for dev in dev_list}
        assert dev_names == set(devices)

    assert len(device_manager.devices.get_devices_with_tags("someting")) == 0


def test_get_software_triggered_devices(test_config_yaml, dm_with_devices):
    config_content = test_config_yaml
    device_manager = dm_with_devices

    # Only eiger has softwareTrigger set to True in test config
    software_triggered_devices = []
    for dev_name, dev_cfg in config_content.items():
        if dev_cfg.get("softwareTrigger", None):
            software_triggered_devices.append(device_manager.devices.get(dev_name))

    dev_list = device_manager.devices.get_software_triggered_devices()
    dev_names = {dev.name for dev in dev_list}
    assert dev_names == {dev.name for dev in software_triggered_devices}


def test_show_tags(test_config_yaml, dm_with_devices):
    config_content = test_config_yaml
    device_manager = dm_with_devices

    available_tags = defaultdict(lambda: [])
    for dev_name, dev in config_content.items():
        tags = dev.get("deviceTags")
        if tags is None:
            continue
        for tag in tags:
            available_tags[tag].append(dev_name)

    assert set(device_manager.devices.show_tags()) == set(available_tags.keys())


@pytest.mark.parametrize(
    "scan_motors_in,readout_priority_in",
    [([], {}), (["samx"], {}), ([], {"monitored": ["samx"]}), ([], {"baseline": ["samx"]})],
)
def test_monitored_devices_are_unique(dm_with_devices, scan_motors_in, readout_priority_in):
    device_manager = dm_with_devices
    scan_motors = [device_manager.devices.get(dev) for dev in scan_motors_in]
    devices = device_manager.devices.monitored_devices(
        scan_motors=scan_motors, readout_priority=readout_priority_in
    )
    device_names = set(dev.name for dev in devices)
    assert len(device_names) == len(devices)


@pytest.mark.parametrize(
    "scan_motors_in,readout_priority_in",
    [
        ([], {}),
        ([], {"monitored": ["samx"], "baseline": [], "on_request": []}),
        ([], {"monitored": [], "baseline": ["samx"], "on_request": []}),
        ([], {"monitored": ["samx", "samy"], "baseline": [], "on_request": ["bpm4i"]}),
    ],
)
def test_monitored_devices_with_readout_priority(
    dm_with_devices, scan_motors_in, readout_priority_in
):
    device_manager = dm_with_devices
    scan_motors = [device_manager.devices.get(dev) for dev in scan_motors_in]
    monitored_devices = device_manager.devices.monitored_devices(
        scan_motors=scan_motors, readout_priority=readout_priority_in
    )
    baseline_devices = device_manager.devices.baseline_devices(
        scan_motors=scan_motors, readout_priority=readout_priority_in
    )
    primary_device_names = set(dev.name for dev in monitored_devices)
    baseline_devices_names = set(dev.name for dev in baseline_devices)

    assert len(primary_device_names & baseline_devices_names) == 0

    assert len(set(readout_priority_in.get("on_request", [])) & baseline_devices_names) == 0
    assert len(set(readout_priority_in.get("on_request", [])) & primary_device_names) == 0


@pytest.mark.parametrize(
    "scan_motors_in,readout_priority_in",
    [
        ([], {}),
        ([], {"monitored": ["samx"], "baseline": [], "on_request": []}),
        ([], {"monitored": [], "baseline": ["samx"], "on_request": []}),
        ([], {"monitored": ["samx", "samy"], "baseline": [], "on_request": ["bpm4i"]}),
        (
            [],
            {
                "monitored": ["samx", "samy"],
                "baseline": [],
                "on_request": ["bpm4i"],
                "async": ["bpm3i"],
            },
        ),
        (
            [],
            {
                "monitored": ["samx", "samy"],
                "baseline": [],
                "on_request": ["bpm4i"],
                "async": ["bpm3i"],
                "continuous": ["bpm6i"],
            },
        ),
        (
            [],
            {
                "monitored": ["samx"],
                "baseline": ["samy"],
                "on_request": ["bpm4i"],
                "async": ["bpm3i"],
                "continuous": ["bpm6i"],
            },
        ),
    ],
)
def test_baseline_devices(dm_with_devices, scan_motors_in, readout_priority_in):
    device_manager = dm_with_devices
    scan_motors = [device_manager.devices.get(dev) for dev in scan_motors_in]
    monitored_devices = device_manager.devices.monitored_devices(
        scan_motors=scan_motors, readout_priority=readout_priority_in
    )
    baseline_devices = device_manager.devices.baseline_devices(
        scan_motors=scan_motors, readout_priority=readout_priority_in
    )
    async_devices = device_manager.devices.async_devices(readout_priority=readout_priority_in)
    continuous_devices = device_manager.devices.continuous_devices(
        readout_priority=readout_priority_in
    )
    on_request_devices = device_manager.devices.on_request_devices(
        readout_priority=readout_priority_in
    )

    primary_device_names = set(dev.name for dev in monitored_devices)
    baseline_devices_names = set(dev.name for dev in baseline_devices)
    async_devices_names = set(dev.name for dev in async_devices)
    continuous_devices_names = set(dev.name for dev in continuous_devices)
    on_request_devices_names = set(dev.name for dev in on_request_devices)

    primary_device_names.intersection(readout_priority_in.get("monitored", [])) == set(
        readout_priority_in.get("monitored", [])
    )
    baseline_devices_names.intersection(readout_priority_in.get("baseline", [])) == set(
        readout_priority_in.get("baseline", [])
    )
    async_devices_names.intersection(readout_priority_in.get("async", [])) == set(
        readout_priority_in.get("async", [])
    )
    continuous_devices_names.intersection(readout_priority_in.get("continuous", [])) == set(
        readout_priority_in.get("continuous", [])
    )
    on_request_devices_names.intersection(readout_priority_in.get("on_request", [])) == set(
        readout_priority_in.get("on_request", [])
    )

    assert len(primary_device_names & baseline_devices_names) == 0

    assert len(set(readout_priority_in.get("on_request", [])) & baseline_devices_names) == 0
    assert len(set(readout_priority_in.get("on_request", [])) & primary_device_names) == 0
    assert len(set(readout_priority_in.get("async", [])) & primary_device_names) == 0
    assert len(set(readout_priority_in.get("continuous", [])) & primary_device_names) == 0

    assert (
        set(primary_device_names).intersection(
            set(baseline_devices_names),
            set(async_devices_names),
            set(continuous_devices_names),
            set(on_request_devices_names),
        )
    ) == set()

    assert (
        set(baseline_devices_names).intersection(
            set(async_devices_names),
            set(continuous_devices_names),
            set(on_request_devices_names),
            set(primary_device_names),
        )
        == set()
    )

    assert (
        set(async_devices_names).intersection(
            set(continuous_devices_names),
            set(on_request_devices_names),
            set(primary_device_names),
            set(baseline_devices_names),
        )
        == set()
    )


@pytest.mark.parametrize(
    "readout_priority_in",
    [
        {"monitored": ["samx"], "async": ["samx"]},
        {"monitored": ["samx"], "continuous": ["samx", "samy"]},
    ],
)
def test_readoutpriority_raises_with_conflicting_input(dm_with_devices, readout_priority_in):
    dm = dm_with_devices
    with pytest.raises(ValueError):
        dm.devices.monitored_devices(readout_priority=readout_priority_in)


@pytest.mark.parametrize(
    "readout_priority_in, priority_out, raises_exception",
    [
        ({"monitored": ["samx"], "baseline": ["samx"]}, {"monitored": ["samx"]}, False),
        (
            {"monitored": ["samx"], "continuous": ["samx", "samy"]},
            {"monitored": ["samx"], "continuous": ["samy"]},
            True,
        ),
        (
            {"monitored": ["samx"], "on_request": ["samx", "samy"]},
            {"monitored": ["samx"], "on_request": ["samy"]},
            False,
        ),
        (
            {"baseline": ["samx"], "on_request": ["samx", "samy"]},
            {"baseline": ["samx"], "on_request": ["samy"]},
            False,
        ),
    ],
)
def test_readoutpriority_highest_priority_wins(
    dm_with_devices, readout_priority_in, priority_out, raises_exception
):
    dm = dm_with_devices
    if raises_exception:
        with pytest.raises(ValueError):
            monitored_devices = dm.devices.monitored_devices(readout_priority=readout_priority_in)
            baseline_devices = dm.devices.baseline_devices(readout_priority=readout_priority_in)
            async_devices = dm.devices.async_devices(readout_priority=readout_priority_in)
            continuous_devices = dm.devices.continuous_devices(readout_priority=readout_priority_in)
            on_request_devices = dm.devices.on_request_devices(readout_priority=readout_priority_in)
        return

    monitored_devices = dm.devices.monitored_devices(readout_priority=readout_priority_in)
    baseline_devices = dm.devices.baseline_devices(readout_priority=readout_priority_in)
    async_devices = dm.devices.async_devices(readout_priority=readout_priority_in)
    continuous_devices = dm.devices.continuous_devices(readout_priority=readout_priority_in)
    on_request_devices = dm.devices.on_request_devices(readout_priority=readout_priority_in)

    monitored_device_names = set(dev.name for dev in monitored_devices)
    baseline_device_names = set(dev.name for dev in baseline_devices)
    async_device_names = set(dev.name for dev in async_devices)
    continuous_device_names = set(dev.name for dev in continuous_devices)
    on_request_device_names = set(dev.name for dev in on_request_devices)

    assert (
        monitored_device_names.intersection(
            baseline_device_names,
            async_device_names,
            continuous_device_names,
            on_request_device_names,
        )
        == set()
    )
    assert set(priority_out.get("monitored", [])).intersection(monitored_device_names) == set(
        priority_out.get("monitored", [])
    )
    assert set(priority_out.get("baseline", [])).intersection(baseline_device_names) == set(
        priority_out.get("baseline", [])
    )
    assert set(priority_out.get("async", [])).intersection(async_device_names) == set(
        priority_out.get("async", [])
    )
    assert set(priority_out.get("continuous", [])).intersection(continuous_device_names) == set(
        priority_out.get("continuous", [])
    )
    assert set(priority_out.get("on_request", [])).intersection(on_request_device_names) == set(
        priority_out.get("on_request", [])
    )


def test_device_config_update_callback(dm_with_devices):
    device_manager = dm_with_devices
    dev_config_msg = messages.DeviceConfigMessage(action="update", config={"samx": {}})
    msg = MessageObject(value=dev_config_msg, topic="")

    with mock.patch.object(device_manager, "parse_config_message") as parse_config_message:
        device_manager._device_config_update_callback(msg, parent=device_manager)
        parse_config_message.assert_called_once_with(dev_config_msg)


def test_disabled_device_not_in_monitored(dm_with_devices):
    assert "motor1_disabled" in dm_with_devices.devices
    monitored_devices = dm_with_devices.devices.monitored_devices()
    assert "motor1_disabled" not in [dev.name for dev in monitored_devices]


def test_get_bec_signals(dm_with_devices):
    device_manager = dm_with_devices
    with pytest.raises(typeguard.TypeCheckError):
        device_manager.get_bec_signals("non_existing_filter")

    preview_signals = device_manager.get_bec_signals("PreviewSignal")
    assert preview_signals
    eiger = list(filter(lambda x: x[0] == "eiger", preview_signals))[0]
    assert eiger[1] == "preview"
    assert isinstance(eiger[2], dict)
