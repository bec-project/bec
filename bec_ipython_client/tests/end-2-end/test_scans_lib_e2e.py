import time

import numpy as np
import pytest
import yaml

from bec_lib.alarm_handler import AlarmBase
from bec_lib.devicemanager import DeviceConfigError
from bec_lib.endpoints import MessageEndpoints
from bec_lib.logger import bec_logger

logger = bec_logger.logger


@pytest.mark.timeout(100)
def test_grid_scan_lib(bec_client_lib):
    bec = bec_client_lib
    scans = bec.scans
    bec.metadata.update({"unit_test": "test_grid_scan_bec_client_lib"})
    dev = bec.device_manager.devices
    scans.umv(dev.samx, 0, dev.samy, 0, relative=False)
    status = scans.grid_scan(dev.samx, -5, 5, 10, dev.samy, -5, 5, 10, exp_time=0.01, relative=True)
    status.wait()
    assert len(status.scan.live_data) == 100
    assert status.scan.num_points == 100


@pytest.mark.timeout(100)
def test_mv_scan_lib(bec_client_lib):
    bec = bec_client_lib
    scans = bec.scans
    bec.metadata.update({"unit_test": "test_mv_scan_bec_client_lib"})
    dev = bec.device_manager.devices
    scans.mv(dev.samx, 10, dev.samy, 20, relative=False).wait()
    current_pos_samx = dev.samx.read()["samx"]["value"]
    current_pos_samy = dev.samy.read()["samy"]["value"]
    assert np.isclose(
        current_pos_samx, 10, atol=dev.samx._config["deviceConfig"].get("tolerance", 0.05)
    )
    assert np.isclose(
        current_pos_samy, 20, atol=dev.samy._config["deviceConfig"].get("tolerance", 0.05)
    )


@pytest.mark.timeout(100)
def test_mv_raises_limit_error(bec_client_lib):
    bec = bec_client_lib
    scans = bec.scans
    bec.metadata.update({"unit_test": "test_mv_raises_limit_error"})
    dev = bec.device_manager.devices
    dev.samx.limits = [-50, 50]
    with pytest.raises(AlarmBase) as exc:
        scans.mv(dev.samx, 1000, relative=False).wait()


@pytest.mark.timeout(100)
def test_async_callback_data_matches_scan_data_lib(bec_client_lib):
    bec = bec_client_lib
    scans = bec.scans  # not needed but to silence pylint...
    bec.metadata.update({"unit_test": "test_async_callback_data_matches_scan_data"})
    dev = bec.device_manager.devices
    reference_container = {"data": [], "metadata": {}}

    def dummy_callback(data, metadata):
        logger.info(f"callback metadata: {metadata}")
        reference_container["metadata"] = metadata
        reference_container["data"].append(data)

    s = scans.line_scan(dev.samx, 0, 1, steps=10, relative=False, async_callback=dummy_callback)
    s.wait()
    while len(reference_container["data"]) < 10:
        time.sleep(0.1)
    assert len(s.scan.live_data) == 10
    assert len(reference_container["data"]) == 10

    for ii, msg in enumerate(s.scan.live_data.messages.values()):
        assert msg.content == reference_container["data"][ii]


@pytest.mark.timeout(100)
def test_config_updates(bec_client_lib):
    bec = bec_client_lib
    bec.metadata.update({"unit_test": "test_config_updates"})
    dev = bec.device_manager.devices
    dev.rt_controller.limits = [-80, 80]
    assert dev.rt_controller.limits == [-80, 80]
    dev.rt_controller.limits = [-50, 50]
    assert dev.rt_controller.limits == [-50, 50]

    dev.rt_controller.velocity.set(10).wait()
    assert dev.rt_controller.velocity.read()["rt_controller_velocity"]["value"] == 10
    assert dev.rt_controller.velocity.read(cached=False)["rt_controller_velocity"]["value"] == 10
    assert dev.rt_controller.read_configuration()["rt_controller_velocity"]["value"] == 10
    assert (
        dev.rt_controller.read_configuration(cached=False)["rt_controller_velocity"]["value"] == 10
    )

    dev.rt_controller.velocity.put(5)
    assert dev.rt_controller.velocity.get() == 5

    dev.rt_controller.velocity.set(10).wait()
    assert dev.rt_controller.velocity.get() == 10

    dev.rt_controller.setpoint.put(5)
    assert dev.rt_controller.setpoint.get() == 5

    dev.rt_controller.setpoint.set(10).wait()
    assert dev.rt_controller.setpoint.get() == 10
    assert dev.rt_controller.dummy_controller.some_var == 10
    dev.rt_controller.dummy_controller.some_var = 20
    assert dev.rt_controller.dummy_controller.some_var == 20
    dev.rt_controller.dummy_controller.some_var = 10
    val = dev.rt_controller.readback.get()
    assert np.isclose(val, dev.rt_controller.position, atol=0.05)


@pytest.mark.timeout(100)
def test_dap_fit(bec_client_lib):
    bec = bec_client_lib
    bec.metadata.update({"unit_test": "test_dap_fit"})
    dev = bec.device_manager.devices
    scans = bec.scans

    dev.bpm4i.sim.select_model("GaussianModel")
    params = dev.bpm4i.sim.params
    params.update(
        {"noise": "uniform", "noise_multiplier": 10, "center": 5, "sigma": 1, "amplitude": 200}
    )
    dev.bpm4i.sim.params = params
    time.sleep(1)

    res = scans.line_scan(dev.samx, 0, 8, steps=50, relative=False)
    res.wait()

    while True:
        fit = bec.dap.GaussianModel.fit(res.scan, "samx", "samx", "bpm4i", "bpm4i")
        if np.isclose(fit.center, 5, atol=0.5):
            break
        time.sleep(1)

    bec.dap.GaussianModel.select("bpm4i")
    bec.dap.GaussianModel.auto_run = True

    res = scans.line_scan(dev.samx, 0, 8, steps=20, relative=False)
    res.wait()

    while True:
        time.sleep(1)
        fit = bec.dap.GaussianModel.get_data()
        if res.scan.scan_id != fit.report["input"]["scan_id"]:
            continue
        if not np.isclose(fit.center, 5, atol=0.5):
            continue
        break


@pytest.mark.timeout(100)
@pytest.mark.parametrize(
    "config, raises_error, deletes_config, disabled_device",
    [
        (
            {
                "hexapod": {
                    "deviceClass": "ophyd_devices.SynDeviceOPAAS",
                    "deviceConfig": {},
                    "deviceTags": ["user motors"],
                    "readoutPriority": "baseline",
                    "enabled": True,
                    "readOnly": False,
                },
                "eyefoc": {
                    "deviceClass": "ophyd_devices.SimPositioner",
                    "deviceConfig": {
                        "delay": 1,
                        "limits": [-50, 50],
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "deviceTags": ["user motors"],
                    "enabled": True,
                    "readOnly": False,
                },
            },
            True,
            False,
            [],
        ),
        (
            {
                "hexapod": {
                    "deviceClass": "ophyd_devices.SynDeviceOPAAS",
                    "deviceConfig": {},
                    "deviceTags": ["user motors"],
                    "readoutPriority": "baseline",
                    "enabled": True,
                    "readOnly": False,
                },
                "eyefoc": {
                    "deviceClass": "ophyd_devices.SimPositioner",
                    "deviceConfig": {
                        "delay": 1,
                        "limits": [-50, 50],
                        "tolerance": 0.01,
                        "update_frequency": 400,
                    },
                    "readoutPriority": "baseline",
                    "deviceTags": ["user motors"],
                    "enabled": True,
                    "readOnly": False,
                },
            },
            False,
            False,
            [],
        ),
        (
            {
                "hexapod": {
                    "deviceClass": "ophyd_devices.SynDeviceOPAAS",
                    "deviceConfig": {},
                    "deviceTags": ["user motors"],
                    "readoutPriority": "baseline",
                    "enabled": True,
                    "readOnly": False,
                },
                "eyefoc": {
                    "deviceClass": "ophyd_devices.utils.bec_utils.DeviceClassConnectionError",
                    "deviceConfig": {},
                    "readoutPriority": "baseline",
                    "deviceTags": ["user motors"],
                    "enabled": True,
                    "readOnly": False,
                },
            },
            True,
            False,
            ["eyefoc"],
        ),
        (
            {
                "hexapod": {
                    "deviceClass": "SynDeviceOPAAS",
                    "deviceConfig": {},
                    "deviceTags": ["user motors"],
                    "readoutPriority": "baseline",
                    "enabled": True,
                    "readOnly": False,
                },
                "eyefoc": {
                    "deviceClass": "ophyd_devices.utils.bec_utils.DeviceClassInitError",
                    "deviceConfig": {},
                    "readoutPriority": "baseline",
                    "deviceTags": ["user motors"],
                    "enabled": True,
                    "readOnly": False,
                },
            },
            True,
            True,
            [],
        ),
        (
            {
                "hexapod": {
                    "deviceClass": "SynDeviceOPAAS",
                    "deviceConfig": {},
                    "deviceTags": ["user motors"],
                    "readoutPriority": "baseline",
                    "enabled": True,
                    "readOnly": False,
                },
                "eyefoc": {
                    "deviceClass": "ophyd_devices.WrongDeviceClass",
                    "deviceConfig": {},
                    "readoutPriority": "baseline",
                    "deviceTags": ["user motors"],
                    "enabled": True,
                    "readOnly": False,
                },
            },
            True,
            True,
            [],
        ),
    ],
    ids=[
        "invalid_config_missing_readoutPriority",
        "valid_config_no_error",
        "invalid_device_class_connection_error",
        "invalid_device_class_init",
        "invalid_device_class",
    ],
)
def test_config_reload(
    bec_test_config_file_path, bec_client_lib, config, raises_error, deletes_config, disabled_device
):
    bec = bec_client_lib
    bec.metadata.update({"unit_test": "test_config_reload"})
    runtime_config_file_path = bec_test_config_file_path.parent / "e2e_runtime_config_test.yaml"

    # write new config to disk
    with open(runtime_config_file_path, "w") as f:
        f.write(yaml.dump(config))
    num_devices = len(bec.device_manager.devices)
    if raises_error:
        with pytest.raises(DeviceConfigError):
            bec.config.update_session_with_file(runtime_config_file_path)
        if deletes_config:
            assert len(bec.device_manager.devices) == 0
        elif disabled_device:
            assert len(bec.device_manager.devices) == 2
        else:
            assert len(bec.device_manager.devices) == num_devices
    else:
        bec.config.update_session_with_file(runtime_config_file_path)
        assert len(bec.device_manager.devices) == 2
    for dev in disabled_device:
        assert bec.device_manager.devices[dev].enabled is False


def test_config_add_remove_device(bec_client_lib):
    bec = bec_client_lib
    bec.metadata.update({"unit_test": "test_config_add_device"})
    dev = bec.device_manager.devices
    config = {
        "new_device": {
            "deviceClass": "ophyd_devices.SimPositioner",
            "deviceConfig": {
                "delay": 1,
                "limits": [-50, 50],
                "tolerance": 0.01,
                "update_frequency": 400,
            },
            "readoutPriority": "baseline",
            "deviceTags": ["user motors"],
            "enabled": True,
            "readOnly": False,
        }
    }
    bec.config.send_config_request(action="add", config=config)
    assert "new_device" in dev

    bec.config.send_config_request(action="remove", config={"new_device": {}})
    assert "new_device" not in dev

    config = bec.connector.get(MessageEndpoints.device_config())
    device_configs = config.content["resource"]
    available_devices = [dev["name"] for dev in device_configs]
    assert "new_device" not in available_devices
    assert "samx" in available_devices


def test_computed_signal(bec_client_lib):
    bec = bec_client_lib
    bec.metadata.update({"unit_test": "test_computed_signal"})
    dev = bec.device_manager.devices
    scans = bec.scans

    res = scans.line_scan(dev.samx, -0.1, 0.1, steps=10, relative=False, exp_time=0)
    res.wait()
    out = res.scan.data.devices.pseudo_signal1.read()
    assert "value" in out["pseudo_signal1"]

    def compute_signal1(*args, **kwargs):
        return 5

    dev.pseudo_signal1.set_compute_method(compute_signal1)
    dev.pseudo_signal1.set_input_signals()

    assert dev.pseudo_signal1.read(cached=False)["pseudo_signal1"]["value"] == 5


def test_cached_device_readout(bec_client_lib):
    bec = bec_client_lib
    bec.metadata.update({"unit_test": "test_cached_device_readout"})
    dev = bec.device_manager.devices

    dev.samx.setpoint.put(5)
    data = dev.samx.setpoint.get()
    assert data == 5

    orig_velocity = dev.samx.velocity.get()
    dev.samx.velocity.put(10)
    data = dev.samx.velocity.get()
    assert data == 10

    config = dev.samx.read_configuration()
    assert config["samx_velocity"]["value"] == 10

    dev.samx.velocity.put(orig_velocity)

    data = dev.hexapod.x.readback.read(cached=False)
    timestamp = data["hexapod_x"]["timestamp"]
    data = dev.hexapod.x.readback.read(cached=True)
    assert data["hexapod_x"]["timestamp"] == timestamp

    # check that .get also updates the cache
    dev.hexapod.x.readback.get(cached=False)
    timestamp_2 = dev.hexapod.x.readback.read(cached=True)["hexapod_x"]["timestamp"]
    assert timestamp_2 != timestamp

    dev.hexapod.x.readback.get(cached=True)
    timestamp_3 = dev.hexapod.x.readback.read(cached=True)["hexapod_x"]["timestamp"]
    assert timestamp_3 == timestamp_2


def test_interactive_scan(bec_client_lib):
    bec = bec_client_lib
    bec.metadata.update({"unit_test": "test_interactive_scan"})
    dev = bec.device_manager.devices
    scans = bec.scans

    with scans.interactive_scan(monitored=[dev.samx, dev.samy], exp_time=0.1) as scan:
        for ii in range(10):
            samx_status = dev.samx.set(ii)
            samy_status = dev.samy.set(ii)
            samx_status.wait()
            samy_status.wait()
            scan.trigger()
            scan.read_monitored_devices(devices=[dev.samx, dev.samy])
        report = scan.status

    report.wait()

    while len(report.scan.live_data) != 10:
        time.sleep(0.1)
    assert len(report.scan.live_data.samx.samx.val) == 10
