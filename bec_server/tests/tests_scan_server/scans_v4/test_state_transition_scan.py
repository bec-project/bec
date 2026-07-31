from unittest import mock

import pytest
from ophyd_devices.sim.sim_positioner import SimPositioner

from bec_lib import messages
from bec_lib.device import Positioner, _PermissiveDeviceModel
from bec_lib.endpoints import MessageEndpoints
from bec_lib.messages import AvailableBeamlineStatesMessage, BeamlineStateConfig
from bec_server.device_server.devices.device_serializer import get_device_info
from bec_server.scan_server.tests.scan_hook_tests import run_scan_tests


def assert_scan_open_called(scan):
    scan.actions.open_scan = mock.MagicMock()
    scan.open_scan()
    scan.actions.open_scan.assert_not_called()


def assert_scan_stage_called(scan):
    scan.actions.stage_all_devices = mock.MagicMock()
    scan.stage()
    scan.actions.stage_all_devices.assert_not_called()


def assert_scan_pre_scan_called(scan):
    scan.actions.pre_scan_all_devices = mock.MagicMock()
    scan.pre_scan()
    scan.actions.pre_scan_all_devices.assert_not_called()


def assert_scan_unstage_called(scan):
    scan.actions.unstage_all_devices = mock.MagicMock()
    scan.unstage()
    scan.actions.unstage_all_devices.assert_not_called()


def assert_scan_close_scan_called(scan, status=None):
    scan.actions.close_scan = mock.MagicMock()
    scan.close_scan()
    scan.actions.close_scan.assert_not_called()


ACQUIRE_DEFAULT_HOOK_TESTS = [
    ("open_scan", [assert_scan_open_called]),
    ("stage", [assert_scan_stage_called]),
    ("pre_scan", [assert_scan_pre_scan_called]),
    ("unstage", [assert_scan_unstage_called]),
    ("close_scan", [assert_scan_close_scan_called]),
]


@pytest.fixture
def state_transition_connector(connected_connector):
    connected_connector.xadd(
        MessageEndpoints.available_beamline_states(),
        {
            "data": messages.AvailableBeamlineStatesMessage(
                states=[
                    messages.BeamlineStateConfig(
                        name="test",
                        title="Test state",
                        state_type="AggregatedState",
                        parameters={
                            "states": {
                                "alignment": {
                                    "devices": {
                                        "samx": {
                                            "value": 1.5,
                                            "low_limit": {"value": -2},
                                            "high_limit": {"value": 2},
                                            "signals": {"velocity": {"value": 0.5}},
                                        },
                                        "samy": {
                                            "value": 0.5,
                                            "low_limit": {"value": -1},
                                            "high_limit": {"value": 1},
                                        },
                                    }
                                }
                            }
                        },
                    )
                ]
            )
        },
    )
    return connected_connector


def publish_state_config(connector, *, label, devices):
    """Publish one aggregated-state label for transition tests."""
    connector.xadd(
        MessageEndpoints.available_beamline_states(),
        {
            "data": AvailableBeamlineStatesMessage(
                states=[
                    BeamlineStateConfig(
                        name="test",
                        title="Test state",
                        state_type="AggregatedState",
                        parameters={"states": {label: {"devices": devices}}},
                    )
                ]
            )
        },
    )


@pytest.fixture
def simulated_positioner_factory(device_manager):
    def factory(name):
        dev = SimPositioner(name=name)
        config = _PermissiveDeviceModel(
            enabled=True,
            deviceClass="ophyd_devices.sim.sim_positioner.SimPositioner",
            readoutPriority="baseline",
        )
        info = get_device_info(dev, connect=True)
        dev_man_obj = Positioner(
            name=name,
            info=info,
            config=config,
            class_name=config.deviceClass,
            parent=device_manager,
        )
        return dev_man_obj

    return factory


@pytest.mark.timeout(20)
@pytest.mark.parametrize(("hook_name", "hook_tests"), ACQUIRE_DEFAULT_HOOK_TESTS)
def test_state_transition_default_hooks(
    v4_scan_assembler, state_transition_connector, nth_done_status_mock, hook_name, hook_tests
):
    """Test default hooks open_scan, stage, pre_scan, unstage, and close_scan for the StateTransitionScan."""
    scan = v4_scan_assembler(
        "_v4_state_transition",
        state_name="test",
        target_label="alignment",
        connector=state_transition_connector,
    )

    run_scan_tests(scan, [(hook_name, hook_tests)], nth_done_status_mock=nth_done_status_mock)


@pytest.mark.timeout(20)
def test_state_transition_prepare_scan(
    v4_scan_assembler, state_transition_connector, device_manager, simulated_positioner_factory
):
    """Test prepare scan hook for the StateTransitionScan."""
    samy = simulated_positioner_factory("samy")
    samx = simulated_positioner_factory("samx")
    device_manager.add_device(samy, replace=True)
    device_manager.add_device(samx, replace=True)
    scan = v4_scan_assembler(
        "_v4_state_transition",
        state_name="test",
        target_label="alignment",
        connector=state_transition_connector,
    )

    scan.prepare_scan()

    devices_to_set = {(device.name, value) for device, value in scan._devices_to_set}
    limits_to_set = {
        device_name: (device.name, low_limit, high_limit)
        for device_name, (device, low_limit, high_limit) in scan._limits_to_set.items()
    }
    signals_to_set = {(signal.full_name, value) for signal, value in scan._signals_to_set}

    assert devices_to_set == {("samx", 1.5), ("samy", 0.5)}
    assert limits_to_set == {"samx": ("samx", -2, 2), "samy": ("samy", -1, 1)}
    assert signals_to_set == {("samx_velocity", 0.5)}


@pytest.mark.timeout(20)
def test_state_transition_scan_core(
    v4_scan_assembler, state_transition_connector, device_manager, simulated_positioner_factory
):
    samx = simulated_positioner_factory("samx")
    samy = simulated_positioner_factory("samy")
    device_manager.add_device(samy, replace=True)
    device_manager.add_device(samx, replace=True)
    scan = v4_scan_assembler(
        "_v4_state_transition",
        state_name="test",
        target_label="alignment",
        connector=state_transition_connector,
    )
    scan.prepare_scan()
    signal_by_name = {signal.full_name: signal for signal, _ in scan._signals_to_set}
    velocity_set_status = mock.MagicMock()
    signal_by_name["samx_velocity"].set = mock.MagicMock(return_value=velocity_set_status)
    with (
        mock.patch.object(
            scan.components, "get_start_positions", return_value=[0, 0]
        ) as mock_get_start_positions,
        mock.patch.object(scan.components, "move_and_wait") as mock_move_and_wait,
        mock.patch.object(
            scan.actions, "add_scan_report_instruction_readback"
        ) as mock_add_scan_report_instruction_readback,
        mock.patch.object(scan, "_set_limits") as mock_set_limits,
    ):
        scan.scan_core()
        mock_get_start_positions.assert_called_once()
        mock_add_scan_report_instruction_readback.assert_called_once_with(
            devices=[scan.device_manager.devices["samx"], scan.device_manager.devices["samy"]],
            start=[0, 0],
            stop=[1.5, 0.5],
        )
        mock_move_and_wait.assert_called_once_with(
            [scan.device_manager.devices["samx"], scan.device_manager.devices["samy"]], [1.5, 0.5]
        )
        signal_by_name["samx_velocity"].set.assert_called_once_with(0.5)
        velocity_set_status.wait.assert_called_once_with()
        mock_set_limits.assert_called_once()


@pytest.mark.timeout(20)
def test_state_transition_prepare_scan_resolves_user_parameters(
    v4_scan_assembler, connected_connector, device_manager, simulated_positioner_factory
):
    samx = simulated_positioner_factory("samx")
    samx._config["userParameter"] = {"position": 0, "low": -3, "high": 3, "velocity": False}
    device_manager.add_device(samx, replace=True)
    publish_state_config(
        connected_connector,
        label="dynamic",
        devices={
            "samx": {
                "at": "position",
                "low_limit": {"at": "low"},
                "high_limit": {"at": "high"},
                "signals": {"velocity": {"at": "velocity"}},
            }
        },
    )
    scan = v4_scan_assembler(
        "_v4_state_transition",
        state_name="test",
        target_label="dynamic",
        connector=connected_connector,
    )

    scan.prepare_scan()

    assert [(device.name, value) for device, value in scan._devices_to_set] == [("samx", 0)]
    assert type(scan._devices_to_set[0][1]) is int
    assert {
        name: (device.name, low, high) for name, (device, low, high) in scan._limits_to_set.items()
    } == {"samx": ("samx", -3, 3)}
    assert [(signal.full_name, value) for signal, value in scan._signals_to_set] == [
        ("samx_velocity", False)
    ]
    assert type(scan._signals_to_set[0][1]) is bool


@pytest.mark.timeout(20)
@pytest.mark.parametrize(
    "target_config",
    [
        {"at": "missing"},
        {"low_limit": {"at": "missing"}},
        {"high_limit": {"at": "missing"}},
        {"signals": {"velocity": {"at": "missing"}}},
    ],
)
def test_state_transition_prepare_scan_rejects_missing_user_parameter(
    v4_scan_assembler,
    connected_connector,
    device_manager,
    simulated_positioner_factory,
    target_config,
):
    samx = simulated_positioner_factory("samx")
    device_manager.add_device(samx, replace=True)
    publish_state_config(connected_connector, label="dynamic", devices={"samx": target_config})
    scan = v4_scan_assembler(
        "_v4_state_transition",
        state_name="test",
        target_label="dynamic",
        connector=connected_connector,
    )

    with pytest.raises(ValueError, match="User parameter 'missing'"):
        scan.prepare_scan()
