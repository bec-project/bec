from unittest import mock

import pytest
from ophyd_devices.sim.sim_positioner import SimPositioner

from bec_lib import messages
from bec_lib.device import Positioner, _PermissiveDeviceModel
from bec_lib.endpoints import MessageEndpoints
from bec_server.device_server.devices.device_serializer import get_device_info
from bec_server.scan_server.tests.scan_hook_tests import (
    assert_close_scan_waits_for_baseline_and_closes,
    assert_pre_scan_called,
    assert_prepare_scan_reads_baseline_devices,
    assert_scan_open_called,
    assert_stage_all_devices_called,
    assert_unstage_all_devices_called,
    run_scan_tests,
)

ACQUIRE_DEFAULT_HOOK_TESTS = [
    ("open_scan", [assert_scan_open_called]),
    ("stage", [assert_stage_all_devices_called]),
    ("pre_scan", [assert_pre_scan_called]),
    ("unstage", [assert_unstage_all_devices_called]),
    ("close_scan", [assert_close_scan_waits_for_baseline_and_closes]),
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


@pytest.fixture
def simulated_samx(device_manager):
    # dev_obj = SimPositioner(name="samx")
    name = "samx"
    dev = SimPositioner(name=name)
    config = _PermissiveDeviceModel(
        enabled=True,
        deviceClass="ophyd_devices.sim.sim_positioner.SimPositioner",
        readoutPriority="baseline",
    )
    info = get_device_info(dev, connect=True)
    dev_man_obj = Positioner(
        name=name, info=info, config=config, class_name=config.deviceClass, parent=device_manager
    )
    return dev_man_obj


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
    v4_scan_assembler, state_transition_connector, device_manager, simulated_samx
):
    """Test prepare scan hook for the StateTransitionScan."""
    device_manager.add_device(simulated_samx, replace=True)
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
    v4_scan_assembler, state_transition_connector, device_manager, simulated_samx
):
    device_manager.add_device(simulated_samx, replace=True)
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
    scan.components.get_start_positions = mock.MagicMock(return_value=[0, 0])
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
        # mock_add_scan_report_instruction_readback.assert_called_once_with(
        #     devices=[scan.device_manager.devices["samx"], scan.device_manager.devices["samy"]],
        #     start=[0, 0],
        #     stop=[1.5, 0.5],
        # )
        mock_move_and_wait.assert_called_once_with(
            [scan.device_manager.devices["samx"], scan.device_manager.devices["samy"]], [1.5, 0.5]
        )
        mock_set_limits.assert_called_once()
