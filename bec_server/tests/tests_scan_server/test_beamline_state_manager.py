import time
from unittest import mock

import pytest

from bec_lib import bl_states, messages
from bec_lib.endpoints import MessageEndpoints
from bec_lib.tests.fixtures import dm_with_devices
from bec_server.scan_server import beamline_state_manager
from bec_server.scan_server.beamline_state_manager import BeamlineStateManager


@pytest.fixture
def state_manager(connected_connector, dm_with_devices):
    manager = BeamlineStateManager(connected_connector, device_manager=dm_with_devices)
    yield manager


@pytest.fixture
def fake_bl_states(monkeypatch):
    class FakeState(bl_states.BeamlineState[bl_states.DeviceWithinLimitsStateConfig]):
        CONFIG_CLASS = bl_states.DeviceWithinLimitsStateConfig

        def __init__(self, config=None, redis_connector=None, **kwargs):
            super().__init__(config=config, redis_connector=redis_connector, **kwargs)
            self.started = False
            self.restart_count = 0

        def evaluate(self, *args, **kwargs):
            return None

        def start(self):
            self.started = True

        def restart(self):
            self.restart_count += 1

    monkeypatch.setattr(beamline_state_manager.bl_states, "DeviceWithinLimitsState", FakeState)
    return FakeState


def test_state_manager_fetches_states(dm_with_devices, fake_bl_states):
    """
    Test that the BeamlineStateManager fetches all available beamline states on initialization.
    """

    connector = mock.MagicMock()
    state_manager = BeamlineStateManager(connector, device_manager=dm_with_devices)
    connector.register.assert_has_calls(
        [
            mock.call(
                MessageEndpoints.available_beamline_states(),
                cb=state_manager._handle_state_update,
                from_start=True,
            ),
            mock.call(MessageEndpoints.device_config_update(), cb=state_manager.restart_all),
        ]
    )


@pytest.mark.timeout(5)
def test_state_manager_updates_states(state_manager, connected_connector, fake_bl_states):
    """
    Test that the BeamlineStateManager updates its states correctly when receiving an update message.
    """

    # Initial state: no states
    assert len(state_manager._states) == 0

    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State1",
                state_type="DeviceWithinLimitsState",
                parameters={
                    "name": "State1",
                    "device": "samx",
                    "low_limit": 0.0,
                    "high_limit": 10.0,
                },
            )
        ]
    )

    connected_connector.xadd(
        MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
    )

    # Give it some time to process
    while len(state_manager._states) < 1:
        time.sleep(0.1)

    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State1",
                state_type="DeviceWithinLimitsState",
                parameters={
                    "name": "State1",
                    "device": "samx",
                    "low_limit": 0.0,
                    "high_limit": 10.0,
                },
            ),
            messages.BeamlineStateConfig(
                name="State2",
                state_type="DeviceWithinLimitsState",
                parameters={
                    "name": "State2",
                    "device": "samy",
                    "low_limit": 0.0,
                    "high_limit": 10.0,
                },
            ),
        ]
    )

    connected_connector.xadd(
        MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
    )

    # Give it some time to process
    while len(state_manager._states) < 2:
        time.sleep(0.1)

    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State2",
                state_type="DeviceWithinLimitsState",
                parameters={
                    "name": "State2",
                    "device": "samy",
                    "low_limit": 0.0,
                    "high_limit": 10.0,
                },
            )
        ]
    )
    connected_connector.xadd(
        MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
    )
    # Give it some time to process
    while len(state_manager._states) > 1:
        time.sleep(0.1)

    assert len(state_manager._states) == 1
    assert "State2" in state_manager._states


def test_state_manager_rejects_abstract_state_type(state_manager):
    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State1",
                state_type="DeviceBeamlineState",
                parameters={"name": "State1", "device": "samx"},
            )
        ]
    )

    with pytest.raises(ValueError, match="not a concrete beamline state"):
        state_manager.update_states(msg)


@pytest.mark.timeout(5)
def test_states_restarted_when_device_config_updated(
    state_manager, connected_connector, fake_bl_states
):
    state_mock = mock.MagicMock()
    state_manager._states["test"] = state_mock
    connected_connector.send(
        MessageEndpoints.device_config_update(), messages.DeviceConfigMessage(action="reload")
    )

    while state_mock.restart.call_count == 0:
        time.sleep(0.1)
