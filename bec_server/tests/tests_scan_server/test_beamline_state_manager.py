import time
from unittest import mock

import pytest

from bec_lib import bl_states, messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.scan_server import beamline_state_manager
from bec_server.scan_server.beamline_state_manager import BeamlineStateManager


@pytest.fixture
def state_manager(connected_connector):
    manager = BeamlineStateManager(connected_connector)
    yield manager


@pytest.fixture
def fake_bl_states(monkeypatch):
    class FakeState(bl_states.BeamlineState[bl_states.DeviceStateConfig]):
        CONFIG_CLASS = bl_states.DeviceStateConfig

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

    monkeypatch.setattr(beamline_state_manager.bl_states, "ShutterState", FakeState)
    return FakeState


def test_state_manager_fetches_states():
    """
    Test that the BeamlineStateManager fetches all available beamline states on initialization.
    """

    connector = mock.MagicMock()
    state_manager = BeamlineStateManager(connector)
    connector.register.assert_called_once_with(
        MessageEndpoints.available_beamline_states(),
        cb=state_manager._handle_state_update,
        parent=state_manager,
        from_start=True,
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
                title="Shutter",
                state_type="ShutterState",
                parameters={"name": "State1", "title": "Shutter", "device": "shutter1"},
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
                title="Shutter",
                state_type="ShutterState",
                parameters={"name": "State1", "title": "Shutter", "device": "shutter1"},
            ),
            messages.BeamlineStateConfig(
                name="State2",
                title="Shutter2",
                state_type="ShutterState",
                parameters={"name": "State2", "title": "Shutter2", "device": "shutter2"},
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
                title="Shutter2",
                state_type="ShutterState",
                parameters={"name": "State2", "title": "Shutter2", "device": "shutter2"},
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
