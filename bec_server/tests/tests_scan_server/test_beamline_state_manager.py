import time
from unittest import mock

import pytest

from bec_lib import messages
from bec_lib.endpoints import MessageEndpoints
from bec_server.scan_server.beamline_state_manager import BeamlineStateManager


@pytest.fixture
def state_manager(connected_connector):
    manager = BeamlineStateManager(connected_connector)
    yield manager


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
def test_state_manager_updates_states(state_manager, connected_connector):
    """
    Test that the BeamlineStateManager updates its states correctly when receiving an update message.
    """

    # Initial state: no states
    assert len(state_manager.states) == 0

    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State1",
                title="Shutter",
                state_type="ShutterState",
                parameters={"device": "shutter1"},
            )
        ]
    )

    connected_connector.xadd(
        MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
    )

    # Give it some time to process
    while len(state_manager.states) < 1:
        time.sleep(0.1)

    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State1",
                title="Shutter",
                state_type="ShutterState",
                parameters={"device": "shutter1"},
            ),
            messages.BeamlineStateConfig(
                name="State2",
                title="Shutter2",
                state_type="ShutterState",
                parameters={"device": "shutter2"},
            ),
        ]
    )

    connected_connector.xadd(
        MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
    )

    # Give it some time to process
    while len(state_manager.states) < 2:
        time.sleep(0.1)

    msg = messages.AvailableBeamlineStatesMessage(
        states=[
            messages.BeamlineStateConfig(
                name="State2",
                title="Shutter2",
                state_type="ShutterState",
                parameters={"device": "shutter2"},
            )
        ]
    )
    connected_connector.xadd(
        MessageEndpoints.available_beamline_states(), {"data": msg}, max_size=1
    )
    # Give it some time to process
    while len(state_manager.states) > 1:
        time.sleep(0.1)

    assert len(state_manager.states) == 1
    assert state_manager.states[0].name == "State2"
