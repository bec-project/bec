from __future__ import annotations

import inspect
from unittest import mock

import pytest
from pydantic import BaseModel

from bec_lib import bl_states, messages
from bec_lib.bl_state_manager import (
    BeamlineStateClientBase,
    BeamlineStateManager,
    build_signature_from_model,
)
from bec_lib.endpoints import MessageEndpoints
from bec_lib.redis_connector import MessageObject


@pytest.fixture
def state_manager(connected_connector):
    client = mock.MagicMock()
    client.connector = connected_connector
    manager = BeamlineStateManager(client)
    yield manager


class TestHelpers:
    def test_build_signature_from_model(self):
        class DemoConfig(BaseModel):
            foo: int = 1
            bar: str = "abc"

        signature = build_signature_from_model(DemoConfig)

        assert list(signature.parameters) == ["foo", "bar"]
        assert signature.parameters["foo"].kind == inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters["foo"].annotation is int
        assert signature.parameters["bar"].default == "abc"


class TestConfigModels:
    def test_beamline_state_config_valid_name(self):
        config = bl_states.BeamlineStateConfig(name="shutter_open", title="Shutter")
        assert config.name == "shutter_open"

    @pytest.mark.parametrize("invalid_name", ["state-name", "class", "add", "remove", "show_all"])
    def test_beamline_state_config_invalid_name(self, invalid_name):
        with pytest.raises(ValueError):
            bl_states.BeamlineStateConfig(name=invalid_name)

    def test_device_state_config_keeps_string_device_and_signal(self):
        config = bl_states.DeviceStateConfig(name="state", device="samx", signal="samx")
        assert config.device == "samx"
        assert config.signal == "samx"


class TestBeamlineStateBase:
    def test_beamline_state_initialization_and_update(self):
        class ConcreteState(bl_states.BeamlineState[bl_states.BeamlineStateConfig]):
            CONFIG_CLASS = bl_states.BeamlineStateConfig

            def evaluate(self, *args, **kwargs):
                return messages.BeamlineStateMessage(
                    name=self.config.name, status="valid", label="ok"
                )

        state = ConcreteState(name="test_state")

        assert state.config.name == "test_state"
        assert state.connector is None
        assert state._last_state is None

        state.update_parameters(title="Test State")
        assert state.config.title == "Test State"


class TestDeviceBeamlineState:
    def test_start_requires_connector(self):
        state = bl_states.ShutterState(name="shutter_open", device="shutter1", signal="shutter1")

        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            state.start()

    def test_start_registers_device_callback(self, connected_connector):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="shutter1",
            signal="shutter1",
            redis_connector=connected_connector,
        )

        with mock.patch.object(connected_connector, "register") as register:
            state.start()

        register.assert_called_once_with(
            MessageEndpoints.device_readback("shutter1"),
            cb=state._update_device_state,
            parent=state,
        )

    def test_stop_unregisters_device_callback(self, connected_connector):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="shutter1",
            signal="shutter1",
            redis_connector=connected_connector,
        )

        with mock.patch.object(connected_connector, "unregister") as unregister:
            state.stop()

        unregister.assert_called_once_with(
            MessageEndpoints.device_readback("shutter1"), cb=state._update_device_state
        )

    def test_update_device_state_publishes_when_state_changes(self, connected_connector):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="shutter1",
            signal="shutter1",
            redis_connector=connected_connector,
        )

        msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "open", "timestamp": 1.0}},
            metadata={"stream": "primary"},
        )
        msg_obj = MessageObject(value=msg, topic="test")

        state._update_device_state(msg_obj, parent=state)

        assert state._last_state is not None
        assert state._last_state.status == "valid"
        out = connected_connector.xread(
            MessageEndpoints.beamline_state("shutter_open"), from_start=True
        )
        assert out is not None
        assert out[0]["data"].status == "valid"


class TestConcreteStates:
    def test_shutter_state_open_and_closed(self, connected_connector):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="shutter1",
            signal="shutter1",
            redis_connector=connected_connector,
        )

        open_msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "OPEN", "timestamp": 1.0}},
            metadata={"stream": "primary"},
        )
        closed_msg = messages.DeviceMessage(
            signals={"shutter1": {"value": "closed", "timestamp": 2.0}},
            metadata={"stream": "primary"},
        )

        assert state.evaluate(open_msg).status == "valid"
        assert state.evaluate(closed_msg).status == "invalid"

    def test_device_within_limits_state(self, connected_connector):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="sample_x",
            min_limit=0.0,
            max_limit=10.0,
            tolerance=0.1,
            redis_connector=connected_connector,
        )

        valid = messages.DeviceMessage(
            signals={"sample_x": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        warning = messages.DeviceMessage(
            signals={"sample_x": {"value": 0.05, "timestamp": 2.0}}, metadata={"stream": "primary"}
        )
        invalid = messages.DeviceMessage(
            signals={"sample_x": {"value": 11.0, "timestamp": 3.0}}, metadata={"stream": "primary"}
        )
        missing = messages.DeviceMessage(
            signals={"sample_x": {"timestamp": 4.0}}, metadata={"stream": "primary"}
        )

        assert state.evaluate(valid).status == "valid"
        assert state.evaluate(warning).status == "warning"
        assert state.evaluate(invalid).status == "invalid"
        assert state.evaluate(missing).status == "invalid"


class TestBeamlineStateManager:
    def test_manager_registers_for_state_updates(self, connected_connector):
        client = mock.MagicMock()
        client.connector = connected_connector

        with mock.patch.object(connected_connector, "register") as register:
            BeamlineStateManager(client)

        register.assert_called_once_with(
            MessageEndpoints.available_beamline_states(),
            cb=mock.ANY,
            parent=mock.ANY,
            from_start=True,
        )

    def test_on_state_update_creates_client_attribute(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "shutter1"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])

        state_manager._on_state_update({"data": update}, parent=state_manager)

        assert "shutter_open" in state_manager._states
        assert isinstance(state_manager._states["shutter_open"], bl_states.DeviceStateConfig)
        assert isinstance(getattr(state_manager, "shutter_open"), BeamlineStateClientBase)

    def test_update_parameters_from_client_updates_state_and_publishes(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="limits",
            title="Limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "limits",
                "title": "Limits",
                "device": "samx",
                "min_limit": 0.0,
                "max_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager.limits.update_parameters(tolerance=0.25)

        assert state_manager._states["limits"].tolerance == 0.25

        out = state_manager._connector.xread(
            MessageEndpoints.available_beamline_states(), from_start=True
        )
        assert out
        assert isinstance(out[-1]["data"], messages.AvailableBeamlineStatesMessage)

    def test_client_get_returns_unknown_without_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "shutter1"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        result = state_manager.shutter_open.get()
        assert result == {"status": "unknown", "label": "No state information available."}

    def test_client_get_returns_latest_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "shutter1"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager._connector.xadd(
            MessageEndpoints.beamline_state("shutter_open"),
            {
                "data": messages.BeamlineStateMessage(
                    name="shutter_open", status="valid", label="ok"
                )
            },
            max_size=1,
        )

        result = state_manager.shutter_open.get()
        assert result == {"status": "valid", "label": "ok"}

    def test_add_and_remove_publish_updates(self, state_manager):
        state = bl_states.DeviceStateConfig(
            name="shutter_open", title="Shutter Open", device="shutter1"
        )

        state_manager.add(state)
        assert "shutter_open" in state_manager._states

        state_manager.remove("shutter_open")
        assert "shutter_open" not in state_manager._states

    def test_client_delete_removes_state(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            title="Shutter Open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "title": "Shutter Open", "device": "shutter1"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager.shutter_open.delete()

        assert "shutter_open" not in state_manager._states

    def test_show_all_prints_table(self, state_manager, capsys):
        state = bl_states.DeviceStateConfig(
            name="shutter_open", title="Shutter Open", device="shutter1"
        )
        state_manager.add(state)

        state_manager.show_all()

        captured = capsys.readouterr()
        assert "shutter_open" in (captured.out + captured.err)
