from __future__ import annotations

import inspect
import threading
import time
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
from bec_lib.tests.fixtures import dm_with_devices


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

        config = DemoConfig()
        signature = build_signature_from_model(config)

        assert list(signature.parameters) == ["foo", "bar"]
        assert signature.parameters["foo"].kind == inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters["foo"].annotation is int
        assert signature.parameters["bar"].default == "abc"


class TestConfigModels:
    def test_beamline_state_config_valid_name(self):
        config = bl_states.BeamlineStateConfig(name="shutter_open")
        assert config.name == "shutter_open"

    @pytest.mark.parametrize("invalid_name", ["state-name", "class", "add", "remove", "show_all"])
    def test_beamline_state_config_invalid_name(self, invalid_name):
        with pytest.raises(ValueError):
            bl_states.BeamlineStateConfig(name=invalid_name)

    def test_device_state_config_keeps_string_device_and_signal(self):
        config = bl_states.DeviceStateConfig(name="state", device="samx", signal="samx")
        assert config.device == "samx"
        assert config.signal == "samx"

    def test_device_state_config_accepts_matching_signal_device(self, dm_with_devices):
        config = bl_states.DeviceStateConfig(
            name="state", device=dm_with_devices.devices.bpm4i, signal=dm_with_devices.devices.bpm4i
        )

        assert config.device == "bpm4i"
        assert config.signal == "bpm4i"

    def test_device_state_config_rejects_mismatched_signal_for_signal_device(self, dm_with_devices):
        with pytest.raises(ValueError, match="does not match signal device"):
            bl_states.DeviceStateConfig(
                name="state", device=dm_with_devices.devices.bpm4i, signal="bpm5i"
            )


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


class TestDeviceBeamlineState:
    def test_start_requires_connector(self, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open", device="samy", signal="samy", device_manager=dm_with_devices
        )

        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            state.start()

    def test_start_registers_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with mock.patch.object(connected_connector, "register") as register:
            state.start()

        register.assert_called_once_with(
            MessageEndpoints.device_readback("samx"), cb=state._update_device_state
        )

    def test_stop_unregisters_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with mock.patch.object(connected_connector, "unregister") as unregister:
            state.start()
            state.stop()

        unregister.assert_called_once_with(
            MessageEndpoints.device_readback("samx"), cb=state._update_device_state
        )

    def test_update_device_state_publishes_when_state_changes(
        self, connected_connector, dm_with_devices
    ):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        msg = messages.DeviceMessage(
            signals={"samx": {"value": "open", "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        msg_obj = MessageObject(value=msg, topic="test")

        state._update_device_state(MessageObject(value=msg, topic="test"))

        assert state._last_state is not None
        assert state._last_state.status == "valid"
        out = connected_connector.xread(
            MessageEndpoints.beamline_state("shutter_open"), from_start=True
        )
        assert out is not None
        assert out[0]["data"].status == "valid"


class TestConcreteStates:

    @pytest.fixture(scope="function")
    def aggregated_state_config(self):
        return bl_states.AggregatedStateConfig(
            name="alignment",
            states={
                "alignment": {
                    "devices": {
                        "samx": {
                            "readback": {"value": 0, "abs_tol": 0.1},
                            "velocity": {"value": 5, "abs_tol": 0.1},
                            "low_limit": {"value": -20, "abs_tol": 0.1},
                            "high_limit": {"value": 20, "abs_tol": 0.1},
                        },
                        "samy": {"readback": {"value": 0, "abs_tol": 0.1}},
                    }
                },
                "measurement": {
                    "devices": {
                        "samx": {
                            "readback": {"value": 19, "abs_tol": 0.1},
                            "velocity": {"value": 5, "abs_tol": 0.1},
                            "low_limit_travel": {"value": -20, "abs_tol": 0.1},
                            "high_limit_travel": {"value": 20, "abs_tol": 0.1},
                        },
                        "samy": {"readback": {"value": 2, "abs_tol": 0.1}},
                    }
                },
                "test": {"devices": {"samy": {"readback": {"value": 0, "abs_tol": 0.1}}}},
            },
        )

    def test_shutter_state_open_and_closed(self, connected_connector, dm_with_devices):
        state = bl_states.ShutterState(
            name="shutter_open",
            device="samx",
            signal="samx",
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        open_msg = messages.DeviceMessage(
            signals={"samx": {"value": "OPEN", "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        closed_msg = messages.DeviceMessage(
            signals={"samx": {"value": "closed", "timestamp": 2.0}}, metadata={"stream": "primary"}
        )

        assert state.evaluate(open_msg).status == "valid"
        assert state.evaluate(closed_msg).status == "invalid"

    def test_device_within_limits_state(self, connected_connector, dm_with_devices):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            low_limit=0.0,
            high_limit=10.0,
            tolerance=0.1,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        valid = messages.DeviceMessage(
            signals={"samx": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        warning = messages.DeviceMessage(
            signals={"samx": {"value": 0.05, "timestamp": 2.0}}, metadata={"stream": "primary"}
        )
        invalid = messages.DeviceMessage(
            signals={"samx": {"value": 11.0, "timestamp": 3.0}}, metadata={"stream": "primary"}
        )
        missing = messages.DeviceMessage(
            signals={"samx": {"timestamp": 4.0}}, metadata={"stream": "primary"}
        )

        assert state.evaluate(valid).status == "valid"
        assert state.evaluate(warning).status == "warning"
        assert state.evaluate(invalid).status == "invalid"
        assert state.evaluate(missing).status == "invalid"

    def test_device_within_limits_state_accepts_signal_backed_device(
        self, connected_connector, dm_with_devices
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="bpm4i_within_limits",
            device="bpm4i",
            signal="bpm4i",
            low_limit=-1.0,
            high_limit=10.0,
            tolerance=0.1,
        )

        msg = messages.DeviceMessage(
            signals={"bpm4i": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )

        assert state.signal_name == "bpm4i"
        assert state.evaluate(msg).status == "valid"

    def test_aggregated_state_init(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):

        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()
        # We should now have subscriptions on samx limits, readback and read_configuration, and samy readback
        info = [
            MessageEndpoints.device_readback("samx"),
            MessageEndpoints.device_read_configuration("samx"),
            MessageEndpoints.device_limits("samx"),
            MessageEndpoints.device_readback("samy"),
        ]
        for endpoint in info:
            assert endpoint.endpoint in state.connector._topics_cb

    def test_aggregated_state_evaluation(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        with (
            mock.patch.object(state, "evaluate", return_value=None) as evaluate,
            mock.patch.object(state, "_emit_state") as emit_state,
        ):

            msg_with_2_states = messages.DeviceMessage(
                signals={"samx": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
            )
            msg_obj = MessageObject(
                value=msg_with_2_states, topic=MessageEndpoints.device_readback("samx").endpoint
            )
            state._update_aggregated_state(msg_obj, device="samx", source="readback")
            evaluate.assert_called_once_with(affected_labels=set(["alignment", "measurement"]))
            emit_state.assert_not_called()  # As evaluate is mocked to return None, _emit_state should not be called

    def test_aggregated_state_evaluate(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state._build_rules()
        state._cache_message(
            "samx",
            "readback",
            messages.DeviceMessage(
                signals={"samx": {"value": 0, "timestamp": 1.0}}, metadata={"stream": "primary"}
            ),
        )
        state._cache_message(
            "samx",
            "configuration",
            messages.DeviceMessage(
                signals={"samx_velocity": {"value": 5, "timestamp": 1.0}},
                metadata={"stream": "baseline"},
            ),
        )
        state._cache_message(
            "samx",
            "limits",
            messages.DeviceMessage(
                signals={
                    "low": {"value": -20, "timestamp": 1.0},
                    "high": {"value": 20, "timestamp": 1.0},
                },
                metadata={"stream": "baseline"},
            ),
        )
        state._cache_message(
            "samy",
            "readback",
            messages.DeviceMessage(
                signals={"samy": {"value": 0, "timestamp": 1.0}}, metadata={"stream": "primary"}
            ),
        )

        msg = state.evaluate(affected_labels={"alignment"})

        assert msg.status == "valid"
        assert msg.label == "alignment"
        assert state._current_labels == ["alignment"]

        state._cache_message(
            "samx",
            "readback",
            messages.DeviceMessage(
                signals={"samx": {"value": 3, "timestamp": 2.0}}, metadata={"stream": "primary"}
            ),
        )

        msg = state.evaluate(affected_labels={"alignment"})

        assert msg.status == "invalid"
        assert msg.label == "No matching state"
        assert state._current_labels == []

    def test_aggregated_state_exception_handling(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()
        msg = messages.DeviceMessage(
            signals={"samx": {"value": 0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        msg_obj = MessageObject(value=msg, topic=MessageEndpoints.device_readback("samx").endpoint)

        with (
            mock.patch.object(
                state, "evaluate", side_effect=RuntimeError("broken state")
            ) as evaluate,
            mock.patch.object(connected_connector, "raise_alarm") as raise_alarm,
        ):
            state._update_aggregated_state(msg_obj, device="samx", source="readback")

        evaluate.assert_called_once_with(affected_labels={"alignment", "measurement"})
        raise_alarm.assert_called_once()
        out = connected_connector.xread(
            MessageEndpoints.beamline_state("alignment"), from_start=True
        )
        assert out[-1]["data"].status == "unknown"
        assert out[-1]["data"].label == "broken state"
        assert state.raised_warning is True

    def test_aggregated_state_transitions_between_labels(
        self, connected_connector, dm_with_devices, aggregated_state_config
    ):
        state = bl_states.AggregatedState(
            name=aggregated_state_config.name,
            config=aggregated_state_config,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        def update(device, source, signals):
            msg = messages.DeviceMessage(signals=signals, metadata={"stream": "primary"})
            msg_obj = MessageObject(value=msg, topic=state._endpoint(device, source).endpoint)
            state._update_aggregated_state(msg_obj, device=device, source=source)
            out = connected_connector.xread(
                MessageEndpoints.beamline_state("alignment"), from_start=True
            )
            return out[-1]["data"]

        msg = update("samx", "configuration", {"samx_velocity": {"value": 5, "timestamp": 1.0}})
        assert msg.status == "invalid"

        update(
            "samx",
            "limits",
            {"low": {"value": -20, "timestamp": 1.0}, "high": {"value": 20, "timestamp": 1.0}},
        )
        update("samx", "readback", {"samx": {"value": 0, "timestamp": 1.0}})
        msg = update("samy", "readback", {"samy": {"value": 0, "timestamp": 1.0}})
        assert msg.status == "valid"
        assert set(msg.label.split("|")) == {"alignment", "test"}

        msg = update("samx", "readback", {"samx": {"value": 19, "timestamp": 2.0}})
        assert msg.status == "valid"
        assert msg.label == "test"

        msg = update("samy", "readback", {"samy": {"value": 2, "timestamp": 2.0}})
        assert msg.status == "valid"
        assert msg.label == "measurement"


class TestBeamlineStateManager:
    def test_manager_registers_for_state_updates(self, connected_connector):
        client = mock.MagicMock()
        client.connector = connected_connector

        with mock.patch.object(connected_connector, "register") as register:
            BeamlineStateManager(client)

        register.assert_called_once_with(MessageEndpoints.available_beamline_states(), cb=mock.ANY)

    def test_manager_is_ready_when_no_state_update_exists(self, connected_connector):
        client = mock.MagicMock()
        client.connector = connected_connector

        manager = BeamlineStateManager(client)

        assert manager.ready is True
        assert manager._states == {}

    def test_manager_loads_existing_state_update_on_init(self, connected_connector):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "device": "samy"},
        )
        connected_connector.xadd(
            MessageEndpoints.available_beamline_states(),
            {"data": messages.AvailableBeamlineStatesMessage(states=[config])},
            max_size=1,
        )
        client = mock.MagicMock()
        client.connector = connected_connector

        manager = BeamlineStateManager(client)

        assert manager.ready is True
        assert "shutter_open" in manager._states
        assert isinstance(getattr(manager, "shutter_open"), BeamlineStateClientBase)

    def test_manager_rejects_abstract_state_type_on_init(self, connected_connector):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            state_type="DeviceBeamlineState",
            parameters={"name": "shutter_open", "device": "samy"},
        )
        connected_connector.xadd(
            MessageEndpoints.available_beamline_states(),
            {"data": messages.AvailableBeamlineStatesMessage(states=[config])},
            max_size=1,
        )
        client = mock.MagicMock()
        client.connector = connected_connector

        with pytest.raises(ValueError, match="not a concrete beamline state"):
            BeamlineStateManager(client)

    def test_on_state_update_creates_client_attribute(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "device": "samy"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])

        state_manager._on_state_update({"data": update}, parent=state_manager)

        assert "shutter_open" in state_manager._states
        assert isinstance(state_manager._states["shutter_open"], bl_states.ShutterStateConfig)
        assert isinstance(getattr(state_manager, "shutter_open"), BeamlineStateClientBase)

    def test_update_parameters_from_client_updates_state_and_publishes(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="limits",
            state_type="DeviceWithinLimitsState",
            parameters={"name": "limits", "device": "samx", "low_limit": 0.0, "high_limit": 10.0},
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

    def test_external_parameter_update_refreshes_existing_client_state(self, state_manager):
        initial = messages.BeamlineStateConfig(
            name="limits",
            state_type="DeviceWithinLimitsState",
            parameters={"name": "limits", "device": "samx", "low_limit": 0.0, "high_limit": 10.0},
        )
        state_manager._on_state_update(
            {"data": messages.AvailableBeamlineStatesMessage(states=[initial])},
            parent=state_manager,
        )

        updated = messages.BeamlineStateConfig(
            name="limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "limits",
                "device": "samx",
                "low_limit": 1.0,
                "high_limit": 9.0,
                "tolerance": 0.25,
            },
        )
        state_manager._on_state_update(
            {"data": messages.AvailableBeamlineStatesMessage(states=[updated])},
            parent=state_manager,
        )

        assert state_manager._states["limits"].low_limit == 1.0
        assert state_manager._states["limits"].high_limit == 9.0
        assert state_manager._states["limits"].tolerance == 0.25
        assert state_manager.limits._state.model_dump(exclude_none=True) == updated.parameters

    def test_client_get_returns_unknown_without_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "device": "samy"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        result = state_manager.shutter_open.get()
        assert result == {"status": "unknown", "label": "No state information available."}

    def test_client_get_returns_latest_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "device": "samy"},
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

    def test_add_waits_for_initial_state_message(self, state_manager):
        state = bl_states.ShutterStateConfig(name="shutter_open", device="samy")

        def publish_initial_state():
            time.sleep(0.05)
            state_manager._connector.xadd(
                MessageEndpoints.beamline_state("shutter_open"),
                {
                    "data": messages.BeamlineStateMessage(
                        name="shutter_open", status="valid", label="ok"
                    )
                },
                max_size=1,
            )

        publisher = threading.Thread(target=publish_initial_state)
        publisher.start()
        try:
            state_manager.add(state)
        finally:
            publisher.join()

        assert state_manager.shutter_open.get() == {"status": "valid", "label": "ok"}

    def test_add_rejects_abstract_device_state_config(self, state_manager):
        state = bl_states.DeviceStateConfig(name="shutter_open", device="samy")

        with pytest.raises(ValueError, match="not a concrete beamline state"):
            state_manager.add(state)

    def test_add_and_delete_publish_updates(self, state_manager):
        state = bl_states.ShutterStateConfig(name="shutter_open", device="samy")

        with mock.patch.object(state_manager, "_wait_for_initial_state"):
            state_manager.add(state)
        assert "shutter_open" in state_manager._states

        state_manager.delete("shutter_open")
        assert "shutter_open" not in state_manager._states

    def test_client_remove_state(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="shutter_open",
            state_type="ShutterState",
            parameters={"name": "shutter_open", "device": "samy"},
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager.shutter_open.remove()

        assert "shutter_open" not in state_manager._states

    def test_show_all_prints_table(self, state_manager, capsys):
        state = bl_states.ShutterStateConfig(name="shutter_open", device="samy")
        with mock.patch.object(state_manager, "_wait_for_initial_state"):
            state_manager.add(state)

        state_manager.show_all()

        captured = capsys.readouterr()
        assert "shutter_open" in (captured.out + captured.err)
