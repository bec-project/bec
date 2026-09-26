from __future__ import annotations

import inspect
import threading
import time
from unittest import mock

import numpy as np
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
        config = bl_states.BeamlineStateConfig(name="sample_x_limits")
        assert config.name == "sample_x_limits"

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
        state = bl_states.DeviceWithinLimitsState(
            name="sample_y_limits",
            device="samy",
            signal="samy",
            low_limit=0.0,
            high_limit=10.0,
            device_manager=dm_with_devices,
        )

        with pytest.raises(RuntimeError, match="Redis connector is not set"):
            state.start()

    def test_start_registers_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        with mock.patch.object(connected_connector, "register") as register:
            state.start()

        register.assert_called_once_with(
            MessageEndpoints.device_readback("samx"), cb=state._update_device_state
        )

    def test_stop_unregisters_device_callback(self, connected_connector, dm_with_devices):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
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
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )

        msg = messages.DeviceMessage(
            signals={"samx": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )

        state._update_device_state(MessageObject(value=msg, topic="test"))

        assert state._last_state is not None
        assert state._last_state.status == "valid"
        out = connected_connector.xread(
            MessageEndpoints.beamline_state("sample_x_limits"), from_start=True
        )
        assert out is not None
        assert out[0]["data"].status == "valid"


class TestConcreteStates:
    @pytest.mark.parametrize(
        "low_limit, high_limit", [(1.0, 10.0), (1.0, None), (None, 10.0), (None, None)]
    )
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(float("nan"), id="nan"),
            pytest.param(float("inf"), id="positive-infinity"),
            pytest.param(float("-inf"), id="negative-infinity"),
            pytest.param(np.float32("nan"), id="numpy-nan"),
            pytest.param(np.float64("inf"), id="numpy-infinity"),
            pytest.param(None, id="none"),
            pytest.param("5.0", id="numeric-string"),
            pytest.param("unavailable", id="nonnumeric-string"),
            pytest.param(5 + 0j, id="complex"),
            pytest.param(np.complex64(5), id="numpy-complex"),
            pytest.param({}, id="dict"),
            pytest.param([5.0], id="list"),
            pytest.param(np.array(5.0), id="zero-dimensional-array"),
            pytest.param(np.array([5.0]), id="single-value-array"),
            pytest.param(np.array([5.0, 6.0]), id="array"),
        ],
    )
    def test_device_within_limits_rejects_invalid_readbacks(
        self, dm_with_devices, low_limit, high_limit, value
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            low_limit=low_limit,
            high_limit=high_limit,
            device_manager=dm_with_devices,
        )
        state.update_device_signal_info()
        msg = messages.DeviceMessage(signals={"samx": {"value": value}})

        result = state.evaluate(msg)

        assert result.status == "invalid"
        assert result.name == "sample_x_limits"
        assert "samx" in result.label

    @pytest.mark.parametrize("signals", [{}, {"samx": {}}, {"samx": {"value": None}}])
    def test_device_within_limits_rejects_missing_readbacks(self, dm_with_devices, signals):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits", device="samx", device_manager=dm_with_devices
        )
        state.update_device_signal_info()

        result = state.evaluate(messages.DeviceMessage(signals=signals))

        assert result.status == "invalid"
        assert "not found" in result.label

    @pytest.mark.parametrize(
        "value, expected_status",
        [
            (-0.01, "invalid"),
            (0.0, "warning"),
            (0.05, "warning"),
            (0.1, "valid"),
            (5, "valid"),
            (np.int64(5), "valid"),
            (np.float32(5.0), "valid"),
            (np.float64(5.0), "valid"),
            (True, "valid"),
            (False, "warning"),
            (np.bool_(True), "valid"),
            (np.bool_(False), "warning"),
            (9.9, "valid"),
            (9.95, "warning"),
            (10.0, "warning"),
            (10.01, "invalid"),
        ],
    )
    def test_device_within_limits_preserves_finite_readbacks(
        self, dm_with_devices, value, expected_status
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            low_limit=0.0,
            high_limit=10.0,
            tolerance=0.1,
            device_manager=dm_with_devices,
        )
        state.update_device_signal_info()

        result = state.evaluate(messages.DeviceMessage(signals={"samx": {"value": value}}))

        assert result.status == expected_status

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), "unavailable"])
    def test_invalid_readback_replaces_valid_state_and_recovers(
        self, connected_connector, dm_with_devices, value
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        for readback, expected_status in [(5.0, "valid"), (value, "invalid"), (5.0, "valid")]:
            msg = messages.DeviceMessage(signals={"samx": {"value": readback}})
            state._update_device_state(
                MessageObject(topic=MessageEndpoints.device_readback("samx").endpoint, value=msg)
            )

            out = connected_connector.xread(
                MessageEndpoints.beamline_state("sample_x_limits"), from_start=True
            )
            assert out is not None
            assert out[0]["data"].status == expected_status

    def test_device_within_limits_state_valid_and_invalid(
        self, connected_connector, dm_with_devices
    ):
        state = bl_states.DeviceWithinLimitsState(
            name="sample_x_limits",
            device="samx",
            signal="samx",
            low_limit=0.0,
            high_limit=10.0,
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        valid_msg = messages.DeviceMessage(
            signals={"samx": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )
        invalid_msg = messages.DeviceMessage(
            signals={"samx": {"value": 11.0, "timestamp": 2.0}}, metadata={"stream": "primary"}
        )

        assert state.evaluate(valid_msg).status == "valid"
        assert state.evaluate(invalid_msg).status == "invalid"

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
            redis_connector=connected_connector,
            device_manager=dm_with_devices,
        )
        state.start()

        msg = messages.DeviceMessage(
            signals={"bpm4i": {"value": 5.0, "timestamp": 1.0}}, metadata={"stream": "primary"}
        )

        assert state.signal_name == "bpm4i"
        assert state.evaluate(msg).status == "valid"


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
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
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
        assert "sample_y_limits" in manager._states
        assert isinstance(getattr(manager, "sample_y_limits"), BeamlineStateClientBase)

    def test_manager_rejects_abstract_state_type_on_init(self, connected_connector):
        config = messages.BeamlineStateConfig(
            name="generic_device_state",
            state_type="DeviceBeamlineState",
            parameters={"name": "generic_device_state", "device": "samy"},
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
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])

        state_manager._on_state_update({"data": update}, parent=state_manager)

        assert "sample_y_limits" in state_manager._states
        assert isinstance(
            state_manager._states["sample_y_limits"], bl_states.DeviceWithinLimitsStateConfig
        )
        assert isinstance(getattr(state_manager, "sample_y_limits"), BeamlineStateClientBase)

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
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        result = state_manager.sample_y_limits.get()
        assert result == {"status": "unknown", "label": "No state information available."}

    def test_client_get_returns_latest_status_message(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager._connector.xadd(
            MessageEndpoints.beamline_state("sample_y_limits"),
            {
                "data": messages.BeamlineStateMessage(
                    name="sample_y_limits", status="valid", label="ok"
                )
            },
            max_size=1,
        )

        result = state_manager.sample_y_limits.get()
        assert result == {"status": "valid", "label": "ok"}

    def test_add_waits_for_initial_state_message(self, state_manager):
        state = bl_states.DeviceWithinLimitsStateConfig(
            name="sample_y_limits", device="samy", low_limit=0.0, high_limit=10.0
        )

        def publish_initial_state():
            time.sleep(0.05)
            state_manager._connector.xadd(
                MessageEndpoints.beamline_state("sample_y_limits"),
                {
                    "data": messages.BeamlineStateMessage(
                        name="sample_y_limits", status="valid", label="ok"
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

        assert state_manager.sample_y_limits.get() == {"status": "valid", "label": "ok"}

    def test_add_rejects_abstract_device_state_config(self, state_manager):
        state = bl_states.DeviceStateConfig(name="generic_device_state", device="samy")

        with pytest.raises(ValueError, match="not a concrete beamline state"):
            state_manager.add(state)

    def test_add_and_delete_publish_updates(self, state_manager):
        state = bl_states.DeviceWithinLimitsStateConfig(
            name="sample_y_limits", device="samy", low_limit=0.0, high_limit=10.0
        )

        with mock.patch.object(state_manager, "_wait_for_initial_state"):
            state_manager.add(state)
        assert "sample_y_limits" in state_manager._states

        state_manager.delete("sample_y_limits")
        assert "sample_y_limits" not in state_manager._states

    def test_client_remove_state(self, state_manager):
        config = messages.BeamlineStateConfig(
            name="sample_y_limits",
            state_type="DeviceWithinLimitsState",
            parameters={
                "name": "sample_y_limits",
                "device": "samy",
                "low_limit": 0.0,
                "high_limit": 10.0,
            },
        )
        update = messages.AvailableBeamlineStatesMessage(states=[config])
        state_manager._on_state_update({"data": update}, parent=state_manager)

        state_manager.sample_y_limits.remove()

        assert "sample_y_limits" not in state_manager._states

    def test_show_all_prints_table(self, state_manager, capsys):
        state = bl_states.DeviceWithinLimitsStateConfig(
            name="sample_y_limits", device="samy", low_limit=0.0, high_limit=10.0
        )
        with mock.patch.object(state_manager, "_wait_for_initial_state"):
            state_manager.add(state)

        state_manager.show_all()

        captured = capsys.readouterr()
        assert "sample_y_limits" in (captured.out + captured.err)
