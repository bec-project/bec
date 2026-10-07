"""Regression coverage for deferred GUI imports and shell shutdown."""

from __future__ import annotations

import builtins
import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

import pytest

from bec_ipython_client import BECIPythonClient, main
from bec_ipython_client.gui_client import LazyBECGuiClient


@pytest.fixture
def widgets_unavailable(monkeypatch):
    original_import = builtins.__import__
    attempts = []

    def import_without_widgets(name, *args, **kwargs):
        if name.startswith("bec_widgets"):
            attempts.append(name)
            raise ModuleNotFoundError("No module named 'bec_widgets'")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_widgets)
    return attempts


@pytest.fixture
def gui_factory(monkeypatch):
    module = ModuleType("bec_widgets.cli.client_utils")
    factory = mock.Mock()
    module.BECGuiClient = factory
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return factory


@pytest.fixture
def client():
    # Exercise the real GUI property and shutdown without connecting to Redis.
    instance = BECIPythonClient.__new__(BECIPythonClient)
    instance.__dict__.update(
        _client=mock.Mock(),
        _gui=LazyBECGuiClient(),
        started=True,
        start=mock.Mock(),
        _ip=mock.Mock(),
    )
    return instance


@pytest.mark.parametrize("nogui", [True, False])
def test_shutdown_after_startup_namespace_cleanup(nogui, widgets_unavailable, monkeypatch, client):
    monkeypatch.setattr(main, "BECIPythonClient", mock.Mock(return_value=client))
    monkeypatch.setattr(
        "bec_lib.plugin_helper.get_ipython_client_startup_plugins", mock.Mock(return_value={})
    )
    with mock.patch.dict(
        main.main_dict,
        {
            "config": None,
            "wait_for_server": False,
            "args": SimpleNamespace(nogui=nogui, gui_id=None),
            "startup_file": None,
        },
        clear=True,
    ):
        namespace = runpy.run_path(str(Path(main.__file__).with_name("bec_startup.py")))
        assert len(widgets_unavailable) == (0 if nogui else 1)
        assert namespace["gui"] is client._gui
        namespace.clear()
        client.shutdown(per_thread_timeout_s=1)
        client._client.shutdown.assert_called_once_with(1)
        assert len(widgets_unavailable) == (0 if nogui else 1)


@pytest.mark.parametrize("gui_id", [None, "existing-gui"])
def test_gui_can_be_started_later_and_is_created_once(gui_factory, gui_id):
    gui = LazyBECGuiClient(gui_id=gui_id)
    gui_factory.assert_not_called()
    assert repr(gui) == "LazyBECGuiClient(uninitialized)"
    gui.close()
    gui_factory.assert_not_called()

    gui.show()
    gui.show()
    gui.set_rpc_timeout(5)
    gui.some_setting = "value"
    gui.close()

    gui_factory.assert_called_once_with()
    real_gui = gui_factory.return_value
    assert real_gui.show.call_count == 2
    real_gui.set_rpc_timeout.assert_called_once_with(5)
    assert real_gui.some_setting == "value"
    real_gui.close.assert_called_once_with()
    if gui_id:
        real_gui.connect_to_gui_server.assert_called_once_with(gui_id)
    else:
        real_gui.connect_to_gui_server.assert_not_called()


def test_missing_widgets_on_explicit_use_does_not_break_shutdown(widgets_unavailable):
    gui = LazyBECGuiClient()
    with pytest.raises(ImportError, match="Install bec-widgets"):
        gui.show()
    gui.close()
    assert len(widgets_unavailable) == 1


def test_gui_completion_does_not_import_widgets_before_initialization(widgets_unavailable):
    gui = LazyBECGuiClient()

    assert "close" in dir(gui)
    assert widgets_unavailable == []


def test_gui_completion_exposes_initialized_client_methods(gui_factory):
    real_client = gui_factory.return_value
    real_client.show = mock.Mock()
    real_client.set_rpc_timeout = mock.Mock()
    gui = LazyBECGuiClient()

    gui.show()

    assert dir(gui) == dir(real_client)
    assert "show" in dir(gui)
    assert "set_rpc_timeout" in dir(gui)
    gui_factory.assert_called_once_with()


def test_failed_gui_connection_can_be_retried(gui_factory):
    failed_client = mock.Mock()
    failed_client.connect_to_gui_server.side_effect = RuntimeError("Connection failed")
    connected_client = mock.Mock()
    gui_factory.side_effect = [failed_client, connected_client]
    gui = LazyBECGuiClient(gui_id="existing-gui")

    with pytest.raises(RuntimeError, match="Connection failed"):
        gui.get_client()

    assert gui.get_client() is connected_client
    assert gui.get_client() is connected_client
    assert gui_factory.call_count == 2
    failed_client.connect_to_gui_server.assert_called_once_with("existing-gui")
    connected_client.connect_to_gui_server.assert_called_once_with("existing-gui")


def test_gui_property_reports_missing_widgets(client, widgets_unavailable):
    with pytest.raises(ImportError, match="Install bec-widgets") as exc:
        client.gui
    assert isinstance(exc.value.__cause__, ModuleNotFoundError)
    client.shutdown()
    client._client.shutdown.assert_called_once_with(None)
    assert len(widgets_unavailable) == 1


def test_gui_property_returns_real_client_and_caches_it(client, gui_factory):
    gui_factory.assert_not_called()
    assert client.gui is gui_factory.return_value
    assert client.gui is gui_factory.return_value
    client.shutdown()
    gui_factory.assert_called_once_with()
    gui_factory.return_value.close.assert_called_once_with()


def test_gui_cleanup_failure_does_not_prevent_core_shutdown(client, gui_factory):
    real_gui = client.gui
    real_gui.close.side_effect = RuntimeError("GUI cleanup failed")

    with mock.patch.object(main, "logger") as logger:
        client.shutdown(per_thread_timeout_s=1)

    real_gui.close.assert_called_once_with()
    client._client.shutdown.assert_called_once_with(1)
    logger.error.assert_called_once_with("Error closing GUI client: GUI cleanup failed")
