"""This module tests the bec_lib.client module."""

from types import SimpleNamespace

import pytest

import bec_lib.client as client_module
from bec_lib.client import BECClient, LazyDAPPlugins, SystemConfig
from bec_lib.service_config import ServiceConfig
from bec_lib.tests.fixtures import bec_client_mock


def test_system_config():
    """Test the SystemConfig class."""
    config = SystemConfig(file_suffix="suff", file_directory="dir")
    assert config.file_suffix == "suff"
    assert config.file_directory == "dir"
    config = SystemConfig()
    assert config.file_suffix is None
    assert config.file_directory is None
    config.file_suffix = "suff_-"
    config.file_directory = "/dir_-/blabla"
    assert config.file_suffix == "suff_-"
    assert config.file_directory == "dir_-/blabla"
    with pytest.raises(ValueError):
        config = SystemConfig(file_suffix="@")
        config = SystemConfig(file_directory="ä")


def test_show_all_commands(bec_client_mock, capsys):
    """Test the show_all_commands method."""
    client = bec_client_mock
    client.show_all_commands()
    captured = capsys.readouterr()
    assert "User macros" in captured.out
    assert "Scans" in captured.out


def test_lazy_dap_plugins_uses_captured_factory():
    """Lazy DAP initialization must preserve patched/injected DAP factories."""

    class DummyDAPPlugins:
        def __init__(self, parent):
            self.parent = parent
            self.GaussianModel = object()
            self.refresh_called = False

        def refresh(self):
            self.refresh_called = True

    parent = object()
    dap = LazyDAPPlugins(parent, DummyDAPPlugins)

    assert "uninitialized" in repr(dap)
    assert dap.GaussianModel is dap._dap_plugins.GaussianModel
    assert dap._dap_plugins.parent is parent

    dap.refresh()
    assert dap._dap_plugins.refresh_called is True


def test_lazy_dap_plugins_dir_does_not_materialize():
    """Tab completion/introspection must not trigger DAP plugin initialization."""

    class FailingDAPPlugins:
        def __init__(self, parent):
            raise AssertionError("DAPPlugins should not be initialized by dir()")

    dap = LazyDAPPlugins(object(), FailingDAPPlugins)

    assert "_dap_plugins" in dir(dap)
    assert dap._dap_plugins is None


def test_start_services_keeps_dap_lazy_until_first_access(monkeypatch):
    """Starting client services should not import DAP plugins eagerly."""

    constructed = []

    class DummyDAPPlugins:
        def __init__(self, parent):
            constructed.append(parent)
            self.GaussianModel = object()
            self.refresh_called = False

        def refresh(self):
            self.refresh_called = True

    client = BECClient(
        forced=True, config=ServiceConfig(config={"redis": {"host": "localhost", "port": 1}})
    )
    client.connector = object()
    client.device_manager = object()
    client.macros = SimpleNamespace(load_all_user_macros=lambda: None)

    monkeypatch.setattr(client, "_load_scans", lambda: None)
    monkeypatch.setattr(client, "_start_device_manager", lambda: None)
    monkeypatch.setattr(client, "_start_scan_queue", lambda: None)
    monkeypatch.setattr(client, "_start_alarm_handler", lambda: None)
    monkeypatch.setattr(client, "_update_username", lambda: None)
    monkeypatch.setattr(client_module, "ConfigHelperUser", lambda device_manager: object())
    monkeypatch.setattr(client_module, "ScanHistory", lambda client: object())
    monkeypatch.setattr(client_module, "DeviceMonitorPlugin", lambda connector: object())
    monkeypatch.setattr(client_module, "BeamlineStateManager", lambda client: object())
    monkeypatch.setattr(client_module, "DAPPlugins", DummyDAPPlugins)

    client._start_services()

    assert isinstance(client.dap, LazyDAPPlugins)
    assert constructed == []

    dir(client.dap)
    assert constructed == []

    assert client.dap.GaussianModel is client.dap._dap_plugins.GaussianModel
    assert constructed == [client]
