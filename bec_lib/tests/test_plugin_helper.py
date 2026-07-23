import sys
from types import ModuleType, SimpleNamespace
from unittest import mock

import pytest

import bec_lib
from bec_lib import plugin_helper


@pytest.mark.parametrize(
    "class_spec, out_name",
    [("bec_lib.messages.BECMessage", "BECMessage"), ("bec_lib.messages.BECStatus", "BECStatus")],
)
def test_get_plugin_class(class_spec, out_name):
    cls = plugin_helper.get_plugin_class(class_spec, [bec_lib])
    assert cls.__name__ == out_name


@pytest.mark.parametrize(
    "class_spec", ["bec_lib.nonexistent_module.NonexistentClass", "bec_lib.NonexistentClass"]
)
def test_get_plugin_class_module_not_found(class_spec):
    with pytest.raises((ModuleNotFoundError, AttributeError)):
        plugin_helper.get_plugin_class(class_spec, [bec_lib])


def test_module_dist_info():
    result = plugin_helper.module_dist_info("bec_lib")
    assert result["dir_info"] == {"editable": True}
    assert result["url"] is not None


def test_reload_plugin_modules_reloads_plugin_tree():
    plugin_module = mock.MagicMock()
    plugin_module.__name__ = "bec_plugin"
    plugin_scans_module = mock.MagicMock()
    plugin_scans_module.__name__ = "bec_plugin.scans"
    plugin_scan_submodule = mock.MagicMock()
    plugin_scan_submodule.__name__ = "bec_plugin.scans.custom_scan"
    importlib_mock = mock.MagicMock()
    importlib_mock.import_module.return_value = plugin_module

    with (
        mock.patch("bec_lib.plugin_helper.plugin_package_name", return_value="bec_plugin"),
        mock.patch.object(plugin_helper, "importlib", importlib_mock),
        mock.patch(
            "bec_lib.plugin_helper._get_available_plugins.cache_clear"
        ) as clear_available_plugins,
        mock.patch("bec_lib.plugin_helper._import_module.cache_clear") as clear_import_module,
        mock.patch.dict(
            sys.modules,
            {
                plugin_module.__name__: plugin_module,
                plugin_scans_module.__name__: plugin_scans_module,
                plugin_scan_submodule.__name__: plugin_scan_submodule,
            },
            clear=False,
        ),
    ):
        plugin_helper.reload_plugin_modules()

    clear_available_plugins.assert_called_once_with()
    clear_import_module.assert_called_once_with()
    importlib_mock.reload.assert_any_call(plugin_module)
    importlib_mock.reload.assert_any_call(plugin_scans_module)
    importlib_mock.reload.assert_any_call(plugin_scan_submodule)


def test_reload_plugin_modules_skips_stale_modules():
    plugin_module = mock.MagicMock()
    plugin_module.__name__ = "bec_plugin"
    stale_module = mock.MagicMock()
    stale_module.__name__ = "bec_plugin.scans.deleted_scan"
    importlib_mock = mock.MagicMock()
    importlib_mock.import_module.return_value = plugin_module
    missing_spec_error = ModuleNotFoundError(
        "spec not found for the module 'bec_plugin.scans.deleted_scan'"
    )
    missing_spec_error.name = stale_module.__name__
    importlib_mock.reload.side_effect = [missing_spec_error, None]

    with (
        mock.patch("bec_lib.plugin_helper.plugin_package_name", return_value="bec_plugin"),
        mock.patch.object(plugin_helper, "importlib", importlib_mock),
        mock.patch("bec_lib.plugin_helper.logger.warning") as logger_warning,
        mock.patch.dict(
            sys.modules,
            {plugin_module.__name__: plugin_module, stale_module.__name__: stale_module},
            clear=False,
        ),
    ):
        plugin_helper.reload_plugin_modules()
        assert stale_module.__name__ not in sys.modules

    logger_warning.assert_called_once()
    importlib_mock.reload.assert_any_call(plugin_module)
    importlib_mock.reload.assert_any_call(stale_module)


def test_get_scan_component_plugins(monkeypatch):
    from bec_server.scan_server.scans.scan_components import ScanComponents

    package = ModuleType("example_plugin.scans.scan_customization")
    package.__path__ = ["fake_path"]

    module = ModuleType("example_plugin.scans.scan_customization.scan_components")
    plugin_components = type("PluginComponents", (ScanComponents,), {"__module__": module.__name__})
    module.PluginComponents = plugin_components

    monkeypatch.setattr(plugin_helper, "plugin_package_name", lambda: "example_plugin")
    monkeypatch.setattr(
        plugin_helper,
        "_import_module",
        lambda name: {package.__name__: package, module.__name__: module}[name],
    )
    monkeypatch.setattr(
        plugin_helper.pkgutil,
        "iter_modules",
        lambda path, prefix: [SimpleNamespace(name=module.__name__)],
    )

    result = plugin_helper.get_scan_component_plugins()

    assert result == [plugin_components]


def test_get_file_writer_storage_copy_plugin(monkeypatch):
    handler = mock.Mock()

    monkeypatch.setattr(
        plugin_helper.importlib.metadata,
        "entry_points",
        lambda group: [SimpleNamespace(name="plugin_storage_copy", load=lambda: handler)],
    )
    plugin_helper.get_file_writer_storage_copy_plugin.cache_clear()

    result = plugin_helper.get_file_writer_storage_copy_plugin()

    assert result is handler


def test_get_file_writer_storage_copy_plugin_returns_none_without_match(monkeypatch):
    monkeypatch.setattr(plugin_helper.importlib.metadata, "entry_points", lambda group: [])
    plugin_helper.get_file_writer_storage_copy_plugin.cache_clear()

    result = plugin_helper.get_file_writer_storage_copy_plugin()

    assert result is None


def test_get_file_writer_storage_copy_plugin_warns_for_multiple_matches(monkeypatch):
    first_handler = mock.Mock()
    second_handler = mock.Mock()

    monkeypatch.setattr(
        plugin_helper.importlib.metadata,
        "entry_points",
        lambda group: [
            SimpleNamespace(name="plugin_storage_copy", load=lambda: first_handler),
            SimpleNamespace(name="plugin_storage_copy", load=lambda: second_handler),
        ],
    )
    plugin_helper.get_file_writer_storage_copy_plugin.cache_clear()

    with mock.patch("bec_lib.plugin_helper.logger.warning") as mock_warning:
        result = plugin_helper.get_file_writer_storage_copy_plugin()

    assert result is first_handler
    mock_warning.assert_called_once()


def test_get_file_writer_storage_copy_plugin_returns_none_on_load_error(monkeypatch):
    def _load():
        raise RuntimeError("boom")

    monkeypatch.setattr(
        plugin_helper.importlib.metadata,
        "entry_points",
        lambda group: [SimpleNamespace(name="plugin_storage_copy", load=_load)],
    )
    plugin_helper.get_file_writer_storage_copy_plugin.cache_clear()

    with mock.patch("bec_lib.plugin_helper.logger.error") as mock_error:
        result = plugin_helper.get_file_writer_storage_copy_plugin()

    assert result is None
    mock_error.assert_called_once()


def test_get_file_writer_storage_copy_plugin_returns_none_for_noncallable(monkeypatch):
    monkeypatch.setattr(
        plugin_helper.importlib.metadata,
        "entry_points",
        lambda group: [SimpleNamespace(name="plugin_storage_copy", load=lambda: "not-callable")],
    )
    plugin_helper.get_file_writer_storage_copy_plugin.cache_clear()

    with mock.patch("bec_lib.plugin_helper.logger.error") as mock_error:
        result = plugin_helper.get_file_writer_storage_copy_plugin()

    assert result is None
    mock_error.assert_called_once()
