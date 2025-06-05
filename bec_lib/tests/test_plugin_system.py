import importlib
import os
import subprocess
import sys
import traceback

import copier
import pytest
from pytest import TempPathFactory

from bec_lib import metadata_schema, plugin_helper


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "-v", "install", package])


def uninstall(package):
    subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", package])


PLUGIN_REPO = "https://github.com/bec-project/plugin_copier_template.git"

TEST_SCHEMA_FILE = """
from bec_lib.metadata_schema import BasicScanMetadata

class ExampleSchema(BasicScanMetadata):
   treatment_description: str
   treatment_temperature_k: int

"""

TEST_SCHEMA_REGISTRY = """
from .example_schema import ExampleSchema

METADATA_SCHEMA_REGISTRY = {
    "test_scan_fail_on_type": "FailOnType",
    "example_scan": ExampleSchema,
}

DEFAULT_SCHEMA = None

"""

TEST_SCAN_CLASS = """
from bec_server.scan_server.scans import ScanBase
class ScanForTesting(ScanBase):
    ...
"""


class TestPluginSystem:
    @pytest.fixture(scope="class", autouse=True)
    def setup_env(self, tmp_path_factory: TempPathFactory):
        print("\n\nSetting up plugin for tests: generating files...\n")
        TestPluginSystem._tmp_plugin_dir = tmp_path_factory.mktemp("test_plugin")
        TestPluginSystem._tmp_plugin_name = TestPluginSystem._tmp_plugin_dir.name
        TestPluginSystem._plugin_script = (
            os.path.dirname(os.path.abspath(__file__))
            + "/../util_scripts/create_plugin_structure.py"
        )
        print("Done. Modifying files with test code...\n")
        # run plugin generation script
        try:
            copier.run_copy(
                PLUGIN_REPO,
                str(TestPluginSystem._tmp_plugin_dir),
                defaults=True,
                data={
                    "project_name": TestPluginSystem._tmp_plugin_name,
                    "widget_plugins_input": [{"name": "test_widget", "use_ui": True}],
                },
                unsafe=True,
            )
        except Exception:
            # If there are permission issues on the test runner with making git commits, it's not really important
            print(
                f"Encountered error in setting up test repo: \n {traceback.format_exc()} \n Attempting to continue anyway..."
            )

        # add some test things
        with open(
            TestPluginSystem._tmp_plugin_dir
            / f"{TestPluginSystem._tmp_plugin_name}/scans/__init__.py",
            "w+",
        ) as f:
            f.write(TEST_SCAN_CLASS)

        with open(
            TestPluginSystem._tmp_plugin_dir
            / f"{TestPluginSystem._tmp_plugin_name}/scans/metadata_schema/metadata_schema_registry.py",
            "w",
        ) as f:
            f.write(TEST_SCHEMA_REGISTRY)

        with open(
            TestPluginSystem._tmp_plugin_dir
            / f"{TestPluginSystem._tmp_plugin_name}/scans/metadata_schema/example_schema.py",
            "w",
        ) as f:
            f.write(TEST_SCHEMA_FILE)

        print("\nDone. Installing into environment...\n")

        # install into current environment
        install(TestPluginSystem._tmp_plugin_dir)
        importlib.invalidate_caches()
        plugin_helper._get_available_plugins.cache_clear()
        plugin_helper.get_metadata_schema_registry.cache_clear()

        piplist = subprocess.Popen(("pip", "list"), stdout=subprocess.PIPE)
        output = subprocess.check_output(("grep", "test_plugin"), stdin=piplist.stdout)
        piplist.wait()
        print("$ pip list | grep test_plugin: \n" + output.decode("utf-8"))

        print("Done. Yielding to test class...\n")

        yield

        print("\n\nDone. Uninstalling test plugin:\n")

        uninstall(TestPluginSystem._tmp_plugin_name)
        importlib.invalidate_caches()
        plugin_helper._get_available_plugins.cache_clear()
        metadata_schema._METADATA_SCHEMA_REGISTRY = {}
        del sys.modules["bec_lib.metadata_schema"]
        TestPluginSystem._tmp_plugin_dir = None

    def test_generated_files_in_plugin_deployment(self):
        files = os.listdir(TestPluginSystem._tmp_plugin_dir)
        for file in [
            TestPluginSystem._tmp_plugin_name,
            "pyproject.toml",
            ".git_hooks",
            ".gitignore",
            "LICENSE",
            "tests",
            "bin",
            ".gitlab-ci.yml",
        ]:
            assert file in files
        files = os.listdir(TestPluginSystem._tmp_plugin_dir / TestPluginSystem._tmp_plugin_name)
        for file in ["scans"]:
            assert file in files

    def test_test_created_files_in_plugin_deployment(self):
        files = os.listdir(
            TestPluginSystem._tmp_plugin_dir
            / f"{TestPluginSystem._tmp_plugin_name}/scans/metadata_schema"
        )
        for file in ["example_schema.py", "metadata_schema_registry.py"]:
            assert file in files

    def test_plugin_module_import_from_generated_file(self):
        try:
            package_spec = importlib.util.spec_from_file_location(
                TestPluginSystem._tmp_plugin_name,
                TestPluginSystem._tmp_plugin_dir
                / TestPluginSystem._tmp_plugin_name
                / "__init__.py",
            )
            plugin_module = importlib.util.module_from_spec(package_spec)
            package_spec.loader.exec_module(plugin_module)

            md_reg_mod_name = (
                TestPluginSystem._tmp_plugin_name
                + ".scans.metadata_schema.metadata_schema_registry"
            )
            md_reg_spec = importlib.util.spec_from_file_location(
                md_reg_mod_name,
                TestPluginSystem._tmp_plugin_dir
                / TestPluginSystem._tmp_plugin_name
                / "scans/metadata_schema"
                / "metadata_schema_registry.py",
            )
            md_reg_module = importlib.util.module_from_spec(md_reg_spec)
            md_reg_spec.loader.exec_module(md_reg_module)
            assert md_reg_module.METADATA_SCHEMA_REGISTRY is not None
        finally:
            for mod in [TestPluginSystem._tmp_plugin_name, md_reg_mod_name]:
                if mod in sys.modules:
                    del sys.modules[mod]

    def test_plugin_modules_import_from_file(self):
        importlib.import_module(TestPluginSystem._tmp_plugin_name)
        for submod in [
            "scans",
            "devices",
            "bec_widgets",
            "bec_ipython_client",
            "services",
            "file_writer",
            "deployments",
            "device_configs",
        ]:
            importlib.import_module(TestPluginSystem._tmp_plugin_name + "." + submod)

    def test_plugin_helper_for_scans(self):
        plugin_scans_modules = plugin_helper._get_available_plugins("bec.scans")
        assert len(plugin_scans_modules) > 0
        scan_plugins = plugin_helper.get_scan_plugins()
        assert "ScanForTesting" in scan_plugins.keys()

    def test_plugin_helper_for_metadata_schema(self):
        metadata_schema_plugin_module = plugin_helper._get_available_plugins(
            "bec.scans.metadata_schema"
        )
        assert len(metadata_schema_plugin_module) > 0
        metadata_registry, default_schema = plugin_helper.get_metadata_schema_registry()
        assert set(["test_scan_fail_on_type", "example_scan"]) == set(metadata_registry.keys())
        assert default_schema is None
