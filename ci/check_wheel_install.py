"""Fail wheel-based CI tests if a BEC package is imported from the source checkout."""

from __future__ import annotations

import importlib
import json
import sysconfig
from importlib.metadata import distribution
from pathlib import Path


def pytest_sessionstart(session) -> None:
    """Verify the packages used by the actual pytest process come from installed wheels."""
    site_packages = Path(sysconfig.get_path("purelib")).resolve()
    for name in (
        "bec_lib",
        "bec_server",
        "bec_ipython_client",
        "pytest_bec_e2e",
        "ophyd_devices",
        "bec_testing_plugin",
    ):
        package = importlib.import_module(name)
        origin = Path(package.__file__).resolve()
        assert origin.is_relative_to(site_packages), f"{name} imported from {origin}"
        direct_url = distribution(name).read_text("direct_url.json")
        assert direct_url is not None, f"{name} was not installed from a local wheel"
        metadata = json.loads(direct_url)
        assert metadata["url"].endswith(".whl"), f"{name} is not a wheel: {metadata}"
        assert not metadata.get("dir_info", {}).get("editable"), f"{name} is editable"
        print(f"Verified wheel import: {name} from {origin}")
