import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def subscan_test_plugin(request, tmp_path_factory):
    """Expose the test scan to the scan server's normal plugin loader."""
    if not request.config.getoption("--start-servers"):
        yield
        return
    plugin_path = tmp_path_factory.mktemp("subscan_plugin")
    dist_info = plugin_path / "bec_subscan_test-0.0.dist-info"
    dist_info.mkdir()
    (dist_info / "entry_points.txt").write_text(
        "[bec.scans]\nsubscan_test = test_subscans_e2e\n", encoding="utf-8"
    )
    with pytest.MonkeyPatch.context() as patch:
        paths = [str(plugin_path), str(Path(__file__).parent)]
        if python_path := os.environ.get("PYTHONPATH"):
            paths.append(python_path)
        patch.setenv("PYTHONPATH", os.pathsep.join(paths))
        yield
