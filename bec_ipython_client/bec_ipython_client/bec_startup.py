import os
import subprocess
import sys

import numpy as np  # not needed but always nice to have

from bec_ipython_client.main import BECIPythonClient as _BECIPythonClient
from bec_ipython_client.main import main_dict as _main_dict
from bec_lib import plugin_helper
from bec_lib.logger import bec_logger as _bec_logger
from bec_lib.redis_connector import RedisConnector as _RedisConnector

try:
    from bec_widgets.cli.auto_updates import AutoUpdates
    from bec_widgets.cli.client import BECDockArea as _BECDockArea
except ImportError:
    _BECDockArea = None

logger = _bec_logger.logger

bec = _BECIPythonClient(
    _main_dict["config"], _RedisConnector, wait_for_server=_main_dict["wait_for_server"]
)
_main_dict["bec"] = bec

try:
    bec.start()
except Exception:
    sys.excepthook(*sys.exc_info())
else:
    if bec.started and not _main_dict["args"].nogui and _BECDockArea is not None:
        gui = bec.gui = _BECDockArea()
        gui.show()
        if gui.auto_updates is None:
            AutoUpdates.create_default_dock = True
            AutoUpdates.enabled = True
            gui.auto_updates = AutoUpdates(gui=gui)
        if gui.auto_updates.create_default_dock:
            gui.auto_updates.start_default_dock()
        fig = gui.auto_updates.get_default_figure()

    if _main_dict["args"].flint:
        redis_data_url = f"redis://{bec._client._service_config.redis_data}"
        flint_process = subprocess.Popen(
            [sys.executable, "-m", "bliss.flint", "--no-rpc", "--no-persistence", "-s", "bec"],
            env=dict(os.environ) | {"REDIS_DATA_URL": redis_data_url},
            cwd=os.environ[
                "HOME"
            ],  # prevent unexpected imports from current directory (e.g when developing - starting from Python 3.11 can use -P option)
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

    _available_plugins = plugin_helper.get_ipython_client_startup_plugins(state="post")
    if _available_plugins:
        for name, plugin in _available_plugins.items():
            logger.success(f"Loading plugin: {plugin['source']}")
            base = os.path.dirname(plugin["module"].__file__)
            with open(os.path.join(base, "post_startup.py"), "r", encoding="utf-8") as file:
                # pylint: disable=exec-used
                exec(file.read())

    else:
        bec._ip.prompts.status = 1

    if not bec._hli_funcs:
        bec.load_high_level_interface("bec_hli")

if _main_dict["startup_file"]:
    with open(_main_dict["startup_file"], "r", encoding="utf-8") as file:
        # pylint: disable=exec-used
        exec(file.read())
