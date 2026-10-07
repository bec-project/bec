import os
import sys

import numpy as np  # not needed but always nice to have

from bec_ipython_client.main import BECIPythonClient as _BECIPythonClient
from bec_ipython_client.main import main_dict as _main_dict
from bec_lib import plugin_helper
from bec_lib.acl_login import BECAuthenticationError
from bec_lib.logger import bec_logger as _bec_logger
from bec_lib.redis_connector import RedisConnector as _RedisConnector

logger = _bec_logger.logger


bec = _BECIPythonClient(
    _main_dict["config"],
    _RedisConnector,
    wait_for_server=_main_dict["wait_for_server"],
    gui_id=_main_dict["args"].gui_id,
)
_main_dict["bec"] = bec


try:
    bec.start()
except (BECAuthenticationError, KeyboardInterrupt) as exc:
    logger.error(f"{exc} Exiting.")
    os._exit(0)
except Exception:
    sys.excepthook(*sys.exc_info())
else:
    if bec.started:
        gui = bec._gui  # pylint: disable=protected-access
        if not _main_dict["args"].nogui:
            try:
                gui.show()
            except ImportError:
                logger.warning(
                    "BEC Widgets is unavailable; skipping GUI startup. "
                    "Install bec-widgets in this Python environment to use the GUI."
                )

    _available_plugins = plugin_helper.get_ipython_client_startup_plugins(state="post")
    if _available_plugins:
        for name, plugin in _available_plugins.items():
            logger.success(f"Loading plugin: {plugin['source']}")
            base = os.path.dirname(plugin["module"].__file__)
            with open(os.path.join(base, "post_startup.py"), "r", encoding="utf-8") as file:
                # pylint: disable=exec-used
                try:
                    exec(file.read())
                except Exception as exc:
                    logger.error(f"Error running `post startup` for plugin {name}: {exc}")

    else:
        bec._ip.prompts.status = 1


if _main_dict["startup_file"]:
    with open(_main_dict["startup_file"], "r", encoding="utf-8") as file:
        # pylint: disable=exec-used
        exec(file.read())
