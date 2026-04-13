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


class _LazyBECGuiClient:
    """Defer BEC Widgets import while preserving the interactive ``gui`` object."""

    def __init__(self, gui_id: str | None = None):
        self._gui_id = gui_id
        self._client = None

    def _materialize(self):
        if self._client is None:
            try:
                from bec_widgets.cli.client_utils import BECGuiClient
            except ImportError:
                logger.warning("BEC Widgets is not available; skipping GUI startup.")
                bec.gui = None
                globals().pop("gui", None)
                raise

            self._client = BECGuiClient()
            if self._gui_id:
                self._client.connect_to_gui_server(self._gui_id)
            bec.gui = self._client
            globals()["gui"] = self._client
        return self._client

    def __getattr__(self, name):
        return getattr(self._materialize(), name)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            return super().__setattr__(name, value)
        return setattr(self._materialize(), name, value)

    def __repr__(self) -> str:
        if self._client is None:
            return "LazyBECGuiClient(uninitialized)"
        return repr(self._client)


bec = _BECIPythonClient(
    _main_dict["config"], _RedisConnector, wait_for_server=_main_dict["wait_for_server"]
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
        gui = bec.gui = _LazyBECGuiClient(gui_id=_main_dict["args"].gui_id)
        if not _main_dict["args"].nogui:
            try:
                gui.show()
            except ImportError:
                logger.warning("BEC Widgets is not available")
                pass

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
