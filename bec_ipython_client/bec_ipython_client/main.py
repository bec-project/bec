import argparse
import os
import sys
from importlib.metadata import version
from typing import Iterable, Literal, Tuple

import IPython
import redis
import redis.exceptions
import requests
from IPython.terminal.ipapp import TerminalIPythonApp
from IPython.terminal.prompts import Prompts, Token

from bec_ipython_client.beamline_mixin import BeamlineMixin
from bec_ipython_client.bec_magics import BECMagics
from bec_ipython_client.callbacks.ipython_live_updates import IPythonLiveUpdates
from bec_ipython_client.signals import ScanInterruption, SigintHandler
from bec_lib import plugin_helper
from bec_lib.acl_login import BECAuthenticationError
from bec_lib.alarm_handler import AlarmBase
from bec_lib.bec_errors import DeviceConfigError
from bec_lib.bec_service import parse_cmdline_args
from bec_lib.callback_handler import EventType
from bec_lib.client import BECClient
from bec_lib.logger import bec_logger
from bec_lib.redis_connector import RedisConnector
from bec_lib.service_config import ServiceConfig

logger = bec_logger.logger


class CLIBECClient(BECClient):

    def _wait_for_server(self):
        super()._wait_for_server()

        # NOTE: self._BECClient__init_params is a name mangling attribute of the parent class
        # pylint: disable=no-member
        cmdline_args = self._BECClient__init_params["config"].config.get("cmdline_args")
        # set stderr logger level to SUCCESS (will not show messages <= INFO level)
        # (see issue #318), except if user explicitely asked for another level from cmd line
        if not cmdline_args or not cmdline_args.get("log_level"):
            # pylint: disable=protected-access
            bec_logger._stderr_log_level = "SUCCESS"
            bec_logger._update_sinks()


class BECIPythonClient:

    # local_only_types is a container for objects that should not be resolved through
    # the CLIBECClient but directly through the BECIPythonClient. While this is not
    # needed for normal usage, it is required, e.g. for mocks.
    _local_only_types: Tuple = ()

    def __init__(
        self,
        config: ServiceConfig | None = None,
        connector_cls: type[RedisConnector] | None = None,
        wait_for_server=True,
        forced=False,
    ) -> None:
        self._client = CLIBECClient(
            config,
            connector_cls,
            wait_for_server,
            forced,
            parent=self,
            name="BECIPythonClient",
            prompt_for_acl=True,
        )
        self._ip = IPython.get_ipython()
        self.started = False
        self._sighandler = None
        self._beamline_mixin = None
        self._exit_event = None
        self._exit_handler_thread = None
        self._live_updates = None
        self.gui = None
        self._client.callbacks.register(
            event_type=EventType.NAMESPACE_UPDATE, callback=self._update_namespace_callback
        )

    def __getattr__(self, name):
        return getattr(self._client, name)

    def __setattr__(self, name, value):
        if isinstance(value, self._local_only_types):
            return super().__setattr__(name, value)
        if name in self.__dict__ or name in self.__class__.__dict__:
            super().__setattr__(name, value)
        elif "_client" in self.__dict__ and hasattr(self._client, name):
            setattr(self._client, name, value)
        else:
            super().__setattr__(name, value)

    def __dir__(self) -> Iterable[str]:
        return dir(self._client) + dir(self.__class__)

    def __str__(self) -> str:
        return "BECIPythonClient\n\nTo get a list of available commands, type `bec.show_all_commands()`"

    def start(self):
        """start the client"""
        if self.started:
            return
        try:
            self._client.start()
        except KeyboardInterrupt:
            raise KeyboardInterrupt("Login aborted.")

        bec_logger.add_console_log()
        self._sighandler = SigintHandler(self)
        self._beamline_mixin = BeamlineMixin()
        self._live_updates = IPythonLiveUpdates(self)
        self._configure_ipython()
        self.started = self._client.started

    def bl_show_all(self):
        self._beamline_mixin.bl_show_all()

    def _set_ipython_prompt_scan_number(self, scan_number: int):
        if self._ip:
            self._ip.prompts.scan_number = scan_number + 1

    def _refresh_ipython_username(self):
        if not self._ip:
            return
        self._ip.prompts.username = self._client.username

    def _configure_ipython(self):

        if self._ip is None:
            return

        self._ip.prompts = BECClientPrompt(ip=self._ip, client=self._client, username="unknown")
        self._refresh_ipython_username()
        self._load_magics()
        self._ip.events.register("post_run_cell", log_console)
        self._ip.set_custom_exc((Exception,), _ip_exception_handler)  # register your handler
        # represent objects using __str__, if overwritten, otherwise use __repr__
        self._ip.display_formatter.formatters["text/plain"].for_type(
            object,
            lambda o, p, cycle: o.__str__ is object.__str__ and p.text(repr(o)) or p.text(str(o)),
        )

    def _update_namespace_callback(self, action: Literal["add", "remove"], ns_objects: dict):
        """Callback to update the global namespace of ipython.

        Within BEC, the callback is triggered by the CallbackHandler when
        the namespace changes, e.g. load_high_level_interface, load_user_script, etc.

        Args:
            action (Literal["add", "remove"]): action to perform
            ns_objects (dict): objects to add or remove

        """
        if self._ip is None:
            return
        if action == "add":
            for name, obj in ns_objects.items():
                self._ip.user_global_ns[name] = obj
        elif action == "remove":
            for name, obj in ns_objects.items():
                self._ip.user_global_ns.pop(name)

    def _set_error(self):
        if self._ip is not None:
            self._ip.prompts.status = 0

    def _set_busy(self):
        if self._ip is not None:
            self._ip.prompts.status = 1

    def _set_idle(self):
        if self._ip is not None:
            self._ip.prompts.status = 2

    def _load_magics(self):
        magics = BECMagics(self._ip, self)
        self._ip.register_magics(magics)

    def shutdown(self):
        """shutdown the client and all its components"""
        try:
            self.gui.close()
        except AttributeError:
            pass
        self._client.shutdown()


def _ip_exception_handler(self, etype, evalue, tb, tb_offset=None):
    if issubclass(etype, (AlarmBase, ScanInterruption, DeviceConfigError)):
        print(f"\x1b[31m BEC alarm:\x1b[0m {evalue}")
        return
    if issubclass(etype, redis.exceptions.NoPermissionError):
        # pylint: disable=protected-access
        msg = f"The current user ({bec._client.username}) does not have the required permissions.\n {evalue}"
        logger.info(f"Unauthorized: {msg}")
        print(f"\x1b[31m Unauthorized:\x1b[0m {msg}")
        return
    self.showtraceback((etype, evalue, tb), tb_offset=None)  # standard IPython's printout


class BECClientPrompt(Prompts):
    def __init__(self, ip, username, client, status=0):
        self._username = username
        self.session_name = "bec"
        self.client = client
        self.status = status
        super().__init__(ip)

    def in_prompt_tokens(self, cli=None):
        if self.status == 0:
            status_led = Token.OutPromptNum
        elif self.status == 1:
            status_led = Token.PromptNum
        else:
            status_led = Token.Prompt
        try:
            next_scan_number = str(self.client.queue.next_scan_number)
        except Exception:
            next_scan_number = "?"

        if self.client.active_account:
            username = f"{self.client.active_account} | {self.username}"
        else:
            username = self.username
        return [
            (status_led, "\u2022"),
            (Token.Prompt, " " + username),  # BEC ACL username and pgroup
            (Token.Prompt, "@" + self.session_name),
            (Token.Prompt, " ["),
            (Token.PromptNum, str(self.shell.execution_count)),
            (Token.Prompt, "/"),
            (Token.PromptNum, next_scan_number),
            (Token.Prompt, "] "),
            (Token.Prompt, "❯❯ "),
        ]

    @property
    def username(self):
        """current username"""
        return self._username

    @username.setter
    def username(self, value):
        self._username = value


def log_console(execution_info):
    """log the console input"""
    logger.log("CONSOLE_LOG", f"{execution_info.info.raw_cell}")


# pylint: disable=wrong-import-position
# pylint: disable=protected-access
# pylint: disable=unused-import
# pylint: disable=ungrouped-imports

main_dict = {}

sys.modules["bec_ipython_client.main"] = sys.modules[
    __name__
]  # properly register module when file is executed directly, like in test


def main():
    """
    Main function to start the BEC IPython client.
    """

    available_plugins = plugin_helper.get_ipython_client_startup_plugins(state="pre")

    parser = argparse.ArgumentParser(
        prog="BEC IPython client", description="BEC command line interface"
    )
    parser.add_argument("--version", action="store_true", default=False)
    parser.add_argument("--nogui", action="store_true", default=False)
    parser.add_argument(
        "--gui-id",
        action="store",
        default=None,
        help="ID of the GUI to connect to, if not set, a new GUI will be created",
    )
    parser.add_argument("--dont-wait-for-server", action="store_true", default=False)
    parser.add_argument("--post-startup-file", action="store", default=None)

    for plugin in available_plugins.values():
        if hasattr(plugin["module"], "extend_command_line_args"):
            plugin["module"].extend_command_line_args(parser)

    args, left_args, config = parse_cmdline_args(parser, config_name="client")

    # remove already parsed args from command line args
    sys.argv = sys.argv[:1] + left_args

    if args.version:
        print(f"BEC IPython client: {version('bec_ipython_client')}")
        sys.exit(0)

    if available_plugins and config.is_default():
        # check if config is defined in a plugin;
        # in this case the plugin config takes precedence over
        # the default config
        for plugin in available_plugins.values():
            if hasattr(plugin["module"], "get_config"):
                config = plugin["module"].get_config()
                break

    main_dict["config"] = config
    main_dict["args"] = args
    main_dict["wait_for_server"] = not args.dont_wait_for_server
    main_dict["startup_file"] = args.post_startup_file

    app = TerminalIPythonApp()
    app.interact = True
    app.initialize(argv=["-i", os.path.join(os.path.dirname(__file__), "bec_startup.py")])

    try:
        app.start()
    finally:
        if "bec" in main_dict:
            # bec object is inserted into main_dict by bec_startup
            main_dict["bec"].shutdown()


if __name__ == "__main__":
    main()
