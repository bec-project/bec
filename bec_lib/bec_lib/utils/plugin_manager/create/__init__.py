"""Create a BEC plugin in the currently installed repository"""

import typer

from bec_lib.utils.plugin_manager.create import device, scan

_app = typer.Typer(
    name="create",
    help="Create a BEC plugin in the currently installed repository",
    rich_markup_mode="rich",
)
_app.add_typer(device._app)
_app.add_typer(scan._app)

try:
    from bec_widgets.utils.bec_plugin_manager.create import widget

    _app.add_typer(widget._app)
except ImportError:
    pass
