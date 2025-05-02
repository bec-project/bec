"""Create a BEC plugin in the currently installed repository"""

import typer

from bec_lib.utils.plugin_manager.create import device, scan, widget

_app = typer.Typer(name="create", help="Create a BEC plugin in the currently installed repository")
_app.add_typer(device._app)
_app.add_typer(scan._app)
_app.add_typer(widget._app)
