import typer

from bec_lib.logger import bec_logger
from bec_lib.utils.plugin_manager import create

logger = bec_logger.logger
_app = typer.Typer(name="bec-plugin-manager", rich_markup_mode="rich")
_app.add_typer(create._app)

try:
    from bec_widgets.utils.bec_plugin_manager.edit_ui import open_and_watch_ui_editor

    @_app.command()
    def edit_ui(widget_name: str):
        """Edit the .ui file for a given widget plugin in bec-designer. Will recompile the python module
        for the file as changes are made."""
        open_and_watch_ui_editor(widget_name)

except ImportError:
    pass


def main():
    """Initial entrypoint for bec-plugin-manager"""
    _app()
