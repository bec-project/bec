import traceback
from pathlib import Path
from typing import Annotated

import copier
import typer

from bec_lib.logger import bec_logger
from bec_lib.plugin_helper import plugin_repo_path
from bec_lib.utils.plugin_manager._constants import ANSWER_KEYS
from bec_lib.utils.plugin_manager._util import existing_data, git_stage_files, make_commit
from bec_lib.utils.plugin_manager.edit_ui import open_and_watch_ui_editor

logger = bec_logger.logger
_app = typer.Typer(rich_markup_mode="rich")


def _commit_added_widget(repo: Path, name: str):
    git_stage_files(repo, [".copier-answers.yml"])
    git_stage_files(repo / repo.name / "bec_widgets" / "widgets" / name, [])
    make_commit(repo, f"plugin-manager added new widget: {name}")
    logger.info(f"Committing new widget {name}")


def _widget_exists(widget_list: list[dict[str, str]], name: str):
    return name in [w["name"] for w in widget_list]


def _editor_cb(ctx: typer.Context, value: bool):
    if value and not ctx.params["use_ui"]:
        raise typer.BadParameter("Can only open the editor if creating a .ui file!")
    return value


@_app.command()
def widget(
    name: Annotated[str, typer.Argument(help="Enter a name for your widget in snake_case")],
    use_ui: Annotated[
        bool, typer.Option(prompt=True, help="Generate a .ui file for use in bec-designer.")
    ] = True,
    open_editor: Annotated[
        bool,
        typer.Option(
            prompt=True, help="Open the created widget in bec-designer.", callback=_editor_cb
        ),
    ] = True,
):
    """Create a new widget plugin with the given name.

If [bold white]use_ui[/bold white] is set, a bec-designer .ui file will also be created. If \
[bold white]open_editor[/bold white] is additionally set, the .ui file will be opened in \
bec-designer and the compiled python version will be updated when changes are made."""
    if (formatted_name := name.lower().replace("-", "_")) != name:
        logger.warning(f"Adjusting widget name from {name} to {formatted_name}")
    if not formatted_name.isidentifier():
        logger.error(
            f"{name} is not a valid name for a widget (even after converting to {formatted_name}) - please enter something in snake_case"
        )
        exit(-1)
    logger.info(f"Adding new widget {formatted_name} to the template...")
    try:
        repo = Path(plugin_repo_path())
        plugin_data = existing_data(repo, [ANSWER_KEYS.VERSION, ANSWER_KEYS.WIDGETS])
        if _widget_exists(plugin_data[ANSWER_KEYS.WIDGETS], formatted_name):
            logger.error(f"Widget {formatted_name} already exists!")
            exit(-1)
        plugin_data[ANSWER_KEYS.WIDGETS].append({"name": formatted_name, "use_ui": use_ui})
        copier.run_update(
            repo,
            data=plugin_data,
            defaults=True,
            unsafe=True,
            overwrite=True,
            vcs_ref=plugin_data[ANSWER_KEYS.VERSION],
        )
        _commit_added_widget(repo, formatted_name)
    except Exception:
        logger.error(traceback.format_exc())
        logger.error("exiting...")
        exit(-1)
    logger.success(f"Added widget {formatted_name}!")
    if open_editor:
        open_and_watch_ui_editor(formatted_name)
