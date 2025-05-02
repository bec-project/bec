import subprocess
from pathlib import Path

from watchdog.events import (
    DirCreatedEvent,
    DirModifiedEvent,
    DirMovedEvent,
    FileClosedEvent,
    FileCreatedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer

from bec_lib.logger import bec_logger
from bec_lib.plugin_helper import plugin_repo_path

logger = bec_logger.logger


class RecompileHandler(FileSystemEventHandler):
    def __init__(self, in_file: Path, out_file: Path) -> None:
        super().__init__()
        self.in_file = str(in_file)
        self.out_file = str(out_file)

    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        self.recompile(event)

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        self.recompile(event)

    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        self.recompile(event)

    def recompile(self, event: FileSystemEvent) -> None:
        if event.src_path == self.in_file or event.dest_path == self.in_file:
            self._recompile()

    def _recompile(self):
        logger.success(".ui file modified, recompiling...")
        code = subprocess.call(
            ["pyside6-uic", "--absolute-imports", self.in_file, "-o", self.out_file]
        )
        logger.success(f"compilation exited with code {code}")


def open_and_watch_ui_editor(widget_name: str):
    logger.info(f"Opening the editor for {widget_name}... ")

    try:
        from bec_widgets.utils.bec_designer import open_designer
    except ImportError:
        logger.error("BEC Widgets must be installed to use the UI editor tool")
        exit(-1)

    repo = Path(plugin_repo_path())
    widget_dir = repo / repo.name / "bec_widgets" / "widgets" / widget_name
    ui_file = widget_dir / f"{widget_name}.ui"
    ui_outfile = widget_dir / f"{widget_name}_ui.py"
    recompile_handler = RecompileHandler(ui_file, ui_outfile)
    observer = Observer()
    observer.schedule(recompile_handler, str(ui_file.parent))
    observer.start()
    try:
        open_designer([str(ui_file)])
    finally:
        observer.stop()
        observer.join()
    logger.info("Editing session ended, exiting...")
