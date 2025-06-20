import os
import subprocess
from contextlib import contextmanager
from pathlib import Path

import yaml

from bec_lib.logger import bec_logger

logger = bec_logger.logger


def existing_data(repo: Path, keys: list[str]) -> dict[str, str | list | dict]:
    answers_file = Path(repo) / ".copier-answers.yml"
    with open(answers_file) as f:
        old_answers = yaml.safe_load(f)
    try:
        return {key: old_answers[key] for key in keys}
    except KeyError as e:
        raise ValueError(f"Item {e.args[0]} could not be fetched from the answers file") from e


@contextmanager
def _goto_dir(path: Path):
    current = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(current)


def git_stage_files(directory: Path, filenames: list[str] = []):
    "`git add` all the files in `filenames` in the given directory or all files if filenames is empty"
    logger.info(f"Adding {filenames if filenames else 'all files'} in {directory} to git...")
    with _goto_dir(directory):
        return subprocess.call(["git", "add"] + (filenames if filenames else ["*"]))


def make_commit(repo: Path, message: str):
    with _goto_dir(repo):
        subprocess.call(["git", "commit", "-m", message])
