from __future__ import annotations

import subprocess
import threading
from typing import TYPE_CHECKING

from bec_lib import plugin_helper
from bec_lib.logger import bec_logger

if TYPE_CHECKING:  # pragma: no cover
    from bec_server.scihub.atlas.atlas_connector import AtlasConnector

logger = bec_logger.logger


class AtlasPluginRepoEmitter:
    _GIT_TIMEOUT_S = 60

    def __init__(self, atlas_connector: AtlasConnector, poll_interval_s: float = 600.0) -> None:
        self.atlas_connector = atlas_connector
        self._poll_interval_s = poll_interval_s
        self._last_plugin_repo_metrics: dict[str, int | bool] | None = None
        self._shutdown_event = threading.Event()
        self._poll_thread = threading.Thread(
            target=self._poll_plugin_repo, daemon=True, name="atlas_plugin_repo_emitter"
        )
        self._emit_plugin_repo_metrics()
        self._poll_thread.start()

    def _poll_plugin_repo(self) -> None:
        while not self._shutdown_event.wait(timeout=self._poll_interval_s):
            self._emit_plugin_repo_metrics()

    def _emit_plugin_repo_metrics(self) -> None:
        metrics = self._get_plugin_repo_metrics()
        if metrics is None:
            return
        if metrics == self._last_plugin_repo_metrics:
            return
        self._last_plugin_repo_metrics = metrics
        self.atlas_connector.connector.publish_metrics("plugin_repo", metrics)

    def _get_plugin_repo_metrics(self) -> dict[str, int | bool] | None:
        metrics = {
            "is_dirty": False,
            "commits_ahead_of_main": 0,
            "commits_behind_main": 0,
            "lines_added_since_main": 0,
            "lines_deleted_since_main": 0,
        }
        try:
            if plugin_helper.plugins_installed() != 1:
                return metrics
            repo_path = plugin_helper.plugin_repo_path()
        except ValueError as exc:
            logger.warning(f"Failed to resolve plugin repo path: {exc}")
            return metrics

        status_result = self._safe_run_git_command(
            repo_path,
            "status",
            "--porcelain",
            failure_message=f"Failed to inspect plugin repo status for {repo_path}",
        )
        if status_result is None:
            return None

        if status_result.returncode != 0:
            logger.warning(
                f"Failed to inspect plugin repo status for {repo_path}: {status_result.stderr.strip()}"
            )
            return None

        metrics["is_dirty"] = self._parse_git_status(status_result.stdout)["is_dirty"]

        fetch_result = self._safe_run_git_command(
            repo_path,
            "fetch",
            "origin",
            "main",
            failure_message=f"Failed to fetch origin/main for {repo_path}",
        )
        if fetch_result is None:
            return None
        if fetch_result.returncode != 0:
            logger.warning(
                f"Failed to fetch origin/main for {repo_path}: {fetch_result.stderr.strip()}"
            )
            return None

        compare_result = self._safe_run_git_command(
            repo_path,
            "rev-list",
            "--left-right",
            "--count",
            "origin/main...HEAD",
            failure_message=f"Failed to compare plugin repo against origin/main for {repo_path}",
        )
        if compare_result is None:
            return None
        if compare_result.returncode != 0:
            logger.warning(
                f"Failed to compare plugin repo against origin/main for {repo_path}: {compare_result.stderr.strip()}"
            )
            return None

        metrics.update(self._parse_rev_list_counts(compare_result.stdout))

        diff_result = self._safe_run_git_command(
            repo_path,
            "diff",
            "--numstat",
            "origin/main...HEAD",
            failure_message=f"Failed to summarize plugin repo diff against origin/main for {repo_path}",
        )
        if diff_result is None:
            return None
        if diff_result.returncode != 0:
            logger.warning(
                f"Failed to summarize plugin repo diff against origin/main for {repo_path}: {diff_result.stderr.strip()}"
            )
            return None

        metrics.update(self._parse_numstat(diff_result.stdout))
        return metrics

    @staticmethod
    def _run_git_command(repo_path: str, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", repo_path, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=AtlasPluginRepoEmitter._GIT_TIMEOUT_S,
        )

    @staticmethod
    def _safe_run_git_command(
        repo_path: str, *args: str, failure_message: str
    ) -> subprocess.CompletedProcess[str] | None:
        try:
            return AtlasPluginRepoEmitter._run_git_command(repo_path, *args)
        except FileNotFoundError:
            logger.warning("git is not available. Cannot inspect plugin repo status.")
            return None
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"{failure_message}: {exc}")
            return None

    @staticmethod
    def _parse_git_status(stdout: str) -> dict[str, bool]:
        return {"is_dirty": bool(stdout.strip())}

    @staticmethod
    def _parse_rev_list_counts(stdout: str) -> dict[str, int]:
        behind_str, ahead_str = stdout.strip().split(maxsplit=1)
        return {"commits_ahead_of_main": int(ahead_str), "commits_behind_main": int(behind_str)}

    @staticmethod
    def _parse_numstat(stdout: str) -> dict[str, int]:
        lines_added = 0
        lines_deleted = 0
        for line in stdout.splitlines():
            added_str, deleted_str, _path = line.split("\t", maxsplit=2)
            if added_str != "-":
                lines_added += int(added_str)
            if deleted_str != "-":
                lines_deleted += int(deleted_str)
        return {"lines_added_since_main": lines_added, "lines_deleted_since_main": lines_deleted}

    def shutdown(self) -> None:
        self._shutdown_event.set()
        self._poll_thread.join(timeout=1)
