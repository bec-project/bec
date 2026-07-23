import subprocess
from unittest import mock

from bec_server.scihub.atlas.atlas_plugin_repo_emitter import AtlasPluginRepoEmitter


def test_plugin_repo_emitter_emits_dirty_state(atlas_connector):
    with (
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugins_installed",
            return_value=1,
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugin_repo_path",
            return_value="/tmp/plugin-repo",
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.subprocess.run",
            side_effect=[
                mock.Mock(returncode=0, stdout=" M bec_plugin/scans/test.py\n", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="2\t3\n", stderr=""),
                mock.Mock(
                    returncode=0,
                    stdout="12\t4\tbec_plugin/scans/test.py\n3\t1\tbec_plugin/devices/device.py\n",
                    stderr="",
                ),
            ],
        ),
        mock.patch.object(atlas_connector.connector, "publish_metrics") as mock_publish_metrics,
    ):
        emitter = AtlasPluginRepoEmitter(atlas_connector, poll_interval_s=60)
        emitter.shutdown()

    mock_publish_metrics.assert_called_once_with(
        "plugin_repo",
        {
            "is_dirty": True,
            "commits_ahead_of_main": 3,
            "commits_behind_main": 2,
            "lines_added_since_main": 15,
            "lines_deleted_since_main": 5,
        },
    )


def test_plugin_repo_emitter_does_not_emit_unchanged_metric(atlas_connector):
    with (
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugins_installed",
            return_value=1,
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugin_repo_path",
            return_value="/tmp/plugin-repo",
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.subprocess.run",
            side_effect=[
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="1\t4\n", stderr=""),
                mock.Mock(returncode=0, stdout="5\t2\tbec_plugin/file.py\n", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="1\t4\n", stderr=""),
                mock.Mock(returncode=0, stdout="5\t2\tbec_plugin/file.py\n", stderr=""),
            ],
        ),
    ):
        emitter = AtlasPluginRepoEmitter(atlas_connector, poll_interval_s=60)
        try:
            with mock.patch.object(atlas_connector.connector, "publish_metrics") as mock_publish:
                emitter._emit_plugin_repo_metrics()
        finally:
            emitter.shutdown()

    mock_publish.assert_not_called()


def test_plugin_repo_emitter_emits_false_when_no_plugin_is_installed(atlas_connector):
    with (
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugins_installed",
            return_value=0,
        ),
        mock.patch.object(atlas_connector.connector, "publish_metrics") as mock_publish_metrics,
    ):
        emitter = AtlasPluginRepoEmitter(atlas_connector, poll_interval_s=60)
        emitter.shutdown()

    mock_publish_metrics.assert_called_once_with(
        "plugin_repo",
        {
            "is_dirty": False,
            "commits_ahead_of_main": 0,
            "commits_behind_main": 0,
            "lines_added_since_main": 0,
            "lines_deleted_since_main": 0,
        },
    )


def test_plugin_repo_emitter_emits_ahead_behind_state(atlas_connector):
    with (
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugins_installed",
            return_value=1,
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugin_repo_path",
            return_value="/tmp/plugin-repo",
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.subprocess.run",
            side_effect=[
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="5\t0\n", stderr=""),
                mock.Mock(returncode=0, stdout="7\t9\tbec_plugin/file.py\n", stderr=""),
            ],
        ),
        mock.patch.object(atlas_connector.connector, "publish_metrics") as mock_publish_metrics,
    ):
        emitter = AtlasPluginRepoEmitter(atlas_connector, poll_interval_s=60)
        emitter.shutdown()

    mock_publish_metrics.assert_called_once_with(
        "plugin_repo",
        {
            "is_dirty": False,
            "commits_ahead_of_main": 0,
            "commits_behind_main": 5,
            "lines_added_since_main": 7,
            "lines_deleted_since_main": 9,
        },
    )


def test_plugin_repo_emitter_does_not_emit_new_metrics_on_git_exception(atlas_connector):
    with (
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugins_installed",
            return_value=1,
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.plugin_helper.plugin_repo_path",
            return_value="/tmp/plugin-repo",
        ),
        mock.patch(
            "bec_server.scihub.atlas.atlas_plugin_repo_emitter.subprocess.run",
            side_effect=[
                mock.Mock(returncode=0, stdout="", stderr=""),
                subprocess.TimeoutExpired(cmd=["git"], timeout=10),
            ],
        ),
        mock.patch.object(atlas_connector.connector, "publish_metrics") as mock_publish_metrics,
    ):
        emitter = AtlasPluginRepoEmitter(atlas_connector, poll_interval_s=60)
        emitter.shutdown()

    mock_publish_metrics.assert_not_called()
