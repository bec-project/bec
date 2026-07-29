import subprocess
from string import Template
from unittest import mock

import psutil

from bec_server.bec_server_utils.service_handler import ServiceDesc
from bec_server.bec_server_utils.subprocess_launch import (
    TerminalProc,
    _kill_process_and_children,
    _stop_psutil_process,
    _stop_subprocess,
    detect_terminal,
    subprocess_start,
    subprocess_stop,
)


def test_detect_terminal_returns_first_available():
    detect_terminal.cache_clear()
    try:
        with mock.patch(
            "bec_server.bec_server_utils.subprocess_launch.shutil.which",
            side_effect=[None, "/usr/bin/konsole"],
        ):
            terminal = detect_terminal()
    finally:
        detect_terminal.cache_clear()

    assert terminal.cmd == "konsole"


def test_subprocess_start_without_terminal_runs_processes_in_background():
    services = {
        "scan_server": ServiceDesc(
            Template("$base_path/scan_server"), "bec-scan-server", args=["--flag"]
        )
    }

    with (
        mock.patch(
            "bec_server.bec_server_utils.subprocess_launch.detect_terminal",
            side_effect=RuntimeError,
        ),
        mock.patch("bec_server.bec_server_utils.subprocess_launch.subprocess.Popen") as mock_popen,
    ):
        processes = subprocess_start("/tmp/bec", services)

    mock_popen.assert_called_once_with(
        ["bec-scan-server", "--flag"], cwd="/tmp/bec", stdout=subprocess.DEVNULL
    )
    assert processes == [mock_popen.return_value]


def test_subprocess_stop_uses_stop_subprocess_for_terminal_process():
    process = mock.Mock(args=["xterm"], pid=123)

    with (
        mock.patch(
            "bec_server.bec_server_utils.subprocess_launch._stop_subprocess"
        ) as mock_stop_subprocess,
        mock.patch(
            "bec_server.bec_server_utils.subprocess_launch._kill_process_and_children"
        ) as mock_kill_processes,
    ):
        subprocess_stop([process], timeout_s=7)

    mock_stop_subprocess.assert_called_once_with(process, timeout_s=7)
    mock_kill_processes.assert_not_called()


def test_subprocess_stop_uses_kill_processes_for_spawn_child_terminal():
    process = mock.Mock(args=["custom-term"], pid=456)
    terminals = (TerminalProc("custom-term", args=["-e"], spawn_child=True),)

    with (
        mock.patch("bec_server.bec_server_utils.subprocess_launch.TERMINALS", terminals),
        mock.patch(
            "bec_server.bec_server_utils.subprocess_launch._stop_subprocess"
        ) as mock_stop_subprocess,
        mock.patch(
            "bec_server.bec_server_utils.subprocess_launch._kill_process_and_children"
        ) as mock_kill_processes,
    ):
        subprocess_stop([process], timeout_s=9)

    mock_kill_processes.assert_called_once_with(456, timeout_s=9)
    mock_stop_subprocess.assert_not_called()


def test_subprocess_stop_uses_kill_processes_for_non_terminal_process():
    process = mock.Mock(args=["python"], pid=789)

    with mock.patch(
        "bec_server.bec_server_utils.subprocess_launch._kill_process_and_children"
    ) as mock_kill_processes:
        subprocess_stop([process], timeout_s=11)

    mock_kill_processes.assert_called_once_with(789, timeout_s=11)


def test_stop_subprocess_kills_after_timeout():
    process = mock.Mock()
    process.wait.side_effect = [subprocess.TimeoutExpired(cmd="cmd", timeout=3), None]

    _stop_subprocess(process, timeout_s=3)

    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert process.wait.call_args_list == [mock.call(timeout=3), mock.call(timeout=3)]


def test_stop_psutil_process_kills_after_timeout():
    process = mock.Mock()
    process.wait.side_effect = [psutil.TimeoutExpired(seconds=4, pid=55), None]

    _stop_psutil_process(process, timeout_s=4)

    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert process.wait.call_args_list == [mock.call(timeout=4), mock.call(timeout=4)]


def test_kill_process_and_children_stops_children_before_parent():
    child1 = mock.Mock()
    child2 = mock.Mock()
    parent = mock.Mock()
    parent.children.return_value = [child1, child2]

    with (
        mock.patch(
            "bec_server.bec_server_utils.subprocess_launch.psutil.Process", return_value=parent
        ),
        mock.patch(
            "bec_server.bec_server_utils.subprocess_launch._stop_psutil_process"
        ) as mock_stop_psutil_process,
    ):
        _kill_process_and_children(999, timeout_s=5)

    assert mock_stop_psutil_process.call_args_list == [
        mock.call(child1, timeout_s=5),
        mock.call(child2, timeout_s=5),
        mock.call(parent, timeout_s=5),
    ]


def test_kill_process_and_children_ignores_missing_parent_process():
    with mock.patch(
        "bec_server.bec_server_utils.subprocess_launch.psutil.Process",
        side_effect=psutil.NoSuchProcess(pid=12),
    ):
        _kill_process_and_children(12, timeout_s=2)
