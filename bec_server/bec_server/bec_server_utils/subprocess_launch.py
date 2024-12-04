import functools
import os
import shutil
import subprocess
from dataclasses import dataclass

import psutil


@dataclass
class TerminalProc:
    """cmd is the terminal process to launch, args should"""

    cmd: str
    args: list[str]
    spawn_child: bool  # indicate if a child process has to be found to really stop the terminal


TERMINALS = (
    TerminalProc("xfce4-terminal", args=["--disable-server", "-H", "-e"], spawn_child=False),
    TerminalProc("konsole", args=["--hold", "-e"], spawn_child=False),
    TerminalProc("xterm", args=["-hold", "-e"], spawn_child=False),
)


@functools.cache
def detect_terminal():
    all_terms = list(TERMINALS)
    for term in all_terms:
        if shutil.which(term.cmd):
            return term
    raise RuntimeError("Could not detect any suitable terminal to launch processes")


def subprocess_start(bec_path: str, services: dict):
    processes = []

    for ii, service_info in enumerate(services.items()):
        service, service_config = service_info

        if os.environ.get("CONDA_DEFAULT_ENV"):
            cmd = f"{os.environ['CONDA_EXE']} run -n {os.environ['CONDA_DEFAULT_ENV']} --no-capture-output {service_config.command}"
        else:
            cmd = service_config.command

        service_path = service_config.path.substitute(base_path=bec_path)
        # service_config adds a subdirectory to each path, here we do not want the subdirectory
        cwd = os.path.abspath(os.path.join(service_path, ".."))
        try:
            term = detect_terminal()
        except RuntimeError:
            # no terminal: execute servers in background
            processes.append(subprocess.Popen(cmd.split(), cwd=cwd, stdout=subprocess.DEVNULL))
        else:
            processes.append(subprocess.Popen([term.cmd] + term.args + [cmd], cwd=cwd))
    return processes


def _kill_process_and_children(pid):
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for child in children:
        child.terminate()
        child.wait()
    parent.terminate()
    parent.wait()


def subprocess_stop(processes=None):
    # For "bec-server stop" to be able to stop processes it would
    # need PID files for example... So, for now only consider to do
    # something considering we get Popen objects (like in tests)
    if not processes:
        return
    for process in processes:
        cmd = process.args[0]
        for term in TERMINALS:
            if term.cmd == cmd:
                if term.spawn_child:
                    _kill_process_and_children(process.pid)
                else:
                    process.terminate()
                    process.wait()
                break
        else:
            # not in terminal
            # if command is launched via 'conda',
            # there might be children processes
            _kill_process_and_children(process.pid)
