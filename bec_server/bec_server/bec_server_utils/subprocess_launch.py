import functools
import os
import shutil
import subprocess

TERMINALS = (
    "x-terminal-emulator",
    "mate-terminal",
    "gnome-terminal",
    "terminator",
    "xfce4-terminal",
    "urxvt",
    "rxvt",
    "termit",
    "Eterm",
    "xterm",
    "konsole",
)


@functools.cache
def detect_terminal():
    all_terms = [os.environ.get("TERMINAL", "")] + list(TERMINALS)
    for term in all_terms:
        if shutil.which(term):
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
            processes.append(subprocess.Popen([detect_terminal(), "-H", "-e", cmd], cwd=cwd))
        except RuntimeError:
            # no terminal: just execute servers in background
            processes.append(subprocess.Popen(cmd.split(), cwd=cwd))
    return processes


def subprocess_stop():
    # do nothing for now... would require pid files or something to keep track
    # of the started processes
    ...
