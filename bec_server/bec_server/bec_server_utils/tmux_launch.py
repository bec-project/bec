import os
import signal
import time

import libtmux
import psutil
from libtmux.exc import LibTmuxException


def activate_venv(pane, service_name, service_path):
    """
    Activate the python environment for a service.
    """

    # check if the current file was installed with pip install -e (editable mode)
    # if so, the venv is the service directory and it's called <service_name>_venv
    # otherwise, we simply take the currently running venv ;
    # in case of no venv, maybe it is running within a Conda environment

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    if "site-packages" in __file__:
        venv_base_path = os.path.dirname(
            os.path.dirname(os.path.dirname(__file__.split("site-packages", maxsplit=1)[0]))
        )
        pane.send_keys(f"source {venv_base_path}/bin/activate")
    elif os.path.exists(f"{service_path}/{service_name}_venv"):
        pane.send_keys(f"source {service_path}/{service_name}_venv/bin/activate")
    elif os.path.exists(f"{base_dir}/bec_venv"):
        pane.send_keys(f"source {base_dir}/bec_venv/bin/activate")
    elif os.getenv("CONDA_PREFIX"):
        pane.send_keys(f"conda activate {os.path.basename(os.environ['CONDA_PREFIX'])}")


def get_new_session(tmux_session_name, window_label):
    tmux_server = libtmux.Server()
    session = None
    for i in range(2):
        try:
            session = tmux_server.new_session(
                tmux_session_name,
                window_name=f"{window_label}. Use `ctrl+b d` to detach.",
                kill_session=True,
            )
        except LibTmuxException:
            # retry once... sometimes there is a hiccup in creating the session
            time.sleep(1)
            continue
        else:
            break
    return session


def tmux_start(bec_path: str, services: dict):
    """
    Launch services in a tmux session. All services are launched in separate panes.
    Services config dict contains "tmux_session_name" (default: "bec") and "window_label" (default: "BEC server",
    must be the same for the same session).

    Args:
        bec_path (str): Path to the BEC source code
        services (dict): Dictionary of services to launch. Keys are the service names, values are path and command templates.

    """
    sessions = {}
    for service, service_config in services.items():
        tmux_session_name = service_config.tmux_session.name
        if tmux_session_name not in sessions:
            tmux_window_label = service_config.tmux_session.window_label
            session = get_new_session(tmux_session_name, tmux_window_label)
            pane = session.attached_window.attached_pane
            sessions[tmux_session_name] = session
        else:
            session = sessions[tmux_session_name]
            pane = session.attached_window.split_window(vertical=False)

        activate_venv(
            pane,
            service_name=service,
            service_path=service_config.path.substitute(base_path=bec_path),
        )

        pane.send_keys(service_config.command)

        wait_func = service_config.wait_func
        if callable(wait_func):
            wait_func()

    for session in sessions.values():
        session.attached_window.select_layout("tiled")
        session.mouse_all_flag = True
        session.set_option("mouse", "on")


def tmux_stop(session_name="bec"):
    """
    Stop the services from given session
    """
    tmux_server = libtmux.Server()
    avail_sessions = tmux_server.sessions.filter(session_name=session_name)
    if avail_sessions:
        session = avail_sessions[0]
        for bash_pid in map(int, [p.pane_pid for p in session.panes]):
            # bash_pid is tmux "bash -c" (= primary) pid for the pane
            for proc in psutil.Process(bash_pid).children():
                # there should be only 1 child pid
                os.kill(proc.pid, signal.SIGTERM)
        try:
            session.kill_session()
        except LibTmuxException:
            # session exits itself if no more panes
            pass
