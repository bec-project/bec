import os
import time
from typing import TYPE_CHECKING

import libtmux
import psutil
from libtmux import Session
from libtmux.constants import PaneDirection
from libtmux.exc import LibTmuxException

if TYPE_CHECKING:
    from bec_server.bec_server_utils.service_handler import ServiceDesc


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
    if os.environ.get("INVOCATION_ID"):
        # running within systemd
        os.makedirs("/tmp/tmux-shared", exist_ok=True)
        os.chmod("/tmp/tmux-shared", 0o777)
        tmux_server = libtmux.Server(socket_path="/tmp/tmux-shared/default")
    elif os.path.exists("/tmp/tmux-shared/default"):
        # if we have a shared socket, use it
        tmux_server = libtmux.Server(socket_path="/tmp/tmux-shared/default")
    else:
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
    if os.environ.get("INVOCATION_ID") and os.path.exists("/tmp/tmux-shared/default"):
        # running within systemd
        os.chmod("/tmp/tmux-shared/default", 0o777)
    return session


def tmux_start(bec_path: str, services: dict[str, "ServiceDesc"]):
    """
    Launch services in a tmux session. All services are launched in separate panes.
    Services config dict contains "tmux_session_name" (default: "bec") and "window_label" (default: "BEC server",
    must be the same for the same session).

    Args:
        bec_path (str): Path to the BEC source code
        services (dict): Dictionary of services to launch. Keys are the service names, values are path and command templates.

    """
    sessions: dict[str, Session] = {}
    for service, service_config in services.items():
        tmux_session_name = service_config.tmux_session.name
        if tmux_session_name not in sessions:
            tmux_window_label = service_config.tmux_session.window_label
            session = get_new_session(tmux_session_name, tmux_window_label)
            pane = session.active_window.active_pane
            sessions[tmux_session_name] = session
        else:
            session = sessions[tmux_session_name]
            pane = session.active_window.split(direction=PaneDirection.Right)

        activate_venv(
            pane,
            service_name=service,
            service_path=service_config.path.substitute(base_path=bec_path),
        )

        command = " ".join((service_config.command, *service_config.args))
        pane.send_keys(command)

        wait_func = service_config.wait_func
        if callable(wait_func):
            wait_func()

    for session in sessions.values():
        session.active_window.select_layout("tiled")
        session.mouse_all_flag = True
        session.set_option("mouse", "on")


def tmux_stop(session_name="bec", timeout=5):
    """
    Stop the services from the given tmux session.

    1. Send Ctrl+C (SIGINT) to all panes.
    2. Wait up to `timeout` seconds for processes to exit.
    3. Kill remaining processes if not exited.
    4. Kill the tmux session.
    """
    # connect to tmux server
    if os.path.exists("/tmp/tmux-shared/default"):
        tmux_server = libtmux.Server(socket_path="/tmp/tmux-shared/default")
    else:
        tmux_server = libtmux.Server()

    avail_sessions = tmux_server.sessions.filter(session_name=session_name)
    if not avail_sessions:
        return

    session = avail_sessions[0]

    # collect all child PIDs for panes
    all_children = []
    for bash_pid in map(int, [p.pane_pid for p in session.panes]):
        try:
            parent_proc = psutil.Process(bash_pid)
            children = parent_proc.children(recursive=True)
            all_children.extend(children)
        except psutil.NoSuchProcess:
            continue

    # Send Ctrl+C to each pane
    for pane in session.panes:
        pane.send_keys("^C")  # sends SIGINT via tmux

    # Wait for processes to exit
    start_time = time.time()
    while time.time() - start_time < timeout:
        alive = [p for p in all_children if p.is_running()]
        if not alive:
            break
        time.sleep(0.1)

    # Kill remaining processes forcefully
    for proc in alive:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass

    # Kill tmux session
    try:
        session.kill_session()
    except LibTmuxException:
        # session may already exit itself if all panes are gone
        pass
