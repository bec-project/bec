from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

import libtmux
import psutil
from libtmux.exc import LibTmuxException

if TYPE_CHECKING:
    from bec_server.bec_server_utils.service_handler import ServiceDesc


def activate_venv(pane: libtmux.Pane, service_name: str, service_path: str) -> None:
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


def get_new_session(tmux_session_name: str, window_label: str) -> libtmux.Session | None:
    """
    Create a new tmux session with the given name and window label.

    Args:
        tmux_session_name (str): Name of the tmux session
        window_label (str): Label for the tmux window

    Returns:
        libtmux.Session | None: The created tmux session object
    """
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


def tmux_start(bec_path: str, services: dict[str, ServiceDesc]) -> None:
    """
    Launch services in a tmux session. All services are launched in separate panes.
    Services config dict contains "tmux_session_name" (default: "bec") and "window_label" (default: "BEC server",
    must be the same for the same session).

    Args:
        bec_path (str): Path to the BEC source code
        services (dict[str, ServiceDesc]): Dictionary of services to launch. Keys are the service names, values are path and command templates.

    """
    sessions: dict[str, libtmux.Session] = {}
    session_windows: dict[str, libtmux.Window] = {}

    for service, service_config in services.items():
        tmux_session_name = service_config.tmux_session.name
        separate_window = service_config.separate_window

        if tmux_session_name not in sessions:
            tmux_window_label = service_config.tmux_session.window_label
            session = get_new_session(tmux_session_name, tmux_window_label)
            if session is None:
                raise RuntimeError(f"Failed to create tmux session '{tmux_session_name}'")
            pane = session.attached_window.active_pane
            sessions[tmux_session_name] = session
            if not separate_window:
                session_windows[tmux_session_name] = session.attached_window
        else:
            session = sessions[tmux_session_name]
            if separate_window:
                # Create a new window for this service
                window = session.new_window(window_name=service)
                pane = window.active_pane
            else:
                # Split the current window to create a new pane
                if tmux_session_name not in session_windows:
                    session_windows[tmux_session_name] = session.attached_window
                pane = session_windows[tmux_session_name].split_window(vertical=False)

        if pane is None:
            raise RuntimeError(f"Failed to create pane for service '{service}'")

        # Set pane title to service name for easy identification
        pane.window.set_window_option("pane-border-status", "top")
        pane.window.set_window_option("pane-border-format", "#{pane_title}")
        pane.cmd("select-pane", "-T", service)

        activate_venv(
            pane,
            service_name=service,
            service_path=service_config.path.substitute(base_path=bec_path),
        )

        pane.send_keys(service_config.command)

        wait_func = service_config.wait_func
        if callable(wait_func):
            wait_func()

    # Apply tiled layout only to windows with multiple panes (not separate windows)
    for window in session_windows.values():
        window.select_layout("tiled")

    for session in sessions.values():
        session.mouse_all_flag = True
        session.set_option("mouse", "on")


def _get_tmux_server() -> libtmux.Server:
    """Get tmux server instance, using shared socket if available."""
    if os.path.exists("/tmp/tmux-shared/default"):
        return libtmux.Server(socket_path="/tmp/tmux-shared/default")
    return libtmux.Server()


def _find_pane_by_title(session: libtmux.Session, service_name: str) -> libtmux.Pane | None:
    """
    Find a pane in a session by its title (service name).

    Args:
        session (libtmux.Session): Tmux session object
        service_name (str): Name of the service to find

    Returns:
        libtmux.Pane | None: Pane object if found, None otherwise
    """
    for pane in session.panes:
        pane_title = pane.display_message("#{pane_title}", get_text=True)
        # display_message returns a list when get_text=True
        if isinstance(pane_title, list):
            pane_title = pane_title[0] if pane_title else ""
        if pane_title and pane_title.strip() == service_name:
            return pane
    return None


def _stop_pane_processes(pane: libtmux.Pane, timeout: int = 5) -> list[psutil.Process]:
    """
    Stop processes in a pane gracefully, then forcefully if needed.

    Args:
        pane(libtmux.Pane): Tmux pane object
        timeout (int): Timeout in seconds for waiting for process to exit

    Returns:
        list[psutil.Process]: List of child processes that were found
    """
    # Get child processes before stopping
    try:
        pane_pid = pane.pane_pid
        if pane_pid is None:
            return []
        bash_pid = int(pane_pid)
        parent_proc = psutil.Process(bash_pid)
        children = parent_proc.children(recursive=True)
    except psutil.NoSuchProcess:
        children = []

    # Stop the service
    pane.send_keys("^C")

    # Wait for processes to exit
    start_time = time.time()
    while time.time() - start_time < timeout:
        alive = [p for p in children if p.is_running()]
        if not alive:
            break
        time.sleep(0.1)

    # Kill remaining processes forcefully
    for proc in alive:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass

    return children


def tmux_stop(session_name: str = "bec", timeout: int = 5) -> None:
    """
    Stop the services from the given tmux session.

    1. Send Ctrl+C (SIGINT) to all panes.
    2. Wait up to `timeout` seconds for processes to exit.
    3. Kill remaining processes if not exited.
    4. Kill the tmux session.

    Args:
        session_name (str): Name of the tmux session (default: "bec")
        timeout (int): Timeout in seconds for waiting for processes to exit (default: 5)
    """
    tmux_server = _get_tmux_server()

    avail_sessions = tmux_server.sessions.filter(session_name=session_name)
    if not avail_sessions:
        return

    session = avail_sessions[0]

    # Stop all panes
    for pane in session.panes:
        _stop_pane_processes(pane, timeout)

    # Kill tmux session
    try:
        session.kill_session()
    except LibTmuxException:
        # session may already exit itself if all panes are gone
        pass


def tmux_stop_service(service_name: str, session_name: str = "bec", timeout: int = 5) -> bool:
    """
    Stop a single service in a specific pane within a tmux session.

    Args:
        service_name (str): Name of the service to stop
        session_name (str): Name of the tmux session (default: "bec")
        timeout (int): Timeout in seconds for waiting for process to exit (default: 5)

    Returns:
        bool: True if service was stopped successfully, False otherwise
    """
    tmux_server = _get_tmux_server()

    avail_sessions = tmux_server.sessions.filter(session_name=session_name)
    if not avail_sessions:
        return False

    session = avail_sessions[0]
    target_pane = _find_pane_by_title(session, service_name)

    if not target_pane:
        return False

    # Stop the service processes
    _stop_pane_processes(target_pane, timeout)
    return True


def tmux_start_service(
    bec_path: str, service_name: str, service_config, session_name: str = "bec"
) -> bool:
    """
    Start a single service in an existing tmux session.

    Args:
        bec_path (str): Path to the BEC source code
        service_name (str): Name of the service to start
        service_config: ServiceDesc object containing service configuration
        session_name (str): Name of the tmux session (default: "bec")

    Returns:
        bool: True if service was started successfully, False otherwise
    """
    tmux_server = _get_tmux_server()

    avail_sessions = tmux_server.sessions.filter(session_name=session_name)
    if not avail_sessions:
        return False

    session = avail_sessions[0]
    target_pane = _find_pane_by_title(session, service_name)

    if not target_pane:
        return False

    # Start the service
    activate_venv(
        target_pane,
        service_name=service_name,
        service_path=service_config.path.substitute(base_path=bec_path),
    )
    target_pane.send_keys(service_config.command)

    # Wait if a wait function is provided
    wait_func = service_config.wait_func
    if callable(wait_func):
        wait_func()

    return True


def tmux_restart_service(
    bec_path: str, service_name: str, service_config, session_name: str = "bec", timeout: int = 5
) -> bool:
    """
    Restart a single service in a specific pane within a tmux session.

    Args:
        bec_path (str): Path to the BEC source code
        service_name (str): Name of the service to restart
        service_config: ServiceDesc object containing service configuration
        session_name (str): Name of the tmux session (default: "bec")
        timeout (int): Timeout in seconds for waiting for process to exit (default: 5)

    Returns:
        bool: True if service was restarted successfully, False otherwise
    """
    # Stop the service
    if not tmux_stop_service(service_name, session_name, timeout):
        return False

    # Start the service
    return tmux_start_service(bec_path, service_name, service_config, session_name)
