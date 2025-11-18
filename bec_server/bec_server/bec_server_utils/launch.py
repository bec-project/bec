import argparse
import os

import libtmux

from bec_server.bec_server_utils.service_handler import ServiceHandler


def main():
    """
    Launch the BEC server in a tmux session. All services are launched in separate panes.
    """
    parser = argparse.ArgumentParser(description="Utility tool managing the BEC server")
    command = parser.add_subparsers(dest="command")
    start = command.add_parser("start", help="Start the BEC server")
    start.add_argument(
        "--config", type=str, default=None, help="Path to the BEC service config file"
    )

    start.add_argument(
        "--start-redis", action="store_true", default=False, help="Start Redis server"
    )
    start.add_argument(
        "--no-persistence", action="store_true", default=False, help="Do not load/save RDB file"
    )
    start.add_argument(
        "--interface",
        type=str,
        default=None,
        help="Interface to use (tmux, iterm2, systemctl, subprocess)",
    )
    start.add_argument(
        "--service",
        type=str,
        default=None,
        help="Start a specific service only (e.g., scan_server, device_server)",
    )
    stop = command.add_parser("stop", help="Stop the BEC server")
    stop.add_argument(
        "--service",
        type=str,
        default=None,
        help="Stop a specific service only (e.g., scan_server, device_server)",
    )
    restart = command.add_parser("restart", help="Restart the BEC server")
    restart.add_argument(
        "--config", type=str, default=None, help="Path to the BEC service config file"
    )
    restart.add_argument(
        "--interface",
        type=str,
        default=None,
        help="Interface to use (tmux, iterm2, systemctl, subprocess)",
    )
    restart.add_argument(
        "--service",
        type=str,
        default=None,
        help="Restart a specific service only (e.g., scan_server, device_server)",
    )
    command.add_parser("attach", help="Open the currently running BEC server session")

    args = parser.parse_args()
    try:
        # 'stop' has no config
        config = args.config
    except AttributeError:
        config = None

    interface = getattr(args, "interface", None)

    service_handler = ServiceHandler(
        bec_path=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        config_path=config,
        interface=interface,
        start_redis=args.start_redis if "start_redis" in args else False,
        no_persistence=args.no_persistence if "no_persistence" in args else False,
    )
    if args.command == "start":
        if hasattr(args, "service") and args.service:
            service_handler.start_service(args.service)
        else:
            service_handler.start()
    elif args.command == "stop":
        if hasattr(args, "service") and args.service:
            service_handler.stop_service(args.service)
        else:
            service_handler.stop()
    elif args.command == "restart":
        if hasattr(args, "service") and args.service:
            service_handler.restart_service(args.service)
        else:
            service_handler.restart()
    elif args.command == "attach":
        if os.path.exists("/tmp/tmux-shared/default"):
            # if we have a shared socket, use it
            server = libtmux.Server(socket_path="/tmp/tmux-shared/default")
        else:
            server = libtmux.Server()
        session = server.find_where({"session_name": "bec"})
        if session is None:
            print("No BEC session found")
            return
        session.attach_session()


if __name__ == "__main__":
    import sys

    sys.argv = ["bec-server", "start"]
    main()
