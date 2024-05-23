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
        "--no-tmux", action="store_true", default=False, help="Do not start processes in tmux"
    )
    start.add_argument(
        "--start-redis", action="store_true", default=False, help="Start Redis server"
    )
    start.add_argument(
        "--no-persistence", action="store_true", default=False, help="Do not load/save RDB file"
    )
    command.add_parser("stop", help="Stop the BEC server")
    restart = command.add_parser("restart", help="Restart the BEC server")
    restart.add_argument(
        "--config", type=str, default=None, help="Path to the BEC service config file"
    )
    command.add_parser("attach", help="Open the currently running BEC server session")

    args = parser.parse_args()
    try:
        # 'stop' has no config
        config = args.config
    except AttributeError:
        config = None

    service_handler = ServiceHandler(
        bec_path=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        config_path=config,
        no_tmux=args.no_tmux if "no_tmux" in args else False,
        start_redis=args.start_redis if "start_redis" in args else False,
        no_persistence=args.no_persistence if "no_persistence" in args else False,
    )
    if args.command == "start":
        service_handler.start()
    elif args.command == "stop":
        service_handler.stop()
    elif args.command == "restart":
        service_handler.restart()
    elif args.command == "attach":
        server = libtmux.Server()
        session = server.find_where({"session_name": "bec"})
        if session is None:
            print("No BEC session found")
            return
        session.attach_session()


if __name__ == "__main__":
    main()
