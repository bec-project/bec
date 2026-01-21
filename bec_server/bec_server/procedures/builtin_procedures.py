import builtins
import time

from bec_lib.client import BECClient
from bec_lib.logger import bec_logger
from bec_lib.scan_report import ScanReport

logger = bec_logger.logger


def log_message_args_kwargs(*args, **kwargs):
    """Log the args and kwargs from the procedure execution message. Intended for testing."""
    logger.success(
        f"Builtin procedure log_message_args_kwargs called with args: {args} and kwargs: {kwargs}"
    )


def sleep(*, time_s: float):
    """Sleep for time_s seconds. Intended for testing, or for adding a pause between elements in a queue."""
    logger.success(f"Sleeping for {time_s} s.")
    time.sleep(time_s)


def run_scan(scan_name: str, args: tuple, parameters: dict, *, bec: BECClient):
    """Run a scan on the server in a procedure worker, and wait for it to finish. Will translate
    any string arguments into devices if they exist in the device manager."""

    args = tuple(
        bec.device_manager.devices[arg] if arg in bec.device_manager.devices else arg  # type: ignore
        for arg in args
    )
    if (scan := getattr(bec.scans, scan_name, None)) is None:
        raise ValueError(f"Scan {scan_name} doesn't exist in this client!")
    scan_report: ScanReport = scan(*args, **parameters)
    scan_report.wait()


def run_script(script_id: str, *, bec: BECClient):
    """Run the script with the given script ID on the server. Mainly intended for use from the GUI IDE view
    or as a helper for similar utilities."""
    bec._run_script(script_id)


def run_macro(macro_name: str, params: tuple[tuple, dict] | None = None, *, bec: BECClient):
    """Run the given macro. Must be a macro automatically loaded from the plugin repository, for security reasons.
    For a nicer interface from the BEC console, the `bec.proc.run_macro` helper can be used, which passes on arguments
    in a more natural fashion."""
    if params is None:
        params = ((), {})
    if (macro := builtins.__dict__.get("_user_macros", {}).get(macro_name)) is not None:
        macro["cls"](*params[0], **params[1])
    else:
        raise ValueError(f"Macro {macro_name} not found in the client namespace!")
