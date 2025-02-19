from bec_lib.client import BECClient
from bec_lib.logger import bec_logger
from bec_lib.scan_report import ScanReport

logger = bec_logger.logger


def log_message_args_kwargs(*args, **kwargs):
    """Log the args and kwargs from the procedure execution message. Intended for testing."""
    logger.info(
        f"Builtin procedure log_message_args_kwargs called with args: {args} and kwargs: {kwargs}"
    )


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
