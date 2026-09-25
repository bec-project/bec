from bec_lib.scan_report import ScanReport

# BECClient injects the active scans object into builtins when loading this interface.


def umv(*args) -> ScanReport:
    """Updated absolute move (i.e. blocking) for one or more devices.

    Returns:
        ScanReport: Status object.

    Examples:
        >>> umv(dev.samx, 1)
        >>> umv(dev.samx, 1, dev.samy, 2)
    """
    return scans.umv(*args, relative=False)  # noqa: F821


def umvr(*args) -> ScanReport:
    """Updated relative move (i.e. blocking) for one or more devices.

    Returns:
        ScanReport: Status object.

    Examples:
        >>> umvr(dev.samx, 1)
        >>> umvr(dev.samx, 1, dev.samy, 2)
    """
    return scans.umv(*args, relative=True)  # noqa: F821


def mv(*args) -> ScanReport:
    """Absolute move for one or more devices.

    Returns:
        ScanReport: Status object.

    Examples:
        >>> mv(dev.samx, 1)
        >>> mv(dev.samx, 1, dev.samy, 2)
    """
    return scans.mv(*args, relative=False)  # noqa: F821


def mvr(*args) -> ScanReport:
    """Relative move for one or more devices.

    Returns:
        ScanReport: Status object.

    Examples:
        >>> mvr(dev.samx, 1)
        >>> mvr(dev.samx, 1, dev.samy, 2)
    """
    return scans.mv(*args, relative=True)  # noqa: F821
