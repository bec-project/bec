class ScanAbortion(Exception):
    pass


class ScanHalting(Exception):
    pass


class LimitError(Exception):
    pass


class DeviceMessageError(Exception):
    pass


class DeviceInstructionError(Exception):
    pass
