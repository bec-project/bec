from bec_messages import messages


class EmitterBase:
    def __init__(self, connector) -> None:
        self.connector = connector

    def on_init(self, scan_id: str):
        pass

    def on_scan_point_emit(self, scan_id: str, point_id: int):
        pass

    def on_baseline_emit(self, scan_id: str):
        pass

    def on_cleanup(self, scan_id: str):
        pass

    def on_scan_status_update(self, status_msg: messages.ScanStatusMessage):
        pass

    def shutdown(self):
        pass
