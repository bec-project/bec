from threading import Event, Thread

from bec_lib.callback_handler import EventType
from bec_lib.client import BECClient


class BeamlineMonitor:
    def __init__(self, client: BECClient) -> None:
        self.client = client
        self.client.callbacks.register(EventType.DEVICE_UPDATE, self.handle_device_update)

    def handle_device_update(self, *args, **kwargs):
        args
        kwargs


if __name__ == "__main__":
    client = BECClient()
    shutdown_event = Event()

    def run():
        monitor = BeamlineMonitor(client)
        shutdown_event.wait()

    monitor_thread = Thread(target=run)
    monitor_thread.start()

    try:
        monitor_thread.join()
    except KeyboardInterrupt:
        shutdown_event.set()
        monitor_thread.join(3)
