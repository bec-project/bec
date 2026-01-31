from __future__ import annotations

from typing import TYPE_CHECKING

import rich
from IPython.core.magic import Magics, line_magic, magics_class

from bec_lib.metadata_schema import get_metadata_schema_for_scan

if TYPE_CHECKING:
    from bec_ipython_client import BECIPythonClient


@magics_class
class BECMagics(Magics):
    def __init__(self, shell, client: BECIPythonClient):
        super(BECMagics, self).__init__(shell)
        self.client = client

    @line_magic
    def abort(self, line):
        "Request a scan abortion"
        return self.client.queue.request_scan_abortion()

    @line_magic
    def reset(self, line):
        "Request a scan queue reset"
        return self.client.queue.request_queue_reset()

    @line_magic
    def resume(self, line):
        "Resume the scan"
        self.client.queue.request_scan_continuation()
        if self.client._live_updates:
            return self.client._live_updates.continue_request()
        return None

    @line_magic
    def pause(self, line):
        "Request a scan pause"
        return self.client.queue.request_scan_interruption(deferred_pause=False)

    @line_magic
    def deferred_pause(self, line):
        "Request a deferred pause"
        return self.client.queue.request_scan_interruption(deferred_pause=True)

    @line_magic
    def restart(self, line):
        "Request a scan restart"
        request_id = self.client.queue.request_scan_restart()
        return request_id

    @line_magic
    def halt(self, line):
        "Request a scan halt, i.e. abort without cleanup."
        return self.client.queue.request_scan_halt()

    @line_magic
    def server_restart(self, line):
        "Request a server restart"
        return self.client._request_server_restart()

    @line_magic
    def schema(self, line):
        "print the metadata schema for a given scan"
        scans = self.shell.user_ns["scans"]
        if not hasattr(scans, line):
            print(f"Scan {line} does not exist.")
        else:
            rich.print_json(data=get_metadata_schema_for_scan(line).model_json_schema())

    @line_magic
    def su(self, line):
        "Switch user"
        # pylint: disable=protected-access
        self.client._client.acl.login(line)
        self.client._client._update_username()
        self.client._refresh_ipython_username()
