from bec_server.data_processing.dap_framework_refactoring.dap_blocks import DAPBlockMessage


class DAPWorker:

    def __init__(self):
        self.blocks = []

    def add_blocks(self, blocks: list[dict]):
        """Add a block to the pipeline."""
        self.blocks = [block["block"](**block["kwargs"]) for block in blocks]

    def clear(self):
        """Clear the pipeline."""
        self.blocks = []

    def run(self, msg: DAPBlockMessage) -> DAPBlockMessage:
        """Run the pipeline."""
        for block in self.blocks:
            msg = block.run(msg, **block.kwargs)
        return msg
