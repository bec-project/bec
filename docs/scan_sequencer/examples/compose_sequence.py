"""Compose a sequence with typed components and print its JSON.

Run from the repository root:
    python docs/scan_sequencer/examples/compose_sequence.py

The sequence visits each position twice, moving before acquiring one time scan.
This script only constructs and prints the document; it does not access hardware.
"""

from __future__ import annotations

from bec_lib.scan_sequencer import Schedule
from bec_lib.scan_sequencer.components import ForEach, Repeat, ScanCall, Sequence, Set

sequence = Schedule(
    id="alternating_positions",
    revision=1,
    root=Repeat(
        id="repeat_measurement",
        count=2,
        body=ForEach(
            id="positions",
            variable="position",
            values=[1, -1],
            body=Sequence(
                id="measure",
                children=[
                    Set(id="set_position", device="samx", value={"ref": "position"}, wait=True),
                    ScanCall(
                        id="acquire",
                        name="time_scan",
                        positional=[1, 0.1],
                        arguments={"metadata": {"position": {"ref": "position"}}},
                    ),
                ],
            ),
        ),
    ),
)

if __name__ == "__main__":
    print(sequence.model_dump_json(indent=2))

# In an existing BEC IPython session, load this file with %run -i, then:
#
# from bec_lib.scan_sequencer import Sequencer
#
# document = sequence.model_dump_json()
# sequencer = Sequencer(bec)
# sequencer.validate(document)
# print(sequencer.export(document, format="description"))
# print(sequencer.export(document, format="python"))
#
# To execute through the existing procedure queue:
# status = bec.proc.request_new("run_sequence", ((document,), {}), queue="sequences")
# status.wait(raise_on_failure=True)
