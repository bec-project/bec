"""Use the simulated ophyd control layer for e2e helper unit tests."""

import os

os.environ.setdefault("OPHYD_CONTROL_LAYER", "dummy")
