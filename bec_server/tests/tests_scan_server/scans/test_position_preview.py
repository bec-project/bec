"""Preview metadata must reproduce the trajectory executed by each scan."""

import inspect
import json

import numpy as np
import pytest

from bec_lib import position_generators
from bec_lib.scan_preview import generate_scan_positions
from bec_server.scan_server.scans import position_generators as legacy_generators


@pytest.mark.parametrize(
    "name,args,kwargs",
    [
        ("line_scan", ("samx", -1, 1, "samy", -2, 2), {"steps": 5}),
        ("log_scan", ("samx", -1, 1, "samy", -2, 2), {"steps": 5}),
        ("grid_scan", ("samx", -1, 1, 3, "samy", -2, 2, 4), {"snaked": False}),
        ("hexagonal_scan", ("samx", -1, 1, 0.5, "samy", -2, 2, 0.5), {}),
        ("fermat_scan", ("samx", -1, 1, "samy", -2, 2, 0.5), {"spiral_type": 1}),
        ("round_scan", ("samx", "samy", 0, 2, 3, 4), {"center_1": 1}),
        ("round_roi_scan", ("samx", -2, 2, "samy", -2, 2, 0.5, 4), {"center_2": 1}),
        ("multi_region_line_scan", ("samx",), {"regions": [(-1, 0, 3), (0, 2, 4)]}),
        ("multi_region_grid_scan", ("samx", "samy"), {"regions": [((-1, 1, 3), (-2, 2, 4))]}),
    ],
)
def test_advertised_preview_matches_scan(scan_assembler, name, args, kwargs):
    scan = scan_assembler(name, *args, relative=False, **kwargs)
    cls = type(scan)
    bound = inspect.signature(cls).bind(*args, relative=False, **kwargs)
    bound.apply_defaults()
    info = json.loads(
        json.dumps(
            {
                "position_preview": cls.position_preview,
                "position_preview_parameter_mapping": cls.position_preview_parameter_mapping,
                "arg_bundle_size": cls.arg_bundle_size,
            }
        )
    )
    preview = generate_scan_positions(info, bound.arguments)
    scan.prepare_scan()
    np.testing.assert_allclose(preview, scan.positions)
    assert scan.scan_info.position_preview == info["position_preview"]
    assert (
        scan.scan_info.position_preview_parameter_mapping
        == info["position_preview_parameter_mapping"]
    )

    published_info = scan.actions._build_scan_status_info({}, {}, None)
    assert published_info["position_preview"] == info["position_preview"]
    assert (
        published_info["position_preview_parameter_mapping"]
        == info["position_preview_parameter_mapping"]
    )


def test_legacy_import_preserves_function_and_direction_identity():
    assert legacy_generators.line_scan_positions is position_generators.line_scan_positions
    assert legacy_generators.Direction is position_generators.Direction
