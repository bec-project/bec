# Device backend extraction: local validation

Validation used matching BEC and ophyd_devices feature checkouts in a dedicated
Python3.12.2 environment on macOS arm64. Hardware access was mocked. The Redis
workflows provision their own temporary loopback Redis instances; existing services
were not used. No ADSim or full scan-server scan was run.

## Results

- Neutral contracts, complete device-server suite and adapter regressions: **332 passed**.
- Complete ophyd_devices test suite: **457 passed**, with the optional constructor-PV
  fixture described below. Existing simulation numeric/deprecation warnings remain.
- Public protocol/provider typing: positive example has **0 errors**; negative
  example has **4 expected assignment errors**. The checker asserts diagnostic
  locations, including rejection of native metadata keys in BEC readings.
- Black/isort checks and git diff whitespace checks passed. All changed Python
  files parsed with the Python3.11 grammar; other runtime versions were not run.
- Pylint errors-only passed for production dispatch, contracts and adapters. Full
  lint of new modules retained only four pre-existing complexity notes in the
  relocated serializer.
- All three package wheels built with Hatchling and contained the new modules.
- Independent read-only code, architecture and compatibility reviews closed all
  introduced critical/high/medium findings in their stated scopes. Additional
  probes covered40 native completion/close races with exactly-once notification.

The focused suite includes real Redis workflows for the synchronous provider and
an opaque test-only asyncio provider. These cover request completion, pending
readback, file/progress/preview/dynamic publication and indices, stop and teardown.
The asyncio proof additionally covers dependencies across owner loops, cancellation
before admission, borrowed dependency observation, slow callbacks and bounded cleanup.

## Reproduce the focused checks

Install both matching checkouts into the same dedicated environment as described
in [the backend guide](device_backend.md). From the BEC checkout, set
`OPD_CHECKOUT` to the companion ophyd_devices checkout:

```sh
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 OPHYD_CONTROL_LAYER=dummy python -m pytest -q \
  -p bec_lib.tests.fixtures -p random_order -p pytest_timeout --random-order \
  --timeout=20 -p no:cacheprovider --tb=short --show-capture=no \
  bec_lib/tests/test_device_backend.py bec_server/tests/tests_device_server \
  "$OPD_CHECKOUT/tests/test_bec_adapters.py"

OPHYD_CONTROL_LAYER=dummy python bec_lib/tests/typing/check_device_backend.py

OPHYD_CONTROL_LAYER=dummy python -m pylint --errors-only \
  bec_lib/bec_lib/device_backend.py bec_server/bec_server/device_server \
  "$OPD_CHECKOUT/ophyd_devices/bec_adapters"
```

From the companion checkout, using the same environment:

```sh
PYTHONPATH="$PWD/tests" PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 OPHYD_CONTROL_LAYER=dummy \
  python -m pytest -q -p bec_lib.tests.fixtures -p constructor_pv_fixture \
  -p random_order -p pytest_timeout --random-order --timeout=20 \
  -p no:cacheprovider --tb=short --show-capture=no tests
```

`constructor_pv_fixture` is opt-in. It substitutes the repository MockPV only for
`test_override_suffixes` and `test_override_suffixes_warns_on_nonimplemented`, whose
native EPICS constructors cannot allocate PVs under the dummy control layer. It
preserves their suffix/warning assertions without network I/O. Other tests retain
their normal fixtures. Disable automatic plugin loading to avoid unrelated GUI/Xvfb
plugin requirements; BEC, random-order and timeout plugins are loaded explicitly.

## Broader test and release limits

The wider local BEC run is **not fully green**. With plugin-generation tests excluded,
it ended with **3123 passed,3 actor workflow failures,5 skipped**. Failures were
polling/subscription/start-request timeouts in the unmodified actor tests. Earlier
plugin scaffold/system tests also failed because they require exactly one installed
beamline plugin and a pyside6-uic command visible to subprocesses. No claim is made
that these checks passed, and these unrelated areas were not changed to obtain a
passing run. The temporary plugin installed by those fixtures was removed from
the isolated environment afterwards.

Linux and the remaining Python CI matrix were not executed locally. Production
ophyd-async integration and full scan validation are deferred. This extraction
preserves BEC's push message model and existing device/configuration interfaces.
The two PRs must be reviewed and deployed together. Allocate actual first-release
dependency floors before publishing package releases; current checkout versions
and old numeric floors alone do not establish companion compatibility.
