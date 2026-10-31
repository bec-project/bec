# Device backend extraction: local validation

Validation used matching BEC and ophyd_devices feature checkouts in a dedicated
Python3.12.2 environment on macOS arm64. Unit tests used mock PVs and simulation.
Service workflows and the complete end-to-end suite provisioned temporary Redis
instances and real isolated BEC services. Full scans and actual Podman/subprocess
procedure execution were exercised. The negative EPICS test used an intentionally
unreachable prefix with pyepics. No real hardware or ADSim was used.

## Results

- Complete end-to-end suite: **81 passed** (79 non-ACL + 2 separately isolated ACL
  tests), with no skipped cases. Includes full scan workflows, config reloads,
  unreachable-device handling, actors, worker-image build, actual Podman/subprocess
  procedure execution and credential reuse.
- Neutral contracts, complete device-server suite and adapter regressions: **371 passed**.
- Complete ophyd_devices test suite: **473 passed**, with the optional constructor-PV
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
- Six focused review/fix cycles covered all seven review angles across both PRs.
  The final independent A/B, C/D and E/F/G reviewers reported no remaining
  introduced/exposed findings. Controlled native completion/close probes and
  25,040 failure-window state checks supplemented the regression suites.

The focused suite includes real Redis workflows for the synchronous provider and
an opaque test-only asyncio provider. These cover request completion, pending
readback, file/progress/preview/dynamic publication and indices, stop and teardown.
The asyncio proof additionally covers dependencies across owner loops, cancellation
before admission, borrowed dependency observation, slow callbacks and bounded cleanup.
New regressions cover receipt-time scan context, queued publication before both
pending and immediate completion, per-tree fence isolation, causal peer updates,
publication failures, bounded error summaries, recovery admission/drain and batch
error readiness/envelope identity.

The complete 81-case e2e run passed before the final teardown-error refinement.
After that refinement, all **10 affected configuration/unreachable-device e2e cases**
passed again, alongside the complete focused and device-library suites above.
The final cleanup regressions distinguish historical publication errors from
failed native unsubscribe work and keep global shutdown admission fenced.

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

## Reproduce the full end-to-end checks

Install matching `bec_lib`, `bec_server`, `bec_ipython_client`, `pytest_bec_e2e`
and ophyd_devices checkouts in the dedicated environment. Procedure tests need
working Podman, the normal requirements image and a host-networked `local_bec` pod.
For a remote Podman VM, set `BEC_TEST_CONTAINER_REDIS_HOST` to a hostname through
which its containers can reach the host Redis (for this run,
`host.containers.internal`). This changes only the test worker's Redis route.

Use a short temporary path on macOS for Redis Unix sockets and a PATH without
xterm for headless service launch. From the BEC checkout, set `E2E_RUN` to a fresh
temporary directory, create its `main` and `acl` subdirectories, and run:

```sh
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 OPHYD_CONTROL_LAYER=pyepics python -m pytest -v \
  -p bec_lib.tests.fixtures -p pytest_bec_e2e.plugin -p pytest_redis \
  -p random_order -p pytest_timeout --random-order --timeout=180 \
  --start-servers --bec-redis-cmd "$(command -v redis-server)" \
  --basetemp="$E2E_RUN/main-tmp" --files-path="$E2E_RUN/main" \
  --ignore=bec_ipython_client/tests/end-2-end/test_acl_e2e.py \
  bec_ipython_client/tests/end-2-end

PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 OPHYD_CONTROL_LAYER=pyepics python -m pytest -v \
  -p bec_lib.tests.fixtures -p pytest_bec_e2e.plugin -p pytest_redis \
  -p random_order -p pytest_timeout --random-order --timeout=180 \
  --start-servers --flush-redis --bec-redis-cmd "$(command -v redis-server)" \
  --basetemp="$E2E_RUN/acl-tmp" --files-path="$E2E_RUN/acl" \
  bec_ipython_client/tests/end-2-end/test_acl_e2e.py
```

The final run passed all 79 non-ACL tests and both ACL tests. Earlier dummy-layer
runs did not validate the deliberately unreachable EPICS device; use pyepics for
the complete suite. No existing CA discovery lists or gateways were replaced.
The task-owned Podman pod and initially stopped VM were cleaned up afterwards;
the pre-existing Docker ADSim container was left running.

## Broader test and release limits

The earlier wider local BEC unit run was **not fully green** and was not rerun in
this follow-up. With plugin-generation tests excluded, it ended with
**3123 passed,3 actor workflow failures,5 skipped**. Failures were
polling/subscription/start-request timeouts in the unmodified actor tests. Earlier
plugin scaffold/system tests also failed because they require exactly one installed
beamline plugin and a pyside6-uic command visible to subprocesses. No claim is made
that these checks passed, and these unrelated areas were not changed to obtain a
passing run. The temporary plugin installed by those fixtures was removed from
the isolated environment afterwards.

Linux and the remaining Python CI matrix were not executed locally. Production
ophyd-async integration remains follow-up work. This extraction
preserves BEC's push message model and existing device/configuration interfaces.
The two PRs must be reviewed and deployed together. Allocate actual first-release
dependency floors before publishing package releases; current checkout versions
and old numeric floors alone do not establish companion compatibility.
