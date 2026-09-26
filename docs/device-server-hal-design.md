# Device server hardware abstraction boundary

## Objective and scope

Separate the device server's BEC transport and request tracking from Ophyd execution. The first
implementation remains Ophyd, but another hardware layer must be injectable without changing
`device_server.py` or `RequestHandler`. This is an internal Python API boundary: existing Redis
endpoints, messages, device configurations and client RPC payloads remain compatible. No HTTP
server, new hardware implementation, dependency removal or per-device backend selection is part
of this change.

Worktree: `/private/tmp/bec-device-server-hal`. Branch: `refactor/device-server-hal`.
Base: `main`, `1aef9ef4ac334758f5251a0dee190b6d55ba3e86`. The original checkout and its untracked
adapter experiments are not a foundation for this work. The initial implementation did not request commits, pushes or PRs; branch publication was
authorized subsequently.

## Findings from the existing implementation

`device_server.py` combines transport, aggregation, raw hardware access, subscription cleanup,
enum conversion and stage state checks. Its request handler discovers device identities by
inspecting native status objects. RPC processing calls back into these hardware-specific helpers.
The device manager owns construction, connections, callbacks, config application and serialization.
Config requests also access native travel-limit signals and destruction directly.

Existing downstream tools import `DeviceManagerDS`, `DSDevice`, and serializer helpers from their
current paths. `DSDevice.obj` must stay the raw hardware object. The public static construction
helper and its device-class lookup must retain their calling convention. Wire contracts in
`bec_lib/messages.py` and `MessageEndpoints` need no changes.

## Intended architecture

### Neutral service and request layer

`device_server.py` retains service startup/shutdown, Redis instruction and stop callbacks,
validation, request error reporting and instruction dispatch. It must have no imports from
`ophyd` or `ophyd_devices`, no native type checks, and no direct hardware method calls.

`device_layer.py` defines typed protocols for an injectable layer and four explicit routes:

- `instructions`: dispatch the established set/read/trigger/kickoff/complete/stage/unstage/pre_scan
  actions, stop selected devices, and perform native status completion side effects.
- `rpc`: execute calls and preserve both RPC replies and instruction completion responses.
- `serialization`: describe a device using the existing device-info schema.
- `configuration`: handle config requests, lifecycle changes and native config application.

The layer owns its device manager and exposes initialize/shutdown lifecycle hooks and native
exception-to-device-name discovery. The default layer factory imports the Ophyd implementation
lazily. A keyword-only factory injection enables a fake or future layer with no Ophyd import.
These APIs describe the existing service-level boundary; simultaneous mixed backends will need
a later routing policy and configuration-schema decision.

Move `RequestHandler` into a neutral module and re-export it from `device_server.py`. Define a
general `DeviceStatus` protocol in a neutral status module with `device_name`, `status_type`,
`done`, `success`, `exception()` and `add_callback(callback)`. RequestHandler owns the instruction
association separately from operation status. Callbacks receive the
general status, not the native object. The Ophyd status adapter holds the native status and
per-operation context; the request handler never reads `.device`, `.obj`, `.kind` or `.__dict__`.
It inspects every status and treats unsuccessful completion without an exception as a failure.

### Ophyd implementation

Create a dedicated `device_server/ophyd/` package with instruction, RPC, serialization,
configuration, manager and status modules. Move existing native implementations with minimal
semantic changes. Explicit route methods replace cross-calls through `DeviceServer`; avoid a
catch-all `__getattr__` facade. Retain small compatibility modules at existing manager,
serializer, config-handler and RPC import paths where useful to downstream callers.

The manager delegates device-info extraction to the serialization route and native config
application to the configuration route. Config cancellation, rollback, device ordering and
publication semantics stay intact. The Ophyd layer continues constructing all hardware objects
on the device server. No raw object becomes a client-facing adapter.

## Status and lifecycle invariants

- Native status callbacks may run synchronously during registration. Bind instruction and device
  context first, append the generalized status to the request, then register callbacks without
  holding a non-reentrant handler lock.
- Cache refresh and temporary subscription cleanup occur before terminal instruction completion.
  Completion-side readback must not register or replace the original request.
- Cleanup must accept subscription ID zero and run once per operation. Failures in completion
  side effects must resolve the request with error information instead of leaving it pending.
- Several immediate statuses in one instruction must produce exactly one terminal response.
  Zero-status operations and patched status counts must still complete.
- Preserve synchronous list/None results for stage and unstage, optional None from pre_scan,
  rejection of None from complete, and the existing legacy kickoff signature behavior.
- Preserve status RPC dictionaries (`type`, `RID`, `success`, `timeout`, `done`, `settle_time`),
  namedtuple serialization, `result_is_status`, read-only checks, error information and stop IDs.
- Preserve retry/raise/buffer behavior, native staging checks, enum conversion, lazy signal
  description, USER_ACCESS, disabled-device restoration and failed-config rollback.

## Development rounds and review gates

1. Review this design against implementation and downstream callers before coding. Resolve
   interface ambiguities and record corrections here.
2. Implement neutral status/request handling and extract the Ophyd routes. Preserve import
   compatibility, update tests to target the new owning modules, and add neutral-layer tests.
   Run the relevant tests, save a complete diff (including new files), and run all seven angles
   of `bec-focused-review`. Reviewers are read-only; the coordinator applies accepted fixes as
   part of the user's implementation request.
3. Address verified review findings with regression tests, run the affected package suite and
   simulated service integration when practical, then repeat the focused review on the final
   delta. Finish only with reported evidence and any material limitations.

Each independent writer uses an isolated worktree and returns a patch; integration is owned by
the coordinator. Read-only architecture/review agents share the integration worktree. Use a
worktree-local virtualenv with local editable installs, leaving the shared `bec-312` installs
untouched. Do not restart or alter the user's running services.

## Validation and acceptance

- Existing device server, RPC, config handler, serializer and manager tests pass in random order.
- Native pending and already-complete statuses both work through the generalized interface.
- A plain non-Ophyd status and fake layer complete an instruction through the real request path.
- Tests cover failure without exception, multiple immediate statuses, nested subscription cleanup
  including ID zero, completion-side errors, and original-request retention during cache refresh.
- Tests prove all four API routes are used and preserve serialization/config behavior.
- AST/import isolation checks ensure the neutral service and request/status modules do not
  depend on Ophyd; fake-layer construction must avoid importing the default backend.
- Run affected server package tests; exercise simulated instruction transport/integration if the
  environment supports it. Report blockers precisely rather than claiming unrun coverage.
- Run Black/isort on changed Python files and inspect the final diff for unrelated changes.

## Design review record

Initial research reviewed both core callers and sibling `ophyd_devices` imports. It rejected
adopting the untracked broad capability adapters because they depend on unrelated untracked
code. It identified immediate-callback ordering, request replacement during readback, false
success without an exception, and subscription ID zero as required regression cases for the
new boundary. Independent review of this written design follows before implementation.

Both independent reviewers approved the boundary with the following corrections, accepted
before implementation:

- Adapter completion is adapter-owned: native completion runs guarded, once-only side effects,
  captures errors, then publishes settled done/success/exception and invokes generic callbacks.
  A second status cannot observe completion while the first status's finalizer still runs.
- Cleanup still runs after a request has been removed or stopped. Request existence controls
  responses, not hardware cleanup.
- Bootstrap order is connector/service, RequestHandler, `layer_factory(server)`, route/manager
  binding, layer initialization, then instruction subscription. Config and serialization routes
  exist before config loading begins.
- Neutral validation may use device collections, enabled/read-only flags, per-device metadata,
  and scan metadata. It must never access `.obj`. Legacy compatibility modules may load Ophyd;
  neutral imports must not load them. Verify this in a fresh subprocess.
- The layer owns manager shutdown exactly once. Stop instruction admission and quiesce executor
  work before layer shutdown, disable late callback publication before closing transport, and
  retain cleanup for late native completion. Tests will check shutdown ordering.
- Configuration routes must actually handle config parse/application and lifecycle operations;
  merely exposing an unused attribute is insufficient.

Design review completed; implementation may begin with these invariants.

## Implementation and review round 1

Implemented the neutral layer/status protocols, extracted RequestHandler, and moved Ophyd
instructions, RPC, manager, configuration and serialization into the backend package. Compatibility
modules preserve established imports and raw `DSDevice.obj` behavior. A per-worktree environment
contains editable BEC packages; shared user environments are unchanged.

The initial integrated device-server suite passed 253 tests in random order. Independent focused
review covered A/B/D (all hunks, removed behavior and lifecycle), C/E (callers and reuse), and F/G
(tests/docs and conventions). Verdict: request changes. Verified findings were:

1. A cache-refresh failure in a completed RPC status skipped the per-device terminal message,
   leaving `bec_lib.device.Status.wait()` pending despite the instruction error response.
2. Shutdown set its admission guard before an IDLE publication that could raise and skip cleanup.
3. The manager compatibility module dropped the formerly importable `DeviceConfigError`.
4. The `ResponseState` compatibility import needed explicit export declaration for Pylint.

The review verified generic failure handling, concurrent/immediate completion, request replacement
protection, cold-import isolation, configuration rollback, serialization and normal RPC payloads.
No findings were attributed to merely moved pre-existing code.

## Implementation and review round 2

Completion effects now report per-device failure even when cleanup, staging or cache updates fail.
RPC status serialization uses the adapted outcome with the existing wire keys and boolean success.
Shutdown avoids status publication, drains accepted work, tolerates callback-unregister failures,
closes transport even when backend cleanup raises, and permits retry after incomplete shutdown.
The manager exception and ResponseState exports are restored explicitly. Added regressions cover
immediate and delayed RPC finalization failure, actual client `Status.wait()`, config cache refresh,
shutdown transport/backend failures, and device destruction after unregister failure.

The device-server suite passes 259 tests. The first full server-package run passed 1,397 tests and
had 33 setup errors. All 33 reproduce on a pristine archive of base `1aef9ef4a`: three Podman-dependent
fixtures fail without Podman, and thirty procedure fixtures encounter an existing shared-client
singleton left by DAP tests. These unrelated failures are not changed by this implementation.
Downstream static device construction passed two tests. Final review and simulated end-to-end
validation follow after these fixes.

## Final review and validation

Round 2 reviewers approved production behavior and confirmed all runtime/compatibility findings
were fixed. They identified one order-dependent regression-test assertion: error logging also
uses `connector.xadd`, so the assertion counted a log message as a device response. Round 3 scopes
the assertion to `MessageEndpoints.device_req_status("client-status")`, ignores keyword-only log
calls, and retains the exactly-one-terminal-response check. The original isolated reproduction
passes; the reviewer reran 26 relevant tests with random-order seed 182743 and all passed.

Final evidence (Python 3.12, worktree-local `.venv`):

- Device-server suite: **259 passed** with `--random-order`.
- Server package excluding the DAP test module and three unavailable Podman fixtures:
  **1,432 passed, 3 deselected**. The DAP module passed separately (**1 passed**), for **1,433
  server tests passed** across isolated processes. Separating DAP avoids the verified existing
  singleton leak without omitting procedure or actor coverage.
- Simulated end-to-end tests with fresh service subprocesses and an isolated Redis fixture:
  **4 passed** (motion, limit failure, configuration/RPC changes, cached reads).
- Downstream `ophyd_devices` construction compatibility: **2 passed**.
- Black and isort checks pass for all 26 changed Python files; `git diff --check` is clean.
  Pylint reports 10.00/10 for the neutral modules and extracted instruction/RPC/status/backend
  modules. The repository's existing Pylint configuration emits its known option warnings.
- Fresh-process import blocking proves the neutral service can be imported and instantiated
  with a fake hardware layer without loading `ophyd` or `ophyd_devices`.
- The source checkout's unrelated modifications and untracked adapter experiments remain intact.
  No commits, pushes or pull requests were created during the initial development phase.

Reproduce the principal checks from this worktree:

```bash
.venv/bin/python -m pytest --random-order -q bec_server/tests/tests_device_server
.venv/bin/python -m pytest --random-order -q bec_server/tests \
  --ignore=bec_server/tests/tests_data_processing/test_dap_server.py \
  -k 'not test_api_utils_build and not test_api_utils_run and not test_api_utils_image_exists'
.venv/bin/python -m pytest --random-order -q \
  bec_server/tests/tests_data_processing/test_dap_server.py
PATH="$PWD/.venv/bin:$PATH" OPHYD_CONTROL_LAYER=dummy .venv/bin/python -m pytest -v \
  --start-servers --files-path /private/tmp/bec-hal-e2e \
  bec_ipython_client/tests/end-2-end/test_scans_lib_e2e.py \
  -k 'test_mv_scan_lib or test_mv_raises_limit_error or test_config_updates or test_cached_device_readout'
```

Review snapshots and test logs are retained under `/private/tmp/bec-hal-review-round1`,
`/private/tmp/bec-hal-review-round2`, `/private/tmp/bec-hal-review-round3`, and
`/private/tmp/bec-hal-*.log`. The helper worktrees remain available at
`/private/tmp/bec-device-server-hal-status` and `/private/tmp/bec-device-server-hal-config`;
all delivered changes are integrated in `/private/tmp/bec-device-server-hal`.

Future layers implement the `DeviceLayer` contract and are supplied through `layer_factory`.
Mixed per-device backend selection and making Ophyd an optional distribution dependency remain
future work. Existing Redis endpoints and message schemas are unchanged.

Final focused-review verdict: **approve**. All seven angles are covered by the completed rounds;
the final cross-file/reuse, lifecycle/correctness, and tests/conventions reviews report no unresolved
introduced findings. The review-driven test-only follow-up is approved as well.

## Clean-code follow-up: plan before implementation

The follow-up starts from published commit `63cb6e64342bcf1d0912d8a97ba363d3183294c0`.
General simplicity and explicit ownership drive this work. A possible future ophyd-async layer
is a compatibility constraint, not a reason to add another backend or an execution framework.
The final branch will remain one commit above main; amend it and push with an explicit lease
against the published commit after local validation and focused reviews.

Planned changes:

1. Make configuration execution one-way. The configuration implementation owns parsing and
   device changes; its transport handler owns subscription, scheduling, cancellation and reply
   publication. Retain necessary legacy entry points without routing work back to its origin.
   Remove production branches used only by tests that construct inconsistent private state.
2. Compose instruction and RPC routes with their actual dependencies (manager, connector,
   request handler and small callbacks), rather than a back-reference to the whole service.
   RequestHandler takes only its connector. Give the neutral manager requirements a precise
   type, make cancellation explicit, and document submission/completion and callback threading.
3. Use a typed pending-request record with clear lock ownership. RequestHandler owns instruction
   identity; neutral DeviceStatus no longer requires an instruction. Register a status together
   with its instruction, capture that association once, and keep the stale-request identity guard.
   Expose narrow query/completion methods instead of making RPC inspect request dictionaries.
4. Publish per-device completion from the shared neutral request machinery after backend
   finalization settles the status. This publication remains independent of aggregate-request
   existence and stopped-request suppression, but stops during shutdown. Aggregate completion
   must wait for each registered completion callback to finish publication, including immediate
   and concurrent callbacks; status.done alone cannot establish that ordering. Capture callback
   outcomes in the request record and retain finalization failures in both response channels.
5. Keep native subscription cleanup, staging and cache refresh in Ophyd. Normalize RPC statuses
   once to OphydStatus, remove forwarding/type-check duplication, and expose deliberately shared
   backend helpers instead of accessing another route's private implementation.
6. Expose the stateless serialization module as its route, retaining existing function imports.
   Test observable responses/configuration/lifecycle behavior rather than forwarding topology.

Explicit preservation requirements: native status cleanup still runs after request removal;
subscription zero is valid; immediately finished statuses cannot resolve an instruction early;
stale callbacks cannot complete a replacement request; shutdown drains and suppresses publication;
RPC wire keys, error information, retries, read-only behavior and configuration rollback stay intact.
The adapter's started/done states and the service's closing/shutdown-complete states remain distinct.
No message schemas, endpoint names, dependencies or mixed-backend routing are changed.

A future asynchronous layer can own a persistent event loop behind synchronous service entry
points. Layer entry points must document whether work is complete on return or will report later;
completion callbacks may arrive from any thread and must be marshalled off an event loop before
calling synchronous request publication. No event-loop implementation is added in this round.

Review gates: review this plan before coding; implement independently owned components in isolated
worktrees; integrate and run focused randomized tests; run all seven focused-review angles plus
explicit checks for ownership, duplicate state, redundant wrappers, API clarity and test coupling.
Resolve findings and repeat review for every development round. Run the affected server package,
simulated e2e smoke tests, formatting/lint and full remote CI before declaring completion.


### Clean-code design review

The independent review approved the plan before implementation. It confirmed that completion
publication must be registered even if the aggregate request has already been removed, and that
aggregation must count published callback outcomes rather than settled status flags. Use one
pending-request record and retain its identity guard. A publication error is a transport failure
reported through the aggregate request, rather than a mutation of the settled hardware outcome;
this deliberately removes the old incidental coupling between Redis errors and RPC status fields.
Native/finalization failures still agree in both response channels. The configuration route raises
on application failure after rollback; the handler alone interprets those failures and sends replies.

Implementation ownership: configuration/serialization and their tests are developed in
`/private/tmp/bec-hal-clean-config`; neutral request tracking/status and its tests in
`/private/tmp/bec-hal-clean-requests`; composition, native finalization, RPC and integration tests
are owned by the integration worktree. Helpers return patches without commits. Publication amends
the existing feature commit after integration/review so the remote history contains one change.

### Clean-code development and focused-review rounds

Round 1 implemented the planned ownership and dependency changes, removed the serialization
forwarder, and replaced request dictionaries with a private typed record and immutable snapshots.
Tests now assert published outcomes and applied configuration rather than internal forwarding.
All seven focused-review angles were covered, with additional checks for redundant state,
wrappers, lock ownership, API clarity and a future second HAL. The review approved the design and
configuration paths, but found that a failed first-status running notification could prevent
completion callback registration. Immediate and delayed regression cases both reproduced the
missing per-device reply. The initial device-server suite passed 279 tests before those new cases.

Round 2 attached the callback in a finally block. The 281 device-server tests passed, but the
cross-file review found that an immediately completed status could publish success before the
original progress-publication exception reached service recovery, which then recreated the request
and published an error. New service-level tests reproduced both that duplicate terminal response
and premature failure of a delayed operation. This round was rejected despite the passing unit
suite, because the handler-only regression did not exercise the service's recovery path.

Round 3 handles a failed first-status running notification locally: log the transport failure,
then attach the completion observer outside the request lock. The operation has already started;
its final result still comes from completion, without recreating aggregate tracking. This is
simpler than the rejected finally approach: no second lock acquisition, extra state or terminal
history is needed. Initial registration failures before hardware starts still propagate, and
terminal per-device publication failures still produce an aggregate error. All four new cases
cover observable responses through the handler and through the service.

The clean-code review also verified one-way configuration execution, explicit composition,
backend-owned finalization, stable RPC serialization and neutral per-device publication. The
configuration cancellation path now reports a cleanup failure accurately instead of claiming
cancellation succeeded; a dedicated regression covers it. No asynchronous runtime or speculative
backend machinery was added.

Round 4 corrected one test-only review finding: a fresh service can publish an error log through
the same Redis connector as the per-device response. The isolated regression reproduced two
`xadd` calls, while a suite run could hide that due to logger initialization order. The assertion
now selects the device-status endpoint and verifies its successful payload, without counting
unrelated log traffic. Production behavior is unchanged from the approved round 3.

### Clean-code validation

- The final server package run passed 1,445 tests, with three local Podman-dependent tests
  deselected. The actor module passed separately (11 tests), as did the DAP module (one test).
  These modules are isolated because existing client-singleton state can leak between test
  modules; the earlier combined run's actor authentication failure did not reproduce alone.
- All four simulated end-to-end checks passed: motion, limit failure, configuration updates and
  cached device reads. No real hardware was used.
- Black, isort and diff whitespace checks passed. Targeted production Pylint scored 10.00/10.
- The final focused review includes the general clean-code checks as well as the future-HAL
  boundary. Lifecycle/correctness and caller/reuse reviewers approve; the tests/conventions
  review is rechecked after the endpoint-specific assertion above.

Final clean-code focused-review verdict: **approve**. The endpoint-specific immediate/delayed
regressions passed together in a fresh process. All seven angles and the additional clean-code
checks have no unresolved introduced findings; no further material simplifications were identified.
