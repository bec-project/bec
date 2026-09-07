# Review walkthrough for PR #1035 "feat: add bec signal info"

- PR: https://github.com/bec-project/bec/pull/1035 (branch `feature/bec_signal_info`)
- Reviewed head: `9203872` (review started on `5cdeeff`, re-checked after the `9203872` follow-up)
- Reviewer: Jan Wyzula, 2026-09-07
- This branch only adds comments plus this file. It is **not** meant to be merged. Delete it when
  you are done: `git push origin --delete review/feature/bec_signal_info`

## How to walk through it

Every finding is an inline comment tagged `[REVIEW-n]` placed directly above the line it refers
to. List them all with:

```bash
grep -rn "\[REVIEW-" --include="*.py" bec_lib bec_server
```

Findings are ordered by severity below. The same tag can appear in more than one file when the
fix spans both sides (scan server and device server).

## Verdict

Request changes. CI is green and the code is tidy, but the feature cannot work as shipped: it
depends on an ophyd_devices change that does not exist on any branch, and the one failure path
that would reveal this is swallowed. Both are hidden because the unit tests fake the signal
class.

| Tag | Severity | Where | Summary |
| --- | --- | --- | --- |
| REVIEW-1 | Blocking | [device_server.py:1136](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/device_server/device_server.py#L1136) | `BECMessageSignal` has no `signal_info` attribute, so the snapshot is always empty |
| REVIEW-2 | Blocking | [device_server.py:580](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/device_server/device_server.py#L580), [scan_actions.py:1552](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/scan_server/scans/scan_actions.py#L1552) | No `device_instr_id` on the instruction, so any failure raises `KeyError` inside the except block and is swallowed |
| REVIEW-3 | Blocking | [messages.py:881](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_lib/bec_lib/messages.py#L881), [device_server.py:1146](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/device_server/device_server.py#L1146) | Type contract for `signal_info` is undefined; a foreign `SignalInfo` instance is rejected by pydantic |
| REVIEW-4 | Design | [direct_scan_worker.py:293](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/scan_server/direct_scan_worker.py#L293) | Fire-and-forget: no ordering guarantee relative to `pre_scan` and data |
| REVIEW-5 | Design | [endpoints.py:373](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_lib/bec_lib/endpoints.py#L373), [device_server.py:1171](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/device_server/device_server.py#L1171) | One shared stream, `max_size=10`, no expiry |
| REVIEW-6 | Design | [direct_scan_worker.py:91](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/scan_server/direct_scan_worker.py#L91) | Only v4 direct scans broadcast; legacy generator scans never do |
| REVIEW-7 | Minor | [device_server.py:1103](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/device_server/device_server.py#L1103) | "Device not found" branch is unreachable through `handle_device_instructions` |
| REVIEW-8 | Minor | [device_server.py:1111](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/device_server/device_server.py#L1111) | Helper redefined per loop iteration; docstring says "recursively" |
| REVIEW-9 | Minor | [scan_actions.py:1541](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/scan_server/scans/scan_actions.py#L1541) | Device list computed before the early-return guard |
| REVIEW-10 | Minor | [scan_actions.py:1548](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/scan_server/scans/scan_actions.py#L1548) | `scan_id` in parameter vs `scan_id + _metadata_suffix` in metadata |
| REVIEW-11 | Tests | [test_device_server.py:548](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/tests/tests_device_server/test_device_server.py#L548) | Test fakes hide REVIEW-1 and REVIEW-3 |
| REVIEW-OK | Resolved | [device_server.py:1125](https://github.com/bec-project/bec/blob/review/feature/bec_signal_info/bec_server/bec_server/device_server/device_server.py#L1125) | `9203872` fixed the crash on Signal-only locked devices |

## Blocking findings in detail

### REVIEW-1: the `signal_info` attribute does not exist

The device server keys on `hasattr(item, "signal_info")`. In ophyd_devices the `SignalInfo`
model is only built inside `BECMessageSignal.describe()`; the individual fields live as plain
attributes (`data_type`, `saved`, `ndim`, `scope`, `role`, `enabled`, `acquisition_group`,
`signals`, `signal_metadata`). Checked on:

- `ophyd_devices` main, `feature/bec_signal_restructure`, `feat/async_signal_refactoring`
- installed `ophyd_devices` 1.44.2: a `SimCamera` yields `preview` (`PreviewSignal`) and
  `file_event` (`FileEventSignal`), both with `hasattr(sig, "signal_info") == False`

Result: every scan publishes `BECSignalInfoMessage(info={})`. The PR description says the
class is being moved from ophyd_devices, but no companion PR is open or linked.

Options: land and link the ophyd_devices side first, or build the dict from the attributes in
the device server (which also side-steps REVIEW-3).

### REVIEW-2: failures are swallowed

`scan_actions._broadcast_bec_signal_info` sends the instruction with `metadata={}` and no
`ScanStubStatus`, so `_send` adds `scan_id`, `RID` and `queue_id` but no `device_instr_id`.
In `DeviceServer.handle_device_instructions` the generic `except Exception` branch calls
`_ensure_request_registered()`, which returns early without an id, and then indexes
`instructions.metadata["device_instr_id"]`, raising `KeyError` inside the handler. The handler
runs via `self.executor.submit(...)` on a `ThreadPoolExecutor(max_workers=4)` and the future
is never inspected. The original traceback is logged once by `logger.error(content)`; the
`KeyError` is lost. This is why end2end passed even though, before `9203872`, every scan on
the demo config crashed in this method (see REVIEW-OK).

Suggested minimal fix in the except path:

```python
instr_id = instructions.metadata.get("device_instr_id")
if instr_id is not None:
    self.requests_handler.set_finished(instr_id, success=False, error_info=error_info)
```

Better: give the broadcast a real status object like every other action, which also enables
REVIEW-4.

### REVIEW-3: the value type of `signal_info` is undefined

`BECSignalInfoMessage.info` is typed `dict[str, dict[str, SignalInfo]]` with the new
`bec_lib.messages.SignalInfo`. Verified with pydantic 2.11.10: a field typed as model `A`
rejects an instance of an identically shaped model `B` with
`Input should be a valid dictionary or instance of A [type=model_type]`. Only a dict or the
bec_lib class passes. If ophyd_devices later exposes `signal_info` as its own `SignalInfo`
instance, message construction fails and lands in REVIEW-2.

The two definitions have already drifted: bec_lib adds `use_alias`, and `signals` /
`signal_metadata` default to empty containers in bec_lib but to `None` in ophyd_devices.

Pick one contract: ophyd_devices imports `SignalInfo` from `bec_lib.messages` (and the copy
there is deleted, with a minimum bec_lib pin), or the device server calls `.model_dump()` on
`BaseModel` values before building the message.

## Design points

- **REVIEW-4**: the broadcast is fire-and-forget and the device server runs instructions on
  four worker threads. Nothing stops `pre_scan` and the first data points from arriving before
  the signal-info entry. If the file writer or widgets need it at scan start, the scan server
  should wait on it.
- **REVIEW-5**: a single shared stream with `max_size=10` and no expiry. Scan definitions, scan
  groups and short scans evict entries fast. A scan_id-keyed endpoint with an expiry would
  match how other per-scan data is stored.
- **REVIEW-6**: the hook lives in `DirectScanWorker.run`; legacy generator scans never emit
  the message. Probably intended, but consumers must treat it as optional and the PR should
  say so.

## Tests

- **REVIEW-11**: the device-server test patches `BECMessageSignal` with fake classes and
  passes `signal_info` as a dict. It cannot catch REVIEW-1 or REVIEW-3. A test that walks a
  real `SimCamera`, plus one with a `ReadOnlySignal` in the device list, would have caught all
  blocking issues before `9203872`.
- The roundtrip test, the endpoint contract test, the scan-actions tests and the worker
  ordering test are fine as they are.

## What I verified and how

- Fetched all changed files at the PR head via the GitHub API; no local checkout of the PR.
- Compared `5cdeeff...9203872`: the only change is the `isinstance(device_obj, Device)` guard.
- Read `ophyd_devices/utils/bec_signals.py` on main and the feature branches listed above.
- Ran in a local env with pydantic 2.11.10, ophyd 1.10.7, ophyd_devices 1.44.2:
  cross-class model rejection, `ophyd.Signal` has no `walk_signals`, `ReadOnlySignal` and
  `ComputedSignal` are not `Device` subclasses, `SimCamera` BEC signals lack `signal_info`.
- `demo_config.yaml`: `ring_current_sim` is `ophyd_devices.ReadOnlySignal` with
  `readoutPriority: monitored` and default ownership, so it is locked by every scan.
- Copilot's five comments on the earlier commit `17963fe` are all addressed at the current
  head.
- CI on `5cdeeff` was fully green (unit, matrix 3.11-3.13, end2end, child repos, plugin repos,
  codecov patch).
