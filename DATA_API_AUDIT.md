# DataAPI audit — `feature/data_api`

*Date: 2026-07-28. Audited head: `b3b6b912` ("f", local rebase of `origin/feature/data_api` onto
current main; commits `7f5e17da` feat: data api → `dd417a20` wip - async signals → `b3b6b912` f).
Companion widget draft: `bec_widgets` `draft/heatmap_data_api` head `820afb20` — see
`bec_widgets_data_api/HEATMAP_DATA_API_AUDIT.md`.*

All work described here lives on branch `data_api` in the `bec_data_api` worktree, as separate
commits **on top of** the untouched prototype commits. Nothing was pushed.

**Reading guide.** Sections 1–3 and Appendix A are the audit of the prototype and remain a
reference to the **original code** — every line number cites `b3b6b912` (bec) / `820afb20`
(bec_widgets), i.e. the state *before* any fix. [Section 4](#4-recommendations--what-was-implemented-and-why)
is the living part: it records what was implemented afterwards and why, including the v2
redesign that replaced the prototype's contract, issues that only live use exposed (§4.2) and
functionality the migration regressed and restored (§4.3). The current contract is specified in
[`bec_lib/bec_lib/data_api/DESIGN.md`](bec_lib/bec_lib/data_api/DESIGN.md).

---

## 1. Scope and method

The audit covered `bec_lib/bec_lib/data_api/` (`data_api.py`, `plugins.py`), its unit tests, the
e2e addition in `test_scans_lib_e2e.py`, and the heatmap widget draft as an integration probe.
Eight independent audit passes were run over distinct dimensions (real-client API consistency,
subscription logic, plugin/synchronization logic, threading, memory/performance, architecture,
test quality, widget integration), producing **54 deduplicated findings** (8 critical, 28 major,
15 minor, 3 info). Every finding in the registry (Appendix A) carries its original evidence:
either an **executed probe** (repro scripts run against this worktree's env `bec_312_data_api`,
with output quoted) or an **exact code-path citation** into the prototype and the real client
code it talks to.

Proof discipline for fixes: each fixed finding has a regression test in
`bec_lib/tests/test_data_api.py` that **fails on the unfixed code** and passes after the fix.
This was verified mechanically by checking out the prototype `data_api/` directory underneath the
new tests:

| Checked-out code state | New-test result |
| --- | --- |
| `b3b6b912` (prototype head) | 2/3 thread-safety, 2/2 alignment, 5/5 lifecycle, 9/9 state tests **fail** |
| after each fix commit | all previously failing tests for that commit pass |
| final head | **101/101** in `test_data_api.py`; full `bec_lib` suite 1303 passed (only the 8 pre-existing `TestPluginSystem` setup errors, which also occur on the untouched primary checkout) |

## 2. Verdict in one paragraph

The DataAPI concept — one facade that aligns monitored and async sources per subscription so
widgets can drop their hand-rolled fetching — is sound and worth pursuing. The prototype,
however, was not usable beyond the demo path: it had **no thread-safety story at all** while
being driven from two different dispatcher threads plus the Qt thread; its positional
synchronization silently produced **wrong data pairings** whenever a bundle was built or changed
mid-scan (which is exactly what the heatmap does on `plot()`); and its lifecycle leaked —
an abandoned `live=True` subscription was pinned forever and kept resubscribing to every new
scan. Several client touchpoints were guessed wrong (`acquisition_group` location, queue-window
scan items), which the 82 unit tests could not see because their mocks encode the same guesses.
The fix commits below make the live-monitored slice correct and safe; the remaining gaps are
scope (history plugin, Waveform-class needs) and contract design, listed as recommendations.

## 3. Fix commits on the audited prototype

These five commits fixed the prototype **in place**, before the v2 redesign; they are what the
"fails pre-fix" proofs in this section were measured against. Most of this code was subsequently
superseded (see §4) — the table is kept because it documents which audited defect each change
answered, and the regression tests it introduced became the behavioural spec the v2
implementation had to satisfy.

| Commit | Content | Proof |
| --- | --- | --- |
| `0b6f970f` fix(data_api): serialize dispatch and defer emission for thread and reentrancy safety | One shared `RLock` across `DataAPI`/`DataSubscription`/`BECLiveDataPlugin`; dispatch iterates snapshots; emissions are collected and buffers trimmed **before** callbacks run; `disconnect()` clears all state. | `TestReentrancyAndThreadSafety` (2 deterministic tests fail pre-fix with RuntimeError/IndexError; threaded stress test) |
| `d89e9ae9` fix(data_api): align bundles by stream position and rebuild on membership change | Buffers keyed `(callback_ref, scan_id)`; every buffer carries an absolute stream offset — overflow drops discard the *same positions* everywhere instead of shifting the pairing; membership changes rebuild the callback bundle from a common origin (monitored re-read from live data, async replayed from per-source emission history). | `TestBundleAlignment` (2 tests fail pre-fix); rewritten overflow test asserts true pairing; the original heatmap-freeze probe now passes |
| `19c2e947` fix(data_api): release dead subscriptions and scope instances per client | `_WeakScanStatusRelay` (live subscriptions no longer pinned); dead-callback pruning through the regular unsubscribe teardown; per-client `DataAPI` instances; `close()` removes the instance instead of leaving a resurrectable zombie. | `TestLifecycleCleanup` (5 tests fail pre-fix) |
| `8b85c05d` fix(data_api): harden subscription state and async update handling | reload/remove-device buffer hygiene; bundle-domain reset; no double resubscribe from `set_callback`; non-raising live rebind (unprovidable sources stay queued); binding deferred during the queue-status window; buffered emissions are snapshots; nested `acquisition_group` resolution; async update-type change resets instead of killing the source; timestamp-lag cursor fix; async-info cache; one terminal-state set. | `TestSubscriptionStateCorrectness` (9 tests fail on prototype head) |
| `f6cd80fa` feat(data_api): expose unbound devices for subscription health checks | `DataSubscription.unbound_devices` — consumers can detect a degraded feed and fall back. | Used by the heatmap health gate (widget tests) |

Widget-side commit (in `bec_widgets_data_api`): `08200a05` fix(heatmap): harden data api
subscription lifecycle and delivery — see the widget audit notes.

**One deliberate semantic change** (commit `d89e9ae9`): adding a device to an actively
subscribed bundle re-aligned and **re-emitted the aligned history** to that callback. This made
mid-scan `add_device` correct at all. *Superseded:* v2 replaced incremental `add_device` with an
atomic `set_sources`, so the rule no longer needs documenting — a membership change is by
construction a rebuild (DESIGN.md §5).

## 4. Recommendations — what was implemented, and why

The ten recommendations below are the ones this audit raised against the prototype. Each entry
keeps its original finding references (`F..` → Appendix A, which still cites the **prototype**
code at `b3b6b912` / `820afb20`) and records what was subsequently built, in which commit, and
why that route was chosen. The design that these fixes follow is
[`bec_lib/bec_lib/data_api/DESIGN.md`](bec_lib/bec_lib/data_api/DESIGN.md).

Two rounds of work sit between the audit and this status:

| Round | Commits | Summary |
| --- | --- | --- |
| **v2 redesign** (bec) | `4b6c38a6` design · `5e362605` models + ordinal engine · `0ec5aea0` facade + live plugin rewrite · `a8448b05` history plugin · `2593436c` group partitioning + legacy async · `63b87bf3` device streams · `af11c666` size gate | Clean-slate contract; the prototype's `plugins.py`, `DataSubscription` and the buffered/non-buffered duality were **deleted**, not deprecated. |
| **widget migration** (bec_widgets) | `2d3c51c6` QtDataSubscription · `e9fe0518` heatmap · `47851dab` scatter · `e0abc7f3` waveform · `2288aa95` multi-waveform + motor map · `235d5614` image | All six plotting widgets consume one contract; the duplicated fetch/async/history/lifecycle code catalogued in the widget inventory is gone. |

### 4.1 Status of the ten recommendations

1. **Index-based alignment (F09/F35 class) — IMPLEMENTED** (`5e362605`, `0ec5aea0`).
   Positional (arrival-count) pairing was the root cause of the whole F03/F09/F27/F37 bug class,
   so it was replaced rather than patched: every point is keyed by an **ordinal** — `point_id`
   for monitored sources, `async_indices[signal]` for async `add`/`replace`, the
   `async_update.index` row for `add_slice`, an arrival counter for unindexed legacy sources.
   A bundle emits an ordinal only when *every* source has it, so gaps and out-of-order delivery
   fill holes instead of shifting pairs. The prototype's stream-offset machinery
   (`d89e9ae9`) became unnecessary and was removed with it.
   *Limit, documented in DESIGN.md §8:* the async↔monitored join is still only as exact as the
   `acquisition_group="monitored"` cadence promise; violations are now **detected and logged
   once per scan** (`alignment.Bundle._check_cadence`) instead of silently mispairing. An exact
   join needs a server-side `point_id` on async messages — out of client-side scope.
2. **Bundle starvation (F23) — IMPLEMENTED** (`409aad1f`). A silent source no longer blocks
   delivery (full-state snapshots), declared sources are pre-registered so a silent source
   visibly holds back `aligned_ordinals` instead of being invisible, updates carry
   `metadata["lagging_sources"]` (logged once per scan), a source that resolved unavailable
   at bind time is retried on the next scan-status update, and `unbound_sources` remains for
   introspection. Deliberately no forced eviction: alignment never silently drops a source.
3. **Memory/CPU of cumulative async sources (F06/F07/F17/F18) — IMPLEMENTED** (`5e362605`,
   `0ec5aea0`, `63b87bf3`). The measured pathologies (82.6 s for 1000 `add` updates, 310 ms per
   `add_slice` update at n=1000, 320 MB retained for 400×500 updates, 60.4 s of pure callback
   CPU for a 36000-point scan) all came from two things: deep-copying the whole cumulative state
   per update, and buffered mode storing one full snapshot per point. Both are gone — sources
   are stored columnarly per ordinal (`add` keeps the fragment, `add_slice` grows its row in
   place, `replace` keeps exactly one point), and emission frequency is bounded backend-side by
   `min_emit_interval` with a guaranteed trailing flush. Endless device streams are additionally
   capped by `max_points` (`63b87bf3`).
   *Still open:* scan-scoped async series are unbounded within a scan (deliberate — a scan is
   finite), and building the snapshot is O(N) per emission, which is why the rate limit exists.
4. **Emission contract (F41) — IMPLEMENTED** (`5e362605`). One shape for everything:
   `callback(SubscriptionUpdate)` with `sources: {(device, entry): SourceData}` holding columnar
   `ordinals`/`values`/`timestamps`/`complete` plus **per-source** metadata (killing the lossy
   first-writer-wins merge). `update.aligned()` returns equal-length columns — the shared
   replacement for the per-widget length-trimming code (waveform WF:1727, heatmap HM:916) — and
   `update.axis(mode, source)` replaces per-widget x-mode resolution. The buffered /
   non-buffered duality was deleted; history is a single `reason="history"` emission rather than
   columnar file data sliced into fake per-point events.
5. **Scope for the stated goal (F32) — IMPLEMENTED** (`a8448b05`, `2593436c`, `63b87bf3`).
   The dead `has_scan_data` contract (F36/F42) was deleted; routing is `resolve()` by ascending
   plugin priority. Coverage now: **LiveDataPlugin** (open scans), **HistoryDataPlugin**
   (terminal scans, file-backed via `ScanDataContainer` on a worker thread, with an in-memory
   fallback for the writer-latency window), **DeviceStreamPlugin** (scan-less
   `device_readback` / `device_monitor_1d` / `device_preview`), plus legacy
   `device_async_readback` devices as unindexed standalone sources. The one-bundle-per-
   subscription rule that could not express Waveform's mix was replaced by **automatic
   partitioning into correlation groups**, each aligned and emitted independently
   (`update.metadata["group"]`) — which is what let Waveform subscribe to monitored + async +
   x-device in one call.
6. **Client ownership (F14) — IMPLEMENTED** (`409aad1f`). `client.data_api` is a lazy
   `BECClient` property (mirroring `client.callbacks`); the registry holds weak values, so an
   entry lives exactly as long as some consumer holds the instance and `id()` keys can no
   longer alias across dead clients. `clear_instance()` remains as a test helper.
7. **Qt delivery helper (F43) — IMPLEMENTED** (`2d3c51c6`, hardened in `e46bb1b3`,
   extended in `f57949ae`). `bec_widgets/utils/qt_data_subscription.py` owns the subscription,
   marshals updates onto the Qt thread, drops stale-scan payloads, exposes `healthy`
   (`unbound_sources`) and the size gate. Widgets subscribe in one line. Rate limiting moved
   *backend-side* (`min_emit_interval`), so the per-widget `pg.SignalProxy` data proxies were
   deleted rather than re-implemented in the wrapper.
8. **Callback-lifetime contract (F44) — IMPLEMENTED** (`0ec5aea0`). `Subscription` is the only
   public subscribe surface and holds its callback **strongly**, so the prototype's trap (a
   lambda passed to `DataAPI.subscribe` dying at the next GC and silently dropping data) cannot
   occur. Weak references survive only where they belong: `_WeakEventRelay` for client
   scan-status/scan-history events, so an abandoned live subscription is still collectable
   (the F01 fix from `19c2e947`, preserved).
9. **Cache invalidation — IMPLEMENTED** (`409aad1f`). The live plugin registers the
   `device_update` client event and clears the async-signal-info cache on device-config
   changes.
10. **Test quality (F45–F48, F53) — IMPLEMENTED.** The e2e test is value-aware
    (`GaussianModel`, per-point comparison — `0ec5aea0`) and **has been executed against a
    real BEC stack** (local Redis + `--start-servers`), which caught and led to fixes for:
    the async storage-name translation in history routing (`444c7c4e`, `58384fe0`), a latent
    bec_lib ordering bug where `SCAN_HISTORY_UPDATE` fired before the registry store
    (`98d42213`), and a leaked history worker thread on close (`5bdf451a`); three consecutive
    passes after the fixes. History fixtures build a real `ScanHistoryMessage` (`755f687c`),
    device streams are exercised through the real fakeredis connector (`5ab70b39`), and the
    heatmap history test reads a **real HDF5 file** through the history plugin end to end.
    Remaining known flake: the heatmap interpolation phase times out in single-test isolation
    (pre-existing, environment-level; passes in suite runs).

### 4.2 Issues found *after* the audit, during live integration

None of these appear in Appendix A: they surfaced only when the ported widgets ran against a
real BEC session, and every one is the same failure mode the audit flagged as F53
(*mocks encoding the implementation's own assumptions*). They are recorded here because they
are the strongest argument for recommendation 10.

| Symptom (as reported from live use) | Root cause | Fix |
| --- | --- | --- |
| `AttributeError: 'QtDataSubscription' object has no attribute '_subscription'` on selecting devices in most widgets | The backend delivers the initial backfill **synchronously inside** `subscribe()`; Qt's auto-connection is then *direct*, so the bridge's filter ran before its own constructor finished — and the first snapshot was also lost for late-connecting consumers | `e46bb1b3` forces `Qt.QueuedConnection` (+ test simulating synchronous delivery) |
| `'_StoredDataInfo' object has no attribute 'get'` on every history load | `ScanHistoryMessage.stored_data_info` values are pydantic models, not dicts; the history plugin called `.get()` on them | `755f687c` reads them as models; fixtures now build a real message |
| Motor map coordinates frozen (no updates at all) | `device_readback` is a **pubsub** endpoint whose callback receives a `MessageObject` (`.topic`/`.value`), not the stream `dict` with `"data"`; every readback was silently discarded | `5ab70b39` unwraps both shapes; regression test publishes through the real connector |
| GUI stalls before the large-dataset prompt could appear | Auto x-mode resolved `scan_report_devices` through `ScanDataContainer.metadata`, a **lazy synchronous HDF5 open on the Qt thread** — and it ran *before* the size gate | `1d117308` resolves from in-memory sources (status message → history message `request_inputs`), file only as last resort |

### 4.3 Functionality regressed by the migration, then restored

- **Large-dataset guard (Waveform).** The port (`e0abc7f3`) deleted
  `_check_dataset_size_and_confirm`, the confirm dialog, `max_dataset_size_mb`,
  `skip_large_dataset_warning` / `skip_large_dataset_check` and their RPC entries; this was
  recorded as a follow-up but is critical functionality. Restored in `af11c666` (backend) +
  `f57949ae` (widget) with the **original names, semantics, dialog and RPC surface** — the
  regenerated `client.py` is byte-identical to the pre-port stub. The implementation is
  materially better than what it replaced: the old check read
  `dataset_obj._info[...]["mem_size"]`, i.e. it **opened the HDF5 file to decide whether the
  file was too large to open**, whereas `DataAPI.estimate_bytes` computes the estimate from
  `stored_data_info` shapes and dtypes with zero file I/O (asserted by test). The gate lives in
  `Subscription` (`size_limit_bytes` → `size_gated`/`estimated_bytes` → `confirm_size()`), so it
  is available to every widget, and the confirmed read runs on the history worker thread — a
  test asserts the delivering thread differs from the caller thread, which is the durable
  guarantee against a frozen GUI.

## 5. Threading contract (as now implemented)

Established from the client code, enforced since `0b6f970f` and carried unchanged into v2
(the class names differ — `Subscription`, `LiveDataPlugin`, `HistoryDataPlugin`,
`DeviceStreamPlugin` — the model does not): scan-segment and scan-status
callbacks run inline on the Redis connector's single `_dispatch_events` thread
(`callback_handler.py` registers `sync=False`; `scan_items.py` runs them from storage updates);
async-signal and device-stream callbacks run on the same dispatcher thread;
`subscribe`/`set_sources`/`close` run on whatever thread the consumer uses (the Qt GUI thread
for widgets), and **`subscribe` executes the live backfill synchronously on that caller
thread**. History reads are the exception: they run on a short-lived worker thread, so a
history subscription (and `confirm_size()`) returns immediately. All of these paths take one
shared reentrant lock per `DataAPI` instance; user callbacks are invoked after internal state is
consistent and **must not block** on other threads that use the data API.

Two consequences that cost real debugging time (§4.2) and are now enforced by tests:
the synchronous backfill means a Qt consumer **must** marshal with an explicitly *queued*
connection — an auto-connection resolves to *direct* here and re-enters the consumer before its
own constructor has finished; and callbacks may arrive on the dispatcher thread, a worker
thread or a coalescing timer thread, so nothing may assume the emitting thread.

## 6. Reproduction artifacts

The audit probes (20 scripts: threading identity, cross-thread corruption, reentrancy, live-pin,
dead-callback retention, quadratic-cost benchmarks, mid-scan misalignment, heatmap rebind/slot
delivery) were executed from the session scratchpad against env `bec_312_data_api`. Their
decisive outputs are quoted verbatim inside the Evidence field of each finding in Appendix A.

The durable, re-runnable form of those repros is the test suite. In v2 the relevant files are
`bec_lib/tests/test_data_api.py` (facade, live plugin, device streams — including
`TestDeviceStreamsThroughRealConnector`, which publishes through the fakeredis connector),
`test_data_api_alignment.py` (ordinal engine; `test_gap_does_not_shift_pairing` is the direct
descendant of the mid-scan misalignment probe), `test_data_api_history.py` (history routing,
size gate, off-thread read) and, widget-side, `test_qt_data_subscription.py` plus the six ported
widget suites.

---

# Appendix A — finding registry (54)

Line numbers refer to the audited prototype heads (`b3b6b912` for bec, `820afb20` for the widget
draft), i.e. **before** the fix commits.
### F01 [CRITICAL] Abandoned live=True DataSubscription is pinned forever by CallbackHandler; documented auto-cleanup can never run

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:77`  
*Category:* memory-leak

**Claim.** DataSubscription(live=True) registers the bound method self._handle_scan_status_update with client.callbacks, which stores a strong reference, so dropping the last user reference never garbage-collects the subscription — __del__/close() (data_api.py:494-500) is unreachable, contradicting the docstring's 'automatically cleaned up when the object is destroyed' (data_api.py:17-18), and every abandoned live subscription keeps rebinding, resubscribing and re-buffering every new scan forever.

**Evidence.** data_api.py:76-79 registers via client.callbacks.register("scan_status", self._handle_scan_status_update); callback_handler.py:41 `self.func = func` (strong ref, no weakref anywhere in CallbackHandler). Probes: 'subscription alive after del+gc.collect(): True'; probe_live_pin.py: 'non-live sub collected after del: True / live sub collected: False'; after abandoning 101 live subs: 'callbacks registered: 102, DataSubscription objects alive: 101, orphan rebound to new scan: scanX'.

**Suggested fix (as filed).** Register a weakly bound trampoline (louie.saferef / weakref.WeakMethod wrapper that self-unregisters when dead), or make close() mandatory and explicit for live mode and drop the auto-cleanup claim.

**Status.** **FIXED** in 19c2e947 — scan-status events go through `_WeakScanStatusRelay` (weakref + self-unregister); test `test_live_subscription_is_collectable_after_del` (fails on d89e9ae9).

### F02 [CRITICAL] Backfill runs _handle_scan_segment_update on the caller thread, racing live dispatch: duplicate emissions and IndexError

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:890`  
*Category:* race-condition

**Claim.** _backfill_monitored_scan_data (called from subscribe() at plugins.py:859/867/875) executes _handle_scan_segment_update synchronously on whichever thread called subscribe (Qt/user thread via add_device/reload, or dispatcher thread via rebind), while live segments execute the same method on the connector dispatcher thread with zero locks; both paths do an unlocked read-modify-write of callback_buffer.monitored_indices and buffer.data, so segments are emitted twice, partially lost, or crash with IndexError.

**Evidence.** plugins.py:890 direct call from subscribe(); unsynchronized window plugins.py:955 `last_processed_index = ...get(key, 0)` -> :971 `monitored_indices[key] = len(values)`; shared append :1275; emit+trim :1386-1408 (`del buffer.data[:min_length]`). Live path proven on 'Thread-2 (_dispatch_events) (RedisConnector)' (callback_handler.py:48-53 sync=False runs inline; scan_manager.py:57-59 -> scan_items.py:119; managed_redis_connection.py:777-780). Probe probe_cross_thread.py (400 points, 30 trials): 'trials with corruption: 29/30; worst duplicate emissions in a trial: 1065; IndexError raised on both dispatcher and user thread' — dispatcher-side IndexError swallowed by callback_handler.py:55-61, user-side propagates from add_device/reload.

**Suggested fix (as filed).** Add a single RLock in BECLiveDataPlugin guarding _subscriptions/_monitored_subscriptions/_async_subscriptions/_async_source_states/_callback_buffers, held by subscribe/unsubscribe/_handle_scan_segment_update/_handle_async_signal_update (deferred-emit list around user callbacks to avoid deadlock); alternatively marshal backfill onto the dispatcher thread by enqueueing it.

**Status.** **FIXED** in 0b6f970f — shared RLock serializes subscribe/backfill against dispatch; regression test `test_concurrent_dispatch_and_subscribe_is_safe`.

### F03 [CRITICAL] Mid-scan incremental add_device emits partial bundles then permanently misaligns all sources

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:875`  
*Category:* data-integrity

**Claim.** Each plugin.subscribe immediately backfills and emits (plugins.py:859/867/875), and synchronization pairs sources purely by arrival count (min over buffer lengths, plugins.py:1375; per-source cursors :955-971), so a subscription built incrementally on a running scan (exactly what DataSubscription.add_device/_resubscribe_all do, one subscribe per device) first emits single-device bundles that are consumed (`del buffer.data[:min_length]`, :1407-1408), and afterwards permanently pairs device N's point k+offset with device M's point k — the documented "dynamically add/remove devices" workflow (data_api.py:25-27) silently produces wrong pairings.

**Evidence.** Probe (3 existing points, subscribe samx then samy, true pairing samy=samx+100): 'samx=0.0 samy=None / ... / samx=3.0 samy=100.0' — three one-source bundles then a permanent 3-point misregistration. Second probe: 'C RESULT: bundle pairs samx value 3.0 with samy value 0.0'. Timestamp probe: samx.value=12.0 (ts 102) paired with samy.value=20.0 (ts 100), off by two forever; same for monitored+async late joiner ('samx=0.0 sig1=40.0', fragment of point 3). data_api.py:282-285 subscribes each device immediately; emission uses only the instantaneous device set (_get_expected_device_count, plugins.py:1368).

**Suggested fix (as filed).** Align monitored data on point_id (available in live_data ScanMessages) instead of arrival order, and/or make bundle membership atomic: defer plugin subscription/backfill until the full device set is registered (batch add or explicit activate()/commit()), re-baselining all per-source cursors to a common point index when membership changes.

**Status.** **FIXED** in d89e9ae9 — bundle membership changes rebuild the whole callback bundle from a common stream origin (monitored re-read from live data, async replayed from recorded history); test `test_midscan_add_device_realigns_bundle` (fails on 0b6f970f). Note the deliberate semantic: a membership change re-emits the aligned history.

### F04 [CRITICAL] unsubscribe()/close() from the user thread races in-flight dispatch: TOCTOU KeyErrors and mutation of dicts/lists being iterated

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:919`  
*Category:* race-condition

**Claim.** DataSubscription.close()/remove_device() on the Qt thread deletes entries of _monitored_subscriptions, _callback_buffers, sub.devices and async_sub.callback_refs while the dispatcher thread is iterating or re-reading those same structures inside _handle_scan_segment_update/_handle_async_signal_update/_check_and_emit_synchronized_data, producing KeyError/RuntimeError and silently dropped updates — and the heatmap performs exactly this pattern (cleanup on the Qt thread mid-scan).

**Evidence.** TOCTOU pairs: plugins.py:909 check vs :919 index with concurrent del at :768; :558-562/:1359-1362 vs del at :760/:801; iteration :921 vs del :764; :1111 vs .remove :791; :1389 vs del :756/:797. probe_close_and_alias.py (b) (200 trials): "31x KeyError: 'test_scan_id'" and "1x KeyError: BoundMethodWeakref(..._buffering_callback)" escaped dispatch — swallowed in live dispatch (update silently lost) or propagated as sporadic KeyError from add_device. Heatmap._cleanup_data_api_subscription (heatmap.py:715, called from :499 and cleanup() :1797) runs on the Qt thread.

**Suggested fix (as filed).** Same plugin-wide RLock as the backfill-race finding: unsubscribe paths take it before mutating; dispatch paths snapshot or hold it while iterating, re-checking keys after user callbacks return.

**Status.** **FIXED** in 0b6f970f — all mutation paths take the shared RLock; dispatch iterates snapshots.

### F05 [CRITICAL] MAX_PENDING_UPDATES caps point count, not bytes: a stalled bundle peer retains multi-GB of cumulative snapshots

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1297`  
*Category:* memory

**Claim.** When one source of a bundle stalls, nothing is emitted and each peer buffer fills to 1000 pending points where each point holds a full deepcopied cumulative array snapshot; the limit drops the oldest (smallest) snapshots, so retained bytes grow without bound (4 GB after 1000 updates of 1000-element arrays, 284 GB at 10 Hz over 1 h), with one WARNING logged per update once capped.

**Evidence.** plugins.py:1297-1301 count-based cap dropping from the appended buffer only; :1375-1379 min_length gate. Probe probe_backlog_stall.py (stalled second async source): 'emissions: 0; pending points: 200 (capped); oldest retained snapshot: 201 arrays; newest: 400; numpy bytes held by one pending buffer: 240.4 MB' + 200 drop-warning lines. Extrapolated at defaults (plugins.py:226): N=1000 -> 4.00 GB, N=36000 -> 284 GB.

**Suggested fix (as filed).** Enforce a byte budget (or store only the incremental fragment plus one shared cumulative reference), rate-limit the drop warning, and emit partial bundles or evict the callback buffer when a peer stalls beyond a timeout.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F06 [CRITICAL] O(N^2) deepcopy of the full cumulative async state on every update: quadratic CPU (up to 310 ms/update at N=1000) and per-callback snapshot memory

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1190`  
*Category:* performance

**Claim.** Every async update deepcopies the entire reconstructed cumulative state (plugins.py:1190; add_slice additionally deepcopies the full rows structure at :1232, and :1206-1210 rebuilds the full concatenated list per add), executed inside the connector callback thread — quadratic total work per scan — and each per-callback pending entry stores that entire snapshot (1274-1275), so a bundle blocked by a lagging sibling accumulates up to MAX_PENDING_UPDATES growing snapshots, multiplied per subscribed callback.

**Evidence.** Probe probe_bench2.py (py 3.12.11): [add/list] n=1000: 82.6 s total, last update 166.48 ms (t500/t250=4.18 = O(N^2)); [add_slice/list] n=1000: 157.1 s, last update 310.47 ms; [add/numpy] 0.469 s but still 4.20x quadratic (~4 GB cumulatively copied). Probe G (det2 silent, 5 add updates from det1): 'pending buffer entries hold full cumulative snapshots, sizes: [3, 6, 9, 12, 15]' with 0 emissions.

**Suggested fix (as filed).** Return the state without deepcopy (document read-only) or copy only the new fragment; buffer only the incremental fragment plus the index needed to reconstruct at emit time; for add_slice mutate rows in place and make the cumulative container append-only.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F07 [CRITICAL] Heatmap plot()/re-config mid-scan permanently desynchronizes the DataAPI feed and freezes the plot

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:743`  
*Category:* correctness

**Claim.** _setup_data_api_subscription adds x, y, z sequentially after set_callback (heatmap.py:742-751); on a scan with N existing points each add_device triggers backfill, emitting N x-only bundles that are drained before y/z exist, leaving plugin alignment off-by-N and the widget's buffered x series N entries longer than y/z, so update_plot's length check (heatmap.py:876-881) skips every subsequent update for the rest of the scan.

**Evidence.** Probe test_midscan_probe.py (exact create_subscription(live=True,buffered=True) -> set_callback -> add_device x3 with 5 recorded points): 'emissions right after setup (backfill of 5 pts): 5; emit 0: x=[0.0..4.0] y=None z=None' then 'x (7) y (2) z (2), widget length check passes: False'; plugin pairs x point 5 with y/z point 0. Trigger paths: plot() during a scan (heatmap.py:467), any device/signal property change mid-scan (_try_auto_plot -> plot(), heatmap.py:1457/1496/1524/1563), leaving history view.

**Suggested fix (as filed).** Add all devices before the subscription can emit (add_device before set_callback, or a batch/deferred-activation API on DataSubscription), and/or have the plugin reset monitored_indices and re-run backfill for the whole bundle whenever a callback's device set changes.

**Status.** **FIXED** by d89e9ae9 (library-side rebuild); the original failing probe `heatmap_audit/test_midscan_probe.py` now passes: x/y/z all length 7, zero misaligned pairs.

### F08 [CRITICAL] Heatmap legacy update path is gated off by subscription existence, not data delivery — a failed device bind yields a permanently blank heatmap

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:831`  
*Category:* correctness

**Claim.** on_scan_progress returns early whenever _data_subscription is not None (heatmap.py:831-832) and on_scan_status suppresses initial sync (823-825), but the subscription's live rebind can silently fail per-device (e.g. z not monitored and not async in the new scan): the ValueError is swallowed into a log warning, x/y stay subscribed while z is dead, update_plot skips on z=None (873-875), and the legacy fallback that would have rendered the scan is disabled — the heatmap stays empty for the whole scan.

**Evidence.** Probe test_rebind_probe.py::test_z_not_providable_in_scan: "rebind raised: ValueError: Cannot add device 'bpm4i' ... resolved bundle is None."; "sub._devices after rebind: {samx: <sub_id>, samy: <sub_id>, bpm4i: None}"; "last payload devices: {'samx', 'samy'}". callback_handler.py:55-61 swallows the exception in production.

**Suggested fix (as filed).** Gate the legacy path on actual DataAPI delivery (fall back if no data_api_update arrived for the current scan_id within a timeout, or expose a per-device subscription-health signal on DataSubscription and fall back when any device fails to bind).

**Status.** **FIXED** in widgets 08200a05 + bec f6cd80fa — `DataSubscription.unbound_devices` exposes degraded bindings and the heatmap keeps the legacy path active unless the feed is healthy; widget test `test_heatmap_unhealthy_subscription_keeps_legacy_updates`.

### F09 [MAJOR] Binding during the queue-status window (status_message None, queue literals in status) silently yields a dead subscription

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:462`  
*Category:* api-mismatch

**Claim.** The real client creates scan items from queue-status updates with status_message=None and status set to InstructionQueueStatus names (PENDING/RUNNING/COMPLETED...); if _bind_to_current_scan_if_available binds in that window, _device_entry_is_monitored returns False (plugins.py:416-417 requires status_message), subscribe returns None with only a warning, the device is stored with sub_id=None and never retried for that scan (rebind guard data_api.py:489), so the live subscription delivers no data for the whole scan; the terminal-state check at data_api.py:462-467 also never matches queue literals.

**Evidence.** scan_items.py:83 `self.status_message: ... | None = None`; scan_items.py:502-528 update_with_queue_status -> add_scan_item(status=queue_item.status) with InstructionQueueStatus names (scan_queue.py:50-59, :1473); status becomes "open"/"closed" only after a ScanStatusMessage (scan_items.py:364-372). plugins.py:416-417: `if scan_item.status_message is None: return False`.

**Suggested fix (as filed).** Treat a scan item with status_message=None as not-yet-bindable (defer binding until the "open" scan_status callback), and retry devices whose sub_id is None when the bound scan's status message arrives.

**Status.** **FIXED** in 8b85c05d — scan items without a status message are not bindable yet; binding waits for the scan_status event; test `test_bind_deferred_during_queue_status_window`.

### F10 [MAJOR] set_callback on a live subscription resubscribes twice and re-emits the full history

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:239`  
*Category:* correctness

**Claim.** set_callback triggers _resubscribe_all twice — once via _bind_to_current_scan_if_available->set_scan_id (line 239, after self._callback was set at 237) and once via the condition at lines 244-245 — and each unsubscribe/resubscribe destroys the plugin's _CallbackBuffer (plugins.py:759-760), losing monitored_indices, so the second pass backfills from index 0 and duplicates every point.

**Evidence.** data_api.py:237-245. Probe following the class's own docstring order (add_device then set_callback, data_api.py:642-643) on a running 3-point scan: 'callback invocations: 6, values seen: [0.0, 1.0, 2.0, 0.0, 1.0, 2.0]'; buffered mode doubles _data_buffer the same way.

**Suggested fix (as filed).** Skip the explicit _resubscribe_all when _bind_to_current_scan_if_available already triggered one (track whether set_scan_id resubscribed), and/or preserve plugin-side monitored_indices across unsubscribe/resubscribe of the same callback_ref.

**Status.** **FIXED** in 8b85c05d — `_bind_to_current_scan_if_available` reports whether it already resubscribed; test `test_set_callback_after_add_device_emits_history_once` (values `[0,1,2]` instead of `[0,1,2,0,1,2]`).

### F11 [MAJOR] reload() in buffered mode duplicates the entire _data_buffer

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:314`  
*Category:* correctness

**Claim.** reload() calls _resubscribe_all without clearing self._data_buffer (only set_scan_id clears it at line 201), and resubscription re-backfills from index 0, so every buffered point is appended a second time.

**Evidence.** reload() body (data_api.py:314-337) has no _data_buffer.clear(). Probe: 'buffer len after initial backfill: 3; after reload(): 6; values: [0.0, 1.0, 2.0, 0.0, 1.0, 2.0]'.

**Suggested fix (as filed).** Clear self._data_buffer in reload() before _resubscribe_all (and in set_callback when a resubscribe is triggered in buffered mode).

**Status.** **FIXED** in d89e9ae9 — `_resubscribe_all` restarts the buffered accumulation before each re-subscription; test `test_reload_does_not_duplicate_buffered_data` (fails on b3b6b912).

### F12 [MAJOR] Sticky _bundle_domain is never reset: empty subscriptions reject new domains and cross-scan acquisition_group changes kill live subscriptions

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:403`  
*Category:* state-machine

**Claim.** _validate_bundle_compatibility sets self._bundle_domain once (data_api.py:403-405) and nothing resets it — not remove_device (289-312), not set_scan_id (178-210, which deliberately strips scan_id via _normalize_bundle_domain :419-439) — so (a) after removing all devices the subscription still enforces the stale domain and raises ValueError for a legitimately different source, and (b) if an async device's acquisition_group changes between scans, the rebind raises inside the swallowed scan-status callback and the live subscription silently never receives data again, left with stale sub_ids (set_scan_id assigns _scan_id at :198 before _resubscribe_all at :208, which raises at :383 mid-loop).

**Evidence.** Probe: add async group g1, remove it (devices: []), add g2 -> 'ValueError on empty subscription: ... resolved bundle is (async_signal, g2)'. data_api.py:413-417 raise; callback_handler.py:57-61 swallows into a log warning. Plugin side correctly resets per scan (_async_source_states keyed by (scan_id, device, entry), plugins.py:531-542) — the facade contradicts it.

**Suggested fix (as filed).** Reset _bundle_domain to None in remove_device when _devices empties and re-derive it on set_scan_id (clear alongside _data_buffer); make _resubscribe_all exception-safe (validate all first, reset _devices values to None on entry).

**Status.** **FIXED** in 8b85c05d — emptied subscriptions reset `_bundle_domain`; live rebind no longer raises through the swallowed dispatch; tests `test_remove_device_purges_buffered_series_and_resets_domain`, `test_live_rebind_survives_unprovidable_device`.

### F13 [MAJOR] Live rebind failure mid-_resubscribe_all is swallowed and leaves devices with stale subscription IDs, never retried

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:382`  
*Category:* error-handling

**Claim.** _resubscribe_all unsubscribes everything (372-374) then validates/resubscribes per device (382-387); when a device is not providable in the newly opened scan, _validate_bundle_compatibility raises mid-loop inside the swallowed scan_status dispatch, and because _scan_id was already updated at line 198 the guards at 194-195 and 489 prevent any retry — the failing device and all devices after it keep dangling sub_ids and silently receive no data for the rest of the session.

**Evidence.** Probe: live sub on scan1 with samx+samy; scan2 opens with only samx monitored; dispatch logs 'Failed to run callback function: ... ValueError: Cannot add device samy ... resolved bundle is None' (callback_handler.py:57-61), then 'samy: sub_id unchanged-stale; registered in plugin: False; after 2nd dispatch still False'. Partial bundle keeps emitting samx only with no user-visible error.

**Suggested fix (as filed).** Make _resubscribe_all transactional: validate all devices first, treat unresolvable sources as queued (store None) instead of raising, and emit an explicit subscription-degraded notification.

**Status.** **FIXED** in 8b85c05d — `_resubscribe_all` pre-clears sub-ids and keeps unprovidable sources queued (warning) instead of raising mid-loop; surviving devices stay live; test `test_live_rebind_survives_unprovidable_device`.

### F14 [MAJOR] Failed or unknown-source subscribe returns None silently, indistinguishable from 'queued', never retried

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:670`  
*Category:* error-handling

**Claim.** DataAPI.subscribe falls through to an implicit None when no plugin can_provide (670-676, only a log warning), and add_device stores that None (line 285) — the same sentinel used for 'queued' (line 277) — so a failed subscription (scan not in storage, typo'd device/signal) is silent, unrecoverable and never retried; the same typo added after a valid device instead raises a cryptic bundle-compatibility ValueError that never says the device is unknown.

**Evidence.** Probe with scan_id absent from storage: 'devices: {(samx, samx): None} | no exception; plugin._subscriptions: {}; callback calls: 0' — the heatmap's try/except catches nothing. Probe: subscribe('typo_device', ...) returned None; add_device('typo_device','typo_entry') on empty sub succeeded; same call after add_device('samx','samx') raised "ValueError: Cannot add device 'typo_device' ... resolved bundle is None." Naming seam: API says device_entry, widgets say signal (heatmap.py:743-748).

**Suggested fix (as filed).** Validate sources against device_manager at add_device/subscribe time and raise a dedicated UnknownDataSourceError uniformly; distinguish QUEUED vs FAILED states and retry failed subscriptions on the next scan-status/segment event.

**Status.** **PARTIALLY FIXED** — queued-vs-failed is now a consistent 'queued, retried on rebind' state (8b85c05d) and `unbound_devices` (f6cd80fa) makes it observable. Up-front validation with a dedicated error type is still recommended.

### F15 [MAJOR] DataAPI singleton silently ignores its client argument and close() leaves a resurrectable zombie instance

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:536`  
*Category:* architecture

**Claim.** DataAPI.__new__/__init__ pin the first client forever and silently discard any later client (536-548, 557-559), so widgets/tests holding a different client get a DataAPI wired to the wrong client's scan storage, connector, and callbacks; close() deletes _initialized but leaves _instance set, so the closed instance keeps handing out subscriptions that silently never receive data, and a later DataAPI(client_c) resurrects the same object rebound to a different client — breaking DataSubscription.close(), which unregisters from the NEW client's handler (data_api.py:358) while the registration lives on the old one. No test covers a second client (test_singleton_pattern passes the same mock twice), and ~15 test sites need DataAPI.clear_instance() to work around the global.

**Evidence.** Probes: 'api_b is api_a: True | api_b.client is client_a: True'; 'after close(): _instance is api_a: True; subscription on closed api: {(samx, samx): None} (silently dead); DataAPI(client_c) after close: same object: True | rebound client is client_c: True'. bec_widgets allows per-widget clients (bec_connector.py:128, :486); bec_lib's own pattern is per-client ownership (client.callbacks, client.queue, client.scan_history); test workaround at test_data_api.py:66-133.

**Suggested fix (as filed).** Drop the singleton: make the client own the instance via a lazy BECClient.data_api property (mirroring client.callbacks), removing clear_instance(); at minimum raise on a different client, have close() clear cls._instance, and add a test asserting the second-client behavior.

**Status.** **FIXED** in 19c2e947 — per-client instance registry; `close()` removes the instance so it cannot be resurrected; tests `test_data_api_per_client_instances`, `test_close_removes_instance`. Recommendation to move ownership onto the client itself stands (see Recommendations).

### F16 [MAJOR] remove_device leaves the removed device's data in the buffered _data_buffer forever

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:289`  
*Category:* correctness

**Claim.** remove_device (289-312) pops the device and unsubscribes but never purges self._data_buffer[device_name], so in buffered mode every subsequent emission still contains the removed device's stale frozen series.

**Evidence.** _data_buffer only cleared on scan change (:201), mode switch (:138) and close (:356). Probe: after remove_device(samy) and a new segment, 'last emission keys: [samx, samy] | stale samy still present: True'.

**Suggested fix (as filed).** In remove_device, delete the matching _data_buffer entry and drop the device_name key when empty.

**Status.** **FIXED** in 8b85c05d — the buffered series of the removed device is purged.

### F17 [MAJOR] Buffered mode stores one full cumulative snapshot per async update in an unbounded buffer: O(N^2) memory, OOM on long scans

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:167`  
*Category:* memory

**Claim.** _buffering_callback blindly appends whatever the plugin emitted (line 167) with no size cap, but for async add/replace sources the plugin emits the entire reconstructed cumulative value per update (plugins.py:1177-1190 deepcopy), so the facade buffer retains every historical snapshot — [v1, v1v2, v1v2v3, ...] — O(N^2) memory, and a ragged list of growing arrays delivered to consumers like the heatmap that expect one scalar per point.

**Evidence.** Probe (three 'replace' updates [1],[1,2],[1,2,3]): 'buffer content: [[1.0], [1.0, 2.0], [1.0, 2.0, 3.0]]' — heatmap's _extract_buffered_series turns this into an unusable ragged nested list. Memory probe: 'n_updates=400 arr_len=500: 400 points (no cap), retained numpy bytes=320.8 MB' matching N(N+1)/2*M*8; extrapolations N=1000/M=1000 -> 4.00 GB, N=36000 (10 Hz x 1 h) -> 5184 GB. No truncation anywhere (data_api.py:73-75, :167; cleared only at :201/:356).

**Suggested fix (as filed).** Store only the latest cumulative snapshot per replace/add-type async source (surface the plugin's update_type in emitted metadata and branch on it), and add an optional max-points/max-bytes cap to buffered subscriptions.

**Status.** **CLOSED in v2** — buffered mode was deleted with the prototype contract; sources are stored columnarly once (fragments, not per-point reconstructions), device streams are bounded by `max_points`, and `f75e015c` made snapshot construction incremental and cached (guarded by `test_data_api_benchmarks.py`).

### F18 [MAJOR] Buffered mode re-emits the entire accumulated buffer on every update: O(N^2) consumer work

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:169`  
*Category:* performance

**Claim.** _buffering_callback re-emits the whole buffer per point (data_api.py:159-176), so any consumer that walks it (the heatmap does, _extract_buffered_series) performs N(N+1)/2 item visits per scan — 1.94 billion visits / 60.4 s of pure callback CPU measured for a 36000-point (10 Hz x 1 h) 3-device scan vs 108k for incremental delivery — each emission also crossing the Qt signal bridge as a fresh payload.

**Evidence.** data_api.py:167 append then re-emit at :176; heatmap.py:771-786 walks fully per emission. Probe probe_buffered_quadratic.py: n=9000 -> 2.74 s; n=18000 -> 11.52 s (scaling 4.20 = O(N^2)); n=36000 -> 60.40 s / 1,944,054,000 visits, excluding the numpy image rebuild and Qt rendering. MAX_PENDING_UPDATES caps only the plugin-side pending buffers (plugins.py:1297).

**Suggested fix (as filed).** Emit only the increment (or (buffer, n_new)) and expose the accumulated buffer via a pull accessor/version counter; store columnar numpy arrays per source; at minimum coalesce emissions with a rate limit.

**Status.** **CLOSED in v2 + perf round** — emissions are coalesced full-state snapshots (`min_emit_interval`, trailing flush); `f75e015c` rebuilds only changed sources per emission (0.11 ms/emission at 4000 pts, benchmark-guarded) and widgets consume async data incrementally (widgets `3d8b4f44`: per-curve append-only buffers, identity-tested, ~4x faster than main's per-message hstack baseline in `tests/unit_tests/benchmarks/`).

### F19 [MAJOR] Buffered emissions alias the internal _data_buffer lists, mutated retroactively while the Qt thread reads them

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:174`  
*Category:* thread-safety

**Claim.** _buffering_callback passes the internal per-signal list objects to the user callback without copying (data_api.py:174), so the consumer's 'snapshot' mutates retroactively on every later update; the heatmap forwards these through a queued Qt signal, so the GUI thread iterates lists the connector dispatcher thread is still appending to — torn reads, transient x/y/z length mismatches that update_plot discards (heatmap.py:876-881), and set_scan_id's _data_buffer.clear() (:201) can empty lists referenced by an in-flight queued payload. The only shape test hides this by deep-copying inside its callback.

**Evidence.** data_api.py:167 append + :174 alias. Probes: 'same list object mutated retroactively ... aliased: True'; probe_close_and_alias.py (a): 'emitted list is the internal buffer list: True ... same list seen by Qt thread now has len=5' (was 3 at emission). Consumer path: heatmap.py:742 set_callback(self.data_api_update.emit) (plain bound method, not QtThreadSafeCallback — bec_dispatcher.py:133-136), :317 queued connect, :861-863 iteration on the Qt thread. test_buffered_mode_accumulates_data needs copy.deepcopy in its callback to pass (test_data_api.py:1806-1807); no test asserts emission isolation.

**Suggested fix (as filed).** Emit shallow copies of the per-signal lists (list(signal_list)) in _buffering_callback — the dict rebuild loop already exists — and add a test asserting emitted buffers are immutable snapshots.

**Status.** **FIXED** in 8b85c05d — buffered emissions carry shallow copies of the per-signal lists; test `test_buffered_emission_is_snapshot`.

### F20 [MAJOR] DataSubscription has no lock: scan-status rebinding on the dispatcher thread races add_device/remove_device/set_callback/close on the Qt thread

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:372`  
*Category:* race-condition

**Claim.** _handle_scan_status_update runs inline on the connector dispatcher thread (registered sync=False, data_api.py:77-79; callback_handler.py:50-52) and performs set_scan_id -> _data_buffer.clear() (:201) and _resubscribe_all, which iterates and rewrites self._devices (:372-387), racing unlocked mutations from the GUI thread (add_device :277/:285, remove_device :307, close :353-356) — 'dictionary changed size during iteration' at :372 or :171, devices left with sub_ids for the old scan, and a TOCTOU between the _user_callback guard at :150 and the call at :176 turns close() into a TypeError.

**Evidence.** probe_threads.py proves client.callbacks(sync=False) handlers run on 'Thread-2 (_dispatch_events) (RedisConnector)'. grep confirms zero threading/Lock usage in data_api.py and plugins.py. Unlocked pairs: :372 iteration vs :277/:285 assignment; :201/:356 clear vs :171 iteration; :150 guard vs :355 `self._user_callback = None`.

**Suggested fix (as filed).** Add an RLock protecting _devices/_data_buffer/_user_callback/_scan_id, taken by every public method, _buffering_callback and _handle_scan_status_update; read _user_callback into a local before the guard and call the local.

**Status.** **FIXED** in 0b6f970f — facade shares the DataAPI RLock; all public methods, the buffering callback and the scan-status handler lock.

### F21 [MAJOR] Static acquisition_group lookup reads a key that does not exist in real device signal info

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:342`  
*Category:* api-mismatch

**Claim.** plugins.py reads `async_signal_info.get("acquisition_group")` (plugins.py:342, :367, :518) on entries from `get_bec_signals()`, but the real serializer only nests it under `entry["describe"]["signal_info"]["acquisition_group"]`, so static bundle-domain resolution always returns None, `allows_runtime_bundle_resolution` always returns True, and the ValueError asserted in tests can never occur in production; the unit tests pass only because they mock the wrong (flat) shape.

**Evidence.** device_serializer.py:234-250 builds entries with only {component_name, signal_class, obj_name, storage_name, kind_int, kind_str, doc, describe, metadata, labels}; acquisition_group only inside describe.signal_info (ophyd_devices/utils/bec_signals.py:188-198). Probe with real AsyncSignal: "TOP-LEVEL acquisition_group: None / nested acquisition_group: monitored". Tests fabricate flat entries with top-level 'acquisition_group' (test_data_api.py:869-879, 934-944, 1019-1029, 1668-1679, 1704-1708), so test_subscription_rejects_async_sources_with_different_static_acquisition_groups (test_data_api.py:1693) passes while plugins.py:342 is always None in production; only the runtime metadata fallback (plugins.py:518 `or metadata.get("acquisition_group")`) works.

**Suggested fix (as filed).** Read the group via entry_info.get("describe", {}).get("signal_info", {}).get("acquisition_group") in _get_async_signal_info/_resolve_async_bundle_domain (or flatten once at fetch), fix the unit-test mocks to the real nested shape, and add a test that builds entry_info via device_serializer output for a BECMessageSignal.

**Status.** **FIXED** in 8b85c05d — `_acquisition_group_from_info` reads `describe.signal_info.acquisition_group` (flat key kept as fallback); test `test_nested_acquisition_group_is_resolved` builds the real serializer shape and fails on b3b6b912.

### F22 [MAJOR] Reentrancy: a callback that unsubscribes/subscribes during emit corrupts the emit loop — RuntimeError/IndexError, duplicates, and silently lost delivery to other subscribers

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:921`  
*Category:* reentrancy

**Claim.** _check_and_emit_synchronized_data invokes user callbacks synchronously (plugins.py:1406) before trimming buffers (:1407-1408) and while _handle_scan_segment_update is still iterating subscriptions.items() (:921); a callback that calls subscribe/reload/add_device re-enters the emit loop, re-emits in-flight points, then breaks the outer frame's indices (IndexError), and a callback that calls close/unsubscribe deletes the entry being iterated (`del subscriptions[callback_ref]` :764; sibling list bug at :1111 vs :791), crashing the iteration so remaining subscribers of that segment silently lose the event (exception swallowed at callback_handler.py:55-61 / managed_redis_connection.py:687-693). Deterministic on one thread; the docstring (data_api.py:20-31) advertises exactly these runtime operations, and none of the 82 tests exercises it.

**Evidence.** probe_reentrancy.py case A (callback calls plugin.subscribe): 'IndexError: list index out of range; emitted (expected [0.0, 1.0]): [0.0, 0.0, 1.0]; duplicate emissions: 1'. Case B (callback A of two unsubscribes): 'RuntimeError: dictionary changed size during iteration; callback_a received: [0.0]; callback_b received: []'. Independently reproduced by probe_gaps.py PROBE 2. If it was the scan's final segment the last points are lost permanently.

**Suggested fix (as filed).** Defer user-callback invocation until internal state is consistent: collect (callback, data, metadata) tuples while iterating, trim buffers first, then invoke callbacks outside all iterations; queue reentrant subscribe/unsubscribe requests; snapshot (list(...)) dicts/lists before iterating; add tests mutating the subscription from inside a callback.

**Status.** **FIXED** in 0b6f970f — emissions are collected and buffers trimmed before callbacks run; tests `test_unsubscribe_from_callback_keeps_sibling_delivery`, `test_subscribe_from_callback_does_not_duplicate_emissions` (both fail on b3b6b912 with RuntimeError/IndexError).

### F23 [MAJOR] One silent source starves the whole bundle forever with no timeout or diagnostics

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1371`  
*Category:* correctness

**Claim.** Emission requires every subscribed source to have >=1 pending point (plugins.py:1371, 1378-1379); a _DataBuffer is only created when data arrives (1268-1271) and async subscriptions are unconditionally counted (1339-1347), so a monitored device absent from live_data or an async signal that never fires blocks all other sources' data for the entire scan, silently; incompatible_sources (:596) only shrinks the count after the source has produced at least one update, so it never rescues a never-producing source. Counting is also asymmetric: monitored devices are excluded when bundle_domain is async (:1334) but async subs are counted for monitored domains.

**Evidence.** Probe C1 (samy never in live_data, 5 segments): 'emissions: 0 | samx pending: 5'. Probe C2 (async sig1 in group monitored never emits): 'emissions after 5 monitored points: 0'.

**Suggested fix (as filed).** Emit per-domain with a staleness timeout or emit partial bundles flagged incomplete; at minimum log when a bundle is blocked by a bufferless source for N segments and drop never-producing sources from expected_device_count.

**Status.** **CLOSED in v2 (`409aad1f`)** — a silent source no longer blocks delivery (full snapshots); declared sources are pre-registered so a silent source visibly stalls `aligned_ordinals`; updates carry `metadata["lagging_sources"]` (logged once per scan); unavailable sources are retried on the next scan-status update; `unbound_sources` remains for introspection.

### F24 [MAJOR] Callback buffers keyed only by callback_ref: two scans sharing a callback emit wrong-scan partial bundles and desync

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:558`  
*Category:* correctness

**Claim.** _callback_buffers is keyed by callback_ref alone (plugins.py:246; _get_or_create_callback_buffer 558-562 ignores a differing scan_id), so one callback subscribed to two scans shares a single buffer/bundle_domain: the second scan's data is rejected as bundle-incompatible, its segment events flush the first scan's pending data as partial bundles stamped with the wrong scan_id (metadata from the passed scan_id, :1388), and the first scan's alignment is permanently broken; incompatible_sources (:596) is likewise keyed without scan.

**Evidence.** Probe B: cb on samx+samy (scan_1) and samq (scan_2); a scan_2 segment produced "data={'samx': {... 111.0 ...}} metadata_scan_id=scan_2" — scan_1's samx point emitted alone under scan_2's id (expected count for scan_2 is 0, `1 < 0` False at :1371); when samy's data arrived, no further emission.

**Suggested fix (as filed).** Key _CallbackBuffer (and incompatible_sources) by (callback_ref, scan_id), and pass the buffer's own scan_id to emission instead of the caller's.

**Status.** **FIXED** in d89e9ae9 — `_callback_buffers` is keyed by `(callback_ref, scan_id)`.

### F25 [MAJOR] MAX_PENDING_UPDATES overflow drops from one buffer only, permanently shifting bundle registration — and the unit test blesses the mispairing

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1301`  
*Category:* data-integrity

**Claim.** Alignment is purely positional (timestamps carried but never compared, :1386-1396) and _enforce_pending_backlog_limit deletes oldest entries from the overflowing buffer only (`del data_buffer.data[:overflow]`, :1301), so after any overflow every subsequent bundle pairs source A's point i with source B's point i-offset for the rest of the scan — silent misregistration (e.g. shifted heatmap pixels); an async source emitting 2 messages per point desynchronizes the same way with no correction. test_pending_buffer_overflow_drops_oldest_updates codifies the mispairing without asserting async values.

**Evidence.** Probe D (MAX_PENDING_UPDATES=2, true pairing samx point p <-> sig1 10*(p+1)): 'bundle 0: samx=1.0 sig1=10.0 / bundle 1: samx=2.0 sig1=20.0 / bundle 2: samx=3.0 sig1=30.0' — off by one permanently including new lockstep data. test_data_api.py:1070-1072 asserts only monitored_values == [2.0, 3.0], never the (monitored, async) pairs; the e2e uses ConstantModel so it cannot detect it either.

**Suggested fix (as filed).** On overflow drop the same number of aligned rows across all buffers of the bundle (or resynchronize via timestamps/async_indices), record the discontinuity in emitted metadata, and make the test assert full (monitored, async) pairs.

**Status.** **FIXED** in d89e9ae9 — buffers carry an absolute stream offset; emission aligns fronts to the common position so a drop discards the matching positions in sibling buffers. The blessing unit test was rewritten to assert the true pairing (samx=2.0 ↔ async=20.0).

### F26 [MAJOR] Async update-type change raises ValueError inside the connector callback and permanently kills the source

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1161`  
*Category:* reliability

**Claim.** _resolve_async_signal_value raises ValueError on a type change (1160-1163) without updating state, so every subsequent message of the new type raises again; the exception is swallowed in the connector dispatch (managed_redis_connection.py:687-693), the source delivers no further data (stalling the whole bundle per the starvation finding), and last_index is never advanced so a later same-type message trips the continuity check and flags async_state_incomplete forever. The raise happens before per-callback buffering (:1102 precedes :1111), so no subscriber receives anything.

**Evidence.** Probe E: add(idx0) then replace(idx1): 'ValueError escaped ... add -> replace'; '3/3 subsequent updates also raised; emissions delivered: 1'; recovered add(idx5) emitted with async_state_incomplete=True permanently.

**Suggested fix (as filed).** Treat a type change like the bundle-domain change at 535-540: log a warning and skip (or reset state via replace semantics) instead of raising inside a connector callback.

**Status.** **FIXED** in 8b85c05d — a type change resets the accumulated state with a warning and flags `async_state_incomplete`; the source keeps delivering; test `test_async_update_type_change_resets_source_instead_of_raising`.

### F27 [MAJOR] Second subscriber to an existing async subscription gets no from_start replay, misaligning or losing history

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1018`  
*Category:* correctness

**Claim.** connector.register(..., from_start=True) runs only for the first callback of a (scan, device, entry) key (:1048); a later callback is merely appended (1018-1022) and never sees already-streamed messages, while its monitored companion IS backfilled from live_data — so the late subscriber's bundles pair monitored history with future async fragments, or emit the history without the async source at all. Two widgets watching the same detector signal in one scan is a standard setup.

**Evidence.** Probe F: 'register calls after cb2 subscribe: still 1 (no from_start replay)'. Async-first: cb2 bundles 'samx=0.0 sig1=40.0 / samx=1.0 sig1=50.0 / samx=2.0 sig1=60.0' (true pairing 10*(p+1)) — off by three for the whole scan. Monitored-first: historic points emitted with sig1=None.

**Suggested fix (as filed).** On reuse, replay the stream history for the new callback (connector stream read from 0, or from the plugin-side _AsyncSourceState/history) before appending it, so its buffers start aligned with the monitored backfill.

**Status.** **FIXED** in d89e9ae9 — per-source emission history is replayed for late-joining callbacks; test `test_late_subscriber_replays_async_history` (fails on 0b6f970f).

### F28 [MAJOR] Monitored values/timestamps length mismatch permanently drops data points; branch untested

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:958`  
*Category:* correctness

**Claim.** _handle_scan_segment_update only buffers values[idx] when idx < len(timestamps) (958-968) but then advances monitored_indices to len(values) (:971), so any point whose timestamp lags its value is skipped forever; no test covers unequal lengths or the scalar-wrap branches (948-951, uncovered per coverage run).

**Evidence.** Probe (PROBE 3): val=[1.0,2.0,3.0], timestamp=[1.0] -> only 1.0 emitted, 'monitored index advanced to: 3'; after timestamps caught up and a second segment: 'buffered points: 0; emitted: [1.0]' — 2.0/3.0 permanently lost. Coverage: plugins.py missing 949, 951.

**Suggested fix (as filed).** Advance monitored_indices only past indices actually buffered; add a test with len(timestamps) < len(values).

**Status.** **FIXED** in 8b85c05d — the cursor only advances past complete (value, timestamp) pairs; lagging points are delivered once timestamps catch up; test `test_monitored_timestamp_lag_recovers`.

### F29 [MAJOR] Dead callback weakrefs are never pruned and disconnect() clears only async state: subscriptions, buffers and connector registrations leak permanently

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1113`  
*Category:* memory-leak

**Claim.** If a callback owner is garbage collected without unsubscribe, every tracking structure (_subscriptions, _monitored_subscriptions, _async_subscriptions.callback_refs, _callback_buffers with pending data, _async_source_states) is retained forever and client.connector.unregister is never called — dead refs are merely skipped with `continue` (922-924, 1113-1114), cleanup exists only in explicit unsubscribe paths (759-768, 804-813); disconnect() (257-275) clears only the two async dicts, leaving _subscriptions/_monitored_subscriptions/_callback_buffers intact, and _get_expected_device_count keeps counting dead async callbacks' subscriptions. Matches the known grow-only-client-structure pattern from the BEC memory audit.

**Evidence.** Probe probe_dead_callbacks.py after `del w; gc.collect()` + 5 updates: '_subscriptions: 2 (refs dead)', '_monitored_subscriptions: {scanA: 1}', dead callback_refs retained, '_callback_buffers: 1' with pending data, 'connector.unregister call count: 0'. connect() registers only "scan_segment" (253-255) so scan end triggers no cleanup.

**Suggested fix (as filed).** Pass an on_delete handler to louie.saferef.safe_ref that schedules pruning of all structures keyed by the dead ref, sweep dead refs in the _handle_* paths instead of `continue`, and clear all tracking dicts in disconnect().

**Status.** **FIXED** in 0b6f970f+19c2e947 — `disconnect()` clears every structure; dispatch prunes dead refs via `_prune_dead_callback` (reuses unsubscribe teardown incl. connector unregister); tests `test_dead_callback_pruned_on_segment_dispatch`, `test_async_source_torn_down_when_all_subscribers_dead`.

### F30 [MAJOR] Cumulative async state keeps growing and deepcopying even when every subscriber is already dead

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1102`  
*Category:* performance

**Claim.** _handle_async_signal_update resolves (accumulates + full deepcopy of) the cumulative value before checking whether any callback is alive (:1102 precedes the liveness loop at 1111-1114), so a fully dead subscription still pays the quadratic CPU cost and its _async_source_states entry keeps growing for the rest of the scan stream.

**Evidence.** Probe probe_dead_callbacks.py: after deleting the only callback owner and feeding 5 more updates: 'cumulative state still grows: len = 10 bytes = 80000'; the deepcopy at :1190 ran on each update with its result discarded.

**Suggested fix (as filed).** Check `any(ref() is not None for ref in async_sub.callback_refs)` first and skip resolution (or unsubscribe the source) when no live consumer remains.

**Status.** **FIXED** in 19c2e947 — dead subscribers are pruned before value resolution; the last one tears the source down entirely.

### F31 [MAJOR] Thread-safety rests on an implicit single-dispatcher-thread invariant already violated by backfill; zero threading/reentrancy tests in the 82-test suite

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:238`  
*Category:* test-coverage

**Claim.** The plugin's two ingestion paths (client.callbacks 'scan_segment' :253-255 and the connector stream callback :1048) only avoid racing each other because one RedisConnector funnels pubsub and stream messages through a single dispatcher thread — an undocumented, unenforced accident (the backfill path already violates it; a second connector or ThreadPoolExecutor-rescheduled callback would too) — while subscribe/unsubscribe/close run on the caller (Qt) thread mutating the four unlocked shared dicts (238-246, no threading primitive in the module); no test starts a second thread or exercises reentrancy.

**Evidence.** probe_threads.py: 'pubsub cb thread / stream cb thread / client.callbacks(sync=False): all Thread-2 (_dispatch_events) (RedisConnector); ALL CALLBACKS ON SINGLE DISPATCHER THREAD: True' (managed_redis_connection.py:115, 322-331, 517-540, 661-685). grep 'thread' in test_data_api.py -> 0 hits; grep 'Lock' in plugins.py -> no hits; every probe in the scratchpad reproduces a failure the suite cannot see.

**Suggested fix (as filed).** Document the threading contract in the module docstring, enforce it with the RLocks from the race findings, and add regression tests mirroring the probes (reentrancy from a callback, close-during-dispatch, subscribe-backfill vs live segment, producer-thread stress).

**Status.** **FIXED** in 0b6f970f — the invariant is no longer load-bearing (explicit RLock); threading regression tests added.

### F32 [MAJOR] DataAPI covers only the live-monitored slice: no history plugin, and the bundle model cannot express Waveform/Image needs — widgets must keep dual code paths

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:298`  
*Category:* architecture

**Claim.** The only plugin defers closed scans to a nonexistent history plugin (plugins.py:298-300; register_plugin is called exactly once with BECLiveDataPlugin, data_api.py:564), there is no device-readback or monitor-endpoint source, and the one-bundle-per-subscription rule (data_api.py:413-417; standalone_async domain includes the device, plugins.py:524 / data_api.py:437-438) cannot express Waveform's standard mix of monitored curves + multiple async curves from different acquisition groups + standalone async sources, x_mode index/timestamp/auto with no x device (waveform.py:707-750, 1243-1281), 25 Hz throttling (waveform.py:201), 2D array slicing (waveform.py:1712-1713), or Image's monitor endpoints (image.py:434-517) — so the heatmap draft retains its full legacy fetch path (heatmap.py:865) and a Waveform port would need one subscription per domain, losing the cross-source synchronization that is the API's purpose, plus a scan-lifecycle/settle notification the (data, metadata) callback contract lacks.

**Evidence.** plugins.py:298-300 comment '# We skip closed scans and instead rely on historical data plugin' — no such plugin exists; heatmap keeps _fetch_scan_data_and_access (heatmap.py:865) plus old on_scan_status/on_scan_progress slots; a second standalone async device raises the bundle-incompatibility ValueError (data_api.py:413-417).

**Suggested fix (as filed).** Define the source taxonomy the API must cover (live monitored, async array-mode, history/file, device readback/monitor streams), allow multiple bundle domains per subscription (grouped emissions) with an atomic add_devices([...]), per-device health reporting, optional emission rate limit and scan open/close events in metadata, and ship the history plugin with the same emission contract before generalizing to other widgets.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F33 [MAJOR] Async Redis wiring never exercised: connector.register mocked in every test although a fakeredis connector is available

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1046`  
*Category:* test-coverage

**Claim.** All async-signal tests replace connector.register with MagicMock and call _handle_async_signal_update directly, so the connector_callback closure (:1046, uncovered), endpoint construction with storage_name (1030-1032), from_start stream delivery, and the {'data': DeviceMessage} payload contract are untested; fixtures always set obj_name == storage_name, so swapping them (endpoint uses storage_name :1031 vs signals.get(obj_name) :1083) would pass every test, and the silent-drop guard for that failure (:1085) is itself uncovered.

**Evidence.** connector.register mocked at test_data_api.py:411, 449, 887, 948, 1112, 1680, 1716 despite mock_client using the real fakeredis connected_connector fixture (conftest.py:36). Contract verified manually: bec_message_handler.py:176-181 xadds {'data': message}; managed_redis_connection.py:478-481, 704-707; hli.py:91-131 from_start.

**Suggested fix (as filed).** Add a unit test that xadds a real DeviceMessage into the fakeredis stream endpoint and asserts the callback fires through the registered connector path, including an AsyncMultiSignal case where obj_name != storage_name.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F34 [MAJOR] Heatmap connects DataAPI updates straight to update_plot with no rate limit, unlike the 5 Hz SignalProxy it replaces

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:317`  
*Category:* performance

**Claim.** data_api_update is connected directly to update_plot (heatmap.py:317) while the legacy path is throttled via pg.SignalProxy(rateLimit=5) (319-321); the plugin invokes the callback once per aligned bundle and buffered mode re-emits the full buffer each time, so a backfill or fast scan queues one full O(N) extraction + grid/interpolation rebuild per point on the GUI thread — O(N^2) GUI-thread work, freezing the UI on large scans.

**Evidence.** Probe: 5 separate emissions for a 5-point backfill, each carrying the full buffer; cross-thread emit is queued per-event to the GUI thread (probe_slot_delivery.py: emitted on 'fake-connector', executed on 'MainThread'); combined with the measured 60.4 s buffer-walk for a 36000-point scan.

**Suggested fix (as filed).** Route data_api_update through a pg.SignalProxy (rateLimit) or QTimer coalescing keeping only the newest payload, or add latest-only semantics to buffered DataSubscription emissions.

**Status.** **FIXED** in widgets 08200a05 — the feed goes through a 5 Hz `pg.SignalProxy` delivering the newest full-state payload (lossless coalescing for buffered mode).

### F35 [MAJOR] Async z-signal fragments that are arrays pass the length check but crash grid rendering

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:1231`  
*Category:* correctness

**Claim.** For an async z-signal in the 'monitored' group the plugin emits the raw per-point fragment verbatim (plugins.py:1104-1108); if the device batches an array per point, _extract_buffered_series appends the list as one element (heatmap.py:786), lengths still match x/y, and get_grid_scan_image's scalar assignment data[x_i, y_i] = z_data[i] (:1231) raises ValueError on every update — SafeSlot only logs it (error_popups.py:298), so the widget silently never draws; an async source emitting one message per k monitored points also shifts bundle alignment by design so the widget skips on length mismatch.

**Evidence.** Probe test_rebind_probe.py::test_async_z_array_fragments (AsyncSignal acquisition_group='monitored' emitting [10+i,20+i,30+i]): "z (2): [[10.0, 20.0, 30.0], [11.0, 21.0, 31.0]]; grid assignment raises: ValueError: setting an array element with a sequence."

**Suggested fix (as filed).** Define the buffered contract explicitly (scalar per aligned point for heatmap use); reject/flatten array-valued z with a clear log message, or have the plugin split multi-point fragments into per-point bundle entries.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F36 [MAJOR] Failure path of _setup_data_api_subscription leaks a half-configured live subscription that emits forever

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:752`  
*Category:* resource-leak

**Claim.** In the except block (752-755) self._data_subscription is still None (assigned only at :757), so _cleanup_data_api_subscription() is a no-op and the local subscription is never closed; DataSubscription.__init__ already registered its scan-status handler strongly with client.callbacks (data_api.py:76-79; callback_handler.py:41), so the orphaned subscription — plus any devices added before the raise, wired to self.data_api_update.emit — survives, rebinds on every new scan, and keeps pushing partial payloads into the widget (or RuntimeError-spamming logs once the widget's C++ object is deleted, since it strongly holds SignalInstance.emit).

**Evidence.** add_device raises synchronously when a scan is bound at plot() time and z resolves to a different bundle domain (data_api.py:413-417; set_callback binds first at :239 -> :470, so subsequent add_device calls subscribe immediately at :280-285 before the failing one aborts). The success path is safe (plugin holds only saferef of _buffering_callback; widget holds the subscription strongly) — this failure path inverts it.

**Suggested fix (as filed).** Assign self._data_subscription = subscription immediately after create_subscription (or call subscription.close() on the local in the except block) so cleanup can reach it.

**Status.** **FIXED** in widgets 08200a05 — the subscription is assigned before device adds, so the cleanup path can close it; widget test `test_heatmap_failed_subscription_setup_closes_partial_subscription`.

### F37 [MINOR] E2E addition is value-blind: ConstantModel + count-only assertions cannot detect pairing errors, and buffered mode has no e2e

*Location (at `b3b6b912` / `820afb20`):* `bec_ipython_client/tests/end-2-end/test_scans_lib_e2e.py:126`  
*Category:* test-coverage

**Claim.** The single e2e test is correctly structured and runs in CI, but asserts only key-sets, per-callback length equality, and totals==100; select_model('ConstantModel') makes every waveform value identical, so the misalignment bug class is undetectable, it never compares against scan data (unlike the neighboring test_async_callback_data_matches_scan_data_lib), never exercises buffered=True (the heatmap's mode), and depends entirely on the runtime acquisition_group metadata path (sim_waveform.py:99 -> bec_signals.py:789-792) — the only reason bundling works given the static-info finding. Minor race: the polling loop sums a list the dispatcher thread appends to.

**Evidence.** Diff 91aaccf5..HEAD: assertions are key-set/length/total only; CI wiring: bec_e2e_install/action.yml:81, ci.yml:57-60.

**Suggested fix (as filed).** Use a non-constant waveform model and assert per-bundle value pairing against scan_item data; add a buffered=True e2e variant.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F38 [MINOR] set_buffered(True) mid-stream yields a partial buffer, contradicting its documented semantics

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:134`  
*Category:* correctness

**Claim.** Switching buffered off->on only flips the flag (131-140; buffer cleared only when switching OFF), so the 'entire accumulated data buffer' promised by the docstring (117-120) contains only points received after the switch — earlier points went through the pass-through branch (153-156) and were never stored.

**Evidence.** data_api.py:134-138: `self._buffered = buffered; if not buffered: self._data_buffer.clear()` — no resubscribe/backfill in the True branch.

**Suggested fix (as filed).** On switching to buffered mode, trigger reload()/resubscription (with the reload duplicate bug fixed) so the buffer is rebuilt from scan storage, or document that buffering starts at switch time.

**Status.** **CLOSED by deletion (v2)** — buffered mode and `set_buffered` no longer exist; every emission is a full-state snapshot, so the documented semantics cannot diverge.

### F39 [MINOR] Emission contract: data shape differs between buffered and non-buffered modes and metadata merge is lossy

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/data_api.py:159`  
*Category:* api-contract

**Claim.** callback(data, metadata) delivers data[dev][entry] as a single {"value","timestamp"} dict in non-buffered mode but a list of such dicts in buffered mode, forcing consumers to type-sniff; buffered mode discards all per-point metadata (only the latest bundle's metadata passed, :176; per-point metadata stored at plugins.py:1274 dropped), and per-bundle metadata is a first-writer-wins setdefault merge (plugins.py:1397-1403) with no device attribution for async_indices/async_update/async_state_incomplete.

**Evidence.** plugins.py:1393-1396 single-dict shape vs data_api.py:159-176 list shape; heatmap must sniff both (heatmap.py:777-780).

**Suggested fix (as filed).** Define one stable emission type (frozen dataclass/TypedDict: per-(device,entry) columnar values+timestamps plus per-point, per-device metadata), identical in both modes with buffered differing only in row count; namespace async metadata per device.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F40 [MINOR] Terminal-status sets disagree: has_scan_data omits "user_completed" and is dead code

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:298`  
*Category:* api-mismatch

**Claim.** BECLiveDataPlugin.has_scan_data treats only ["closed","aborted","halted"] as terminal while the real client defines a fourth terminal literal "user_completed" (which data_api.py:462-467 does include, making the two checks inconsistent), and has_scan_data is never called by DataAPI/DataSubscription at all — a dead, diverging abstract contract.

**Evidence.** plugins.py:298 `if scan_item.status in ["closed", "aborted", "halted"]:` vs messages.py:287 Literal[... "user_completed"] and scan_items.py:361 `terminal_states = {"aborted", "halted", "closed", "user_completed"}`. grep: only non-test references to has_scan_data are its declaration (plugins.py:35) and implementation (plugins.py:277); no caller.

**Suggested fix (as filed).** Define the terminal-state set once (reuse scan_items' terminal_states), add "user_completed", and either wire has_scan_data into DataAPI routing or remove it from the ABC.

**Status.** **PARTIALLY FIXED** in 8b85c05d — one `TERMINAL_SCAN_STATES` constant (incl. `user_completed`) used by both checks. `has_scan_data` remains an uncalled ABC member (see Recommendations).

### F41 [MINOR] Out-of-order scan-segment point_ids cause duplicated and permanently lost monitored points

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:958`  
*Category:* correctness

**Claim.** Monitored progress tracking is positional over SignalData.val, which is rebuilt as values sorted by point index (live_scan_data.py:39), so if a point_id arrives after a higher one, positions shift: the last value is re-emitted and the late point is skipped forever (monitored_indices set to len(values) at :971 regardless).

**Evidence.** Probe H: points 0 and 2 arrive, then 1: emitted samx values '[0.0, 2.0, 2.0]' — 2.0 twice, 1.0 never. Loop at plugins.py:958 assumes stable list positions.

**Suggested fix (as filed).** Track processed point_ids (max processed key per device, iterating SignalData.data.keys()) instead of positions in the sorted value list.

**Status.** **CLOSED in v2 (`5e362605`/`0ec5aea0`)** — monitored data is keyed by `point_id` (ordinal engine): late point_ids fill holes without shifting pairs (`test_gap_does_not_shift_pairing`, `test_out_of_order_insert_fills_hole`).

### F42 [MINOR] Full device-manager walk (get_bec_signals) executed on every incoming async update

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:517`  
*Category:* performance

**Claim.** _resolve_async_bundle_domain calls _get_async_signal_info on every async update, which iterates all devices and all their signals with no caching: 0.287 ms per update measured with 500 devices x 10 signals — ~29% of a 1 kHz update budget on a large beamline.

**Evidence.** Chain: plugins.py:1097 -> :517 -> :469-475 get_bec_signals -> devicemanager.py:830-833 full walk per call. Probe probe_signal_scan.py: '0.287 ms/update (0.29 s per 1000 updates)'.

**Suggested fix (as filed).** Cache the resolved async-signal info per (device_name, device_entry) at subscribe time (already fetched in _subscribe_to_async_signal) and reuse it in _resolve_async_bundle_domain.

**Status.** **FIXED** in 8b85c05d — resolved async-signal info is cached per (device, entry); cache cleared on disconnect. Note: a device-config change during a session is not yet invalidated (acceptable for the prototype, flagged in Recommendations).

### F43 [MINOR] Plugin ABC is premature: dead abstract has_scan_data, magic free-form priority key, hard-wired registration

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:34`  
*Category:* extensibility

**Claim.** The abstract has_scan_data is never called by DataAPI dispatch, get_info() is an untyped dict whose only consumer reads a magic 'priority' key defaulting to 100 sorting a one-element list, and the only plugin is hard-coded in DataAPI.__init__ — the abstraction carries no weight and locks in an unproven surface.

**Evidence.** data_api.py:605 sorts by get_info().get("priority", 100); :564 single hard-coded register_plugin(BECLiveDataPlugin); BECLiveDataPlugin does not override get_info. bec_lib elsewhere uses pydantic models for contracts and importlib entry points for plugin discovery (plugin_helper.py:162, macro_update_handler.py:164).

**Suggested fix (as filed).** Either call has_scan_data in dispatch (routing closed scans to the future history plugin) or delete it; replace get_info with declarative class attributes or a small pydantic PluginInfo; discover a second plugin via entry points.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F44 [MINOR] No Qt-thread delivery helper: every widget must hand-roll the signal bridge (and the heatmap loses rate limiting doing so)

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:1048`  
*Category:* qt-integration

**Claim.** DataAPI callbacks fire on connector/scan-storage background threads — raw connector.register (:1048) and client.callbacks bypass the QtThreadSafeCallback marshalling BECDispatcher provides — so every consuming widget must duplicate a Signal-bridge (heatmap.py:270/317/742), with no shared throttling or decimation hook.

**Evidence.** bec_dispatcher.py:133-136: only QtThreadSafeCallback instances are marshalled; scan_segment/scan_status run inline in the storage thread (scan_items.py:119/132 -> callback_handler.py:49-52).

**Suggested fix (as filed).** Ship the bridge once: a bec_widgets QtDataSubscription wrapper (owns the Signal, optional rateLimit/decimation), or let create_subscription accept a delivery adapter; a downsampling hook belongs at this layer.

**Status.** **CLOSED (`2d3c51c6`/`e46bb1b3`/widgets `3d8b4f44`)** — `QtDataSubscription` is the shared delivery helper (queued Qt-thread marshalling, stale-scan filtering, health, size gate); rate limiting is backend-side (25 Hz live bridges, matching main's SignalProxy cadence) and widget rendering is incremental, benchmarked at or above main's throughput.

### F45 [MINOR] Divergent, undocumented callback-lifetime semantics: plugin weak-refs callbacks (closures die silently), facade strong-holds them

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:838`  
*Category:* api-contract

**Claim.** BECLiveDataPlugin stores callbacks only as louie saferef weak references (plugins.py:838, 1002) with silent dead-ref skips (922-924, 1112-1114), so a closure/lambda passed to the public DataAPI.subscribe dies on the next GC and data is silently dropped, whereas DataSubscription.set_callback holds strong references (data_api.py:232) — two documented usage paths with opposite lifetime guarantees, nothing in the subscribe() docstring (649-668) mentions it, no test covers dead callbacks, and the three dead-ref guard branches (924/1114/1383) are uncovered.

**Evidence.** Probe: subscribing with an inline lambda produced no callback buffer at all (safe_ref died before the first segment); rerun with a module-level function worked. Coverage: plugins.py missing 924, 1114, 1383.

**Suggested fix (as filed).** Pick one ownership model: strong plugin refs with lifetime via unsubscribe()/close(), or document the weak-ref contract loudly and make DataSubscription the only public surface (demote DataAPI.subscribe to _subscribe); add a test documenting the contract.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F46 [MINOR] Measured coverage gaps: 85 lines uncovered including all subscribe/unsubscribe error paths and live-follow edge guards; destructor test asserts nothing

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:676`  
*Category:* test-coverage

**Claim.** Coverage run: data_api.py 92% (missing 151, 333-334, 369, 437-439, 459, 468, 485, 490, 739-756), plugins.py 88% (65 missing). Untested: plugin.subscribe ValueError paths (633, 645-650); unsubscribe by scan_id+callback (676-683) and by callback only (693-702) — DataAPI-level unsubscribe tests use MagicMock plugins so the real matching never runs; unknown-id unsubscribe (714); async unsubscribe buffer cleanup (795-801); segment guard returns (907-917); non-DeviceMessage/missing-signal drops (1080, 1085); update-type-change ValueError (1161); add_slice missing index (1230); standalone_async normalization; live-bind guards (459, 468, 485, 490). test_destructor_cleanup (test_data_api.py:1426-1435) only runs 'del sub' and asserts nothing; plugin.disconnect leaving structures populated is unasserted.

**Evidence.** pytest --cov=bec_lib.data_api output: 'data_api.py 258 20 92% ...' and 'plugins.py 522 65 88% ...'; 82 passed in 0.73s.

**Suggested fix (as filed).** Add tests for each listed error/edge path, especially the two plugin-level unsubscribe variants and subscribe ValueError paths; make the destructor test assert unsubscribe was called.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F47 [MINOR] Hedged assertions in test_handle_scan_segment_update_monitored: 3 points fed, only '>= 1 call' asserted

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/tests/test_data_api.py:507`  
*Category:* weak-assertion

**Claim.** The core monitored-emission test feeds three ScanMessages but asserts only `len(mock_callback.calls) >= 1` followed by `if len(...) > 0:` (507-510), never emission count, ordering, or points 1-2, so duplicate backfill emissions or lost points pass; test_buffered_mode_accumulates_data is similarly loose (1838/1846 `>= 2`), tolerating duplicate re-emission.

**Evidence.** test_data_api.py:507 'assert len(mock_callback.calls) >= 1  # At least one call should have been made'; :510 comment-acknowledged hedge.

**Suggested fix (as filed).** Assert exact call counts and per-call values for all three points.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F48 [MINOR] Heatmap ignores payload scan_id and cannot cancel queued deliveries: stale/new-scan payloads render against the wrong scan geometry

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:860`  
*Category:* correctness

**Claim.** The buffered branch of update_plot (860-863) never checks metadata['scan_id'] against self.scan_id (the metadata parameter is even shadowed at :886), and _cleanup_data_api_subscription (:715) only calls subscription.close(), which cannot retract emissions already queued on the Qt event loop — so (a) when switching to a history scan (:499) a stale live-scan payload can redraw over the loaded history, (b) at scan boundaries the subscription rebinds on the connector thread (data_api.py:472-492) before the widget's independently-ordered on_scan_status reset runs, mapping new z-values through the old scan's geometry/_grid_index (883-920, 1168-1232), and (c) an in-flight dispatcher-thread callback holding data_api_update.emit can fire after cleanup() (:1797) during teardown, raising a swallowed shiboken 'already deleted' RuntimeError.

**Evidence.** heatmap.py:840 signature; :886 `metadata = self.scan_item.metadata["bec"]` overwrites the payload metadata; the plugin stamps every payload with {'scan_id': scan_id} (plugins.py:1388) which is accepted but unused; close-vs-inflight overlap shown in probe_close_and_alias.py (b) (32/200 trials).

**Suggested fix (as filed).** Drop payloads whose metadata['scan_id'] != the active live scan (or when _data_subscription is None), stop shadowing the metadata parameter, and disconnect self.data_api_update before closing the subscription in cleanup.

**Status.** **MOSTLY FIXED** in widgets 08200a05 — buffered payloads are dropped when the subscription is gone, a history view is active, or the payload scan_id mismatches; the metadata parameter is no longer shadowed; widget test `test_heatmap_update_plot_drops_stale_data_api_payload`. The teardown-window RuntimeError (in-flight queued emit after widget deletion) remains a known Qt-lifetime edge.

### F49 [MINOR] Legacy duplicated fetch machinery remains fully active alongside the DataAPI feed, sharing _grid_index

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:865`  
*Category:* design

**Claim.** While a subscription is active, every heatmap_property_changed emission (plot() :469, update_with_scan_history :530, interpolation/oversampling setters :1684/:1703/:1721) still fires sync_signal_update -> SignalProxy, calling update_plot with data=() (a tuple) and dropping into the full legacy _fetch_scan_data_and_access path (864-871); the two writers share _grid_index (get_grid_scan_image unconditionally sets self._grid_index = len(z_data), :1232), so a buffered payload shorter than live storage regresses the index and causes repaint churn; and _setup_data_api_subscription's only caller is plot() (grep: line 467), so a widget configured via config restore without plot() silently never uses DataAPI at all.

**Evidence.** Probe probe_slot_delivery.py call 2: "data_type: 'tuple', data: '()', is_dict: False, sender: 'SignalProxy'" — the proxy path always enters the legacy branch.

**Suggested fix (as filed).** When _data_subscription is active, re-render property-change refreshes from the cached last DataAPI payload instead of the legacy fetch; long-term, delete the legacy path once DataAPI covers history and mid-scan attach.

**Status.** **CLOSED (widgets `288e664a` + `d695bf07`)** — the heatmap's legacy fetch machinery, scan-progress trigger and shared `_grid_index` writer were deleted (`test_heatmap_has_no_legacy_data_path`); a widget restored from a saved configuration now starts its DataAPI feed without `plot()` (`test_heatmap_restored_from_config_starts_data_feed`).

### F50 [MINOR] End-of-scan settle updates are a silent no-op (verify_sender skips QTimer.singleShot) and the DataAPI path has no scan-end reconciliation

*Location (at `b3b6b912` / `820afb20`):* `bec_widgets/widgets/plots/heatmap/heatmap.py:836`  
*Category:* dead-code

**Claim.** The final QTimer.singleShot(100/300, self.update_plot) fallback (836-837) never executes the slot body because @SafeSlot(verify_sender=True) skips invocations whose sender() is None (error_popups.py:284-290), and on the DataAPI path on_scan_progress returns before reaching it (831-832) — so neither path performs a final reconciliation at scan end; if the plugin dropped or misaligned points, the image stays incomplete permanently. Pre-existing on main, but the draft leans on this fallback as its safety net.

**Evidence.** Probe probe_slot_delivery.py: QTimer.singleShot(0, w.update_plot) logged "Sender is None ... skipping method call." and the slot body did not run.

**Suggested fix (as filed).** Call update_plot with _override_slot_params={'verify_sender': False} from the QTimer lambdas, and add an explicit end-of-scan handler on the DataAPI path (one verification fetch or a DataSubscription 'final' emission on scan close).

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F51 [MINOR] Widget tests mock DataAPI away entirely; the buffered-payload contract is self-asserted on both sides with no shared fixture

*Location (at `b3b6b912` / `820afb20`):* `tests/unit_tests/test_heatmap_widget.py:33`  
*Category:* test-coverage

**Claim.** The widget test additions replace DataAPI with _FakeDataAPI/_FakeDataSubscription via monkeypatch and hand-craft the buffered payload instead of obtaining it from BECLiveDataPlugin, so a shape drift between plugin emission (plugins.py:1393-1396 / data_api.py:167-176) and Heatmap._extract_buffered_series would pass both suites; the cross-thread aliasing hazard is invisible because payloads are static.

**Evidence.** git show 820afb20: monkeypatch.setattr of heatmap.DataAPI to a stub; payload built inline as lists of {'value','timestamp'} dicts. Shapes agree today (test_data_api.py:1845-1848) but nothing ties them together.

**Suggested fix (as filed).** Add one integration-style widget test that drives a real DataAPI+BECLiveDataPlugin (mock client, real LiveScanData) and feeds its actual buffered emission into update_plot via the Qt signal.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F52 [INFO] Verified non-issue: louie.saferef's registry does not retain dead BoundMethodWeakref entries

*Location (at `b3b6b912` / `820afb20`):* `/Users/janwyzula/miniforge3/envs/bec_312_data_api/lib/python3.12/site-packages/louie/saferef.py:1`  
*Category:* no-issue

**Claim.** louie's own registry is not a leak source: _all_instances is a weakref.WeakValueDictionary and the remove() closure deletes the key when the target dies; dead-callback retention observed in the plugin is entirely due to the plugin's own dicts holding the BoundMethodWeakref objects.

**Evidence.** Installed saferef source: `_all_instances = weakref.WeakValueDictionary()` with `del self_.__class__._all_instances[self_.key]` in remove(). Probe: 'louie _all_instances: 1' while alive, '0' immediately after `del w; gc.collect()` even though the plugin still held the ref in five structures.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.

### F53 [INFO] _unsubscribe_monitored removes the data buffer but leaves the monitored_indices entry behind

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/bec_lib/data_api/plugins.py:756`  
*Category:* memory-leak

**Claim.** Removing one monitored device deletes callback_buffers[ref].buffers[(dev,entry)] but never the matching monitored_indices key, so repeated add/remove accumulates stale entries and causes stale-index skips if the same device is re-added mid-scan.

**Evidence.** plugins.py:753-756 deletes only the buffer entry; monitored_indices (written at :971) has no deletion anywhere in the file (only read :955, write :971); the whole _CallbackBuffer is dropped only when buffers empties (759-760).

**Suggested fix (as filed).** Also `callback_buffer.monitored_indices.pop(buffer_key, None)` when removing the buffer entry.

**Status.** **FIXED** in d89e9ae9 — `_remove_source_from_callback_buffer` also pops `monitored_indices` and `incompatible_sources`.

### F54 [INFO] mock_client.callbacks is an unconstrained MagicMock, so the event-name/registration contract with CallbackHandler is unverifiable

*Location (at `b3b6b912` / `820afb20`):* `bec_lib/tests/test_data_api.py:34`  
*Category:* mock-fidelity

**Claim.** The fixture stubs client.callbacks with a bare MagicMock returning 'callback_id' (34-36), so a wrong event name or signature would still pass (real register validates EventType and returns int, not str); verified manually correct today, so latent risk rather than a live bug.

**Evidence.** callback_handler.py:104 `event_type = EventType(event_type)`; EventType SCAN_SEGMENT/SCAN_STATUS (callback_handler.py:28-29); real payloads match (scan_items.py:119/132; messages.py:90-94, 1006-1009).

**Suggested fix (as filed).** Use a real CallbackHandler() instance in mock_client (dependency-free) so registration names, signatures, and run() dispatch are exercised.

**Status.** **OPEN** — documented recommendation (see Recommendations); evidence stands as recorded below.
