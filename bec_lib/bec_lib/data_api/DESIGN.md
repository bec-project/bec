# DataAPI v2 — design

*2026-07-29. Supersedes the prototype contract audited in `../../../DATA_API_AUDIT.md`. The
prototype's load-bearing ideas are kept (plugin-routed facade, per-subscription synchronized
bundles, group-compatibility validation, live-follow); the parts the audit and the follow-up
research showed to be structurally wrong are replaced. No backwards compatibility is kept —
unused prototype code is deleted. Research references: scratchpad reports on history access,
widget data-path inventory, and async index provenance (2026-07-29).*

## 1. Goals

- One subscription API that serves **live and history** scans with the **same emission
  contract**, so widgets have a single rendering path.
- Move every piece of data logic that ≥2 widgets duplicate into bec_lib (fetch fork,
  scan-lifecycle handling, async reconstruction, history resolution, alignment, throttling).
- Alignment built on **ordinals**, not arrival order.

## 2. Emission contract (the one shape)

A subscription delivers `callback(update: SubscriptionUpdate)` where:

```python
SourceKey = tuple[str, str]                     # (device_name, entry/signal)

@dataclass(frozen=True)
class SourceData:
    device: str
    entry: str
    kind: Literal["monitored", "async", "unindexed"]
    ordinals: tuple[int, ...]                   # point_ids / async ordinals, sorted
    values: tuple[Any, ...]                     # columnar, same length as ordinals
    timestamps: tuple[float | None, ...]
    complete: bool                              # no known gaps behind the frontier
    metadata: Mapping                           # per-source (async update type, group, ...)

@dataclass(frozen=True)
class SubscriptionUpdate:
    scan_id: str
    reason: Literal["live", "backfill", "history", "rebind"]
    sources: Mapping[SourceKey, SourceData]     # full-state columnar snapshot
    aligned_ordinals: tuple[int, ...]           # ordinals present in EVERY source
    complete: bool                              # all sources complete and frontier-aligned
    def aligned(self) -> dict[SourceKey, tuple]: ...   # equal-length columns at aligned_ordinals
```

Decisions and why:

- **Full-state columnar snapshot per emission.** Widgets redraw the full series each frame
  anyway (`setData`, image rebuild), and history data arrives columnar from the file. Emission
  frequency is bounded backend-side (§5), so total cost is O(N) per frame at a fixed rate —
  the same as today's widget pull, now paid once in bec_lib. Snapshots are immutable copies
  (audit F20).
- **No buffered/non-buffered duality** — the prototype's two shapes (audit F41) forced
  consumers to type-sniff and made buffered mode O(N²). "Buffered" is now simply what every
  emission is; incremental consumers read `update.reason` and per-source frontier movement.
- **History = one emission** with `reason="history"` — no slicing columnar file data into N
  fake per-point events.
- `aligned()` is the shared helper replacing per-widget length-trimming/mismatch code
  (waveform WF:1727, heatmap HM:916, scatter implicit).

## 3. Ordinals and correlation (replaces positional pairing)

Per-source ordinal assignment:

| Source | Ordinal | Basis |
| --- | --- | --- |
| monitored signal | `point_id` of the ScanMessage | scan-server-minted readout index |
| async `add`/`replace` | `async_indices[signal]` counter | device-server per-scan message counter |
| async `add_slice` | `async_update.index` (row) | device-chosen row == trigger count |
| legacy/unindexed async | arrival counter, `kind="unindexed"` | no index exists (device_async_readback) |
| history monitored | dataset row i | file stores per-point rows |
| history async | dataset row i | row i == async ordinal i by writer construction |

Correlation rule: the sources of a subscription are **partitioned automatically into
correlation groups**; each group is aligned independently and emits its own updates
(labelled in ``update.metadata["group"]``), so widgets subscribe to all their sources at
once (Waveform: monitored curves + async curves + x device in one call):

- group **"scan"**: monitored signals + async signals with `acquisition_group="monitored"`.
  Join key: ordinal equality (`point_id == async ordinal`) — this is precisely the cadence
  contract that `acquisition_group="monitored"` declares (one emission per monitored point).
  Cadence violations are *detected*, not silently mispaired: a source whose frontier ordinal
  diverges from the group's marks the update `complete=False` and logs once per scan.
- group **async-`<tag>`**: async signals sharing a free-form acquisition group; ordinal join.
- **standalone**: one group per ungrouped/unindexed source (incl. legacy
  ``device_async_readback`` devices, served with arrival-counter ordinals).

Out-of-order and gapped delivery are handled by construction: an ordinal joins
`aligned_ordinals` only when every source has it; late arrivals fill holes instead of
shifting pairs (kills the audit's F03/F09/F27/F37 bug class at the root).

## 4. Plugins and routing

```python
class DataSourcePlugin(ABC):
    priority: int
    def can_provide(self, sources, scan_ref) -> bool: ...
    def open(self, request: SourceRequest) -> None: ...   # request carries the sink
    def close(self, request) -> None: ...
```

- **LiveDataPlugin (priority 10)**: provides scans that exist in `scan_storage` and are not
  terminal. Feeds the engine from `scan_segment` events (per-`point_id` inserts from
  `live_data`) and `device_async_signal` streams (`from_start=True`, ordinal inserts,
  add/add_slice/replace reconstruction with the audited gap detection). Threading, locking,
  weak-ref pruning, and backfill semantics carry over from the audited implementation.
- **HistoryDataPlugin (priority 50)**: provides terminal scans. `can_provide` answers from
  `client.history` (`ScanHistoryMessage.stored_data_info` — no file I/O) with a
  `scan_storage` fallback for the writer-latency window (terminal scan still in the deque:
  serve monitored from `live_data`). Data path: one **worker-thread** read through
  `ScanDataContainer` (blocking h5py, 1 GB LRU cache) → one `reason="history"` emission.
  Monitored vs async membership comes from the file's `readout_groups`, not live device
  config (devices may have been reconfigured since the scan).
- Routing: first plugin (ascending priority) whose `can_provide` is true. The prototype's
  `has_scan_data` (never called — audit F36/F42) is deleted; its job *is* `can_provide`.
  The live plugin explicitly refuses terminal scans so the history plugin is not shadowed
  for the 10 most recent scans.

## 5. Facade

```python
api = DataAPI(client)                      # one instance per client (registry as audited)
sub = api.subscribe(
    sources=[("samx", "samx"), ("det", "wave")],
    scan="live",                           # follow scans; or a concrete scan_id
    callback=on_update,                    # or QtDataSubscription bec_widgets-side
    min_emit_interval=0.1,                 # backend rate limit, 0 disables
)
sub.set_sources([...])                     # atomic replace; one rebuild, one re-emission
sub.unbound_sources                        # degraded-feed introspection (kept from audit)
sub.close()                                # also automatic on GC (weak scan-status relay)
```

- **Atomic `set_sources`** replaces incremental `add_device` — the prototype's per-add
  rebuild/re-emission semantics (audit F03 fix) had no reason to exist once construction is
  declarative.
- `scan="live"`: live-follow via the audited weak relay; rebinds on scan open; on scan
  **terminal**, the facade performs one final flush and (optionally) re-routes the same
  subscription to the history plugin for the authoritative file data (`reason="history"`),
  replacing every widget's `QTimer.singleShot(100/300)` settle hack.
- `scan=<terminal scan_id>`: routed to the history plugin — this replaces the widgets'
  three `get_history_scan_item`/`update_with_scan_history` resolvers; the facade accepts
  scan_id, scan_number or index via `client.history`.
- **Backend rate limiting** (trailing-edge guaranteed): emissions are coalesced to
  `min_emit_interval` with a timer flush, so per-widget `pg.SignalProxy` wrappers become
  unnecessary; the final state is always delivered.
- Threading model unchanged from the audit: one shared RLock per DataAPI; callbacks fire on
  dispatcher/worker threads and must not block; Qt marshalling is the wrapper's job.

## 6. bec_widgets side

`QtDataSubscription` (new, `bec_widgets/utils/`): owns the api subscription, marshals
updates onto the Qt thread via a queued signal carrying `SubscriptionUpdate`, exposes
`healthy` (unbound sources) and drops stale-scan payloads. Widgets consume:

```python
self._sub = QtDataSubscription(self.client, sources=[...], scan="live", parent=self)
self._sub.updated.connect(self.on_data)    # on_data(update: SubscriptionUpdate)
```

The heatmap is the reference port (live + history through the same path); the widget keeps
only: config handling, `update.aligned()` → image mapping, rendering. Deleted from the
widget: `_fetch_scan_data_and_access`, `_extract_scan_series`, `_extract_buffered_series`,
scan-progress-driven refetch, settle hacks, history resolver.

## 7. Deletions from the prototype

- `has_scan_data` (dead ABC contract), `get_info()["priority"]` magic dict (→ class attr),
  buffered/non-buffered emission duality and `set_buffered`, per-add rebuild semantics,
  positional `_CallbackBuffer`/offset machinery (subsumed by ordinal series),
  `DataSubscription.add_device/remove_device/reload` (→ `set_sources`),
  `create_subscription(live=..., buffered=...)`.
- Kept (audited, still correct): shared-RLock threading model, deferred emission,
  weak scan-status relay, dead-callback pruning, per-client instance registry,
  async-info cache with nested `acquisition_group` resolution, `TERMINAL_SCAN_STATES`.

## 8. Explicit non-goals / known limits (documented, not hidden)

- Exact async↔monitored correlation is only as good as the `acquisition_group="monitored"`
  cadence promise; a protocol addition (point_id stamped into trigger metadata, or
  `add_slice.index` defined as point_id) is the real fix and is out of client-side scope.
  Violations are detected and flagged, never silently mispaired.
- The HDF5 file does not persist `async_indices`; history async ordinals are row numbers.
  If exact historical correlation matters, the file writer should persist the index dataset
  (follow-up for bec_server).
- Scan-less device streams are covered by `DeviceStreamPlugin` (`subscribe(scan=None)`):
  `device_readback`, `device_monitor_1d` (reserved entry name ``"monitor_1d"``) and
  `device_preview` signals, bounded via ``max_points``.
- Legacy `device_async_readback` devices are covered (unindexed standalone sources).
