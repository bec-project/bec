# Device server event snapshots

Status: implemented and validated, 2026-09-27.

## Purpose and scope

Reading-backed device events publish complete snapshots instead of rereading the
root device on every callback. This addresses two confirmed existing problems:

- **E10:** root readback and moving-state callbacks can block ophyd's shared monitor
  thread on sibling PV reads or Redis writes, delaying unrelated device updates.
- **E11:** one failed read can abandon a shared batch after pending markers have
  been cleared, losing a healthy device's final update until another event arrives.

This refactor covers readback, configuration, limits, and moving-state telemetry.
`RequestHandler` and instruction/status completion retain their existing behavior;
there is no separate completion scheduler. Custom waveform, preview, progress,
file, and flyer event paths are outside this change.

## Composition and state

`DeviceManagerDS` composes
[`DeviceEventDispatcher`](../bec_server/bec_server/device_server/devices/event_dispatcher.py).
The dispatcher owns snapshots, scheduling, refresh workers, and publication.
The manager connects existing callbacks and device lifecycle hooks to it.
`DSDevice` does not gain scheduling responsibilities.

Each root object has independent snapshots for these domains:

| Domain | Existing Redis behavior | Compatibility refresh |
| --- | --- | --- |
| Readback | `device_readback`, `DeviceMessage` | Root `.read()` |
| Configuration | `device_read_configuration`, `DeviceMessage` | Root `.read_configuration()` |
| Limits | `device_limits`, existing `low`/`high` fields | Both limit signals' `.get()` |
| Moving state | `device_status`, `DeviceStatusMessage`, `set` | None; callback payload |

Each snapshot stores a complete reading, captured metadata, initialization state,
`version`, `published`, per-field callback versions, and an optional `dirty_version`.
A cached `callback_only` decision records whether callbacks cover the whole reading.
One retained candidate permits publication of a completed refresh while newer work
remains dirty. Retry counters and deadlines apply independently to each domain.

State has four explicit roles: `_Coverage` describes verified field mappings;
`_Snapshot` owns callback capture, invalidation, read merging, and acknowledgement;
`_DeviceState` owns the root's lifecycle and operation gate; and a frozen
`_Publication` carries one captured version. A publication is reused by pending
state and the Redis batch, without a second candidate wrapper. Command-result
ranges remain local to the batch. `ReadToken` delegates successful explicit-read
updates to the snapshot, rather than updating its fields independently.

A single `OrderedDict` holds pending `(root identity, domain)` entries. Repeated
callbacks replace current state without adding queue entries. Storage is bounded
by registered devices and domains, rather than the number of incoming callbacks.

## Coverage and callback capture

Coverage is established outside callbacks and cached after initialization. Traverse
selected read/configuration components using ophyd's `Kind` flags at every ancestor;
combined kinds can contribute to both domains. Match emitted keys to signal object
identities and require a complete baseline plus successful value subscriptions.

The fast path supports standard ophyd device and signal implementations and the
known `ReadOnlySignal`/`SimMonitor` and legacy `ComputedSignal` callback contracts.
Those drivers emit their reading value directly; when they omit the timestamp,
the dispatcher uses their recognized cached timestamp property. Exact method and
descriptor checks keep unrelated custom implementations on the fallback path.
Unknown `read`, `read_configuration`, `get`, or timestamp behavior uses fallback.
Custom ancestors and ambiguous duplicate output names also prevent raw callback
patching: a callback value must not replace a transformed reading with a raw value.
Signals such as `BECProcessedSignal`, whose reads evaluate dependencies, therefore
retain their read semantics. Existing auto-monitor policy is preserved.

A state callback retains its changed payload, replaces a verified field record,
captures metadata, increments the version, and wakes the dispatcher. It performs
no hardware or Redis I/O and never acquires an operation gate. Signal values and
arrays remain shared so pending telemetry can reflect their latest contents without
copying payloads. Reads shallow-copy the field mapping and its records; publication
copies only the outer snapshot mapping. Metadata is deep-copied at admission to keep
scan, point, and request identifiers associated with the captured event, then shared
unchanged internally. Unregistered tokens and buffered results retain no reading data.

Incomplete coverage or a mapped field without a usable value/timestamp marks the
snapshot dirty at the current version. Every subsequent event on a mixed device
can require another compatibility read. This adds no periodic hardware polling.
Root events without a verified field mapping trigger publication without treating
the root name as a signal key. Source measurement timestamps remain unchanged.

Some simulated getters emit callbacks while producing a reading. Known synthetic
callbacks on the root currently being read still patch their fields, but do not
request another read of that domain. A custom other domain whose fields cannot be
patched gets one refresh per external event. Domains already refreshed in that
pass are not invalidated by synthetic callbacks, preventing readback/configuration
cycles even when both reads transform values. Each custom domain retains its latest
complete read; arbitrary getters that produce a new value on every read cannot
provide an atomic measurement shared across both domains. The origin is local to
the reading thread:
callbacks on other threads and other roots remain admitted. This also prevents a
shared readback/configuration field from making the two domains reread each other.
Ordinary signal changes inside a custom read still invalidate the snapshot.
Fully covered simulator and computed-signal graphs need no fallback reads at all.

## Refresh and publication

Four fixed daemon workers perform compatibility reads, with at most one refresh
in flight per root. Workers select pending dirty domains fairly, capture their
version and connection epoch, then read outside the short state lock. Per-device
operation gates serialize compatibility reads with explicit hardware operations.

A completed read must contain all initialized fields. Its merge preserves verified
field callbacks newer than the captured version. It clears `dirty_version` only
when that invalidation is satisfied; later events remain pending. A completed
candidate can publish while a newer refresh is needed, avoiding dependence on a
quiet period. Retired identities and changed connection epochs discard old results.
The last baseline-invalidation version also rejects snapshots staged before a
disconnect or configuration change, even if a new baseline is already initialized.

One publisher captures up to 256 ready snapshots per batch and stages them in a
shared Redis pipeline. It never reads hardware. Serialization holds no root gates.
Root gates are checked without waiting, so a slow dirty read does not block
publication for other roots. Pending
entries rotate fairly after selection. There is no fixed update rate or polling
interval: condition notifications wake workers, and timed waits serve failure retries.

Before committing, the publisher watches the existing destination keys with Redis
`WATCH`, briefly acquires each root's gate to validate its identity and version,
then releases every gate before `MULTI`/`EXEC`. An explicit write completed before
`WATCH` is detected by version validation; one completed afterward aborts the stale
transaction. No endpoint or wire-message version field is added. Explicit reads
can therefore proceed during event serialization and network waits.

A write conflict first retries the retained commands separately by root. Healthy
roots commit even if another root keeps changing, and only a repeatedly conflicting
root enters bounded retry. This preserves E11 isolation without restricting the
ordinary shared batch or imposing an update rate.

The batch tracks each snapshot's pipeline command range. Serialization failures remove
only that snapshot's staged commands. Pipeline execution requests individual
command outcomes; only snapshots whose commands succeed acknowledge their captured
version. Newer callbacks remain pending. A transport failure or incomplete response
retains all unconfirmed work, including healthy final updates with no later event.
Failures retry independently with bounded exponential backoff and log the first
failure in a series. A retry may duplicate a successfully applied latest-state write.

## Explicit reads and compatibility

Explicit instructions continue to perform real reads, preserve device read order,
metadata, returned results, failure handling, and the existing shared Redis batch.
Their root operation gates are acquired once each in a deterministic order before
the batch. Event publication does not retain these gates during Redis I/O.

A successful explicit publication updates the snapshot through `read_context`.
Its captured version prevents an older event candidate from subsequently undoing
the explicit Redis write; callbacks arriving during the read remain pending.
`OnFailure.BUFFER` data does not mark the live snapshot clean. Failed explicit
operations do not acknowledge pending events.

Wire schemas and endpoints remain unchanged, and standalone signals still suppress
configuration event publication. Snapshots represent each field's latest known
value, not a simultaneous acquisition. Event telemetry is asynchronous and coalesced;
clients may miss intermediate positions or moving states. The final pending value
is retained until acknowledged. Mutable payloads may change in place while pending;
serialization does not guarantee an atomic image of a concurrently modified array.
Instruction completion remains a separate contract.

## Initialization, lifecycle, and limits

Register before subscribing, subscribe before the baseline read, then activate after
initialization. Version-aware baseline merging retains callbacks racing with setup.
Managed configuration changes refresh coverage in place and reuse unchanged
subscription IDs. File, flyer, progress, and other one-shot handlers remain installed
throughout. Missing bindings are added before seeding; obsolete bindings are removed
only after the replacement baseline succeeds. Failed application and rollback retain
the existing listeners and dirty baselines for worker retries. A failed first
initialization remains inactive. Configuration refresh retains the existing bounded
wait for the root's hardware operation gate.

Connection metadata invalidates affected snapshots. Reconnection requires refresh;
connection epochs prevent pre-disconnect reads from reinitializing the cache.
Removal operates by object identity, quiesces active I/O, retires subscriptions,
and rejects late callbacks or reads. A root remains reserved until any already
admitted Redis transaction finishes. The publication reservation itself does not
block explicit reads; retirement holds the operation gate while awaiting the
reservation before destruction. A retirement timeout rejects the configuration
change and leaves the running device subscribed. Replacement names cannot inherit
old instance work. Shutdown stops admission and uses one worker join budget. Later
device cleanup only attempts nonblocking retirement; it does not restart the wait
per device or destroy a device underneath a stuck read.

Redis uses the service connector's configured timeouts. A blocked Redis operation
can delay publication, and a hung driver occupies its refresh slot until return or
process exit. Callbacks remain independent of these waits. Additional dirty devices
wait when all refresh slots are occupied; clean snapshots need no refresh slot.
Refresh workers skip operation gates held by explicit reads, so
waiting for a busy root never consumes a refresh slot. Gate release wakes retained
updates without requiring another callback.

## Validation and acceptance

The final device-server suite passed 441 tests with randomized order. The dispatcher
and its tests pass Ruff, including annotation and Google-docstring rules; findings
in integration files match the pre-existing baseline. The dispatcher also passed
pylint; changed Python files passed Black/isort checks and whitespace validation.

A fresh demo deployment completed two 100-point `samx` line scans with zero exposure
in 1.98 and 2.16 seconds, compared with 26.73 and 27.22 seconds before the feedback
and publication-gate fixes, and 2.99 and 3.00 seconds on the base revision. The
benchmark set motor velocity to 1000, moved from 0 to 1, and checked that all 100
points arrived. These are local simulator timings, not a hardware latency claim.
Three earlier integration checks covered movement, cached readout, and configuration.

The broader local unit-test run had 3,083 passes, 16 failures, and five setup errors.
Failures outside the changed modules included local plugin assumptions, an older
lmfit API, unavailable Podman, actor socket errors, and a background configuration
callback exception. The focused suite remains the local acceptance check; CI uses
its own provisioned dependencies and services.

Regression coverage must demonstrate E10 isolation with blocked Redis and sibling
reads, and E11 recovery with failing/healthy devices in either order and no rescue
event. Verify bounded pending state, retained final moving state, command-level and
transport failures, fair batches, and bounded shutdown.

Also cover zero steady-state reads for fully monitored devices, mixed-device refresh,
shared mutable payloads, captured metadata, custom/duplicate-field fallback, callback/read races,
explicit-read ordering and batching, initialization races, reconnect epochs, and
same-name replacement. Run focused randomized tests and the affected device-server
suite. Real-CA IOC validation and representative callback/array latency measurements
remain deployment validation tasks; no hardware-independent latency claim is made.
