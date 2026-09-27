# Scan queue ownership through channels

Status: integrated threaded draft, preserved on `codex/scan-queue-channels-draft`.
Baseline: `1aef9ef4a`.
Updated: 2026-09-27.

## Decision

One coordinator owns queue state. Each named queue has one worker that owns its executing direct
scan. They communicate through typed, in-process channels. A `Condition` implements the channel's
wait/close primitive; it no longer protects the scan queue's application state.

The production implementation replaces the previous scheduler. It is not an adapter around the old
shared deque. The earlier [standalone sketch](../experiments/queue_ownership_draft/README.md) remains
as historical background; the files below are the implementation to review.

| File | Responsibility |
| --- | --- |
| `bec_server/bec_server/scan_server/queue_channels.py` | Buffered FIFO channel, typed assignments/reports, execution cancellation, serial execution lanes |
| `bec_server/bec_server/scan_server/queue_state.py` | Explicit `ScanQueue` and `DirectInstructionQueueItem` records, admission policy and copied descriptions |
| `bec_server/bec_server/scan_server/scan_queue.py` | Public `QueueManager` facade, coordinator loop and owner transitions |
| `bec_server/bec_server/scan_server/scan_worker.py` | Receive assignments, execute, report the exact assignment token |
| `bec_server/bec_server/scan_server/direct_scan_worker.py` | Direct lifecycle hooks, exception cleanup, final device-lock release |

The familiar queue/item objects remain. Workers never receive them. Their status fields have no
hidden publication, cancellation or worker-state side effects.

## Why a condition alone was insufficient

The baseline distributed queue mutation across callbacks, workers, and timer threads. Lock order
could invert, status setters performed I/O, and a worker sometimes identified itself through the
current deque head or a reusable queue name. Publication inspected live scans while workers changed
them. Replacing a polling loop with a condition could improve admission waiting without addressing
any of those ownership problems.

The [condition assessment](../experiments/scan-queue-condition-assessment.md) records that original
investigation. Its tests apply to the recorded baseline, not the replacement implementation.

## Concrete execution flow

1. A callback sends an insertion command and returns. A synchronous caller uses the same command
   with a private reply channel.
2. The owner reserves the request's position and submits construction to the preparation channel.
   The constructor runs outside the owner. Its result includes an initial metadata copy.
3. The owner evaluates the whole admission predicate. If eligible, it reserves an execution token
   and sends number allocation to the ordered I/O channel.
4. After allocation returns, the owner transfers the prepared scan through the queue's work channel.
5. The worker runs the direct lifecycle. Progress reports contain copied metadata. A progress
   acknowledgement waits for the corresponding queue snapshot to reach the connector, providing
   backpressure without blocking the owner.
6. After cleanup and device-lock release, the worker sends a terminal report. The owner matches the
   exact token, writes terminal history, removes that item by identity, and reevaluates admission.

The worker's outer loop is deliberately small:

```python
assignment = work.receive()
report = executor.run(assignment)
manager.worker_report(report)
```

`receive()` sleeps until work arrives or the channel closes. There is no worker-side queue polling,
shared deque traversal, or lookup of the currently active item through a queue name.

## Ownership and channels

| Owner | State / execution |
| --- | --- |
| Coordinator thread | Queue registry, generations, order, locks, admission, copied item metadata, active tokens, pending insertions, idle deadlines |
| Preparation thread | Input validation, constructor execution, initial description; transfers the prepared object on completion |
| One worker per queue generation | Its assigned scan, hooks, actions and progress capture |
| I/O thread | Queue publication, history, counter allocation, stop/restart messages, queue alarms and metrics |
| Lifecycle caller | Closing intake and joining threads, outside the coordinator |

`Channel.send()` never waits for its consumer. `close()` atomically rejects later sends, wakes blocked
receivers, and lets them drain already accepted messages before reporting closure. Private reply
channels carry either a value or an exception. The facade's short submission lock only serializes
submission against closure; it does not protect queue state or cover waits.

Preparation is serial because constructors and assembler contexts should not gain concurrent
behavior as an incidental part of this refactor. I/O is serial so queue effects and allocations keep
owner submission order. Scan data and device instructions continue to use the existing ScanActions
and connector APIs from their execution thread.

## Identity and cancellation

An `ExecutionToken` contains queue generation, queue ID and dispatch sequence. Generation changes
when a named queue is recreated. Progress and terminal reports must match the in-flight token.
Duplicate reports and reports from removed generations cannot retire a successor.

A visible item can move or disappear during execution. `clear` removes visible work, but retains a
private in-flight record until cleanup finishes. Newly accepted work waits behind that record.
A second clear also discards insertions deferred behind cleanup. Restart reserves a fresh item;
preparation may finish after the original has already completed.

`ExecutionControl` is the deliberately small shared capability. It contains a condition for pause
and distinct execution, cleanup and service-shutdown events. Events are never cleared to start
cleanup. A repeated stop, halt or shutdown therefore cannot be erased by exception handling.

Stop delivery has an explicit receipt. The worker waits for issued stop-device sends before
cleanup or final lock release. It seals further cancellation before final release. The device-lock
registry also fences an explicit plugin lock release while a captured stop is being sent, without
holding its global condition across network I/O. This prevents a delayed stop from reaching a
new owner of the same device.

A scan's own exception alarm is emitted after its exception hook. Alarm metadata includes request,
queue and queue-item identity so its asynchronous echo cannot abort a successor. Unexpected worker
failure reports its token independently of alarm delivery.

## Admission, grouping and numbering

`ScanQueue.eligible()` is the sole admission predicate: a prepared head, no in-flight execution,
and an admission state permitting it. Named locks restore the previous state after the last lock
is released. A lock may allow direct device instructions while preventing scans. An empty paused
queue retains the existing automatic reset behavior.

Pause suspends cooperative execution. Deferred pause holds subsequent requests while the current
scan continues. Abort/halt hold admission; user completion and restart preserve admission. Holds
cannot be bypassed by `continue`.

Each direct request has one item and one lifecycle. `queue_group` metadata is preserved, but no
longer merges several direct scans into an item whose worker would only execute its first scan.
There are no streamed execution blocks, scan definitions, or legacy group-closing requests.

Scan/dataset numbers are allocated on the single I/O lane across all named queues before handoff.
Device instructions consume neither number. Dataset hold retains the current dataset number.
Pending numbers are projections based on the last observed scan counter; assigned numbers are fixed.
External counter resets/account switches still use the existing `ScanNumberContainer` semantics;
this change does not add a distributed transaction with other counter writers. Pending projections
refresh on startup/allocation and can temporarily lag such external changes.

## Publication and overload

Snapshots are built from owner metadata, never by reading a live worker scan. Worker progress copies
its report before sending it. Exported snapshots are independently mutable by their caller.

Ordinary pending snapshots coalesce behind an in-flight publication. Progress acknowledgements are
released after a containing snapshot is published. Cancellation snapshots and terminal history are
ordered effects and cannot be discarded by coalescing. History records identify the actual named
queue. The bundler and client readers find the active entry even after reordering moves it away from
the deque head.

The manager caps outstanding requests and outstanding preparation jobs at 1,000. Rejected callers
receive an error and an alarm is scheduled. Cancelled preparations cannot start queued constructors;
an already running constructor must return cooperatively. Completion, control and shutdown messages
remain accepted when insertion is at capacity. Channels themselves are buffered in memory; this is
not durable storage or a general traffic-rate limiter.

Queue I/O failures are logged and retained for `flush()`/`shutdown()` to surface. A failed progress
publication is returned to the worker. Queue control state and cancellation signals remain responsive
while Redis is slow, but hardware stop delivery and visibility still depend on Redis availability.

## Startup, expiry and shutdown

ScanServer constructs the manager, then initializes its assembler and number container before
activating queue callbacks. Idle expiry uses coordinator-owned monotonic deadlines, with no timer
threads. A queue with execution, preparation or deferred work is not idle. Primary does not expire.

Shutdown closes external intake, detaches queues, signals current execution, and closes work
channels. The external caller joins workers while the coordinator still accepts their reports.
It then drains preparation and all I/O completions, including jobs submitted by those completions,
before closing the owner channel last. Reusing a removed queue name is rejected until its old
execution has acknowledged retirement. Number allocation that was already in flight is therefore
retired without executing a scan after shutdown.

Device-manager resources remain alive until queue shutdown completes. A timeout leaves intake
closed and the owner/dependencies alive so outstanding cleanup can finish and shutdown can be
retried. Python cannot preempt an arbitrary stuck plugin. I/O errors are surfaced after orderly
thread teardown.

## Legacy removal and compatibility

The generator worker, generator scan implementations, legacy OTF example, legacy assembly path,
and execution `RequestBlock` / `RequestBlockQueue` / `InstructionQueueItem` handlers are removed.
Discovery accepts direct scans only. Public `scans.ScanBase` now exports the direct base class.

The existing `messages.RequestBlock` **wire description** remains unchanged: clients and bundlers
already use it to describe direct requests. It is a copied data model, not an execution handler.
`ScanArgType` labels and argument unpacking moved into `scans/scan_arguments.py`; `ScanStubStatus`
remains because direct actions use it for device responses.

This intentionally breaks legacy scan plugins and extensions that mutate queue internals.
Direct plugin lifecycle hooks, Redis message schemas, endpoint names and enum values remain.
Plugins must subclass the direct ScanBase and use the queue facade rather than live queue records.
Downstream widgets still receive the same queue/history message shapes.

## Validation

New tests use real owner, preparation, I/O and worker threads with controlled fake scan hooks. They
cover pause-to-lock admission, device-instruction holds, clear during blocked cleanup, deferred clear,
reordering, duplicate/stale reports, queue recreation, slow constructors and Redis, restart preparation,
number allocation across queues, dataset holds, capacity, preparation failure/cancellation, and shutdown
while allocating or cleaning up. Direct-worker tests retain real ScanBase modifier/lifecycle coverage.
Channel and registry tests exercise drain/close, distinct cancellation events and stop/release fencing.
Client/bundler regressions cover active entries away from the head.

Run from the worktree's isolated environment:

```sh
.venv/bin/python -m pytest -p no:xvfb --random-order -q \
  bec_server/tests/tests_scan_server
.venv/bin/python -m pytest -p no:xvfb --random-order -q \
  bec_lib/tests/test_queue_items.py bec_lib/tests/test_scan_items.py \
  bec_server/tests/tests_scan_bundler
```

Fresh-service tests must prepend this worktree's `.venv/bin` to PATH and use `--start-servers`.
Validation results for this integration:

- Final affected scan-server, bundler and client suite: **803 passed** (container utility tests
  requiring Podman excluded after their environment failures were recorded).
- Final focused queue, direct worker and channel suite: **53 passed**, including a final regression
  checking that an idle preparation lane releases its transferred scan. The remaining tests overlap
  the affected-suite scope.
- Fresh-service direct end-to-end tests: **17 passed**, covering all existing cases except the
  generated-plugin test. They use `file_written=False`, so file finalization is not validated.
- Pylint on the queue implementation: no findings; Black/isort and `git diff --check` passed.

The worktree virtualenv uses existing `xtreme_bec` and `bec_testing_plugin` checkout roots on
`PYTHONPATH` for fresh services, matching installed plugin entry points. The broader server-package
run had **1148 passed, 4 failed and 5 errors**. All nine failing/error cases also reproduce on the
original revision: actor startup timeouts/occupied ports, missing Podman, an incompatible installed
lmfit API, and a configuration-handler callback failure. Those unrelated issues remain unchanged.
