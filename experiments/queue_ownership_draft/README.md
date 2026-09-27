# Scan queue ownership: code sketch

Historical sketch. The [design report](../../docs/scan-queue-ownership-design.md) now points to
the integrated production implementation and its tests.

This is a quick, standalone reading companion to the
[design proposal](../../docs/scan-queue-ownership-design.md). It is not wired into BEC and does not
replace the production queue or workers. It uses only Python's standard library.

The target is now **direct scans only**. The first version folded the domain objects into private
records; this revision makes `ScanQueue` and `DirectInstructionQueueItem` explicit again. There is
no generator worker, legacy instruction interpreter, or legacy request-block queue.

| Class | Responsibility |
| --- | --- |
| `QueueManager` | Facade that submits commands and returns replies/snapshots |
| `QueueCoordinator` | One thread that routes commands to its named `ScanQueue` objects |
| `ScanQueue` | Owner-only ordering, admission, pending worker reply, and in-flight identity |
| `DirectInstructionQueueItem` | Request metadata plus an opaque prepared scan until dispatch; status changes have no I/O or worker side effects |
| `ScanWorker` | Worker-owned direct scan and its lifecycle/exception hooks; reports the exact assignment token |

Keeping the familiar domain classes does not make them shared between threads. The worker receives
the scan and a token, not a mutable `ScanQueue` or `DirectInstructionQueueItem` reference.

Read these files in order:

1. [scan_queue.py](scan_queue.py): `ScanQueue.schedule()` is the one admission decision;
   `ScanQueue.finished()` retires the exact reported item. The coordinator routes commands to them.
2. [direct_instruction_queue.py](direct_instruction_queue.py): `DirectInstructionQueueItem.claim()`
   transfers the prepared direct scan; `describe()` reads metadata without inspecting the live scan.
3. [scan_worker.py](scan_worker.py): the worker executes the eight direct lifecycle hooks,
   optionally calls `on_exception`, and releases device locks in `finally` before reporting back.
4. [protocol.py](protocol.py): immutable identities/snapshots, ownership-transfer envelopes, private
   reply semantics, and narrow shared cancellation events.
5. [demo.py](demo.py): direct lifecycle order, two named queues, admission blocking, stale completion,
   clear during cleanup, and shutdown with joins outside the owner.

The shape of the call flow is:

```python
manager.insert_prepared(queue_ref, prepared_scan)  # Production: preparation lane result.

# Worker thread; the owner retains this reply while admission is blocked.
assignment = manager.request_work(queue_ref).result()
for step in SCAN_SEQUENCE:
    assignment.cancellation.checkpoint()
    getattr(assignment.scan, step)()
manager.finished(Finished(assignment.token, "completed"))

# Lifecycle caller, never the coordinator:
manager.begin_shutdown()  # Public intake closed; internal completion remains accepted.
worker.join()
manager.finish_shutdown()
```

The actual draft worker also binds cancellation into the scan adapter, checks the exception-hook
policy, and always releases device locks. `clear()` empties the visible
deque but leaves an independent in-flight token, so another item cannot start during cleanup. Queue
state is thread-confined; the small transport lock only serializes submission with mailbox closure.
Cancellation events intentionally remain shared and are never cleared.

Run from the worktree root:

```sh
.venv/bin/python -m experiments.queue_ownership_draft.demo
```

The demo uses bounded waits and assertions around the interesting handoffs. It is an illustration,
not validation of the full proposal or of real hardware interruption.

## Deliberately unfinished

- Preparation is represented by `insert_prepared()`. Real insertion still needs reservation,
  asynchronous construction, grouping, failure handling, and explicit ownership disposal.
- Redis publication is a comment at the owner boundary. Implement the ordered I/O lane using copied
  snapshots/history and `MessageEndpoints`; do not put connector calls in the owner loop.
- No scan/dataset allocation, direct grouping, automatic idle expiry, history, restart, targeting
  by RID, or queue-generation replacement is implemented. One direct scan per item is shown.
  Legacy streamed request blocks and scan definitions are outside the target, not future draft work.
- Admission uses one illustrative gate. Named locks, restore-after-unlock behavior, and automatic
  empty-queue reset are omitted. Execution pause/continue and full control sequence/phase handling
  are also omitted. Simplified abort/clear behavior is not a compatibility implementation.
- `DirectScan` stands in for the real `ScanBase`. `bind_cancellation()` and
  `release_device_locks()` are illustrative adapter methods, not changes to the public ScanBase API.
  Real integration still needs scan.actions initialization, RPC context, copied progress reports,
  phase-aware interruption inside device waits, and actual registry/stop-scope fencing.
  The demo releases no real device locks; do not use it to control devices.
- The owner mailbox is unbounded and command envelopes are intentionally compact rather than fully
  typed per operation. Capacity accounting, publication credit, cancellation of pending external
  replies, fatal owner/worker supervision, and partial I/O failures still need the design's protocols.
- Lifecycle management assumes one external caller and one correctly bound worker per queue.
  `finish_shutdown()` refuses active execution; if a worker join times out, leave the owner alive.

There is no new message schema, production import, or service configuration. The purpose is to make
the ownership split and worker interaction concrete before implementing the compatibility details.
