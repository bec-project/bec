# Scan queue condition assessment

Investigated commit `1aef9ef4a` in the detached worktree
`/private/tmp/bec-scan-queue-condition`. The original checkout's uncommitted files were not copied.
Production files are unchanged. `test_queue_condition.py` contains an admission-only prototype and
deterministic concurrency probes, not an integrated queue implementation.

## Recommendation

Yes: use a condition for queue admission. It can replace the independent empty, paused, and locked
polling loops with one loop that rechecks all admission rules after each wake-up. This is a meaningful
local simplification, but changing the lock constructor alone is insufficient.

Use `threading.Condition(existing_rlock)` initially. A condition adds atomic release/wait/reacquire
and notification to locking; it does not remove the need for mutual exclusion or reentrancy.
Python's default condition also creates an RLock. See the
[Python condition documentation](https://docs.python.org/3/library/threading.html#condition-objects).

## What becomes simpler

`ScanQueue._next_instruction_queue()` currently mixes retirement, selection, deferred insertion,
empty-queue sleeps, separate locked/paused loops, and an IndexError retry. Its five timed
`signal_event.wait()` call sites use 10 or 100 ms waits. `insert()` has another polling loop waiting
for an empty paused queue to resume. `signal_event` otherwise represents shutdown, not work arrival.

Separate completed-item retirement from admission. Under the condition, normalize deferred work
and empty paused state, check shutdown, test the full admission predicate, and either select an
item or call `wait()`. Each wake-up returns to the same decision point. Preserve the current
paused/stopped-head cleanup rule explicitly; `bool(queue)` alone is not the predicate.

The prototype's `take()` demonstrates this admission loop using the existing
`_queue_should_continue()` policy. It deliberately omits retirement, worker execution, timer
management, and production notification integration, so its size is not an apples-to-apples
measurement of total code reduction.

Insertion should accept work while admission is paused. If preserving today's empty-paused insert
wait is required, it can use the condition, but it remains a blocking producer API and must never
hold the manager lock while waiting. Removing that producer wait is a separate behavior decision.

## Concrete current bug reproduced

In `_next_instruction_queue()`, a worker can pass the LOCKED loop and then wait in the PAUSED loop.
Another thread adds a queue lock with `allow_device_instructions=False`. The worker sees that the
queue is no longer PAUSED, leaves that loop, and selects the head without rechecking LOCKED.

`test_baseline_pause_to_lock_transition_can_admit_a_scan` reproduces selection while the real
`_queue_should_continue()` returns False. It is a characterization test asserting the observed bug,
not a regression test asserting desired behavior. `test_rechecks_every_gate_after_notification`
shows the prototype stays blocked for the same transition. The fix comes from rechecking all rules
in one loop; a condition makes that structure natural, but is not itself what fixes the predicate.

## Required notification contract

Every predicate mutation must occur under the condition's lock, followed by notification:

| Mutation | Existing locations / reason |
| --- | --- |
| Insert or append to an existing item | `insert`, `_insert_now`; work/head metadata can change |
| Queue state / admission locks | `status`, `add_lock`, `remove_lock`; include replacement of an existing lock and removal of a non-final restrictive lock |
| Ordering / removal / clear | manager `_handle_scan_order_change`, queue removal methods and `clear`; a permitted device instruction can become the head |
| Item completion / stop | both item status setters and retirement; cleanup and deferred insertion may become possible |
| Auto-reset re-enabled | `AutoResetCM.__exit__`; an empty paused queue may now resume |
| Shutdown / removal | manager `remove_queue`, `_remove_idle_queue`, worker shutdown integration; Event.set alone does not wake a condition |

Use `notify_all()` initially, especially if insertion/history waiters share the condition.
Notifications are not stored: correctness comes from testing state while holding the same lock,
not from assuming one notification corresponds to one item. Spurious or irrelevant wake-ups must
recheck the predicate. Never wait while holding an unrelated lock needed by producers.

## What a condition does not simplify automatically

- Manager/queue lock ordering. Selection currently holds the queue lock and publishes through the
  manager lock. Controls, lock changes, and reordering enter with the manager lock. Adding condition
  locking to those setters creates the reverse manager-to-queue order. Resolve that ordering before
  wiring notifications; merely appending `with condition: notify_all()` is unsafe.
- The condition releases its own RLock, including recursive acquisitions, but leaves the separate
  manager RLock held. A probe demonstrates this directly. Use one consistent order and avoid
  calling blocking queue operations under the manager lock. A single shared manager lock is another
  design option, but holding it during assembly/publication would serialize independent queues.
- Insert reservations and deferred inserts protect queue lifetime and stopped-item cleanup. Their
  removal requires changes to ownership/lifetime, not just a different waiting primitive.
- Auto-shutdown timers could later become a monotonic idle deadline on a timed condition wait.
  That also needs identity/idleness rechecks and a removal path that never joins the current worker.
  Keep timer behavior separate in the initial refactor.
- Active-item identity after reordering, cross-queue snapshots, worker-side pause checks, and open
  scan-group polling remain separate concerns. This experiment does not establish their correctness.

Recommended scope: first unify admission predicates and establish the mutation/lock-order contract;
then replace queue polling with condition waits. Keep worker execution and idle-lifetime redesign
out of that first change. No message schemas or endpoints need to change. Downstream risk is in
queue control timing and notification completeness, especially pause, restart, clear, and shutdown.

## Validation

The worktree has its own `.venv`, editable installs of all four BEC packages from this worktree,
and dependency paths from the existing `bec-312` environment plus `ophyd_devices`. Imports of
`bec_lib`, `bec_server`, and `bec_ipython_client` were verified to resolve inside this worktree.

- 230 existing tests across `test_scan_server_queue.py`, `test_scan_worker.py`,
  `test_generator_scan_worker.py`, and `test_direct_scan_worker.py` passed with randomized order.
  That run also included the initial 11 experiment cases: 241 passed in total.
- After adding the baseline bug reproducer, all 12 experiment cases passed with randomized order.
  Cases cover empty/paused/locked wake-ups, pause-to-lock transitions, permitted device instruction
  reordering, irrelevant notifications, work arriving before waiting, shutdown, and nested locks.
- Both pytest processes exited successfully. Their mirakuru atexit cleanup emitted a sandbox
  permission error while enumerating OS processes; no test failed from it.
- Black and isort ran on the experiment. No service/e2e validation was performed: production code
  is unchanged, and the prototype does not claim production integration coverage.

Reproduce the experiment from this worktree:

```sh
.venv/bin/python -m pytest -p no:xvfb --random-order -q experiments/test_queue_condition.py
```
