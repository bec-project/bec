# Shared Memory Ring Buffer

The shared-memory ring buffer keeps payload storage and control-plane policy separate.

## Memory Layout

The payload shared-memory object contains only slot bytes:

```text
[ slot 0 payload ][ slot 1 payload ] ... [ slot N payload ]
```

The payload shape, dtype, slot count, and synchronization resource names are distributed through
`RingBufferDescriptor`. This keeps attachment explicit and avoids a mutable metadata header in the
payload memory.

Reader counts live in a second, small shared-memory object:

```text
[ reader_count[0] ][ reader_count[1] ] ... [ reader_count[N] ]
```

The counter memory stores only synchronization state. It does not store write position, slot
availability, processing state, or scheduling policy.

## Locking

Each slot has one logical readers/writer lock built from three named POSIX semaphores:

- `data_lock`: held by a writer exclusively, or collectively by active readers.
- `reader_gate`: lets a waiting writer block new readers from entering the slot.
- `reader_count_lock`: protects updates to `reader_count[index]`.

Readers briefly pass through the gate, increment the shared counter, copy the payload, and decrement
the counter. The first reader acquires the data lock, and the last reader releases it.

Writers acquire the gate first, then the data lock. This allows existing readers to finish, prevents
new readers from entering while the writer waits, and guarantees that no reader observes a partial
write.

## Ownership

`RingBuffer` owns the operating-system resources. It creates and unlinks the payload memory, reader
counter memory, and all named semaphores.

`RingBufferView` only attaches to existing resources. It closes local handles during shutdown and
never unlinks resources.

## Write Position

The ring buffer assumes one writer service per buffer. The writer handle keeps a local circular
cursor and returns the written slot index from `write_data(...)`. Shared memory does not contain a
global write cursor.

Slot reuse, FIFO/LIFO ordering, release decisions, and processing results belong to the broker/event
control layer rather than the shared-memory implementation.

## Timeout Behavior

On macOS, positive semaphore timeouts are not reliable for this code path. Use `timeout=0` for a
non-blocking acquire or `timeout=None` to wait indefinitely.
