# Device-server backend boundary

The device server consumes `bec_lib.device_backend.DeviceProvider` and bound
`DeviceHandler` protocols. The default implementation is
`ophyd_devices.bec_adapters.OphydProvider`. Concrete hardware libraries, constructor
inspection, component traversal, native status/kind/staging representations and
callback diagnostics are implementation details of the device library.

## Ownership and execution

BEC retains instructions, requested-target identity, scan metadata, read-failure
policy, Redis publication and request tracking. `OperationContext` contains the
request fields; native statuses are never modified to attach those fields. A
request becomes terminal after both hardware completion and service callback
processing. A callback/cache failure yields an error response rather than a
premature successful response from another completed member of the request.

A provider implements explicit lifecycle, read, configuration, event and RPC
methods. It returns a `Completion` for pending work; no native device/status base,
writable `__dict__`, common thread or common event loop is required. `Reading`
allows the existing BEC `value` / `timestamp` fields, not a HAL's private read
metadata. Arbitrary RPC values and configuration arguments remain dynamic.

The ophyd adapter calls existing synchronous hooks on their existing caller.
Admission uses a short fence and an in-flight counter; it does not hold a lock
while a hook/RPC waits. Native status callbacks settle the neutral operation and
queue service callbacks onto a completion pool. Native event callbacks snapshot
mutable payloads and enqueue ordered delivery on a separate event worker. Redis
publication stays in BEC. Immediate completions can invoke callbacks on the
registering service thread. After successful provider close, a late observer of a
terminal operation is notified on its registering thread; native producers have
already been fenced and settled.

Successful disposal drains accepted events before returning. File/data events
are not coalesced. Invalidations retain existing BEC monitor selection and batch
policy. Read-generated synchronous notifications are suppressed only inside that
same thread's snapshot scope, preventing feedback loops without suppressing
transport updates on other threads. Delivery failures are logged immediately and
reported again at disposal. Original subscription tokens survive a failed native
destroy so pending requests can still dispose their own temporary registration.
Failed unsubscribe/drain work remains owned for retry.

Provider shutdown fences construction and submission, waits for admitted work,
drains subscriptions and completion delivery, and closes its workers. Drain waits
are bounded (ten seconds in the ophyd provider). A bounded failure is observable
and retains infrastructure for retry. This does not make a custom synchronous
constructor, hook, hardware read or `destroy()` forcibly interruptible: native
methods must honor their device-specific I/O/timeout policy. Legacy stop remains
the native device's hardware contract; the adapter cannot promise stronger
hardware guarantees than that implementation.

An asyncio HAL can own a persistent event loop and bridge coroutines to these
completions. Stop must be independently admitted while acquisition awaits
transport or a dependency. Waiting for a completion from a service worker does
not imply blocking the HAL owner loop. The test-only provider demonstrates this
with opaque raw objects and real asyncio Tasks; it is not production ophyd-async
support. Observer timeout/cancellation does not cancel borrowed peer hardware.

## Compatibility and call-site inventory

| Former server dependency | New owner / service seam |
| --- | --- |
| Device class/plugin lookup, signature injection | `OphydProvider.load_class/construct` |
| Proxy detection and native registration | provider dependency/proxy methods |
| `Kind`, `Staged`, native signal/status checks | handler capabilities, read category, operations and RPC result |
| Component walks, metadata, aliases and offline descriptions | adapter serialization, `describe/runtime_info` |
| Signal callbacks, auto monitors, limits, BEC signal subclasses | handler subscriptions and typed `DeviceEvent` |
| Native status request attributes | server-owned `OperationContext` |
| Native error-owner traceback inspection | `provider.error_device` |
| Native callback diagnostics and logger routing | adapter diagnostics/logging |
| Redis routes, scan context, dynamic indices and batching | BEC manager and message handler, unchanged wire schemas |

`DSDevice.obj` and its delegation remain raw compatibility paths for existing
computed signals, controllers and plugins. Production dispatch uses `.handler`.
The raw `construct_device_obj(config, manager=None)` helper remains available for
static device validation. Historical serializer and callback-monitor module paths
are compatibility aliases to their device-library implementations. The old value
conversion helper delegates explicitly to the library. These seams are not a
fallback that future HALs need to imitate.

Device class names, configurations, hook signatures, allowed client operations,
read-only restrictions and BEC message/endpoints are preserved. RPC reflection
retains the existing server access policy; this extraction adds no new server-side
allowlist and does not expand the advertised client interface. No Bluesky event
or asset-document model is introduced.

## Coordinated installation and release

Use matching feature checkouts in a dedicated environment. From the BEC checkout:

```sh
python -m pip install -e './bec_lib[dev]'
python -m pip install -e /path/to/matching/ophyd_devices
python -m pip install -e './bec_server[dev]'
python -c 'import bec_lib, ophyd_devices; print(bec_lib.__file__, ophyd_devices.__file__)'
```

The device server no longer declares a direct ophyd dependency. Its default
provider dependency remains `ophyd_devices`, which owns the concrete HAL dependency.
Both changed packages must ship together; the server reports a matching-package
upgrade error when the adapter module is missing. The provider requires the new
`bec_lib.device_backend` contract. Existing device-library releases do not have to
implement it. No user device-config migration or backend tag is required.

Release numbers have not been allocated. Keep current package versions for these
unreleased checkouts; before publishing, set the server's `ophyd_devices` floor
and the provider's `bec_lib` floor to the actual first releases containing these
changes. Existing numeric floors alone do not guarantee adapter availability.
Do not publish only one side or claim compatibility with arbitrary older releases.
Restart the device server after installing its coordinated packages.

## Validation

Use `OPHYD_CONTROL_LAYER=dummy` for unit tests. The reproducible static check is:

```sh
python bec_lib/tests/typing/check_device_backend.py
```

It checks the real ophyd provider against the neutral protocols and requires four
intentional negative assignments to fail. The import/AST guard plus the opaque
asyncio runtime proof cover native imports and raw-object access.

The device-server suite includes an isolated Redis workflow for each provider.
It owns a temporary loopback Redis process and exercises request completion,
push readback/file/progress/preview/dynamic data, indices, stop and teardown.
This is service-boundary integration, not a complete scan-server scan. It uses
simulation/mocks and does not connect to real hardware or ADSim. Final local test
commands and environment limits are recorded in
[the validation note](device_backend_validation.md). Linux and the other Python CI versions
remain CI validation, not claims of local execution.
