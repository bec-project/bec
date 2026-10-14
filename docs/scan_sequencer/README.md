# Scan sequencer backend

`bec_lib.scan_sequencer` receives JSON sequences composed by another UI, validates them
and executes existing BEC scans and device operations. It exposes component schemas and
advertised scans for inspection. Composition, editing, submission, revision management
and persistence belong to the calling interface.

## Composition example

examples/compose_sequence.py shows how an external caller can construct a document using
Repeat, ForEach, Sequence, Set and ScanCall. It prints JSON for two visits to each demo
position, with one time scan after each move: four scans in total.

```bash
python docs/scan_sequencer/examples/compose_sequence.py
```

The script also includes commented IPython steps for validation, export and procedure
execution. The composition is example caller code; the backend receives the resulting JSON.

## Receive, inspect and execute

```python
import json
from pathlib import Path
from bec_lib.scan_sequencer import (
    DefinitionLibrary, Sequencer, available_components,
)

# Component inspection works without a BEC connection.
schema = available_components()
component_names = list(schema["discriminator"]["mapping"])
# schema["$defs"] contains field schemas, requirements and descriptions.

# JSON supplied by the UI; these files illustrate the received payloads.
examples = Path("docs/scan_sequencer/examples")
sequence = (examples / "alternating_positions.json").read_text()
definitions = [json.loads((examples / "measure_position.json").read_text())]

sequencer = Sequencer(bec, DefinitionLibrary(definitions))
scan_names = list(sequencer.catalog.entries)
scan_info = sequencer.catalog.entry("time_scan")
sequencer.validate(sequence)  # no hardware execution
print(sequencer.export(sequence, format="description"))
python_source = sequencer.export(sequence, format="python")

# Normal execution is delegated to an existing procedure worker.
status = bec.proc.request_new(
    "run_sequence", ((sequence, definitions), {}), queue="sequences",
)
status.wait(raise_on_failure=True)

# The procedure internally delegates to the same execution entry point:
# sequencer.execute(sequence)
```

Procedure execution requires services with the registered run_sequence procedure.
The existing procedure queue owns ordering, status and cancellation through status.cancel().
The interface supplies the sequence and reusable definitions to that procedure.

Execution uses native Ctrl-C/KeyboardInterrupt, including in procedure workers.
The Python export contains statements for direct use in BEC IPython, where `bec`, `dev`
and `scans` already exist. It emits ordinary loops, scans.<name>(...), signal
.set(...).wait(), and device scans.umv/scans.mv calls, without a wrapper function or namespace
setup. Simple native sequences need no imports. A live catalog identifies target
kinds from actual proxies, including root signals. Offline sequencers created with
`Sequencer.from_catalog(catalog, definitions)` support validation, descriptions and scan-only
Python exports. Execution and Python exports containing Set nodes require a client so target
kinds are resolved before export. Native BEC calls validate dynamic scan arguments.
Outstanding asynchronous sets are checked and drained on exit in both execution modes.
Execution never compiles or evaluates an export.

The examples use `samx`, a settable simulated axis in the bundled test configuration, with values
`1, -1, 1, -1`, and the actual advertised `time_scan(points, interval, exp_time=0, settling_time=0)`
signature. The four scans use one point and a 0.1-second interval each. Check your device configuration
and limits before execution. A real polarization device, its valid values and an OTF scan signature
must come from the beamline configuration and advertised catalog; this backend defines none of them.
`scan_during_set.json` demonstrates scans in a While loop during a checked asynchronous set.

## Generating sequences with an LLM

Give the LLM the component schema, expression schema and current advertised scans before
asking it to generate a sequence document:

```python
from bec_lib.scan_sequencer import available_components, available_expressions

sequencer = Sequencer(bec)
catalog = sequencer.catalog
capabilities = {
    "components": available_components(),
    "expressions": available_expressions(),
    "scans": {name: catalog.entry(name) for name in catalog.entries},
}
```

`available_expressions()` returns JSON Schema generated from the supported expression models,
including examples for loop references, component parameters, literal wrappers, status observations,
boolean operators and comparisons. Component field descriptions identify where expressions are
accepted and include examples of their use. Expressions can be nested inside argument mappings
and lists. For example, a Set node inside a ForEach over `position` can use
`"value": {"ref": "position"}`. The string `"${position}"` is a literal string and does not
interpolate a variable. Use `{"type": "literal", "value": {"ref": "position"}}` to pass that
reference-shaped mapping as data instead of evaluating it.

The calling interface should also provide the configured device and signal names relevant to the
request. The expression schema describes shapes; it cannot establish whether a loop variable,
component parameter or status binding exists at a particular node. After receiving the generated
JSON, call `sequencer.validate(document)` to check scope, scan arguments and live set targets.
Return validation errors to the LLM for correction, then validate the revised document before
calling `sequencer.execute(document)`. Validation does not execute device operations.

## Contracts and scopes

Only top-level `schema_version: 1`, `type: sequence` documents are supported. Nodes are discriminated
Pydantic models: `sequence`, `repeat`, `for_each`, `while`, `set`, `scan_call`, `component_call`.
An empty sequence and an empty ForEach value list are valid. Repeat counts must be positive
integers; booleans, zero and negative values are rejected.
Unknown fields/types, missing definition versions, dependency cycles and duplicate definition-local
node IDs fail validation. Reused definition-local IDs are valid: call and iteration paths distinguish
runtime occurrences in contextual execution errors.

Every loop iteration uses a child lexical scope. Variables can shadow an outer variable and restore
it afterward. Status bindings cannot overwrite visible bindings and do not escape loop bodies.
Component arguments resolve in the caller; validated parameters and schema defaults then enter a
fresh scope with no caller variables or statuses. Components export no internal bindings. The library
indexes caller-owned definitions by ID and version and rejects duplicate registrations.
Dependency resolution includes all transitive calls; definition persistence belongs to the interface.

Expressions use exact `{"ref": "loop_variable"}` and `{"param": "parameter"}` shapes. Statuses
are a separate namespace: `{"type": "status_field", "status_ref": "ramp", "field": "done"}`.
Only checked `done` observations are supported. `not`, `and`, `or`, and `compare` (`eq`, `ne`, `lt`,
`le`, `gt`, `ge`) are structured expressions; boolean operators require booleans. Wrap literal
expression-shaped data as `{"type": "literal", "value": {...}}`. Retrieved parameter and variable
values are data and are not reinterpreted as expressions. Arbitrary Python expressions and non-JSON
literal objects are rejected. Parameter schemas use the existing `fastjsonschema` dependency,
apply defaults, and support local schema references. Remote schema references are rejected.

## Concrete BEC integration

- `Scans._available_scans` holds `ScanObject.scan_info`, populated from
  `MessageEndpoints.available_scans()`. The catalog exposes the actual signature, annotations,
  defaults, docstring, bundle rules and GUI metadata. `catalog.entry(name)` exposes this metadata
  plus a deterministic SHA-256 fingerprint. No scan classes are generated.
- `ScanInputValidator` and `dict_to_signature` perform existing metadata-based validation,
  including positional bundles, required inputs, types and advertised bounds. Offline validation
  checks device-name types; live validation additionally resolves names through the device manager.
  Known values are validated before execution, including every finite ForEach value. Dynamic values
  receive available structural checks and full validation again at their call boundary.
- Execution requires a BEC IPython client, including procedure workers (their existing default).
  Scans use `bec.scans.<name>(*positional, **arguments)` with ordinary blocking IPython behavior.
  The sequencer does not suppress reports, wrap scan handles or poll scan-queue internals.
  Scan arguments can include the standard client `metadata` mapping (including loop references).
  The catalog validates its real `ScanObject` frontend type separately from scan-specific inputs.
- A Set node stores the target proxy’s `dotted_name` in its `device` field: `samx` for a
  device, or `samx.velocity` for a signal (including deeper signal paths). Validation and
  execution use `device_manager.devices[name]`. Actual `Signal` proxies use `signal.set(value)`;
  blocking writes call the returned status’s `.wait()`. Device moves use `bec.scans.umv(target,
  value)` when blocking and `bec.scans.mv(target, value)` otherwise. These are the same interfaces
  behind the IPython `umv` and `mv` helpers. Device objects stay on the device server.
- Only asynchronous sets retain handles: signal `Status` or an `mv` `ScanReport`. Completion
  uses their public `done`/`status` and checked `wait` interfaces, propagating failures.
- run_sequence receives a sequence mapping and a list of definition mappings through existing
  procedure messages and endpoints. No message schema changes are needed.
- The sequencer holds the client directly and reads its current scan catalog for validation,
  execution and export. Set targets are resolved through that client’s device manager. Catalog fingerprints
  are available to interfaces; the backend implements no frozen-definition compatibility policy.

## Completion and failure

`wait=False` starts an operation and continues. All operations, including unbound sets, are tracked;
on successful schedule exit the runtime waits for every outstanding operation and propagates
failures. A While checks its condition before entering a body, so an already-complete status starts
zero iterations. A started body finishes before checking again unless interrupted.
Blocking scans, `umv`, signal `.wait()` and asynchronous status waits use native
Ctrl-C/KeyboardInterrupt. The sequencer does not install a separate cancellation mechanism.

On errors or cancellation, the runtime attempts to cancel outstanding move reports through
`ScanReport.cancel()`. Device `Status` has no general abort API: already-started device sets may
continue. There is no rollback. Procedure aborts use the existing worker's interrupt/termination
behavior; forced process termination cannot guarantee interpreter cleanup or device/scan abort.

## Validation

```bash
.venv/bin/python -m pytest -p no:xvfb --random-order bec_lib/tests/scan_sequencer
.venv/bin/python -m pytest -p no:xvfb --random-order bec_server/tests/tests_scan_server/test_scan_sequencer.py
```

The library tests compare interpreter and exported-Python traces, including reusable calls, scope,
status failures and Ctrl-C propagation/cleanup. The checked-in catalog fixture was generated from real `TimeScan` and `LineScan`
metadata; the server test checks the current advertised TimeScan signature. The e2e test uses fresh
services and a minimal axis/camera configuration:

```bash
.venv/bin/python -m pytest -p no:xvfb -v --files-path ./ --start-servers \
  bec_ipython_client/tests/end-2-end/test_scan_sequencer_e2e.py
```


## Package layout

Component definitions stay at the top level for inspection and extension:

```text
scan_sequencer/
    components.py       # Node/document models and available_components()
    expressions.py      # Structured expression models
    core/
        sequencer.py    # Public validate/execute/export entry point
        evaluation.py   # Lexical scopes and expression evaluation
        resolver.py     # Reusable definitions and parameter binding
        validation.py   # Static tree validation
        catalog.py      # Advertised scan metadata
        status.py       # Checked completion and cancellation of native handles
        runtime.py      # Interpreter and asynchronous cleanup
        python_export.py
        description.py
```

Public imports remain available from bec_lib.scan_sequencer. Extending a component starts
with its model in components.py and corresponding validation, execution and export handling
in core/. Component discovery derives directly from these models.
