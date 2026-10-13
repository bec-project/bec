"""Behavior and generated-code parity using advertised TimeScan metadata."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from bec_lib.device import Signal
from bec_lib.devicemanager import DeviceContainer
from bec_lib.scan_report import ScanReport
from bec_lib.scan_sequencer import (
    BECAdapter,
    Definition,
    DefinitionLibrary,
    ExecutionError,
    ScanCatalog,
    Schedule,
    Sequencer,
    available_components,
)
from bec_lib.scan_sequencer.components import Repeat
from bec_lib.scan_sequencer.core.catalog import fingerprint


@pytest.fixture
def metadata():
    return json.loads((Path(__file__).parent / "scan_info.json").read_text())


@pytest.fixture
def scheduler(metadata):
    return Sequencer(ScanCatalog(metadata))


def schedule(root):
    return Schedule(id="test", revision=1, root=root)


def seq(*children, id="root"):
    return {"id": id, "type": "sequence", "children": list(children)}


def scan(id="acquire", arguments=None, positional=None):
    return {
        "id": id,
        "type": "scan_call",
        "name": "time_scan",
        "arguments": {"points": 1, "interval": 0.1} if arguments is None else arguments,
        "positional": positional or [],
    }


def setting(value=1, id="set", wait=True, bind=None):
    return {
        "id": id,
        "type": "set",
        "device": "samx",
        "value": value,
        "wait": wait,
        "bind_status": bind,
    }


def call(definition="measure", version=1, arguments=None, id="call"):
    return {
        "id": id,
        "type": "component_call",
        "definition": {"id": definition, "version": version},
        "arguments": arguments or {},
    }


class FakeAdapter:
    def __init__(self, catalog, *, scans_to_complete=0, failed=False, interrupt=False):
        self._catalog = catalog
        self.trace = []
        self.scans_to_complete = scans_to_complete
        self.failed = failed
        self.interrupt = interrupt
        self.cancelled = []
        self.set_status = SimpleNamespace(kind="set", complete=scans_to_complete == 0)

    def catalog(self):
        return self._catalog

    def validate_device(self, name):
        if name != "samx":
            raise ValueError("Unknown device")

    def set(self, name, value, *, wait=True):
        self.trace.append(("set", name, value))
        if wait:
            while not self.done(self.set_status):
                pass
            return None
        return self.set_status

    def scan(self, name, positional, arguments):
        self._catalog.validate(name, positional, arguments)
        self.trace.append(("scan", name, positional, arguments))
        if sum(item[0] == "scan" for item in self.trace) >= self.scans_to_complete:
            self.set_status.complete = True

    def done(self, status):
        if status is None:
            return True
        if self.interrupt:
            raise KeyboardInterrupt
        if status.complete and self.failed and status.kind == "set":
            raise RuntimeError("Set failed")
        return status.complete

    def cancel(self, status):
        self.cancelled.append(status.kind)


def execute(scheduler, document, *, exported=False, **options):
    if exported:
        return readable_export_trace(scheduler, document, **options)
    adapter = FakeAdapter(scheduler.catalog, **options)
    scheduler.execute(document, adapter)
    return adapter.trace


def polarization(component=False):
    body = (
        call(arguments={"position": {"ref": "pol"}})
        if component
        else seq(setting({"ref": "pol"}), scan(), id="measure_body")
    )
    return schedule(
        {
            "id": "repeat",
            "type": "repeat",
            "count": 2,
            "body": {
                "id": "polarizations",
                "type": "for_each",
                "variable": "pol",
                "values": [1, -1],
                "body": body,
            },
        }
    )


def add_measure(scheduler):
    scheduler.definitions.add(
        Definition(
            id="measure",
            version=1,
            description="Set position and acquire.",
            parameters={
                "type": "object",
                "properties": {
                    "position": {"type": "number", "default": 0},
                    "scan_arguments": {"type": "object", "default": {"points": 1, "interval": 0.1}},
                },
                "additionalProperties": False,
            },
            body=seq(setting({"param": "position"}), scan(arguments={"param": "scan_arguments"})),
        )
    )


@pytest.mark.parametrize("component", [False, True])
def test_polarization_four_scans_and_export_parity(scheduler, component):
    if component:
        add_measure(scheduler)
    doc = polarization(component)
    interpreted = execute(scheduler, doc)
    assert interpreted == execute(scheduler, doc, exported=True)
    assert [item[2] for item in interpreted if item[0] == "set"] == [1, -1, 1, -1]
    assert len([item for item in interpreted if item[0] == "scan"]) == 4


def test_round_trip(scheduler):
    doc = polarization()
    assert Schedule.model_validate_json(doc.model_dump_json()) == doc
    scheduler.validate(doc)
    assert doc.revision == 1


@pytest.mark.parametrize(
    "root", [seq(), {"id": "repeat", "type": "repeat", "count": 1, "body": seq()}]
)
def test_empty_bodies(scheduler, root):
    doc = schedule(root)
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)
    assert execute(scheduler, doc) == []
    assert "pass" in scheduler.export(doc, format="python")


@pytest.mark.parametrize("count", [True, 0, -1, 1.5, "2"])
def test_repeat_count_strict(count):
    with pytest.raises(ValidationError):
        Repeat(id="x", count=count, body=seq())


@pytest.mark.parametrize("type", ["scan", "custom_scan", "scan_hook"])
def test_unsupported_document_types(type):
    with pytest.raises(ValidationError):
        Schedule(id="x", revision=1, type=type, root=seq())


@pytest.mark.parametrize(
    "root",
    [{"id": "unknown", "type": "move", "device": "samx", "value": 1}, {**scan(), "unexpected": 1}],
)
def test_unsupported_nodes_and_fields(root):
    with pytest.raises(ValidationError):
        schedule(root)


def test_real_scan_signature_defaults_positional_and_validation(scheduler):
    scheduler.validate(schedule(scan(arguments={}, positional=[2, 0.1])))
    scheduler.validate(schedule(scan()))
    assert scheduler.catalog.info("time_scan")["signature"][2]["default"] == 0
    with pytest.raises(ValueError, match="points"):
        scheduler.validate(schedule(scan(arguments={})))
    with pytest.raises(ValueError):
        scheduler.validate(schedule(scan(arguments={"points": "bad", "interval": 0.1})))
    with pytest.raises(ValueError):
        scheduler.validate(schedule(scan(arguments={"points": 2, "interval": -1})))
    with pytest.raises(ValueError, match="Unknown keyword"):
        scheduler.validate(schedule(scan(arguments={"points": 2, "interval": 0.1, "garbage": 1})))
    result = execute(scheduler, schedule(scan(arguments={"exp_time": 0.01}, positional=[2, 0.1])))
    assert result == execute(
        scheduler, schedule(scan(arguments={"exp_time": 0.01}, positional=[2, 0.1])), exported=True
    )
    assert result[0][2:] == ([2, 0.1], {"exp_time": 0.01})


def test_catalog_references_supplied_metadata(metadata):
    catalog = ScanCatalog(metadata)
    assert catalog.info("time_scan") is metadata["time_scan"]
    original = fingerprint(catalog.info("time_scan"))
    metadata["time_scan"]["doc"] = "Changed"
    assert fingerprint(catalog.info("time_scan")) != original


def test_live_catalog_validated_before_hardware(scheduler):
    adapter = FakeAdapter(ScanCatalog({}))
    with pytest.raises(ValueError, match="Unknown scan"):
        scheduler.execute(schedule(seq(setting(), scan())), adapter)
    assert adapter.trace == []


def test_static_validation_before_first_component(scheduler):
    with pytest.raises(ValueError, match="Unknown scan"):
        scheduler.validate(schedule(seq(setting(), {**scan(), "name": "otf_scan"})))
    adapter = FakeAdapter(scheduler.catalog)
    with pytest.raises(ValueError):
        scheduler.execute(schedule(seq(setting(), scan(arguments={}))), adapter)
    assert adapter.trace == []


def test_scope_shadowing_and_no_loop_binding_leak(scheduler):
    doc = schedule(
        {
            "id": "outer",
            "type": "for_each",
            "variable": "x",
            "values": [1],
            "body": seq(
                {
                    "id": "inner",
                    "type": "for_each",
                    "variable": "x",
                    "values": [2],
                    "body": setting({"ref": "x"}, id="inside"),
                },
                setting({"ref": "x"}, id="outside"),
            ),
        }
    )
    trace = execute(scheduler, doc)
    assert trace == execute(scheduler, doc, exported=True)
    assert [item[2] for item in trace] == [2, 1]
    with pytest.raises(ValueError, match="Undefined Reference"):
        scheduler.validate(
            schedule(seq(doc.root.model_dump(), setting({"ref": "x"}, id="leaked"), id="parent"))
        )


def test_duplicate_ids_rejected_but_reusable_ids_allowed(scheduler):
    with pytest.raises(ValueError, match="Duplicate node ID"):
        scheduler.validate(schedule(seq(scan(), scan())))
    add_measure(scheduler)
    doc = schedule(seq(call(id="a"), call(id="b")))
    assert len(execute(scheduler, doc)) == 4
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)


def test_component_scope_isolated_from_caller(scheduler):
    scheduler.definitions.add(Definition(id="private", version=1, body=setting({"ref": "x"})))
    with pytest.raises(ValueError, match="Undefined Reference"):
        scheduler.validate(
            schedule(
                {
                    "id": "loop",
                    "type": "for_each",
                    "variable": "x",
                    "values": [1],
                    "body": call("private"),
                }
            )
        )
    scheduler.definitions.add(Definition(id="status", version=1, body=setting(bind="private")))
    condition = {"type": "status_field", "status_ref": "private", "field": "done"}
    with pytest.raises(ValueError, match="not guaranteed"):
        scheduler.validate(
            schedule(
                seq(
                    call("status"),
                    {"id": "w", "type": "while", "condition": condition, "body": seq(id="empty")},
                )
            )
        )


def test_parameter_defaults_required_and_extra_validation(scheduler):
    add_measure(scheduler)
    assert execute(scheduler, schedule(call()))[0][2] == 0
    for arguments in ({"position": "bad"}, {"extra": 1}, {"scan_arguments": {}}):
        with pytest.raises(ValueError):
            scheduler.validate(schedule(call(arguments=arguments)))
    scheduler.definitions.add(
        Definition(
            id="required",
            version=1,
            parameters={
                "type": "object",
                "properties": {"x": {"type": "integer"}},
                "required": ["x"],
            },
            body=seq(),
        )
    )
    with pytest.raises(ValueError):
        scheduler.validate(schedule(call("required")))


def test_transitive_versioned_dependencies(scheduler):
    add_measure(scheduler)
    wrapper = Definition(id="wrapper", version=1, body=call())
    scheduler.definitions.add(wrapper)
    assert scheduler.definitions.get(("wrapper", 1)) is wrapper
    doc = schedule(call("wrapper"))
    _, dependencies = scheduler.definitions.resolve(doc.root)
    assert {d.id for d in dependencies} == {"measure", "wrapper"}
    scheduler.definitions.add(Definition(id="measure", version=2, body=setting(99)))
    with pytest.raises(ValueError, match="already exists"):
        scheduler.definitions.add(Definition(id="measure", version=1, body=seq()))
    adapter = FakeAdapter(scheduler.catalog)
    scheduler.execute(doc, adapter)
    assert adapter.trace[0][2] == 0
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)


@pytest.mark.parametrize("kind", ["missing", "cycle"])
def test_dependency_errors(scheduler, kind):
    if kind == "missing":
        scheduler.definitions.add(
            Definition(id="outer", version=1, body=call("missing", version=2))
        )
        match = "Missing definition version"
    else:
        scheduler.definitions.add(Definition(id="outer", version=1, body=call("inner")))
        scheduler.definitions.add(Definition(id="inner", version=1, body=call("outer")))
        match = "dependency cycle"
    with pytest.raises(ValueError, match=match):
        scheduler.validate(schedule(call("outer")))


def ramp(body=None):
    return schedule(
        seq(
            setting(wait=False, bind="ramp"),
            {
                "id": "while",
                "type": "while",
                "condition": {
                    "type": "not",
                    "operand": {"type": "status_field", "status_ref": "ramp", "field": "done"},
                },
                "body": body or seq(scan(id="one"), scan(id="two"), id="body"),
            },
        )
    )


@pytest.mark.parametrize("scans_to_complete, expected", [(0, 0), (1, 2), (3, 4)])
def test_while_checks_before_body_and_finishes_started_body(scheduler, scans_to_complete, expected):
    doc = ramp()
    result = execute(scheduler, doc, scans_to_complete=scans_to_complete)
    assert result == execute(scheduler, doc, exported=True, scans_to_complete=scans_to_complete)
    assert sum(item[0] == "scan" for item in result) == expected


@pytest.mark.parametrize("exported", [False, True])
def test_failed_status_is_not_successful_loop_termination(scheduler, exported):
    with pytest.raises(RuntimeError, match="Set failed"):
        execute(scheduler, ramp(), exported=exported, failed=True)


@pytest.mark.parametrize("exported", [False, True])
def test_ctrl_c_during_async_waits_propagates_and_cleans_up(scheduler, exported):
    adapter = FakeAdapter(scheduler.catalog, scans_to_complete=1, interrupt=True)
    with pytest.raises(KeyboardInterrupt):
        if exported:
            readable_export_trace(scheduler, schedule(setting(wait=False)), adapter=adapter)
        else:
            scheduler.execute(schedule(setting(wait=False)), adapter)
    assert adapter.cancelled == ["set"]


@pytest.mark.parametrize(
    "loop",
    [
        {"id": "loop", "type": "repeat", "count": 1, "body": setting(bind="s")},
        {
            "id": "loop",
            "type": "for_each",
            "variable": "x",
            "values": [],
            "body": setting(bind="s"),
        },
        {"id": "loop", "type": "while", "condition": False, "body": setting(bind="s")},
    ],
)
def test_loop_status_bindings_never_escape(scheduler, loop):
    with pytest.raises(ValueError, match="not guaranteed"):
        scheduler.validate(
            schedule(
                seq(
                    loop,
                    {
                        "id": "w",
                        "type": "while",
                        "condition": {"type": "status_field", "status_ref": "s", "field": "done"},
                        "body": seq(id="empty"),
                    },
                )
            )
        )


def test_unsupported_status_fields_and_non_boolean_conditions(scheduler):
    with pytest.raises(ValueError):
        scheduler.validate(
            schedule(
                seq(
                    setting(bind="s"),
                    {
                        "id": "w",
                        "type": "while",
                        "condition": {"type": "status_field", "status_ref": "s", "field": "secret"},
                        "body": seq(id="empty"),
                    },
                )
            )
        )
    with pytest.raises(ValueError, match="boolean"):
        scheduler.validate(schedule({"id": "w", "type": "while", "condition": 1, "body": seq()}))


def test_safe_quoting_and_reserved_literal_wrappers(scheduler):
    value = {"type": "literal", "value": {"ref": "not_a_variable"}}
    text = "'\\\n__import__('os').system('bad')"
    doc = schedule(seq(setting(value), setting(text, id="quoted")))
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)
    assert execute(scheduler, doc)[0][2] == {"ref": "not_a_variable"}
    assert scheduler.export(doc, format="python") == scheduler.export(doc, format="python")
    with pytest.raises((ValueError, ValidationError)):
        scheduler.validate(schedule(setting(float("nan"))))
    with pytest.raises(ValidationError):
        schedule(setting(object()))


def test_ipython_adapter_uses_native_blocking_and_nonblocking_apis(metadata):
    status = SimpleNamespace(done=False, wait=Mock())
    signal = Mock(spec=Signal)
    signal.read_only = False
    signal.set.return_value = status
    device = SimpleNamespace(set=Mock(), read_only=False, velocity=signal)
    report = Mock(spec=ScanReport)
    report.status = "RUNNING"
    client = SimpleNamespace(
        device_manager=SimpleNamespace(devices=DeviceContainer({"samx": device})),
        scans=SimpleNamespace(umv=Mock(), mv=Mock(return_value=report), time_scan=Mock()),
    )
    adapter = BECAdapter(client)
    assert adapter.set("samx", 1) is None
    client.scans.umv.assert_called_once_with(device, 1, relative=False)
    device.set.assert_not_called()
    assert adapter.set("samx", 2, wait=False) is report
    client.scans.mv.assert_called_once_with(device, 2, relative=False)
    assert not adapter.done(report)
    report.status = "COMPLETED"
    assert adapter.done(report)
    report.wait.assert_called_once_with(timeout=0, file_written=False)
    report.status = "FAILED"
    with pytest.raises(RuntimeError, match="FAILED"):
        adapter.done(report)
    adapter.cancel(report)
    report.cancel.assert_called_once()
    assert adapter.set("samx.velocity", 3) is None
    status.wait.assert_called_once_with()
    status.wait.reset_mock()
    assert adapter.set("samx.velocity", 4, wait=False) is status
    assert not adapter.done(status)
    status.wait.assert_not_called()
    status.done = True
    assert adapter.done(status)
    status.wait.assert_called_once_with(timeout=0, raise_on_failure=True)
    status.wait.side_effect = RuntimeError("failed")
    with pytest.raises(RuntimeError, match="failed"):
        adapter.done(status)
    assert adapter.scan("time_scan", [1, 0.1], {}) is None
    client.scans.time_scan.assert_called_once_with(1, 0.1)
    assert adapter.done(None)


def test_error_path_and_cleanup(scheduler):
    doc = schedule(seq(setting(wait=False), scan()))
    adapter = FakeAdapter(scheduler.catalog, failed=True)
    with pytest.raises(ExecutionError) as error:
        scheduler.execute(doc, adapter)
    assert error.value.path == ("root", "set")
    assert adapter.cancelled


def test_comparisons_and_boolean_operators(scheduler):
    condition = {
        "type": "and",
        "operands": [
            {"type": "compare", "operator": "eq", "left": 1, "right": 2},
            {"type": "or", "operands": [True, False]},
        ],
    }
    doc = schedule({"id": "w", "type": "while", "condition": condition, "body": scan()})
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)
    assert execute(scheduler, doc) == []


def test_zero_iteration_reusable_component_and_unknown_input(scheduler):
    add_measure(scheduler)
    doc = schedule(
        {
            "id": "loop",
            "type": "for_each",
            "variable": "position",
            "values": [],
            "body": call(arguments={"position": {"ref": "position"}}),
        }
    )
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)


def test_component_schema_defaults_validated_at_registration():
    library = DefinitionLibrary()
    with pytest.raises(ValueError):
        library.add(
            Definition(
                id="bad",
                version=1,
                parameters={
                    "type": "object",
                    "properties": {"x": {"type": "integer", "default": "bad"}},
                },
                body=seq(),
            )
        )
    with pytest.raises(ValueError, match="local"):
        library.add(
            Definition(
                id="bad",
                version=1,
                parameters={
                    "type": "object",
                    "properties": {"x": {"$ref": "https://invalid.example/schema"}},
                },
                body=seq(),
            )
        )
    library.add(
        Definition(
            id="default_named_parameter",
            version=1,
            parameters={
                "type": "object",
                "properties": {"default": {"type": "integer", "default": 1}},
            },
            body=seq(),
        )
    )


def test_boolean_schema_version_rejected():
    with pytest.raises(ValidationError):
        Schedule(id="test", revision=1, schema_version=True, root=seq())


def test_dynamic_component_parameters_and_literal_values(scheduler):
    scheduler.definitions.add(
        Definition(
            id="observe",
            version=1,
            parameters={
                "type": "object",
                "properties": {"done": {"type": "boolean"}},
                "required": ["done"],
            },
            body=setting({"param": "done"}, id="observe_set"),
        )
    )
    doc = schedule(
        seq(
            setting(bind="s"),
            call(
                "observe",
                arguments={"done": {"type": "status_field", "status_ref": "s", "field": "done"}},
            ),
        )
    )
    result = execute(scheduler, doc)
    assert result[-1][2] is True
    assert result == execute(scheduler, doc, exported=True)


def test_parameter_condition_controls_execution(scheduler):
    scheduler.definitions.add(
        Definition(
            id="maybe",
            version=1,
            parameters={
                "type": "object",
                "properties": {"enabled": {"type": "boolean"}},
                "required": ["enabled"],
            },
            body={"id": "w", "type": "while", "condition": {"param": "enabled"}, "body": scan()},
        )
    )
    doc = schedule(call("maybe", arguments={"enabled": False}))
    assert execute(scheduler, doc) == []
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)


def test_real_line_scan_positional_bundles(scheduler):
    node = {
        "id": "line",
        "type": "scan_call",
        "name": "line_scan",
        "positional": ["samx", -1, 1],
        "arguments": {"steps": 2, "relative": False},
    }
    doc = schedule(node)
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)
    for positional, arguments in [
        ([], node["arguments"]),
        (["samx", 0], node["arguments"]),
        (node["positional"], {}),
        (node["positional"], {"steps": 0, "relative": False}),
    ]:
        with pytest.raises(ValueError):
            scheduler.validate(schedule({**node, "positional": positional, "arguments": arguments}))


def test_entire_loop_argument_mapping_is_validated(scheduler):
    node = {
        "id": "mapping_loop",
        "type": "for_each",
        "variable": "args",
        "values": [{"points": 1, "interval": 0.1}, {"points": 2, "interval": 0.2}],
        "body": scan(arguments={"ref": "args"}),
    }
    doc = schedule(node)
    assert execute(scheduler, doc) == execute(scheduler, doc, exported=True)
    node["values"].append({})
    with pytest.raises(ValueError, match="points"):
        scheduler.validate(schedule(node))


def test_nested_literal_parameter_dictionary_is_not_reinterpreted(scheduler):
    scheduler.definitions.add(
        Definition(
            id="literal",
            version=1,
            parameters={
                "type": "object",
                "properties": {"data": {"type": "object"}},
                "required": ["data"],
            },
            body=setting({"param": "data"}),
        )
    )
    doc = schedule(
        call("literal", arguments={"data": {"type": "literal", "value": {"ref": "data"}}})
    )
    result = execute(scheduler, doc)
    assert result[0][2] == {"ref": "data"}
    assert result == execute(scheduler, doc, exported=True)
    assert '"ref": "data"' in scheduler.export(doc, format="description")


@pytest.mark.parametrize("exported", [False, True])
def test_ctrl_c_during_scan_propagates_and_cleans_up(scheduler, exported):
    adapter = FakeAdapter(scheduler.catalog)
    adapter.scan = Mock(side_effect=KeyboardInterrupt)
    document = schedule(seq(setting(wait=False), scan(), setting(2, id="after")))
    with pytest.raises(KeyboardInterrupt):
        if exported:
            readable_export_trace(scheduler, document, adapter=adapter)
        else:
            scheduler.execute(document, adapter)
    assert adapter.trace == [("set", "samx", 1)]
    assert adapter.cancelled == ["set"]


def test_invalid_mutated_models_revalidate_before_validation(scheduler):
    document = polarization()
    document.root.body.body.children.append({"type": "unknown", "id": "bad"})
    with pytest.raises(ValidationError):
        scheduler.validate(document)


@pytest.mark.parametrize("exported", [False, True])
@pytest.mark.parametrize("target", ["samx", "samx.velocity", "samx.config.setpoint"])
def test_dotted_set_targets_roundtrip_and_execution(scheduler, target, exported):
    status = SimpleNamespace(done=True, wait=Mock())
    signal = Mock(spec=Signal)
    signal.set.return_value = status
    signal.read_only = False
    device = SimpleNamespace(
        set=Mock(return_value=status),
        read_only=False,
        velocity=signal,
        config=SimpleNamespace(setpoint=signal),
    )
    devices = DeviceContainer({"samx": device})
    client = SimpleNamespace(
        device_manager=SimpleNamespace(devices=devices),
        scans=SimpleNamespace(_available_scans={}, umv=Mock(), mv=Mock()),
    )
    adapter = BECAdapter(client)
    document = schedule({**setting(), "device": target})
    restored = Schedule.model_validate_json(document.model_dump_json())
    assert restored.root.device == target
    if exported:
        namespace = {}
        source = scheduler.export(restored, format="python")
        exec(compile(source, "exported_sequence.py", "exec"), namespace)
        namespace["run"](client)
    else:
        scheduler.execute(restored, adapter)
    if "." in target:
        signal.set.assert_called_once_with(1)
        status.wait.assert_called_once_with()
        client.scans.umv.assert_not_called()
    else:
        client.scans.umv.assert_called_once_with(device, 1, relative=False)
        signal.set.assert_not_called()
        status.wait.assert_not_called()
    device.set.assert_not_called()


@pytest.mark.parametrize(
    "target, message",
    [
        ("missing.signal", "Unknown"),
        ("samx.missing", "Unknown"),
        ("samx.config.missing", "Unknown"),
        ("samx.config", "Non-settable"),
        ("samx.readonly", "Read-only"),
    ],
)
def test_invalid_signal_rejected_before_first_set(scheduler, target, message):
    device = SimpleNamespace(
        set=Mock(),
        read_only=False,
        config=SimpleNamespace(),
        readonly=SimpleNamespace(set=Mock(), read_only=True),
    )
    client = SimpleNamespace(
        device_manager=SimpleNamespace(devices=DeviceContainer({"samx": device})),
        scans=SimpleNamespace(_available_scans={}, umv=Mock(), mv=Mock()),
    )
    document = schedule(seq(setting(id="first"), {**setting(id="invalid"), "device": target}))
    with pytest.raises(ValueError, match=message):
        scheduler.execute(document, BECAdapter(client))
    device.set.assert_not_called()
    device.readonly.set.assert_not_called()
    client.scans.umv.assert_not_called()
    client.scans.mv.assert_not_called()


class NativeTestStatus:
    """Signal status facade over the trace adapter used by readable exports."""

    def __init__(self, adapter, handle):
        self.adapter = adapter
        self.handle = handle

    @property
    def done(self):
        return self.adapter.done(self.handle)

    def wait(self, **kwargs):
        while not self.adapter.done(self.handle):
            pass


class NativeTestMove(ScanReport):
    """Public move report facade without queue-storage internals."""

    def __init__(self, adapter, handle):
        super().__init__()
        self.adapter = adapter
        self.handle = handle

    @property
    def status(self):
        return "COMPLETED" if self.adapter.done(self.handle) else "RUNNING"

    def wait(self, **kwargs):
        while not self.adapter.done(self.handle):
            pass

    def cancel(self):
        self.adapter.cancel(self.handle)


def readable_export_trace(scheduler, document, adapter=None, **options):
    adapter = adapter or FakeAdapter(scheduler.catalog, **options)
    signal = Mock(spec=Signal)
    signal.read_only = False
    signal.set.side_effect = lambda value: NativeTestStatus(
        adapter, adapter.set("samx", value, wait=False)
    )
    device = SimpleNamespace(read_only=False, set=Mock(), velocity=signal)
    scans = SimpleNamespace(_available_scans={})

    def move(target, value, *, relative):
        assert target is device
        assert relative is False
        return NativeTestMove(adapter, adapter.set("samx", value, wait=False))

    def blocking_move(target, value, *, relative):
        move(target, value, relative=relative).wait()

    scans.umv = blocking_move
    scans.mv = move
    for name, info in scheduler.catalog.entries.items():

        def run_scan(*args, _name=name, **kwargs):
            scheduler.catalog.validate(_name, list(args), kwargs)
            return adapter.scan(_name, list(args), kwargs)

        setattr(scans, name, run_scan)
        scans._available_scans[name] = SimpleNamespace(scan_info=info)
    client = SimpleNamespace(
        scans=scans,
        device_manager=SimpleNamespace(
            devices=DeviceContainer({"samx": device, "id_mode": signal})
        ),
    )
    namespace = {}
    source = scheduler.export(document, format="python")
    exec(compile(source, "readable_sequence.py", "exec"), namespace)
    namespace["run"](client)
    return adapter.trace


def test_readable_export_live_signal_and_native_scan_calls(scheduler):
    signal = Mock(spec=Signal)
    signal.read_only = False
    scheduler.catalog.device_manager = SimpleNamespace(devices=DeviceContainer({"id_mode": signal}))
    document = schedule(
        {
            "id": "polarizations",
            "type": "for_each",
            "variable": "pol",
            "values": ["Circ+", "Circ-"],
            "body": seq(
                {**setting({"ref": "pol"}), "device": "id_mode"},
                {
                    "id": "twice",
                    "type": "repeat",
                    "count": 2,
                    "body": scan(
                        arguments={
                            "points": 1,
                            "interval": 0.1,
                            "metadata": {"pol": {"ref": "pol"}, "sample_name": "sample1"},
                        }
                    ),
                },
            ),
        }
    )
    # Validation uses the advertised fixture signature.
    # Its scans have no device arguments, so this minimal manager needs no validator proxy API.
    source = scheduler.export(document, format="python")
    assert "for pol in ['Circ+', 'Circ-']:" in source
    assert "dev.id_mode.set(pol).wait()" in source
    assert (
        "scans.time_scan(points=1, interval=0.1, metadata={'pol': pol, 'sample_name': 'sample1'})"
        in source
    )
    assert "ctx." not in source
    assert "BECAdapter" not in source
    assert readable_export_trace(scheduler, document) == [
        ("set", "samx", "Circ+"),
        (
            "scan",
            "time_scan",
            [],
            {"points": 1, "interval": 0.1, "metadata": {"pol": "Circ+", "sample_name": "sample1"}},
        ),
        (
            "scan",
            "time_scan",
            [],
            {"points": 1, "interval": 0.1, "metadata": {"pol": "Circ+", "sample_name": "sample1"}},
        ),
        ("set", "samx", "Circ-"),
        (
            "scan",
            "time_scan",
            [],
            {"points": 1, "interval": 0.1, "metadata": {"pol": "Circ-", "sample_name": "sample1"}},
        ),
        (
            "scan",
            "time_scan",
            [],
            {"points": 1, "interval": 0.1, "metadata": {"pol": "Circ-", "sample_name": "sample1"}},
        ),
    ]


def test_readable_export_safe_literals_and_hygienic_variables(scheduler):
    document = schedule(
        {
            "id": "loop",
            "type": "for_each",
            "variable": "dev",
            "values": ["'\\\n__import__('os')"],
            "body": setting({"ref": "dev"}),
        }
    )
    assert readable_export_trace(scheduler, document) == execute(scheduler, document)
    assert "for dev in" not in scheduler.export(document, format="python")


@pytest.mark.parametrize("invalid_metadata", ["invalid", 1, []])
def test_client_metadata_uses_real_frontend_type(scheduler, invalid_metadata):
    with pytest.raises(ValueError, match="Invalid client metadata"):
        scheduler.validate(
            schedule(scan(arguments={"points": 1, "interval": 0.1, "metadata": invalid_metadata}))
        )


def test_client_metadata_preserves_interpreter_and_export_trace(scheduler):
    document = schedule(
        scan(arguments={"points": 1, "interval": 0.1, "metadata": {"sample_name": "sample1"}})
    )
    assert readable_export_trace(scheduler, document) == execute(scheduler, document)
    assert execute(scheduler, document) == execute(scheduler, document, exported=True)
    assert scheduler.catalog.entry("time_scan")["client_arguments"]["metadata"]["default"] is None


@pytest.mark.parametrize("wait", [True, False])
def test_description_names_set_target_value_and_wait(scheduler, wait):
    document = schedule(
        {
            "id": "polarizations",
            "type": "for_each",
            "variable": "pol",
            "values": [f"value{index}" for index in range(1, 11)],
            "body": seq(
                {**setting({"ref": "pol"}, wait=wait), "device": "id_mode"},
                {"id": "twice", "type": "repeat", "count": 2, "body": scan()},
            ),
        }
    )
    description = scheduler.export(document, format="description")
    expected = "Set id_mode to the current value of pol"
    expected += " and wait for completion." if wait else "; continue asynchronously."
    assert expected in description
    assert description.count("Set id_mode") == 1


def test_description_literal_set_value_is_not_a_variable(scheduler):
    description = scheduler.export(schedule(setting("pol")), format="description")
    assert 'Set samx to "pol" and wait for completion.' in description
    assert "current value" not in description


@pytest.mark.parametrize("exported", [False, True])
def test_async_failure_cleanup_matches_native_export(scheduler, exported):
    adapter = FakeAdapter(scheduler.catalog, failed=True)
    with pytest.raises(RuntimeError, match="Set failed"):
        if exported:
            readable_export_trace(scheduler, ramp(), adapter=adapter)
        else:
            scheduler.execute(ramp(), adapter)
    assert adapter.cancelled == ["set"]


def test_definition_changes_are_used_without_a_snapshot(scheduler):
    definition = Definition(id="editable", version=1, body=setting(1))
    scheduler.definitions.add(definition)
    document = schedule(call("editable"))
    scheduler.validate(document)
    definition.body.value = 2
    assert execute(scheduler, document) == [("set", "samx", 2)]
    assert execute(scheduler, document, exported=True) == [("set", "samx", 2)]


def test_external_json_input_is_validated_before_execution(scheduler):
    document = polarization().model_dump_json()
    adapter = FakeAdapter(scheduler.catalog)
    scheduler.validate(document)
    scheduler.execute(document, adapter)
    assert adapter.trace == execute(scheduler, polarization())
    assert scheduler.export(document, format="python") == scheduler.export(
        polarization(), format="python"
    )
    adapter.trace.clear()
    malformed = json.loads(document)
    malformed["root"]["count"] = -1
    with pytest.raises(ValidationError):
        scheduler.execute(json.dumps(malformed), adapter)
    assert adapter.trace == []


def test_available_components_exposes_serializable_model_schemas():
    schema = available_components()
    assert set(schema["discriminator"]["mapping"]) == {
        "sequence",
        "repeat",
        "for_each",
        "while",
        "scan_call",
        "set",
        "component_call",
    }
    assert schema["discriminator"]["propertyName"] == "type"
    assert schema["$defs"]["Set"]["properties"]["device"]["description"].startswith("Dotted")
    assert "value" in schema["$defs"]["Set"]["required"]
    assert "body" in schema["$defs"]["Repeat"]["required"]
    assert json.loads(json.dumps(schema)) == schema


def test_external_reusable_definitions_are_received_as_json(scheduler):
    definition = Definition(id="external", version=1, body=setting(2))
    scheduler.definitions = DefinitionLibrary([definition.model_dump()])
    document = schedule(call("external")).model_dump_json()
    assert execute(scheduler, document) == [("set", "samx", 2)]
    assert execute(scheduler, document, exported=True) == [("set", "samx", 2)]
