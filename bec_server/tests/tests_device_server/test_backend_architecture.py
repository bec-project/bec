"""Guard the server boundary alongside the opaque-provider runtime proof."""

import ast
from pathlib import Path


def test_production_dispatch_does_not_reintroduce_native_hal_access():
    root = Path(__file__).resolve().parents[2] / "bec_server" / "device_server"
    forbidden = {
        "_staged",
        "_auto_monitor",
        "_signals",
        "_ophyd_labels_",
        "component_names",
        "walk_signals",
        "walk_components",
        "enum_strs",
    }
    failures = []
    for path in root.rglob("*.py"):
        if "tests" in path.relative_to(root).parts:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""]
            else:
                modules = []
            if any(
                module.split(".")[0] in {"ophyd", "ophyd_async", "bluesky", "ophyd_devices"}
                for module in modules
            ):
                failures.append((str(path), node.lineno, "native import"))
            if isinstance(node, ast.Attribute):
                if node.attr in forbidden:
                    failures.append((str(path), node.lineno, node.attr))
                if isinstance(node.value, ast.Attribute) and node.value.attr == "obj":
                    failures.append((str(path), node.lineno, "raw device traversal"))
    assert not failures
