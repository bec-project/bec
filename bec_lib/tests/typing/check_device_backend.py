"""Run positive and intentionally invalid public-interface conformance examples.

Run with the Python environment containing the coordinated editable packages:
    python bec_lib/tests/typing/check_device_backend.py
"""

import json
import os
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory


def main() -> None:
    """Reject missing/mistyped implementations and native reading metadata."""
    fixtures = Path(__file__).parent
    paths = []
    for package in ("bec_lib", "ophyd_devices"):
        spec = find_spec(package)
        if spec is None or spec.origin is None:
            raise RuntimeError(f"Install the matching {package} checkout first")
        paths.append(str(Path(spec.origin).parent.parent))
    with TemporaryDirectory() as directory:
        config = Path(directory) / "pyrightconfig.json"
        for name, expected_errors in (("positive", 0), ("negative", 4)):
            fixture = fixtures / f"device_backend_{name}.py"
            config.write_text(
                json.dumps(
                    {
                        "pythonVersion": "3.11",
                        "typeCheckingMode": "basic",
                        "extraPaths": paths,
                        "include": [os.path.relpath(fixture, directory)],
                    }
                )
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pyright",
                    "--pythonpath",
                    sys.executable,
                    "--outputjson",
                    "-p",
                    str(config),
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            report = json.loads(result.stdout)
            errors = report["summary"]["errorCount"]
            if errors != expected_errors or result.returncode != bool(expected_errors):
                raise AssertionError(result.stdout + result.stderr)
            if name == "negative":
                diagnostics = report["generalDiagnostics"]
                if any(item.get("rule") != "reportAssignmentType" for item in diagnostics):
                    raise AssertionError(report)
                lines = {item["range"]["start"]["line"] for item in diagnostics}
                expected = {
                    index
                    for index, line in enumerate(fixture.read_text().splitlines())
                    if line.startswith(("completion:", "handler:", "provider:", "readings:"))
                }
                if lines != expected:
                    raise AssertionError(report)
            print(f"{name}: {errors} errors (expected {expected_errors})")


if __name__ == "__main__":
    main()
