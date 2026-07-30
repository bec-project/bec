from __future__ import annotations

import argparse
import json
from pathlib import Path

from bec_lib import messages
from bec_lib.serialization import MsgpackSerialization


def write_fixture(path: Path, message: messages.BECMessage) -> None:
    path.write_bytes(MsgpackSerialization.dumps(message))


def load_manifest() -> list[dict]:
    manifest_path = (
        Path(__file__).resolve().parent.parent / "messages" / "testdata" / "messages.json"
    )
    return json.loads(manifest_path.read_text())


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Python msgpack fixtures for Go tests.")
    parser.add_argument(
        "--output-dir", required=True, help="Directory for generated msgpack fixtures."
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for entry in load_manifest():
        filename = entry["filename"]
        metadata = entry.get("metadata", {})
        if entry["type"] == "VariableMessage":
            message = messages.VariableMessage(value=entry["value"], metadata=metadata)
        elif entry["type"] == "ClientRestartMessage":
            message = messages.ClientRestartMessage(reason=entry["reason"], metadata=metadata)
        else:
            raise ValueError(f"Unsupported message type in manifest: {entry['type']}")

        write_fixture(output_dir / filename, message)


if __name__ == "__main__":
    main()
