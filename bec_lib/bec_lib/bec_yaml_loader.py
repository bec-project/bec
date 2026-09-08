import io
import stat
from pathlib import Path

import yaml


def include_constructor(loader, node):
    """
    Include another yaml file.
    """
    filename = Path(loader.construct_scalar(node)).expanduser()
    if not filename.is_absolute():
        base_path = Path(loader.name).resolve().parent
        filename = (base_path / filename).resolve(strict=False)
    if not stat.S_ISREG(filename.stat().st_mode):
        raise yaml.YAMLError(f"Included YAML path '{filename}' is not a regular file.")
    with open(filename, "r", encoding="utf-8") as file_in:
        return {"__include__": {"data": yaml_load(file_in), "filename": filename}}


def bec_loader():
    """
    Returns a yaml loader that can include other yaml files using the !include tag.
    """
    loader = yaml.Loader
    loader.add_constructor("!include", include_constructor)
    return loader


def yaml_load(stream: io.TextIOWrapper | str, process_includes: bool = True) -> dict:
    """
    Load a yaml file with the ability to include other yaml files.

    Args:
        stream (io.TextIOWrapper | str): The yaml file to load. Can be a file object or a string pointing to a file.

    Returns:
        dict: The yaml file as a dictionary.
    """

    if isinstance(stream, str):
        with open(stream, "r", encoding="utf-8") as file_in:
            return _parse_data_stream(file_in, process_includes)
    else:
        return _parse_data_stream(stream, process_includes)


def _strip_includes(d: dict):
    for k, v in dict(d).items():
        if isinstance(v, dict) and "__include__" in v:
            del d[k]
        elif isinstance(v, list):
            remaining = [
                item for item in v if not (isinstance(item, dict) and "__include__" in item)
            ]
            if len(v) == 1 and not remaining:
                del d[k]
            else:
                d[k] = remaining


def _parse_data_stream(stream: io.TextIOWrapper, process_includes: bool = True) -> dict:
    out = yaml.load(stream, Loader=bec_loader())
    if out is None:
        return {}
    if not isinstance(out, dict):
        raise yaml.YAMLError("The YAML document must contain a mapping at its root.")
    included = []
    if not process_includes:
        _strip_includes(out)
    else:
        for k, v in out.items():
            if isinstance(v, dict) and "__include__" in v:
                included.append((k, v))
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and "__include__" in item:
                        included.append((k, item))
    for k, v in included:
        marker = v["__include__"]
        if (
            not isinstance(marker, dict)
            or not isinstance(marker.get("data"), dict)
            or "filename" not in marker
        ):
            raise yaml.YAMLError(
                f"Invalid include marker for '{k}'. Use !include to include another YAML file."
            )
        if k in out:
            out.pop(k)
        for key, value in marker["data"].items():
            if key in out:
                print(
                    f"Warning: Multiple definitions for key {key}. Using the one from {marker['filename']}."
                )
            out[key] = value
    return out
