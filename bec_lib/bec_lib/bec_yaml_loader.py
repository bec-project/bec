import io
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
    with open(filename, "r", encoding="utf-8") as file_in:
        return {"__include__": {"data": yaml_load(file_in), "filename": filename}}


def bec_loader():
    """
    Returns a yaml loader that can include other yaml files using the !include tag.
    """
    loader = yaml.Loader
    loader.add_constructor("!include", include_constructor)
    return loader


def yaml_load(stream: io.TextIOWrapper | str) -> dict:
    """
    Load a yaml file with the ability to include other yaml files.

    Args:
        stream (io.TextIOWrapper | str): The yaml file to load. Can be a file object or a string pointing to a file.

    Returns:
        dict: The yaml file as a dictionary.
    """

    if isinstance(stream, str):
        with open(stream, "r", encoding="utf-8") as file_in:
            return _parse_data_stream(file_in)
    else:
        return _parse_data_stream(stream)


def _parse_data_stream(stream: io.TextIOWrapper) -> dict:
    out = yaml.load(stream, Loader=bec_loader())
    included = []
    for k, v in out.items():
        if "__include__" in v:
            included.append((k, v))
        elif isinstance(v, list):
            for item in v:
                if "__include__" in item:
                    included.append((k, item))
    for k, v in included:
        if k in out:
            out.pop(k)
        for key, value in v["__include__"]["data"].items():
            if key in out:
                print(
                    f"Warning: Multiple definitions for key {key}. Using the one from {v['__include__']['filename']}."
                )
            out[key] = value
    return out
