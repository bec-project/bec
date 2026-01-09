import json
from importlib.metadata import PackageNotFoundError, version


def _try_version(package: str):
    try:
        return version(package)
    except PackageNotFoundError:
        return "not installed"


def print_versions(output_json: bool = False):
    packages = {
        "bec_lib": "BEC Core Library",
        "bec_server": "BEC Server",
        "bec_ipython_client": "BEC IPython Client",
        "bec_widgets": "BEC Widgets",
    }
    versions = {mod: _try_version(mod) for mod in packages.keys()}
    if not output_json:
        for mod, ver in versions.items():
            print(f"{packages.get(mod)}: {ver}")
    else:
        print(json.dumps(versions))
