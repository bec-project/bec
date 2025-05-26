# BEC Lib

bec-lib is a Python library communicating with the [Beamline and Experiment Control (BEC)](https://github.com/bec-project/bec) server. It is primarily used to build new BEC clients such as graphical user interfaces (GUIs) or command line interfaces (CLIs).

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install bec-lib.

```bash
pip install bec-lib
```

## Documentation

The documentation is part of the BEC documentation and can be found [here](https://bec.readthedocs.io/en/latest/).

## Usage

```python
from bec_lib.client import BECClient

# Create a new BECClient instance and start it
bec = BECClient()
bec.start()

# Convenient access to scans and devices
scans = bec.scans
dev = bec.device_manager.devices

# define a dummy callback function
def dummy_callback(data, metadata):
    print(data, metadata)

# add the callback and subscribe to the scan segments
bec.callbacks.register(event_type="scan_segment", callback=dummy_callback, sync=False)

```

## Contributing

Merge requests are very welcome! For major changes, please open an issue first to discuss what you would like to change.
All commits should use the Angular commit scheme:

```
<type>(<scope>): <short summary>
  │       │             │
  │       │             └─⫸ Summary in present tense. Not capitalized. No period at the end.
  │       │
  │       └─⫸ Commit Scope: animations|bazel|benchpress|common|compiler|compiler-cli|core|
  │                          elements|forms|http|language-service|localize|platform-browser|
  │                          platform-browser-dynamic|platform-server|router|service-worker|
  │                          upgrade|zone.js|packaging|changelog|docs-infra|migrations|ngcc|ve|
  │                          devtools
  │
  └─⫸ Commit Type: build|ci|docs|feat|fix|perf|refactor|test
```

The `<type>` and `<summary>` fields are mandatory, the `(<scope>)` field is optional.
##### Type

Must be one of the following:

* **build**: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
* **ci**: Changes to our CI configuration files and scripts (examples: CircleCi, SauceLabs)
* **docs**: Documentation only changes
* **feat**: A new feature
* **fix**: A bug fix
* **perf**: A code change that improves performance
* **refactor**: A code change that neither fixes a bug nor adds a feature
* **test**: Adding missing tests or correcting existing tests


Please make sure to update tests as necessary.

## License

[BSD-3-Clause](https://choosealicense.com/licenses/bsd-3-clause/)
