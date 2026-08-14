# Repository Guidelines — `bec`

`bec` is the core of **BEC** (Beamline Experiment Control): a distributed, service-based control system
for beamlines at large research facilities. Instead of one monolithic application, BEC is a set of small
services that orchestrate devices, scans, and data acquisition, communicating over **Redis** for
real-time messaging and service discovery.

This file is a quick-reference for AI coding agents (and new contributors) working in this repository.
The canonical human contribution guide is [`CONTRIBUTING.md`](CONTRIBUTING.md); user-facing docs live at
<https://bec.readthedocs.io> and are authored in the separate
[`bec-project/bec_docs`](https://github.com/bec-project/bec_docs) repository.

## Project Structure & Module Organization

This repository is a **multi-package monorepo**. Each top-level directory below is an independently
installable and independently versioned Python distribution:

| Path | Package | What it is |
| --- | --- | --- |
| `bec_lib/` | `bec_lib` | Core library: messaging, `MessageEndpoints`, device/scan data model, and the user-facing client (`bec_lib.client`). Everything else depends on this. |
| `bec_server/` | `bec-server` | The services themselves — scan server, device server, scan bundler, file writer, data processing (DAP), SciHub, procedures, and the launcher that supervises them. |
| `bec_ipython_client/` | `bec_ipython_client` | The interactive IPython shell (`bec` command) scientists use at the beamline. |
| `pytest_bec_e2e/` | `pytest-bec-e2e` | A pytest plugin whose fixtures spin up real BEC services for end-to-end tests, reused by downstream repos. |

Supporting directories: `bin/` and `scripts/` (entry points and helpers), `macros/` (scan macros),
`ci/` and `.github/` (CI configuration), `docs/` (package-level notes; the published site lives in
`bec_docs`), `data/` (sample data).

Treat each package's `pyproject.toml` as the source of truth for versions, dependencies, and entry
points. The four packages share a single version number and are released together by
python-semantic-release.

### Orientation: the files worth reading first

- `bec_lib/bec_lib/endpoints.py` — every Redis endpoint in the system. If you need to know how two
  services talk to each other, the answer is here.
- `bec_lib/bec_lib/messages.py` — the pydantic message schemas sent over those endpoints.
- `bec_lib/bec_lib/client.py` — the `BECClient` object (`dev`, `scans`, `queue`, …) that users and GUIs drive.
- `bec_lib/bec_lib/device.py`, `devicemanager.py` — the device abstraction shared by client and server.
- `bec_server/bec_server/scan_server/scans.py` — how scans are defined; the model to copy for a new scan.
- `bec_server/bec_server/bec_server_utils/launch.py` — how the services are started and supervised.

## Local Environment Overlay

If a file named **`AGENTS_PERSONAL.md`** exists next to this one, read it and treat it as an extension
of this file. It carries machine-specific setup — interpreter and environment manager, local paths,
private workflow conventions — and **its instructions take precedence over the generic
"Development Environment" section below**. Everything else in this file still applies.

That file is intentionally untracked and personal to one developer's machine. Do not commit it, do not
reference it from committed files, and do not assume it exists — if it is absent, follow this file as
written.

## Development Environment

Requires **Python 3.11+** (CI tests 3.11, 3.12, and 3.13) and a **Redis server** for anything beyond
unit tests.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows is not supported; see "Platform Notes"
python -m pip install --upgrade pip
```

Install the packages in editable mode. Order matters — `bec_lib` first, since the others depend on it:

```bash
python -m pip install -e ./bec_lib[dev]
python -m pip install -e ./bec_server[dev]
python -m pip install -e ./bec_ipython_client[dev]
python -m pip install -e ./pytest_bec_e2e
```

Confirm the environment points at this checkout rather than a released wheel:

```bash
python -c "import bec_lib; print(bec_lib.__file__)"
```

If you are working in a git worktree or a second clone, re-run the editable installs from that
directory. A single virtualenv can only have one editable install per package, so give each
checkout its own virtualenv rather than sharing one.

### Running BEC locally

Redis must be reachable (default `localhost:6379`). Start the services once and leave them running for
as long as you are working:

```bash
bec-server start      # start all services (tmux session by default)
bec                   # the IPython client, in a second shell
```

The other subcommands act on that already-running session; they are not steps in the startup sequence:

```bash
bec-server attach     # attach to the tmux session to read service logs (detach with Ctrl-b d)
bec-server restart    # restart the services, e.g. after changing server-side code
bec-server stop       # tear everything down when you are done — any client loses its backend
```

`bec-server start --help` lists alternatives such as running services as plain subprocesses. Service
configuration comes from a `service_config.yaml`; see `bec_config_template.yaml` for the shape of the
file and the installation guide in the docs for a worked example.

## Testing

**Unit tests** are fast and require no running services — Redis and hardware are mocked (`fakeredis`,
`pytest-redis`, `unittest.mock`). Run them per package:

```bash
python -m pytest --random-order ./bec_lib/tests
python -m pytest --random-order ./bec_server/tests
python -m pytest --random-order ./bec_ipython_client/tests/client_tests
```

`--random-order` is not optional decoration: CI runs it, and it is how order-dependent test pollution
gets caught. A test that only passes in file order is a broken test.

**End-to-end tests** start real services through the `pytest_bec_e2e` fixtures and need Redis plus a
working device configuration. They are slower and are gated in CI; run them only when you are actually
changing service interaction:

```bash
python -m pytest -v --files-path ./ --start-servers ./bec_ipython_client/tests/end-2-end
```

**Coverage**, close to what CI measures:

```bash
coverage run --branch --source=./bec_lib/bec_lib,./bec_server/bec_server,./bec_ipython_client/bec_ipython_client \
    -m pytest --random-order ./bec_lib/tests ./bec_server/tests ./bec_ipython_client/tests/client_tests
coverage report
```

Guidelines: name test files `test_<feature>.py` and test functions after the behaviour they pin down
(`test_scan_worker_aborts_on_alarm()`, not `test_scan_worker_2()`). Add tests for new features and for
every bug you fix — a bug fix without a regression test invites the bug back. Keep coverage at or above
its current level. Mock Redis and hardware in unit tests; reserve real services for the e2e suite.

## Coding Style & Naming Conventions

- Python 3.11+, 4-space indentation, **100-character** line limit.
- **Black** and **isort** are the source of truth, configured in each `pyproject.toml`. CI fails the
  build on any diff:

  ```bash
  black --line-length=100 --skip-magic-trailing-comma .
  isort --line-length=100 --profile=black --multi-line=3 --trailing-comma .
  ```

- **Pylint** runs in CI over `bec_lib/`, `bec_server/`, and `bec_ipython_client/` and reports a score.
  Do not introduce new warnings.
- `snake_case` for modules, functions, and test files; `PascalCase` for classes.
- Use **f-strings**; avoid `%` formatting and `str.format()`.
- Use `pathlib` rather than string path manipulation or `os.path`.
- Type-annotate new public functions and methods. Pydantic models are the norm for anything crossing a
  service boundary.
- Public functions, classes, and modules get docstrings — they are what the API reference is built from.

## Architecture Notes for Contributors

**Everything goes through Redis.** Services never call each other directly. A service publishes a
message to an endpoint and other services react. This is why adding a feature usually means: define a
message in `bec_lib/messages.py`, add an endpoint in `bec_lib/endpoints.py`, publish from one service,
subscribe in another.

**`MessageEndpoints` is the routing table.** Never hardcode a Redis key string. Always go through
`MessageEndpoints`, so key layout stays changeable in one place.

```python
from bec_lib.endpoints import MessageEndpoints

connector.set_and_publish(MessageEndpoints.device_readback("samx"), msg)
connector.register(MessageEndpoints.scan_status(), cb=self._on_scan_status)
```

**Messages are versioned data contracts.** `bec_lib/messages.py` classes are serialized and consumed by
other services, GUIs, and beamline plugins that may be running a different release. Adding an optional
field is safe; renaming or removing one is a breaking change and needs a `feat!:`/`BREAKING CHANGE:`
commit.

**Devices are configuration, not code.** Device classes live in the separate `ophyd_devices` repository;
`bec` consumes them via a YAML device configuration. If your change is "support a new piece of
hardware", it very likely belongs in `ophyd_devices`, not here.

**Scans are plugin-shaped.** New scan types subclass the scan base classes in
`bec_server/scan_server/scans.py`. Beamline-specific scans belong in a beamline plugin repository
rather than in core.

## Related Repositories

BEC is developed across several repositories under <https://github.com/bec-project>:

- [`ophyd_devices`](https://github.com/bec-project/ophyd_devices) — hardware abstraction (device classes).
- [`bec_widgets`](https://github.com/bec-project/bec_widgets) — PySide6/Qt GUI toolkit.
- [`bec_docs`](https://github.com/bec-project/bec_docs) — the published documentation site.
- Beamline plugin repositories — beamline-specific devices, scans, and widgets.

CI here also triggers downstream builds, so a change to `bec_lib` can break `bec_widgets` or
`ophyd_devices`. When you change a shared interface, check the downstream repositories and coordinate
the change rather than merging and waiting for their pipelines to go red.

## Platform Notes

Code must run on **macOS and Linux**. Windows is not supported or tested. Use `pathlib` and forward
slashes; do not add Windows-specific branches.

## Commit & Pull Request Guidelines

- **Do not commit or push unless explicitly asked to.** Leave the working tree for the human to review.
- **Never open, update, or merge a pull request.** Submitting the change is the human contributor's
  step. An agent's work ends at a reviewed working tree — or at a local commit on a branch, when a
  commit was explicitly requested.
- Branch from `main`; use a descriptive branch name such as `feat/procedure-timeout` or
  `fix/scan-queue-deadlock`.
- **Conventional Commits are mandatory** — `<type>(<scope>): <summary>`, e.g.
  `fix(bec_lib): guard against empty readback`. Allowed types: `build`, `chore`, `ci`, `docs`, `feat`,
  `fix`, `perf`, `refactor`, `style`, `test`. `feat` triggers a minor release, `fix` and `perf` a patch
  release. Breaking changes need `!` after the type or a `BREAKING CHANGE:` footer.
- Commit messages are parsed by python-semantic-release and become the published `CHANGELOG.md`. Keep
  them to a single clean subject line describing the change.
- The pull request itself needs a clear description, linked issues (`closes #123`), and test evidence.
  Leave whoever opens it what that requires: what changed, why, and the output of the tests you ran.
- Update the docs in `bec_docs` when behaviour visible to users changes.
