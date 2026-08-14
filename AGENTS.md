# Repository Guidelines — `bec`

`bec` is the core BEC monorepo. Prefer focused changes, follow existing local patterns, verify the
smallest relevant test scope, and keep downstream compatibility in mind.

This file is an agent-oriented operating manual. The canonical human contribution guide is
[`CONTRIBUTING.md`](CONTRIBUTING.md). User-facing documentation lives at
<https://bec.readthedocs.io> and is authored in the separate
[`bec-project/bec_docs`](https://github.com/bec-project/bec_docs) repository.

## Core Rules

- Do not hardcode Redis keys. Use `MessageEndpoints`.
- Treat `bec_lib/messages.py` as a versioned cross-service contract.
- Adding optional message fields is generally backward-compatible, but consider consumers on older
  releases; renaming or removing fields is breaking.
- Only the device server constructs real device objects; other services use device-manager proxies.
- New hardware support usually belongs in `ophyd_devices` or a plugin repo, not this repo.
- Beamline-specific scans usually belong in beamline plugin repos, not core.
- Follow existing local patterns before introducing a new abstraction.
- Keep diffs focused. Avoid unrelated refactors while fixing a specific issue.
- Add regression tests for bug fixes.
- Do not commit, push, or open PRs unless explicitly asked.

## First Read

Start here when orienting yourself:

- `bec_lib/bec_lib/endpoints.py` — Redis endpoint definitions
- `bec_lib/bec_lib/messages.py` — cross-service message schemas
- `bec_lib/bec_lib/client.py` — user-facing client API
- `bec_lib/bec_lib/device.py`
- `bec_lib/bec_lib/devicemanager.py`
- `bec_server/bec_server/scan_server/scans/scan_base.py` — base scan implementation
- `bec_server/bec_server/bec_server_utils/launch.py` — service launch and supervision
- `CONTRIBUTING.md` — human-facing contribution guide

## Repo Layout

Top-level packages:

- `bec_lib/` — core library used by everything else
- `bec_server/` — backend services
- `bec_ipython_client/` — interactive `bec` client
- `pytest_bec_e2e/` — end-to-end pytest plugin

Each top-level package directory is an independently installable Python distribution. These packages
currently share one release version and are released together.

Supporting directories:

- `bin/` — entry points and helper scripts
- `ci/` and `.github/` — CI configuration
- `docs/` — package-level notes

Related but separate repos:

- `ophyd_devices` — hardware and device classes
- `bec_widgets` — Qt widgets and GUI toolkit
- `bec_docs` — published documentation
- beamline plugin repos — beamline-specific scans, devices, and widgets

Treat each package's `pyproject.toml` as the source of truth for dependencies, scripts, and tool
configuration.

## Local Overlay

If `AGENTS_PERSONAL.md` exists beside this file, treat it as an extension of this file.
Machine-specific environment and workflow instructions in `AGENTS_PERSONAL.md` take precedence over
the generic guidance here.

- `AGENTS_PERSONAL.md` is untracked and local to one developer machine
- do not commit it
- do not reference it from committed files
- do not assume it exists

## Common Task Routing

If you change:

- `bec_lib/messages.py` or `bec_lib/endpoints.py`: inspect usages across this repo; consider
  compatibility with `bec_widgets`, `ophyd_devices`, and plugin consumers; explicitly report
  downstream risk
- `bec_server/bec_server/scan_server/scans/*`: review scan base classes and run relevant scan-server
  tests
- `bec_server/*` service behavior: run relevant server unit tests
- `bec_ipython_client/*`: run client tests
- docs only: no broad test run is required unless commands, paths, or examples changed

If the requested change sounds like one of these, it probably belongs elsewhere:

- new device or hardware class: `ophyd_devices`
- published docs or install guide changes: `bec_docs`
- beamline-specific scan behavior: beamline plugin repo

## BEC Architecture

Services communicate through Redis rather than calling each other directly.

Typical cross-service change flow:

1. Add or update a message schema in `bec_lib/messages.py`.
2. Add or update the endpoint in `bec_lib/endpoints.py`.
3. Publish from one service.
4. Subscribe in another.
5. Add or update regression tests.

`MessageEndpoints` is the routing table. Never introduce literal Redis key strings when an endpoint
helper should own that name.

```python
from bec_lib.endpoints import MessageEndpoints

connector.set_and_publish(MessageEndpoints.device_readback("samx"), msg)
connector.register(MessageEndpoints.scan_status(), cb=self._on_scan_status)
```

Devices are constructed on the device server. All other services interact with proxy device objects
provided by their device manager. Do not instantiate device objects in arbitrary services; follow the
existing device-manager and messaging patterns.

## Validation

Run the smallest relevant test target first. For substantial changes, cross-package changes, or work
that touches shared contracts, run the affected package suite(s) before finishing.

Unit tests are the default. CI runs them with `--random-order`, so local validation should do the
same when practical. Prefer existing Redis test patterns in the affected package. Use `fakeredis`
when Redis semantics matter; use mocks when testing behavior above the Redis layer. Mock hardware in
unit tests; reserve real services for e2e tests. Before adding a new fixture, check for reusable
existing fixtures in the affected package or shared test helpers.

Reference package-level test commands:

```bash
python -m pytest --random-order ./bec_lib/tests
python -m pytest --random-order ./bec_server/tests
python -m pytest --random-order ./bec_ipython_client/tests/client_tests
```

Use end-to-end tests only when service interaction, startup, Redis communication, or client/server
integration is what you are changing:

```bash
python -m pytest -v --files-path ./ --start-servers ./bec_ipython_client/tests/end-2-end
```

When the e2e command includes `--start-servers`, the fixture provisions fresh subprocess services for
that test run, so a separate manual `bec-server restart` is not required.

## Running BEC Locally

Redis must be reachable, usually at `localhost:6379`.

Start services in one shell:

```bash
bec-server start
```

Open the client in another shell:

```bash
bec
```

Useful service commands:

```bash
bec-server attach
bec-server restart
bec-server stop
```

Use `bec-server restart` after changing server-side code when validating against an already-running
local BEC session. Otherwise you may be testing stale service code.

`bec-server start --help` lists alternatives such as subprocess-based launches. Service configuration
comes from a `service_config.yaml`; see `bec_config_template.yaml` for an example shape.

## Style And Change Hygiene

- Python 3.11+, 4-space indentation, 100-character line limit
- prefer `from __future__ import annotations` in new Python modules and when touching files that
  already follow that pattern
- use `f`-strings instead of `%` formatting or `str.format()`
- use `pathlib` instead of manual path-string manipulation
- type-annotate new public functions and methods
- follow the existing docstring style; when writing `Args` and `Returns` sections for typed public
  functions, include the parameter and return types there as well as in the signature
- public functions, classes, and modules should have docstrings
- avoid formatting or import-order churn in untouched files

Run Black and isort on changed files or the affected package. The whole-repo equivalents are:

```bash
black --line-length=100 --skip-magic-trailing-comma .
isort --line-length=100 --profile=black --multi-line=3 --trailing-comma .
```

Pylint runs in CI across `bec_lib/`, `bec_server/`, and `bec_ipython_client/`. Do not introduce new
warnings.

## Development Environment

Requires:

- Python 3.11+
- Redis for anything beyond unit tests

CI currently runs Python 3.11, 3.12, and 3.13.

Editable install order matters because the other packages depend on `bec_lib`:

```bash
python -m pip install -e ./bec_lib[dev]
python -m pip install -e ./bec_server[dev]
python -m pip install -e ./bec_ipython_client[dev]
python -m pip install -e ./pytest_bec_e2e
```

If you are working in a separate clone or git worktree, reinstall editable packages from that checkout.
A single virtualenv cannot point at multiple editable copies of the same package reliably.

## Platform Notes

Code must run on macOS and Linux. Windows is unsupported and untested. Prefer portable `pathlib`
usage and do not add Windows-specific branches unless explicitly requested.

## Commit And PR Notes

- Branch from `main` for new work
- use Conventional Commits
- breaking changes need `!` or a `BREAKING CHANGE:` footer
- leave the eventual PR author with a short summary of what changed, why, and what you validated
- update `bec_docs` when necessary
