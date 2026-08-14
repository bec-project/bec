# CLAUDE.md — `bec`

@AGENTS.md

The guidelines above are imported from [`AGENTS.md`](AGENTS.md) (single source of
truth); [`CONTRIBUTING.md`](CONTRIBUTING.md) is the canonical human contribution guide. The points that
matter most in day-to-day work:

- **Check for `AGENTS_PERSONAL.md` first.** If it exists, it extends `AGENTS.md` with
  machine-specific environment setup and takes precedence over the generic venv/pip instructions there.
  It is untracked and personal — never commit it, and never assume it exists.
- **This is a multi-package repo** — `bec_lib`, `bec_server`, `bec_ipython_client`, `pytest_bec_e2e`.
  Install the ones you touch in editable mode, `bec_lib` first. Tests live per package
  (`bec_lib/tests/`, `bec_server/tests/`, …), not in a single top-level `tests/`.
- **Services communicate only through Redis.** Route every key through `MessageEndpoints`
  (`bec_lib/bec_lib/endpoints.py`) — never hardcode a Redis key. Message schemas live in
  `bec_lib/bec_lib/messages.py` and are a cross-service contract: adding an optional field is safe,
  renaming or removing one is breaking.
- **Run unit tests with `--random-order`** (`python -m pytest --random-order ./bec_lib/tests`). Mock
  Redis and hardware; e2e tests need real services and are a separate, slower suite.
- **Format before finishing**: `black --line-length=100 --skip-magic-trailing-comma .` and
  `isort --line-length=100 --profile=black --multi-line=3 --trailing-comma .`. CI rejects any diff.
- **New hardware support belongs in `ophyd_devices`, not here.** Beamline-specific scans belong in a
  beamline plugin repo.
- **Do not commit or push unless explicitly asked, and never open a pull request.** If you do commit,
  write a single Conventional Commits line — it is parsed into the published changelog. Opening the PR
  is the human's step; leave them the summary and test output they need for it.
