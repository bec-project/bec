"""Regression tests for service configuration CLI overrides."""

from __future__ import annotations

import sys

import pytest

from bec_lib.bec_service import parse_cmdline_args
from bec_lib.service_config import ProcedureConfig, ServiceConfig, ServiceConfigModel


@pytest.mark.parametrize("configured", [True, False])
@pytest.mark.parametrize("override", [True, False, None])
def test_procedure_cli_override(configured, override):
    config = ServiceConfig(
        config={"procedures": {"use_subprocess_worker": configured}},
        cmdline_args={"use_subprocess_proc_worker": override},
    )
    assert config.model.procedures.use_subprocess_worker is (
        configured if override is None else override
    )


def test_cli_override_preserves_config_input():
    data = {"procedures": {"enable_procedures": False, "use_subprocess_worker": False}}
    model = ServiceConfigModel.model_validate(
        data | {"cmdline_args": {"use_subprocess_proc_worker": True, "unknown": True}}
    )
    assert model.procedures.use_subprocess_worker
    assert not model.procedures.enable_procedures
    assert data["procedures"]["use_subprocess_worker"] is False


def test_cli_override_without_procedure_section():
    config = ServiceConfigModel(cmdline_args={"use_subprocess_proc_worker": True})
    assert config.procedures.use_subprocess_worker


def test_cli_override_with_model_section():
    config = ServiceConfigModel(
        procedures=ProcedureConfig(enable_procedures=False),
        cmdline_args={"use_subprocess_proc_worker": True},
    )
    assert config.procedures.use_subprocess_worker
    assert not config.procedures.enable_procedures


@pytest.mark.parametrize("flag", [[], ["--use-subprocess-proc-worker"]])
def test_procedure_worker_cli_selection(monkeypatch, tmp_path, flag):
    path = tmp_path / "server.yaml"
    path.write_text("procedures:\n  use_subprocess_worker: true\n")
    monkeypatch.setattr(sys, "argv", ["scan-server", "--config", str(path), *flag])
    _, _, config = parse_cmdline_args()
    assert config.model.procedures.use_subprocess_worker
