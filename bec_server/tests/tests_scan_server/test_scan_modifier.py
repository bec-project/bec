from __future__ import annotations

from types import SimpleNamespace

import pytest

from bec_server.scan_server.scans.scan_modifier import (
    ScanModifier,
    get_scan_hooks_impl,
    scan_hook,
    scan_hook_impl,
)


class _DummyModifier:
    @scan_hook_impl("stage", "before")
    def before_stage(self):
        pass

    @scan_hook_impl("close_scan", "after")
    def after_close_scan(self):
        pass


def test_scan_hook_marks_method_with_hook_info():
    @scan_hook
    def prepare_scan(self):
        return "ok"

    assert prepare_scan._scan_hook_info == {"method_name": "prepare_scan"}  # type: ignore[attr-defined]
    assert prepare_scan(None) == "ok"


def test_scan_hook_impl_registers_hook_metadata():
    hooks = get_scan_hooks_impl(_DummyModifier)

    assert hooks == {"stage": "before_stage", "close_scan": "after_close_scan"}


def test_scan_hook_impl_rejects_invalid_hook_name():
    with pytest.raises(ValueError, match="Invalid scan hook"):
        scan_hook_impl("not_a_hook")  # type: ignore[arg-type]


def test_scan_hook_impl_rejects_invalid_hook_type():
    with pytest.raises(ValueError, match="Invalid scan hook type"):
        scan_hook_impl("stage", "during")  # type: ignore[arg-type]


def test_scan_modifier_device_is_available_checks_presence_and_enabled_state():
    scan = SimpleNamespace(
        dev={"samx": SimpleNamespace(enabled=True), "samy": SimpleNamespace(enabled=False)},
        actions=None,
        components=None,
        scan_info=None,
    )
    modifier = ScanModifier(scan)

    assert modifier.device_is_available("samx") is True
    assert modifier.device_is_available("missing") is False
    assert modifier.device_is_available("samy") is False
    assert modifier.device_is_available("samy", check_enabled=False) is True
    assert modifier.device_is_available(["samx", "samy"], check_enabled=False) is True
