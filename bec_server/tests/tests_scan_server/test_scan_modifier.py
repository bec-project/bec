from __future__ import annotations

from unittest import mock

import pytest

from bec_server.scan_server.scans.scan_base import ScanBase
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


class _ScopedModifier:
    @scan_hook_impl("at_each_point", "replace", ["_v4_test_scan_modifier"])
    def replace_exact_match(self, ind, pos):
        return self.call_original("at_each_point", ind, pos)

    @scan_hook_impl("post_scan", "after", ["*_scan_modifier"])
    def after_wildcard_match(self):
        pass


class _MultiScopedModifier(ScanModifier):
    @scan_hook_impl("post_scan", "after", ["*_scan_modifier"])
    def after_first_wildcard_match(self):
        pass

    @scan_hook_impl("post_scan", "after", ["_v4_grid_scan"])
    def after_grid_match(self):
        pass


class _AmbiguousScanNameFilteringModifier(ScanModifier):
    @scan_hook_impl("post_scan", "after", ["*_scan_modifier"])
    def after_wildcard_match(self):
        self.scan.modifier_calls.append("after_wildcard_match")

    @scan_hook_impl("post_scan", "after", ["_v4_test_scan_modifier"])
    def after_exact_match(self):
        self.scan.modifier_calls.append("after_exact_match")


class _TestScan(ScanBase):
    scan_name = "_v4_test_scan_modifier"
    scan_type = None

    def __init__(self, *args, **kwargs):
        self.original_hook_calls = []
        self.after_hook_calls = []
        self.modifier_calls = []
        super().__init__(*args, **kwargs)

    @scan_hook
    def at_each_point(self, ind, pos):
        self.original_hook_calls.append((ind, pos))
        return "original-result"

    @scan_hook
    def post_scan(self):
        self.after_hook_calls.append("post_scan")

    def prepare_scan(self):
        pass

    def open_scan(self):
        pass

    def stage(self):
        pass

    def pre_scan(self):
        pass

    def scan_core(self):
        pass

    def unstage(self):
        pass

    def close_scan(self):
        pass

    def on_exception(self, exception: Exception):
        pass


class _OriginalCallingModifier(ScanModifier):
    @scan_hook_impl("at_each_point", "replace")
    def replace_at_each_point(self, ind, pos):
        return self.call_original("at_each_point", ind, pos)


class _ScanNameFilteringModifier(ScanModifier):
    @scan_hook_impl("at_each_point", "replace", ["_v4_test_scan_modifier"])
    def replace_exact_match(self, ind, pos):
        self.scan.modifier_calls.append(("replace_exact_match", ind, pos))
        return self.call_original("at_each_point", ind, pos)

    @scan_hook_impl("post_scan", "after", ["*_scan_modifier"])
    def after_wildcard_match(self):
        self.scan.modifier_calls.append("after_wildcard_match")
        self.scan.after_hook_calls.append("modifier:after_post_scan")


@pytest.fixture
def test_scan(device_manager, connected_connector):
    return _TestScan(
        scan_id="scan-id",
        redis_connector=connected_connector,
        device_manager=device_manager,
        instruction_handler=mock.MagicMock(),
        request_inputs={},
        system_config={},
        scan_modifier=None,
    )


def test_scan_hook_marks_method_with_hook_info():
    @scan_hook
    def prepare_scan(self):
        return "ok"

    scan = mock.MagicMock()
    scan._scan_modifier_hooks = {}
    scan._scan_modifier = None

    assert prepare_scan._scan_hook_info == {"method_name": "prepare_scan"}  # type: ignore[attr-defined]
    assert prepare_scan._scan_hook_original is prepare_scan.__wrapped__  # type: ignore[attr-defined]
    assert prepare_scan(scan) == "ok"


def test_scan_hook_impl_registers_hook_metadata():
    hooks = get_scan_hooks_impl(_DummyModifier)

    assert hooks == {
        "stage": {"before": "before_stage"},
        "close_scan": {"after": "after_close_scan"},
    }


def test_scan_hook_impl_registers_scan_name_filters():
    hooks = get_scan_hooks_impl(_ScopedModifier)

    assert hooks == {
        "at_each_point": {
            "replace": {
                "method_name": "replace_exact_match",
                "scan_names": ["_v4_test_scan_modifier"],
            }
        },
        "post_scan": {
            "after": {"method_name": "after_wildcard_match", "scan_names": ["*_scan_modifier"]}
        },
    }


def test_scan_hook_impl_registers_multiple_same_lifecycle_filters():
    hooks = get_scan_hooks_impl(_MultiScopedModifier)

    assert hooks == {
        "post_scan": {
            "after": [
                {"method_name": "after_first_wildcard_match", "scan_names": ["*_scan_modifier"]},
                {"method_name": "after_grid_match", "scan_names": ["_v4_grid_scan"]},
            ]
        }
    }


def test_scan_hook_impl_rejects_invalid_hook_name():
    with pytest.raises(ValueError, match="Invalid scan hook"):
        scan_hook_impl("not_a_hook")  # type: ignore[arg-type]


def test_scan_hook_impl_rejects_invalid_hook_type():
    with pytest.raises(ValueError, match="Invalid scan hook type"):
        scan_hook_impl("stage", "during")  # type: ignore[arg-type]


def test_scan_hook_impl_rejects_non_list_scan_names():
    with pytest.raises(ValueError, match="scan_names must be a list"):
        scan_hook_impl("stage", "before", "line_scan")  # type: ignore[arg-type]


def test_scan_modifier_device_is_available_checks_presence_and_enabled_state():
    scan = mock.MagicMock()
    scan.dev = {"samx": mock.MagicMock(enabled=True), "samy": mock.MagicMock(enabled=False)}
    scan.actions = None
    scan.components = None
    scan.scan_info = None
    modifier = ScanModifier(scan)

    assert modifier.device_is_available("samx") is True
    assert modifier.device_is_available("missing") is False
    assert modifier.device_is_available("samy") is False
    assert modifier.device_is_available("samy", check_enabled=False) is True
    assert modifier.device_is_available(["samx", "samy"], check_enabled=False) is True


def test_scan_modifier_call_original_calls_bound_original_hook(test_scan):
    test_scan._scan_modifier_hooks = get_scan_hooks_impl(_OriginalCallingModifier)
    test_scan._scan_modifier = _OriginalCallingModifier(test_scan)

    result = test_scan.at_each_point(3, [1, 2])

    assert result is None
    assert test_scan.original_hook_calls == [(3, [1, 2])]


def test_scan_modifier_call_original_raises_for_missing_hook(test_scan):
    modifier = ScanModifier(test_scan)

    with pytest.raises(AttributeError, match="does not expose an original hook"):
        modifier.call_original("stage")


def test_scan_modifier_hook_filters_apply_for_exact_scan_name_match(test_scan):
    test_scan._scan_modifier_hooks = get_scan_hooks_impl(_ScanNameFilteringModifier)
    test_scan._scan_modifier = _ScanNameFilteringModifier(test_scan)

    test_scan.at_each_point(3, [1, 2])

    assert test_scan.modifier_calls == [("replace_exact_match", 3, [1, 2])]
    assert test_scan.original_hook_calls == [(3, [1, 2])]


def test_scan_modifier_hook_filters_skip_non_matching_scan_name(test_scan):
    test_scan.scan_name = "_v4_other_scan"
    test_scan._scan_modifier_hooks = get_scan_hooks_impl(_ScanNameFilteringModifier)
    test_scan._scan_modifier = _ScanNameFilteringModifier(test_scan)

    test_scan.at_each_point(3, [1, 2])

    assert test_scan.modifier_calls == []
    assert test_scan.original_hook_calls == [(3, [1, 2])]


def test_scan_modifier_hook_filters_apply_wildcard_scan_name_match(test_scan):
    test_scan._scan_modifier_hooks = get_scan_hooks_impl(_ScanNameFilteringModifier)
    test_scan._scan_modifier = _ScanNameFilteringModifier(test_scan)

    test_scan.post_scan()

    assert test_scan.modifier_calls == ["after_wildcard_match"]
    assert test_scan.after_hook_calls == ["post_scan", "modifier:after_post_scan"]


def test_scan_modifier_hook_filters_skip_non_matching_wildcard_scan_name(test_scan):
    test_scan.scan_name = "_v4_grid_scan"
    test_scan._scan_modifier_hooks = get_scan_hooks_impl(_ScanNameFilteringModifier)
    test_scan._scan_modifier = _ScanNameFilteringModifier(test_scan)

    test_scan.post_scan()

    assert test_scan.modifier_calls == []
    assert test_scan.after_hook_calls == ["post_scan"]


def test_scan_modifier_hook_filters_allow_disjoint_same_lifecycle_matches(test_scan):
    test_scan.scan_name = "_v4_grid_scan"
    test_scan._scan_modifier_hooks = get_scan_hooks_impl(_MultiScopedModifier)
    test_scan._scan_modifier = _MultiScopedModifier(test_scan)

    test_scan.post_scan()

    assert test_scan.after_hook_calls == ["post_scan"]


def test_scan_modifier_hook_filters_raise_on_ambiguous_match(test_scan):
    test_scan._scan_modifier_hooks = get_scan_hooks_impl(_AmbiguousScanNameFilteringModifier)
    test_scan._scan_modifier = _AmbiguousScanNameFilteringModifier(test_scan)

    with pytest.raises(ValueError, match="Multiple scan modifier implementations matched hook"):
        test_scan.post_scan()
