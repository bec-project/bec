"""Tests for deprecated API logging."""

from __future__ import annotations

import inspect
from unittest import mock

import pytest

from bec_lib.logger import bec_logger
from bec_lib.utils.deprecation import deprecated


def test_deprecated_logs_once(monkeypatch):
    logger = mock.Mock()
    monkeypatch.setattr(bec_logger, "logger", logger)

    @deprecated(remove_in_version="5.0")
    def old_api() -> int:
        return 1

    assert old_api() == 1
    assert old_api() == 1

    logger.bind.assert_called_once_with(deprecation=True)
    logger.bind.return_value.warning.assert_called_once_with(
        "old_api is deprecated and will be removed in version 5.0"
    )


def test_deprecated_stack_logs_each_call_site_once(monkeypatch):
    logger = mock.Mock()
    monkeypatch.setattr(bec_logger, "logger", logger)

    @deprecated(stack=1)
    def old_api() -> int:
        return 1

    first_line = inspect.currentframe().f_lineno + 2
    for _ in range(2):
        old_api()
    second_line = inspect.currentframe().f_lineno + 1
    old_api()

    assert logger.bind.return_value.warning.call_args_list == [
        mock.call(f"old_api is deprecated (called from {__name__}:{first_line})"),
        mock.call(f"old_api is deprecated (called from {__name__}:{second_line})"),
    ]


def test_deprecated_stack_can_skip_an_intermediate_wrapper(monkeypatch):
    logger = mock.Mock()
    monkeypatch.setattr(bec_logger, "logger", logger)

    @deprecated(stack=2)
    def old_api() -> int:
        return 1

    def call_through() -> int:
        return old_api()

    caller_line = inspect.currentframe().f_lineno + 1
    assert call_through() == 1

    logger.bind.return_value.warning.assert_called_once_with(
        f"old_api is deprecated (called from {__name__}:{caller_line})"
    )


def test_deprecated_rejects_negative_stack():
    with pytest.raises(ValueError, match="stack must be non-negative"):
        deprecated(stack=-1)
