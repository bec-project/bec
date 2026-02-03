"""Tests for the scan_repeat module."""

import pytest

from bec_lib.bec_errors import ScanRestart
from bec_lib.messages import ScanQueueMessage
from bec_lib.scan_repeat import TooManyScanRestarts, scan_repeat


def test_scan_repeat_no_exception():
    """Test scan_repeat decorator with no exceptions."""
    call_count = 0

    @scan_repeat(max_repeats=3)
    def test_function():
        nonlocal call_count
        call_count += 1
        return "success"

    result = test_function()
    assert result == "success"
    assert call_count == 1


def test_scan_repeat_with_scan_restart():
    """Test scan_repeat decorator with ScanRestart exceptions."""
    call_count = 0

    @scan_repeat(max_repeats=3)
    def test_function():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            msg = ScanQueueMessage(scan_type="test_scan", parameter={"test": "value"})
            raise ScanRestart(msg)
        return "success"

    result = test_function()
    assert result == "success"
    assert call_count == 3


def test_scan_repeat_max_repeats_exceeded():
    """Test scan_repeat decorator when max_repeats is exceeded."""
    call_count = 0

    @scan_repeat(max_repeats=2)
    def test_function():
        nonlocal call_count
        call_count += 1
        msg = ScanQueueMessage(scan_type="test_scan", parameter={"test": "value"})
        raise ScanRestart(msg)

    with pytest.raises(TooManyScanRestarts) as exc_info:
        test_function()

    assert "Maximum scan restart attempts (2) exceeded" in str(exc_info.value)
    assert call_count == 3


def test_scan_repeat_with_custom_exc_handler_retry():
    """Test scan_repeat with custom exception handler that retries."""
    call_count = 0

    def custom_handler(exc, attempt):
        return True  # Always retry

    @scan_repeat(max_repeats=3, exc_handler=custom_handler)
    def test_function():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("Custom error")
        return "success"

    result = test_function()
    assert result == "success"
    assert call_count == 2


def test_scan_repeat_with_custom_exc_handler_no_retry():
    """Test scan_repeat with custom exception handler that doesn't retry."""
    call_count = 0

    def custom_handler(exc, attempt):
        return False  # Don't retry

    @scan_repeat(max_repeats=3, exc_handler=custom_handler)
    def test_function():
        nonlocal call_count
        call_count += 1
        raise ValueError("Custom error")

    with pytest.raises(ValueError) as exc_info:
        test_function()

    assert "Custom error" in str(exc_info.value)
    assert call_count == 1


def test_scan_repeat_with_custom_exc_handler_conditional_retry():
    """Test scan_repeat with custom exception handler with conditional logic."""
    call_count = 0

    def custom_handler(exc, attempt):
        # Only retry on specific error types
        return isinstance(exc, ValueError)

    @scan_repeat(max_repeats=3, exc_handler=custom_handler)
    def test_function():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("Retryable error")
        return "success"

    result = test_function()
    assert result == "success"
    assert call_count == 2


def test_scan_repeat_with_custom_exc_handler_non_retryable():
    """Test scan_repeat with custom handler rejecting specific exception types."""
    call_count = 0

    def custom_handler(exc, attempt):
        # Don't retry on TypeError
        return not isinstance(exc, TypeError)

    @scan_repeat(max_repeats=3, exc_handler=custom_handler)
    def test_function():
        nonlocal call_count
        call_count += 1
        raise TypeError("Non-retryable error")

    with pytest.raises(TypeError) as exc_info:
        test_function()

    assert "Non-retryable error" in str(exc_info.value)
    assert call_count == 1


def test_scan_repeat_with_custom_exc_handler_max_attempts():
    """Test scan_repeat with custom handler hitting max attempts."""
    call_count = 0

    def custom_handler(exc, attempt):
        return True  # Always retry

    @scan_repeat(max_repeats=3, exc_handler=custom_handler)
    def test_function():
        nonlocal call_count
        call_count += 1
        raise ValueError("Always fails")

    with pytest.raises(TooManyScanRestarts) as exc_info:
        test_function()

    assert "Maximum scan restart attempts (3) exceeded" in str(exc_info.value)
    assert call_count == 4


def test_scan_repeat_nested_calls_no_retry():
    """Test scan_repeat with nested calls - inner calls should not retry."""
    outer_count = 0
    inner_count = 0

    @scan_repeat(max_repeats=10)
    def inner_function():
        nonlocal inner_count
        inner_count += 1
        if inner_count < 2:
            msg = ScanQueueMessage(scan_type="inner_scan", parameter={"test": "inner"})
            raise ScanRestart(msg)
        return "inner_success"

    @scan_repeat(max_repeats=3)
    def outer_function():
        nonlocal outer_count
        outer_count += 1
        result = inner_function()
        if outer_count < 2:
            msg = ScanQueueMessage(scan_type="outer_scan", parameter={"test": "outer"})
            raise ScanRestart(msg)
        return f"outer_success_{result}"

    result = outer_function()
    # Outer function retries once (outer_count = 2)
    # First outer attempt: inner_function raises ScanRestart (inner_count = 1), exception propagates up
    # Second outer attempt: inner_function succeeds (inner_count = 2)
    assert result == "outer_success_inner_success"
    assert outer_count == 2
    # Inner doesn't retry when nested, just called twice (once per outer attempt)
    assert inner_count == 2


def test_scan_repeat_with_arguments():
    """Test scan_repeat decorator with function arguments."""
    call_count = 0

    @scan_repeat(max_repeats=3)
    def test_function(a, b, c=None):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            msg = ScanQueueMessage(scan_type="test_scan", parameter={"test": "value"})
            raise ScanRestart(msg)
        return f"{a}_{b}_{c}"

    result = test_function("x", "y", c="z")
    assert result == "x_y_z"
    assert call_count == 2


def test_scan_repeat_default_max_repeats():
    """Test scan_repeat with default max_repeats value."""
    call_count = 0

    @scan_repeat()
    def test_function():
        nonlocal call_count
        call_count += 1
        msg = ScanQueueMessage(scan_type="test_scan", parameter={"test": "value"})
        raise ScanRestart(msg)

    with pytest.raises(TooManyScanRestarts) as exc_info:
        test_function()

    assert "Maximum scan restart attempts (1) exceeded" in str(exc_info.value)
    assert call_count == 2


def test_scan_repeat_without_exc_handler_non_scan_restart():
    """Test scan_repeat without exc_handler but default=True- non-ScanRestart exceptions should retry."""
    call_count = 0

    @scan_repeat(max_repeats=3, default=True)
    def test_function():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("Some error")
        return "success"

    result = test_function()
    assert result == "success"
    assert call_count == 2


def test_scan_repeat_multiple_nested_levels():
    """Test scan_repeat with multiple levels of nesting."""
    level1_count = 0
    level2_count = 0
    level3_count = 0

    @scan_repeat(max_repeats=2)
    def level3():
        nonlocal level3_count
        level3_count += 1
        msg = ScanQueueMessage(scan_type="level3_scan", parameter={"level": 3})
        raise ScanRestart(msg)

    @scan_repeat(max_repeats=2)
    def level2():
        nonlocal level2_count
        level2_count += 1
        try:
            level3()
        except ScanRestart:
            pass  # Catch and ignore
        msg = ScanQueueMessage(scan_type="level2_scan", parameter={"level": 2})
        raise ScanRestart(msg)

    @scan_repeat(max_repeats=2)
    def level1():
        nonlocal level1_count
        level1_count += 1
        try:
            level2()
        except ScanRestart:
            pass  # Catch and ignore
        if level1_count < 2:
            msg = ScanQueueMessage(scan_type="level1_scan", parameter={"level": 1})
            raise ScanRestart(msg)
        return "success"

    result = level1()
    assert result == "success"
    assert level1_count == 2
    # Level 2 and 3 are called twice (once per level1 attempt) and don't retry when nested
    assert level2_count == 2
    assert level3_count == 2


def test_scan_repeat_exception_handler_receives_correct_attempt():
    """Test that the exception handler receives correct attempt numbers."""
    attempts = []

    def custom_handler(exc, attempt):
        attempts.append(attempt)
        return attempt < 3  # Retry only first 2 attempts

    @scan_repeat(max_repeats=5, exc_handler=custom_handler)
    def test_function():
        raise ValueError("Error")

    with pytest.raises(ValueError):
        test_function()

    assert attempts == [1, 2, 3]


def test_scan_repeat_mixed_exceptions():
    """Test scan_repeat with mixed exception types."""
    call_count = 0

    @scan_repeat(max_repeats=5, default=True)
    def test_function():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            msg = ScanQueueMessage(scan_type="test_scan", parameter={"attempt": 1})
            raise ScanRestart(msg)
        if call_count == 2:
            raise ValueError("First value error")
        if call_count == 3:
            msg = ScanQueueMessage(scan_type="test_scan", parameter={"attempt": 3})
            raise ScanRestart(msg)
        return "success"

    result = test_function()
    assert result == "success"
    assert call_count == 4
