"""
Tests for the Timeout pattern.

Run with: pytest tests/test_timeout.py -v
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from pysilience.timeout import (
    Timeout,
    TimeoutConfig,
    TimeoutError,
    TimeoutEvent,
    TimeoutEventType,
    timeout,
)


# ============================================================================
# CONFIGURATION TESTS
# ============================================================================


class TestTimeoutConfig:
    """Tests for TimeoutConfig dataclass."""

    def test_default_values(self) -> None:
        config = TimeoutConfig()
        assert config.duration == 30.0
        assert config.cancel_running_future is True
        assert config.use_signals is False

    def test_custom_values(self) -> None:
        config = TimeoutConfig(
            duration=5.0,
            cancel_running_future=False,
            use_signals=True,
        )
        assert config.duration == 5.0
        assert config.cancel_running_future is False
        assert config.use_signals is True

    def test_invalid_duration_zero(self) -> None:
        with pytest.raises(ValueError, match="duration must be positive"):
            TimeoutConfig(duration=0)

    def test_invalid_duration_negative(self) -> None:
        with pytest.raises(ValueError, match="duration must be positive"):
            TimeoutConfig(duration=-1.0)

    def test_frozen(self) -> None:
        config = TimeoutConfig()
        with pytest.raises(AttributeError):
            config.duration = 10.0  # type: ignore[misc]


# ============================================================================
# SYNC TIMEOUT TESTS
# ============================================================================


class TestSyncTimeout:
    """Tests for synchronous timeout behavior."""

    def test_success_within_timeout(self) -> None:
        """Function completes before timeout."""

        @timeout(duration=1.0)
        def fast_function() -> str:
            return "success"

        result = fast_function()
        assert result == "success"

    def test_timeout_exceeded(self) -> None:
        """Function exceeds timeout duration."""

        @timeout(duration=0.1)
        def slow_function() -> str:
            time.sleep(1.0)
            return "never returned"

        with pytest.raises(TimeoutError) as exc_info:
            slow_function()

        assert exc_info.value.duration == 0.1
        assert exc_info.value.elapsed is not None
        assert exc_info.value.elapsed >= 0.1

    def test_preserves_function_exception(self) -> None:
        """Original function exceptions are preserved."""

        @timeout(duration=1.0)
        def failing_function() -> None:
            raise ValueError("original error")

        with pytest.raises(ValueError, match="original error"):
            failing_function()

    def test_preserves_return_value(self) -> None:
        """Return value is preserved correctly."""

        @timeout(duration=1.0)
        def returns_dict() -> dict[str, int]:
            return {"a": 1, "b": 2}

        result = returns_dict()
        assert result == {"a": 1, "b": 2}

    def test_preserves_function_metadata(self) -> None:
        """Decorated function preserves name and docstring."""

        @timeout(duration=1.0)
        def documented_function() -> None:
            """This is a docstring."""
            pass

        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "This is a docstring."

    def test_with_arguments(self) -> None:
        """Decorated function receives arguments correctly."""

        @timeout(duration=1.0)
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5
        assert add(a=10, b=20) == 30

    def test_timeout_error_attributes(self) -> None:
        """TimeoutError has correct attributes."""

        @timeout(duration=0.1, name="test-timeout")
        def slow() -> None:
            time.sleep(1.0)

        with pytest.raises(TimeoutError) as exc_info:
            slow()

        error = exc_info.value
        assert error.name == "test-timeout"
        assert error.duration == 0.1
        assert error.elapsed is not None


# ============================================================================
# ASYNC TIMEOUT TESTS
# ============================================================================


class TestAsyncTimeout:
    """Tests for asynchronous timeout behavior."""

    @pytest.mark.asyncio
    async def test_success_within_timeout(self) -> None:
        """Async function completes before timeout."""

        @timeout(duration=1.0)
        async def fast_async() -> str:
            await asyncio.sleep(0.01)
            return "success"

        result = await fast_async()
        assert result == "success"

    @pytest.mark.asyncio
    async def test_timeout_exceeded(self) -> None:
        """Async function exceeds timeout duration."""

        @timeout(duration=0.1)
        async def slow_async() -> str:
            await asyncio.sleep(1.0)
            return "never returned"

        with pytest.raises(TimeoutError) as exc_info:
            await slow_async()

        assert exc_info.value.duration == 0.1

    @pytest.mark.asyncio
    async def test_preserves_async_exception(self) -> None:
        """Original async exceptions are preserved."""

        @timeout(duration=1.0)
        async def failing_async() -> None:
            raise RuntimeError("async error")

        with pytest.raises(RuntimeError, match="async error"):
            await failing_async()

    @pytest.mark.asyncio
    async def test_with_arguments(self) -> None:
        """Async decorated function receives arguments correctly."""

        @timeout(duration=1.0)
        async def async_add(a: int, b: int) -> int:
            await asyncio.sleep(0.01)
            return a + b

        result = await async_add(5, 7)
        assert result == 12


# ============================================================================
# DECORATOR SYNTAX TESTS
# ============================================================================


class TestDecoratorSyntax:
    """Tests for different decorator syntaxes."""

    def test_decorator_without_parentheses(self) -> None:
        """@timeout without parentheses uses defaults."""

        @timeout
        def my_func() -> str:
            return "ok"

        assert my_func() == "ok"

    def test_decorator_with_empty_parentheses(self) -> None:
        """@timeout() uses defaults."""

        @timeout()
        def my_func() -> str:
            return "ok"

        assert my_func() == "ok"

    def test_decorator_with_duration_only(self) -> None:
        """@timeout(duration=X) sets duration."""

        @timeout(duration=5.0)
        def my_func() -> str:
            return "ok"

        assert my_func() == "ok"

    def test_decorator_with_name(self) -> None:
        """@timeout(name=X) sets name."""
        events: list[TimeoutEvent] = []

        t = Timeout(TimeoutConfig(duration=1.0), name="custom-name")
        t.on_event(events.append)

        @t
        def my_func() -> str:
            return "ok"

        my_func()
        assert len(events) == 1
        assert events[0].name == "custom-name"


# ============================================================================
# TIMEOUT CLASS DIRECT USAGE TESTS
# ============================================================================


class TestTimeoutClass:
    """Tests for using Timeout class directly."""

    def test_execute_sync(self) -> None:
        """Use execute() method directly."""
        t = Timeout(TimeoutConfig(duration=1.0))
        result = t.execute(lambda: "direct call")
        assert result == "direct call"

    def test_execute_sync_timeout(self) -> None:
        """execute() raises TimeoutError."""
        t = Timeout(TimeoutConfig(duration=0.1))

        with pytest.raises(TimeoutError):
            t.execute(lambda: time.sleep(1.0))

    @pytest.mark.asyncio
    async def test_execute_async(self) -> None:
        """Use execute_async() method directly."""
        t = Timeout(TimeoutConfig(duration=1.0))

        async def coro() -> str:
            return "async direct"

        result = await t.execute_async(coro())
        assert result == "async direct"

    def test_duration_property(self) -> None:
        """duration property returns configured value."""
        t = Timeout(TimeoutConfig(duration=42.0))
        assert t.duration == 42.0


# ============================================================================
# EVENT TESTS
# ============================================================================


class TestTimeoutEvents:
    """Tests for timeout event emission."""

    def test_success_event(self) -> None:
        """Success event is emitted on completion."""
        events: list[TimeoutEvent] = []

        t = Timeout(TimeoutConfig(duration=1.0), name="test")
        t.on_event(events.append)

        @t
        def my_func() -> str:
            return "ok"

        my_func()

        assert len(events) == 1
        assert events[0].event_type == TimeoutEventType.SUCCESS
        assert events[0].name == "test"
        assert events[0].duration_limit == 1.0
        assert events[0].elapsed < 1.0
        assert events[0].exception is None

    def test_timeout_event(self) -> None:
        """Timeout event is emitted on timeout."""
        events: list[TimeoutEvent] = []

        t = Timeout(TimeoutConfig(duration=0.1), name="test")
        t.on_event(events.append)

        @t
        def slow_func() -> None:
            time.sleep(1.0)

        with pytest.raises(TimeoutError):
            slow_func()

        assert len(events) == 1
        assert events[0].event_type == TimeoutEventType.TIMEOUT
        assert events[0].exception is not None
        assert isinstance(events[0].exception, TimeoutError)

    def test_error_event(self) -> None:
        """Error event is emitted on exception."""
        events: list[TimeoutEvent] = []

        t = Timeout(TimeoutConfig(duration=1.0), name="test")
        t.on_event(events.append)

        @t
        def failing_func() -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError):
            failing_func()

        assert len(events) == 1
        assert events[0].event_type == TimeoutEventType.ERROR
        assert events[0].exception is not None
        assert isinstance(events[0].exception, ValueError)

    def test_multiple_listeners(self) -> None:
        """Multiple event listeners all receive events."""
        events1: list[TimeoutEvent] = []
        events2: list[TimeoutEvent] = []

        t = Timeout(TimeoutConfig(duration=1.0))
        t.on_event(events1.append)
        t.on_event(events2.append)

        @t
        def my_func() -> str:
            return "ok"

        my_func()

        assert len(events1) == 1
        assert len(events2) == 1

    def test_listener_exception_does_not_break_flow(self) -> None:
        """Failing listener doesn't break the function."""

        def bad_listener(event: TimeoutEvent) -> None:
            raise RuntimeError("listener error")

        t = Timeout(TimeoutConfig(duration=1.0))
        t.on_event(bad_listener)

        @t
        def my_func() -> str:
            return "ok"

        # Should not raise despite listener error
        result = my_func()
        assert result == "ok"


# ============================================================================
# EDGE CASES
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_very_short_timeout(self) -> None:
        """Very short timeout works correctly."""

        @timeout(duration=0.001)
        def instant() -> str:
            return "fast"

        # This might or might not timeout depending on system load
        # Just verify it doesn't crash
        try:
            result = instant()
            assert result == "fast"
        except TimeoutError:
            pass  # Also acceptable

    def test_timeout_with_none_return(self) -> None:
        """Functions returning None work correctly."""

        @timeout(duration=1.0)
        def returns_none() -> None:
            pass

        result = returns_none()
        assert result is None

    def test_timeout_reusable(self) -> None:
        """Same timeout instance can be reused."""
        t = Timeout(TimeoutConfig(duration=1.0))

        @t
        def func1() -> int:
            return 1

        @t
        def func2() -> int:
            return 2

        assert func1() == 1
        assert func2() == 2

    def test_multiple_calls(self) -> None:
        """Decorated function can be called multiple times."""

        @timeout(duration=1.0)
        def counter() -> int:
            return 42

        for _ in range(10):
            assert counter() == 42

    @pytest.mark.asyncio
    async def test_concurrent_async_calls(self) -> None:
        """Multiple concurrent async calls work correctly."""

        @timeout(duration=1.0)
        async def async_task(n: int) -> int:
            await asyncio.sleep(0.01)
            return n * 2

        results = await asyncio.gather(
            async_task(1),
            async_task(2),
            async_task(3),
        )

        assert results == [2, 4, 6]


# ============================================================================
# TIMEOUT ERROR TESTS
# ============================================================================


class TestTimeoutErrorClass:
    """Tests for the TimeoutError exception class."""

    def test_basic_error(self) -> None:
        error = TimeoutError("Operation timed out")
        assert str(error) == "Operation timed out"
        assert error.name is None
        assert error.duration is None
        assert error.elapsed is None

    def test_error_with_attributes(self) -> None:
        error = TimeoutError(
            "Timed out",
            name="my-timeout",
            duration=5.0,
            elapsed=5.1,
        )
        assert error.name == "my-timeout"
        assert error.duration == 5.0
        assert error.elapsed == 5.1

    def test_error_str_with_name_and_duration(self) -> None:
        error = TimeoutError(
            "Operation failed",
            name="api-call",
            duration=10.0,
        )
        error_str = str(error)
        assert "api-call" in error_str
        assert "10.0s" in error_str
