"""
Tests for the Fallback pattern.

Run with: pytest tests/test_fallback.py -v
"""

from __future__ import annotations

import pytest

from pysilience.fallback import (
    Fallback,
    FallbackConfig,
    FallbackEvent,
    FallbackEventType,
    create_fallback,
    fallback,
)


class TestFallbackConfig:
    """Tests for FallbackConfig dataclass."""

    def test_default_values(self) -> None:
        config = FallbackConfig()
        assert config.fallback_on == (Exception,)
        assert config.raise_on == ()

    def test_custom_values(self) -> None:
        config = FallbackConfig(
            fallback_on=(IOError, ValueError),
            raise_on=(KeyboardInterrupt,),
        )
        assert config.fallback_on == (IOError, ValueError)
        assert config.raise_on == (KeyboardInterrupt,)

    def test_empty_fallback_on_raises(self) -> None:
        with pytest.raises(ValueError, match="fallback_on must contain at least one"):
            FallbackConfig(fallback_on=())

    def test_frozen(self) -> None:
        config = FallbackConfig()
        with pytest.raises(AttributeError):
            config.fallback_on = (RuntimeError,)  # type: ignore[misc]


class TestSyncFallback:
    """Tests for synchronous fallback behavior."""

    def test_success_no_fallback(self) -> None:
        @fallback(action=lambda exc: "fallback")
        def ok() -> str:
            return "primary"

        assert ok() == "primary"

    def test_fallback_invoked_on_failure(self) -> None:
        @fallback(action=lambda exc: "recovered")
        def failing() -> str:
            raise RuntimeError("boom")

        assert failing() == "recovered"

    def test_fallback_receives_exception(self) -> None:
        received: list[BaseException] = []

        def capture(exc: BaseException) -> str:
            received.append(exc)
            return "caught"

        @fallback(action=capture)
        def failing() -> str:
            raise ValueError("specific error")

        result = failing()
        assert result == "caught"
        assert len(received) == 1
        assert isinstance(received[0], ValueError)
        assert str(received[0]) == "specific error"

    def test_fallback_on_specific_exceptions(self) -> None:
        @fallback(action=lambda exc: -1, fallback_on=(ValueError,))
        def parse(text: str) -> int:
            return int(text)

        assert parse("42") == 42
        assert parse("bad") == -1

    def test_non_matching_exception_propagates(self) -> None:
        @fallback(action=lambda exc: "safe", fallback_on=(IOError,))
        def failing() -> str:
            raise ValueError("not an IOError")

        with pytest.raises(ValueError, match="not an IOError"):
            failing()

    def test_raise_on_bypasses_fallback(self) -> None:
        @fallback(action=lambda exc: "safe", raise_on=(KeyError,))
        def failing() -> str:
            raise KeyError("critical")

        with pytest.raises(KeyError):
            failing()

    def test_raise_on_checked_before_fallback_on(self) -> None:
        @fallback(
            action=lambda exc: "safe",
            fallback_on=(Exception,),
            raise_on=(ValueError,),
        )
        def failing() -> str:
            raise ValueError("should propagate")

        with pytest.raises(ValueError, match="should propagate"):
            failing()

    def test_preserves_function_metadata(self) -> None:
        @fallback(action=lambda exc: None)
        def documented() -> None:
            """Docstring."""

        assert documented.__name__ == "documented"
        assert documented.__doc__ == "Docstring."

    def test_with_arguments(self) -> None:
        @fallback(action=lambda exc: 0)
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5
        assert add(a=10, b=20) == 30

    def test_fallback_action_failure_propagates(self) -> None:
        def bad_fallback(exc: BaseException) -> str:
            raise TypeError("fallback broken")

        @fallback(action=bad_fallback)
        def failing() -> str:
            raise RuntimeError("primary error")

        with pytest.raises(TypeError, match="fallback broken"):
            failing()


class TestAsyncFallback:
    """Tests for asynchronous fallback behavior."""

    @pytest.mark.asyncio
    async def test_success_no_fallback(self) -> None:
        @fallback(action=lambda exc: "fallback")
        async def ok() -> str:
            return "primary"

        assert await ok() == "primary"

    @pytest.mark.asyncio
    async def test_fallback_invoked_on_failure(self) -> None:
        @fallback(action=lambda exc: "recovered")
        async def failing() -> str:
            raise RuntimeError("boom")

        assert await failing() == "recovered"

    @pytest.mark.asyncio
    async def test_fallback_on_specific_exceptions(self) -> None:
        @fallback(action=lambda exc: None, fallback_on=(IOError,))
        async def failing() -> str | None:
            raise OSError("network")

        assert await failing() is None

    @pytest.mark.asyncio
    async def test_non_matching_exception_propagates(self) -> None:
        @fallback(action=lambda exc: "safe", fallback_on=(IOError,))
        async def failing() -> str:
            raise ValueError("not caught")

        with pytest.raises(ValueError, match="not caught"):
            await failing()

    @pytest.mark.asyncio
    async def test_with_arguments(self) -> None:
        @fallback(action=lambda exc: -1)
        async def divide(a: int, b: int) -> int:
            return a // b

        assert await divide(10, 2) == 5
        assert await divide(10, 0) == -1


class TestFallbackClass:
    """Tests for using Fallback class directly."""

    def test_execute_success(self) -> None:
        fb = Fallback(action=lambda exc: "fb", name="test")
        result = fb.execute(lambda: "primary")
        assert result == "primary"

    def test_execute_fallback(self) -> None:
        fb = Fallback(action=lambda exc: "recovered", name="test")

        def fail() -> str:
            raise RuntimeError("oops")

        result = fb.execute(fail)
        assert result == "recovered"

    @pytest.mark.asyncio
    async def test_execute_async_success(self) -> None:
        fb: Fallback[..., str] = Fallback(action=lambda exc: "fb", name="test")

        async def ok() -> str:
            return "async primary"

        result = await fb.execute_async(ok)
        assert result == "async primary"

    @pytest.mark.asyncio
    async def test_execute_async_fallback(self) -> None:
        fb: Fallback[..., str] = Fallback(action=lambda exc: "async recovered", name="test")

        async def fail() -> str:
            raise OSError("network down")

        result = await fb.execute_async(fail)
        assert result == "async recovered"


class TestFallbackEvents:
    """Tests for fallback event emission."""

    def test_success_event(self) -> None:
        events: list[FallbackEvent] = []
        fb = Fallback(action=lambda exc: None, name="test")
        fb.on_event(events.append)

        fb.execute(lambda: "ok")

        assert len(events) == 1
        assert events[0].event_type == FallbackEventType.SUCCESS
        assert events[0].name == "test"
        assert events[0].exception is None
        assert events[0].fallback_exception is None

    def test_recovered_event(self) -> None:
        events: list[FallbackEvent] = []
        fb = Fallback(action=lambda exc: "safe", name="test")
        fb.on_event(events.append)

        def fail() -> str:
            raise ValueError("boom")

        fb.execute(fail)

        assert len(events) == 1
        assert events[0].event_type == FallbackEventType.RECOVERED
        assert events[0].name == "test"
        assert isinstance(events[0].exception, ValueError)
        assert events[0].fallback_exception is None

    def test_fallback_error_event(self) -> None:
        events: list[FallbackEvent] = []

        def bad_action(exc: BaseException) -> str:
            raise TypeError("fallback failed")

        fb = Fallback(action=bad_action, name="test")
        fb.on_event(events.append)

        def fail() -> str:
            raise RuntimeError("primary")

        with pytest.raises(TypeError, match="fallback failed"):
            fb.execute(fail)

        assert len(events) == 1
        assert events[0].event_type == FallbackEventType.FALLBACK_ERROR
        assert isinstance(events[0].exception, RuntimeError)
        assert isinstance(events[0].fallback_exception, TypeError)

    def test_multiple_listeners(self) -> None:
        events1: list[FallbackEvent] = []
        events2: list[FallbackEvent] = []
        fb = Fallback(action=lambda exc: None, name="test")
        fb.on_event(events1.append)
        fb.on_event(events2.append)

        fb.execute(lambda: "ok")

        assert len(events1) == 1
        assert len(events2) == 1

    def test_listener_exception_does_not_break_flow(self) -> None:
        def bad_listener(event: FallbackEvent) -> None:
            raise RuntimeError("listener error")

        fb = Fallback(action=lambda exc: "safe", name="test")
        fb.on_event(bad_listener)

        result = fb.execute(lambda: "ok")
        assert result == "ok"


class TestCreateFallback:
    """Tests for the create_fallback factory function."""

    def test_creates_and_registers(self) -> None:
        from pysilience.core.registry import clear, get

        clear()
        fb = create_fallback(action=lambda exc: None, name="my-fb")
        assert isinstance(fb, Fallback)
        assert fb.name == "my-fb"
        assert get("fallback", "my-fb") is fb
        clear()

    def test_creates_without_register(self) -> None:
        from pysilience.core.registry import clear, get

        clear()
        fb = create_fallback(action=lambda exc: None, name="no-reg", register=False)
        assert isinstance(fb, Fallback)
        assert get("fallback", "no-reg") is None
        clear()
