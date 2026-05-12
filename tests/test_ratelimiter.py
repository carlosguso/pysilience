"""
Tests for the Rate Limiter pattern.

Run with: pytest tests/test_ratelimiter.py -v
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from pysilience.ratelimiter import (
    RateLimiter,
    RateLimiterConfig,
    RateLimiterEvent,
    RateLimiterEventType,
    RateLimitExceeded,
    rate_limiter,
)

# ============================================================================
# CONFIGURATION TESTS
# ============================================================================


class TestRateLimiterConfig:
    def test_default_values(self) -> None:
        config = RateLimiterConfig()
        assert config.limit_for_period == 50
        assert config.limit_refresh_period == 0.5
        assert config.timeout_duration == 5.0

    def test_custom_values(self) -> None:
        config = RateLimiterConfig(
            limit_for_period=10,
            limit_refresh_period=1.0,
            timeout_duration=2.0,
        )
        assert config.limit_for_period == 10
        assert config.limit_refresh_period == 1.0
        assert config.timeout_duration == 2.0

    def test_invalid_limit_for_period(self) -> None:
        with pytest.raises(ValueError, match="limit_for_period must be >= 1"):
            RateLimiterConfig(limit_for_period=0)

    def test_invalid_limit_refresh_period(self) -> None:
        with pytest.raises(ValueError, match="limit_refresh_period must be positive"):
            RateLimiterConfig(limit_refresh_period=0)

    def test_invalid_limit_refresh_period_negative(self) -> None:
        with pytest.raises(ValueError, match="limit_refresh_period must be positive"):
            RateLimiterConfig(limit_refresh_period=-1.0)

    def test_invalid_timeout_duration(self) -> None:
        with pytest.raises(ValueError, match="timeout_duration must be non-negative"):
            RateLimiterConfig(timeout_duration=-0.1)

    def test_frozen(self) -> None:
        config = RateLimiterConfig()
        with pytest.raises(AttributeError):
            config.limit_for_period = 5  # type: ignore[misc]


# ============================================================================
# SYNC RATE LIMITER TESTS
# ============================================================================


class TestSyncRateLimiter:
    def test_execute_success(self) -> None:
        rl = RateLimiter(RateLimiterConfig(limit_for_period=5), name="t")
        assert rl.execute(lambda: 42) == 42

    def test_decorator_success(self) -> None:
        @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0)
        def f(x: int) -> int:
            return x + 1

        assert f(1) == 2

    def test_permits_exhausted_rejects(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=2, limit_refresh_period=10.0, timeout_duration=0.0),
            name="exhaust",
        )
        assert rl.execute(lambda: "a") == "a"
        assert rl.execute(lambda: "b") == "b"

        with pytest.raises(RateLimitExceeded) as exc_info:
            rl.execute(lambda: "c")

        assert exc_info.value.name == "exhaust"
        assert exc_info.value.available_permits == 0

    def test_permits_refresh_after_period(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(
                limit_for_period=1,
                limit_refresh_period=0.1,
                timeout_duration=0.0,
            ),
        )
        assert rl.execute(lambda: "first") == "first"

        with pytest.raises(RateLimitExceeded):
            rl.execute(lambda: "blocked")

        time.sleep(0.15)
        assert rl.execute(lambda: "after-refresh") == "after-refresh"

    def test_waits_for_permit(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(
                limit_for_period=1,
                limit_refresh_period=0.1,
                timeout_duration=1.0,
            ),
        )
        rl.execute(lambda: None)
        start = time.monotonic()
        rl.execute(lambda: None)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.05

    def test_preserves_function_exception(self) -> None:
        @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0)
        def boom() -> None:
            raise ValueError("original error")

        with pytest.raises(ValueError, match="original error"):
            boom()

    def test_preserves_function_metadata(self) -> None:
        @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0)
        def documented() -> None:
            """This is a docstring."""

        assert documented.__name__ == "documented"
        assert documented.__doc__ == "This is a docstring."

    def test_with_arguments(self) -> None:
        @rate_limiter(limit_for_period=10, limit_refresh_period=1.0, timeout_duration=0.0)
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5
        assert add(a=10, b=20) == 30

    def test_available_permits_property(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=3, limit_refresh_period=10.0, timeout_duration=0.0),
        )
        assert rl.available_permits == 3
        rl.execute(lambda: None)
        assert rl.available_permits == 2
        rl.execute(lambda: None)
        assert rl.available_permits == 1


# ============================================================================
# ASYNC RATE LIMITER TESTS
# ============================================================================


class TestAsyncRateLimiter:
    @pytest.mark.asyncio
    async def test_execute_async_success(self) -> None:
        rl = RateLimiter(RateLimiterConfig(limit_for_period=5), name="a")

        async def factory() -> int:
            return 42

        assert await rl.execute_async(factory) == 42

    @pytest.mark.asyncio
    async def test_decorator_async(self) -> None:
        @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0)
        async def g() -> str:
            return "x"

        assert await g() == "x"

    @pytest.mark.asyncio
    async def test_async_permits_exhausted_rejects(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=1, limit_refresh_period=10.0, timeout_duration=0.0),
        )

        async def ok() -> str:
            return "ok"

        assert await rl.execute_async(ok) == "ok"

        with pytest.raises(RateLimitExceeded):
            await rl.execute_async(ok)

    @pytest.mark.asyncio
    async def test_async_waits_for_permit(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(
                limit_for_period=1,
                limit_refresh_period=0.1,
                timeout_duration=1.0,
            ),
        )

        async def op() -> str:
            return "done"

        await rl.execute_async(op)
        start = time.monotonic()
        await rl.execute_async(op)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.05

    @pytest.mark.asyncio
    async def test_async_preserves_exception(self) -> None:
        @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0)
        async def failing() -> None:
            raise RuntimeError("async error")

        with pytest.raises(RuntimeError, match="async error"):
            await failing()

    @pytest.mark.asyncio
    async def test_async_with_arguments(self) -> None:
        @rate_limiter(limit_for_period=10, limit_refresh_period=1.0, timeout_duration=0.0)
        async def add(a: int, b: int) -> int:
            await asyncio.sleep(0.001)
            return a + b

        assert await add(5, 7) == 12


# ============================================================================
# DECORATOR SYNTAX TESTS
# ============================================================================


class TestDecoratorSyntax:
    def test_bare_decorator(self) -> None:
        @rate_limiter
        def f() -> int:
            return 3

        assert f() == 3

    def test_decorator_with_empty_parentheses(self) -> None:
        @rate_limiter()
        def f() -> str:
            return "ok"

        assert f() == "ok"

    def test_decorator_with_params(self) -> None:
        @rate_limiter(limit_for_period=10, limit_refresh_period=1.0, timeout_duration=0.0)
        def f() -> str:
            return "ok"

        assert f() == "ok"


# ============================================================================
# RATE LIMITER CLASS DIRECT USAGE TESTS
# ============================================================================


class TestRateLimiterClass:
    def test_execute_sync(self) -> None:
        rl = RateLimiter(RateLimiterConfig(limit_for_period=5))
        result = rl.execute(lambda: "direct")
        assert result == "direct"

    def test_acquire_returns_wait_time(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=1, limit_refresh_period=0.1, timeout_duration=1.0),
        )
        wait = rl.acquire()
        assert wait >= 0.0
        assert wait < 0.05

    def test_default_name(self) -> None:
        rl = RateLimiter()
        assert rl.name == "ratelimiter"

    def test_custom_name(self) -> None:
        rl = RateLimiter(name="api-limiter")
        assert rl.name == "api-limiter"


# ============================================================================
# EVENT TESTS
# ============================================================================


class TestRateLimiterEvents:
    def test_success_event(self) -> None:
        events: list[RateLimiterEvent] = []
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0),
            name="ev",
        )
        rl.on_event(events.append)
        rl.execute(lambda: None)

        assert len(events) == 1
        assert events[0].event_type == RateLimiterEventType.SUCCESS
        assert events[0].name == "ev"

    def test_rejected_event(self) -> None:
        events: list[RateLimiterEvent] = []
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=1, limit_refresh_period=10.0, timeout_duration=0.0),
            name="rej",
        )
        rl.on_event(events.append)
        rl.execute(lambda: None)

        with pytest.raises(RateLimitExceeded):
            rl.execute(lambda: None)

        rejected = [e for e in events if e.event_type == RateLimiterEventType.REJECTED]
        assert len(rejected) == 1
        assert rejected[0].wait_time > 0.0

    @pytest.mark.asyncio
    async def test_rejected_event_async(self) -> None:
        events: list[RateLimiterEvent] = []
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=1, limit_refresh_period=10.0, timeout_duration=0.0),
            name="rej-async",
        )
        rl.on_event(events.append)

        async def op() -> None:
            pass

        await rl.execute_async(op)

        with pytest.raises(RateLimitExceeded):
            await rl.execute_async(op)

        rejected = [e for e in events if e.event_type == RateLimiterEventType.REJECTED]
        assert len(rejected) == 1
        assert rejected[0].wait_time > 0.0

    def test_error_event(self) -> None:
        events: list[RateLimiterEvent] = []
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0),
            name="err",
        )
        rl.on_event(events.append)

        with pytest.raises(ValueError):
            rl.execute(lambda: (_ for _ in ()).throw(ValueError("x")))

        assert any(e.event_type == RateLimiterEventType.ERROR for e in events)

    def test_listener_exception_does_not_break_flow(self) -> None:
        def bad_listener(event: RateLimiterEvent) -> None:
            raise RuntimeError("listener error")

        rl = RateLimiter(RateLimiterConfig(limit_for_period=5, limit_refresh_period=1.0))
        rl.on_event(bad_listener)
        result = rl.execute(lambda: "ok")
        assert result == "ok"

    def test_multiple_listeners(self) -> None:
        events1: list[RateLimiterEvent] = []
        events2: list[RateLimiterEvent] = []
        rl = RateLimiter(RateLimiterConfig(limit_for_period=5, limit_refresh_period=1.0))
        rl.on_event(events1.append)
        rl.on_event(events2.append)
        rl.execute(lambda: None)

        assert len(events1) == 1
        assert len(events2) == 1


# ============================================================================
# EXCEPTION TESTS
# ============================================================================


class TestRateLimitExceeded:
    def test_basic_error(self) -> None:
        error = RateLimitExceeded("Rate limit exceeded")
        assert str(error) == "Rate limit exceeded"
        assert error.name is None
        assert error.available_permits is None

    def test_error_with_attributes(self) -> None:
        error = RateLimitExceeded(
            "Rate limit exceeded",
            name="api",
            available_permits=0,
            wait_time=0.5,
        )
        assert error.name == "api"
        assert error.available_permits == 0
        assert error.wait_time == 0.5

    def test_error_str_with_name_and_wait_time(self) -> None:
        error = RateLimitExceeded(
            "Rate limit exceeded",
            name="api-limiter",
            wait_time=1.5,
        )
        error_str = str(error)
        assert "api-limiter" in error_str
        assert "1.50s" in error_str

    def test_error_str_with_name_only(self) -> None:
        error = RateLimitExceeded(
            "Rate limit exceeded",
            name="api",
        )
        error_str = str(error)
        assert "[api]" in error_str


# ============================================================================
# EDGE CASES
# ============================================================================


class TestEdgeCases:
    def test_multiple_calls_within_limit(self) -> None:
        @rate_limiter(limit_for_period=10, limit_refresh_period=1.0, timeout_duration=0.0)
        def counter() -> int:
            return 42

        for _ in range(10):
            assert counter() == 42

    def test_rate_limiter_reusable(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(limit_for_period=10, limit_refresh_period=1.0, timeout_duration=0.0),
        )

        @rl
        def func1() -> int:
            return 1

        @rl
        def func2() -> int:
            return 2

        assert func1() == 1
        assert func2() == 2

    def test_concurrent_sync_calls(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(
                limit_for_period=5,
                limit_refresh_period=1.0,
                timeout_duration=0.0,
            ),
            name="concurrent",
        )
        results: list[int] = []
        errors: list[BaseException] = []

        def call(n: int) -> None:
            try:
                results.append(rl.execute(lambda: n))
            except BaseException as e:
                errors.append(e)

        threads = [threading.Thread(target=call, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=2.0)

        assert len(results) == 5
        assert len(errors) == 0

    @pytest.mark.asyncio
    async def test_concurrent_async_calls(self) -> None:
        rl = RateLimiter(
            RateLimiterConfig(
                limit_for_period=5,
                limit_refresh_period=1.0,
                timeout_duration=0.0,
            ),
        )

        async def task(n: int) -> int:
            return n * 2

        results = await asyncio.gather(
            rl.execute_async(lambda: task(1)),
            rl.execute_async(lambda: task(2)),
            rl.execute_async(lambda: task(3)),
        )
        assert results == [2, 4, 6]

    def test_none_return(self) -> None:
        @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=0.0)
        def returns_none() -> None:
            pass

        assert returns_none() is None


# ============================================================================
# REGISTRY TESTS
# ============================================================================


class TestCreateRateLimiter:
    def test_create_and_register(self) -> None:
        from pysilience.core.registry import clear, get

        clear()
        from pysilience.ratelimiter import create_rate_limiter

        rl = create_rate_limiter(name="api")
        assert get("ratelimiter", "api") is rl
        clear()

    def test_create_without_register(self) -> None:
        from pysilience.core.registry import clear, get

        clear()
        from pysilience.ratelimiter import create_rate_limiter

        rl = create_rate_limiter(name="no-reg", register=False)
        assert get("ratelimiter", "no-reg") is None
        assert rl is not None
        clear()
