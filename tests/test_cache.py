"""
Tests for the Cache pattern.

Run with: pytest tests/test_cache.py -v
"""

from __future__ import annotations

import asyncio
import time

import pytest

from pysilience.cache import (
    Cache,
    CacheConfig,
    CacheEvent,
    CacheEventType,
    cache,
    create_cache,
)

# ============================================================================
# CONFIGURATION TESTS
# ============================================================================


class TestCacheConfig:
    def test_default_values(self) -> None:
        config = CacheConfig()
        assert config.max_size == 128
        assert config.ttl is None

    def test_custom_values(self) -> None:
        config = CacheConfig(max_size=64, ttl=30.0)
        assert config.max_size == 64
        assert config.ttl == 30.0

    def test_invalid_max_size_zero(self) -> None:
        with pytest.raises(ValueError, match="max_size must be >= 1"):
            CacheConfig(max_size=0)

    def test_invalid_max_size_negative(self) -> None:
        with pytest.raises(ValueError, match="max_size must be >= 1"):
            CacheConfig(max_size=-1)

    def test_invalid_ttl_zero(self) -> None:
        with pytest.raises(ValueError, match="ttl must be positive"):
            CacheConfig(ttl=0)

    def test_invalid_ttl_negative(self) -> None:
        with pytest.raises(ValueError, match="ttl must be positive"):
            CacheConfig(ttl=-1.0)

    def test_frozen(self) -> None:
        config = CacheConfig()
        with pytest.raises(AttributeError):
            config.max_size = 10  # type: ignore[misc]


# ============================================================================
# SYNC CACHE TESTS
# ============================================================================


class TestSyncCache:
    def test_miss_then_hit(self) -> None:
        """First call is a miss (runs function), second is a hit (cached)."""
        call_count = 0

        @cache(max_size=10, ttl=60.0)
        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        assert compute(5) == 10
        assert call_count == 1
        assert compute(5) == 10
        assert call_count == 1

    def test_different_args_are_cached_separately(self) -> None:
        call_count = 0

        @cache(max_size=10)
        def square(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * x

        assert square(2) == 4
        assert square(3) == 9
        assert call_count == 2
        assert square(2) == 4
        assert call_count == 2

    def test_ttl_expiry(self) -> None:
        call_count = 0

        @cache(max_size=10, ttl=0.05)
        def fetch(key: str) -> str:
            nonlocal call_count
            call_count += 1
            return f"value-{key}"

        assert fetch("a") == "value-a"
        assert call_count == 1
        assert fetch("a") == "value-a"
        assert call_count == 1
        time.sleep(0.1)
        assert fetch("a") == "value-a"
        assert call_count == 2

    def test_lru_eviction(self) -> None:
        c = Cache(CacheConfig(max_size=2), name="lru")
        c.execute("a", lambda: 1)
        c.execute("b", lambda: 2)
        assert c.size == 2
        c.execute("c", lambda: 3)
        assert c.size == 2
        # "a" was LRU and should have been evicted
        call_count = 0

        def fetch_a() -> int:
            nonlocal call_count
            call_count += 1
            return 10

        c.execute("a", fetch_a)
        assert call_count == 1

    def test_lru_order_updated_on_hit(self) -> None:
        c = Cache(CacheConfig(max_size=2), name="lru")
        c.execute("a", lambda: 1)
        c.execute("b", lambda: 2)
        # Access "a" to promote it
        c.execute("a", lambda: 99)
        # Now "b" is the LRU; inserting "c" should evict "b", not "a"
        c.execute("c", lambda: 3)
        call_count = 0

        def fetch_b() -> int:
            nonlocal call_count
            call_count += 1
            return 20

        c.execute("b", fetch_b)
        assert call_count == 1  # "b" was evicted, had to call function

    def test_preserves_function_exception(self) -> None:
        @cache(max_size=10)
        def failing(x: int) -> int:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            failing(1)

    def test_error_does_not_cache(self) -> None:
        """Failed calls should not leave entries in the cache."""
        call_count = 0

        @cache(max_size=10)
        def flaky(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("first call fails")
            return x

        with pytest.raises(RuntimeError):
            flaky(1)
        assert flaky(1) == 1
        assert call_count == 2

    def test_preserves_return_value(self) -> None:
        @cache(max_size=10)
        def returns_dict() -> dict[str, int]:
            return {"a": 1, "b": 2}

        result = returns_dict()
        assert result == {"a": 1, "b": 2}

    def test_preserves_function_metadata(self) -> None:
        @cache(max_size=10)
        def documented_function() -> None:
            """This is a docstring."""

        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "This is a docstring."

    def test_with_kwargs(self) -> None:
        call_count = 0

        @cache(max_size=10)
        def add(a: int, b: int) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        assert add(1, b=2) == 3
        assert call_count == 1
        assert add(1, b=2) == 3
        assert call_count == 1

    def test_kwargs_order_independent(self) -> None:
        call_count = 0

        @cache(max_size=10)
        def add(a: int = 0, b: int = 0) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        assert add(a=1, b=2) == 3
        assert add(b=2, a=1) == 3
        assert call_count == 1


# ============================================================================
# ASYNC CACHE TESTS
# ============================================================================


class TestAsyncCache:
    @pytest.mark.asyncio
    async def test_miss_then_hit(self) -> None:
        call_count = 0

        @cache(max_size=10, ttl=60.0)
        async def fetch(key: str) -> str:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.01)
            return f"val-{key}"

        assert await fetch("x") == "val-x"
        assert call_count == 1
        assert await fetch("x") == "val-x"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_async_error_propagates(self) -> None:
        @cache(max_size=10)
        async def bad() -> None:
            raise RuntimeError("async error")

        with pytest.raises(RuntimeError, match="async error"):
            await bad()

    @pytest.mark.asyncio
    async def test_async_ttl_expiry(self) -> None:
        call_count = 0

        @cache(max_size=10, ttl=0.05)
        async def fetch(key: str) -> str:
            nonlocal call_count
            call_count += 1
            return f"v-{key}"

        assert await fetch("a") == "v-a"
        assert call_count == 1
        await asyncio.sleep(0.1)
        assert await fetch("a") == "v-a"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_async_with_arguments(self) -> None:
        @cache(max_size=10)
        async def multiply(a: int, b: int) -> int:
            await asyncio.sleep(0.01)
            return a * b

        assert await multiply(3, 4) == 12

    @pytest.mark.asyncio
    async def test_concurrent_async_calls_different_keys(self) -> None:
        call_count = 0

        @cache(max_size=10)
        async def fetch(n: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.01)
            return n * 2

        results = await asyncio.gather(fetch(1), fetch(2), fetch(3))
        assert results == [2, 4, 6]
        assert call_count == 3


# ============================================================================
# DECORATOR SYNTAX TESTS
# ============================================================================


class TestDecoratorSyntax:
    def test_decorator_without_parentheses(self) -> None:
        @cache
        def f() -> str:
            return "ok"

        assert f() == "ok"

    def test_decorator_with_empty_parentheses(self) -> None:
        @cache()
        def f() -> str:
            return "ok"

        assert f() == "ok"

    def test_decorator_with_params(self) -> None:
        @cache(max_size=64, ttl=10.0)
        def f() -> str:
            return "ok"

        assert f() == "ok"

    def test_decorator_with_name(self) -> None:
        events: list[CacheEvent] = []
        c = Cache(CacheConfig(max_size=10), name="custom-name")
        c.on_event(events.append)

        @c
        def f() -> str:
            return "ok"

        f()
        assert len(events) == 1
        assert events[0].name == "custom-name"


# ============================================================================
# CACHE CLASS DIRECT USAGE TESTS
# ============================================================================


class TestCacheClass:
    def test_execute_miss_then_hit(self) -> None:
        c = Cache(CacheConfig(max_size=10), name="t")
        call_count = 0

        def fetch() -> str:
            nonlocal call_count
            call_count += 1
            return "result"

        assert c.execute("k1", fetch) == "result"
        assert call_count == 1
        assert c.execute("k1", fetch) == "result"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_execute_async(self) -> None:
        c = Cache(CacheConfig(max_size=10))

        async def fetch() -> str:
            return "async-result"

        assert await c.execute_async("k1", fetch) == "async-result"
        assert await c.execute_async("k1", fetch) == "async-result"

    def test_invalidate_existing_key(self) -> None:
        c = Cache(CacheConfig(max_size=10))
        c.execute("k", lambda: 42)
        assert c.size == 1
        assert c.invalidate("k") is True
        assert c.size == 0

    def test_invalidate_missing_key(self) -> None:
        c = Cache(CacheConfig(max_size=10))
        assert c.invalidate("missing") is False

    def test_invalidate_all(self) -> None:
        c = Cache(CacheConfig(max_size=10))
        c.execute("a", lambda: 1)
        c.execute("b", lambda: 2)
        c.execute("c", lambda: 3)
        assert c.size == 3
        c.invalidate_all()
        assert c.size == 0

    def test_size_property(self) -> None:
        c = Cache(CacheConfig(max_size=10))
        assert c.size == 0
        c.execute("a", lambda: 1)
        assert c.size == 1
        c.execute("b", lambda: 2)
        assert c.size == 2

    def test_default_config(self) -> None:
        c = Cache()
        assert c.config.max_size == 128
        assert c.config.ttl is None

    def test_default_name(self) -> None:
        c = Cache()
        assert c.name == "cache"


# ============================================================================
# EVENT TESTS
# ============================================================================


class TestCacheEvents:
    def test_miss_event(self) -> None:
        events: list[CacheEvent] = []
        c = Cache(CacheConfig(max_size=10), name="ev")
        c.on_event(events.append)

        c.execute("k", lambda: "v")

        assert len(events) == 1
        assert events[0].event_type == CacheEventType.MISS
        assert events[0].name == "ev"
        assert events[0].key == "k"
        assert events[0].exception is None

    def test_hit_event(self) -> None:
        events: list[CacheEvent] = []
        c = Cache(CacheConfig(max_size=10), name="ev")
        c.on_event(events.append)

        c.execute("k", lambda: "v")
        c.execute("k", lambda: "v")

        assert len(events) == 2
        assert events[0].event_type == CacheEventType.MISS
        assert events[1].event_type == CacheEventType.HIT
        assert events[1].key == "k"

    def test_error_event(self) -> None:
        events: list[CacheEvent] = []
        c = Cache(CacheConfig(max_size=10), name="ev")
        c.on_event(events.append)

        with pytest.raises(ValueError):
            c.execute("k", _raise_value_error)

        assert len(events) == 1
        assert events[0].event_type == CacheEventType.ERROR
        assert events[0].exception is not None
        assert isinstance(events[0].exception, ValueError)

    def test_multiple_listeners(self) -> None:
        events1: list[CacheEvent] = []
        events2: list[CacheEvent] = []
        c = Cache(CacheConfig(max_size=10))
        c.on_event(events1.append)
        c.on_event(events2.append)

        c.execute("k", lambda: 1)

        assert len(events1) == 1
        assert len(events2) == 1

    def test_listener_exception_does_not_break_flow(self) -> None:
        def bad_listener(event: CacheEvent) -> None:
            raise RuntimeError("listener error")

        c = Cache(CacheConfig(max_size=10))
        c.on_event(bad_listener)

        result = c.execute("k", lambda: "ok")
        assert result == "ok"


# ============================================================================
# FACTORY + REGISTRY TESTS
# ============================================================================


class TestCreateCache:
    def test_create_cache_registers(self) -> None:
        from pysilience.core.registry import clear, get

        clear()
        c = create_cache(CacheConfig(max_size=10), name="my-cache")
        assert get("cache", "my-cache") is c
        clear()

    def test_create_cache_no_register(self) -> None:
        from pysilience.core.registry import clear, get

        clear()
        create_cache(CacheConfig(max_size=10), name="unreg", register=False)
        assert get("cache", "unreg") is None
        clear()


# ============================================================================
# EDGE CASES
# ============================================================================


class TestEdgeCases:
    def test_none_return_value(self) -> None:
        call_count = 0

        @cache(max_size=10)
        def returns_none() -> None:
            nonlocal call_count
            call_count += 1

        returns_none()
        assert call_count == 1
        returns_none()
        assert call_count == 1

    def test_reusable_instance(self) -> None:
        """Same Cache instance can decorate multiple functions (shared key space)."""
        c = Cache(CacheConfig(max_size=10))

        @c
        def f1(x: int) -> int:
            return x + 1

        @c
        def f2(x: int) -> int:
            return x + 2

        assert f1(1) == 2
        assert f2(2) == 4

    def test_multiple_calls(self) -> None:
        call_count = 0

        @cache(max_size=10)
        def counter(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x

        for _ in range(10):
            assert counter(42) == 42
        assert call_count == 1

    def test_max_size_one(self) -> None:
        c = Cache(CacheConfig(max_size=1))
        c.execute("a", lambda: 1)
        assert c.size == 1
        c.execute("b", lambda: 2)
        assert c.size == 1
        # "a" should be evicted
        call_count = 0

        def fetch_a() -> int:
            nonlocal call_count
            call_count += 1
            return 10

        c.execute("a", fetch_a)
        assert call_count == 1

    def test_no_ttl_entries_never_expire(self) -> None:
        c = Cache(CacheConfig(max_size=10, ttl=None))
        c.execute("k", lambda: "v")
        time.sleep(0.05)
        call_count = 0

        def fetch() -> str:
            nonlocal call_count
            call_count += 1
            return "new"

        assert c.execute("k", fetch) == "v"
        assert call_count == 0

    def test_invalidate_then_miss(self) -> None:
        c = Cache(CacheConfig(max_size=10))
        c.execute("k", lambda: "first")
        c.invalidate("k")
        call_count = 0

        def fetch() -> str:
            nonlocal call_count
            call_count += 1
            return "second"

        assert c.execute("k", fetch) == "second"
        assert call_count == 1


class TestConcurrency:
    """Tests for concurrent access to the same cache key."""

    def test_sync_same_key_computes_once(self) -> None:
        """Multiple threads requesting the same key should compute only once."""
        import threading

        c = Cache(CacheConfig(max_size=10))
        compute_count = 0
        count_lock = threading.Lock()
        barrier = threading.Barrier(5)

        def expensive() -> int:
            nonlocal compute_count
            with count_lock:
                compute_count += 1
            time.sleep(0.05)
            return 42

        results: list[int] = []
        results_lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            result = c.execute("shared", expensive)
            with results_lock:
                results.append(result)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert len(results) == 5
        assert all(r == 42 for r in results)
        assert compute_count == 1, f"Expected 1 computation, got {compute_count}"

    @pytest.mark.asyncio
    async def test_async_same_key_computes_once(self) -> None:
        """Multiple coroutines requesting the same key should compute only once."""
        c = Cache(CacheConfig(max_size=10))
        compute_count = 0

        async def expensive() -> int:
            nonlocal compute_count
            compute_count += 1
            await asyncio.sleep(0.05)
            return 42

        results = await asyncio.gather(
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
        )

        assert all(r == 42 for r in results)
        assert compute_count == 1, f"Expected 1 computation, got {compute_count}"

    def test_sync_error_lets_next_caller_retry(self) -> None:
        """When the computing thread fails, a waiting thread should retry."""
        import threading

        c = Cache(CacheConfig(max_size=10))
        call_count = 0
        count_lock = threading.Lock()
        started = threading.Event()

        def flaky() -> int:
            nonlocal call_count
            with count_lock:
                call_count += 1
                n = call_count
            if n == 1:
                started.set()
                time.sleep(0.05)
                raise RuntimeError("first fails")
            return 99

        results: list[int | BaseException] = []
        results_lock = threading.Lock()

        def worker() -> None:
            try:
                r = c.execute("k", flaky)
                with results_lock:
                    results.append(r)
            except Exception as e:
                with results_lock:
                    results.append(e)

        t1 = threading.Thread(target=worker)
        t1.start()
        started.wait(timeout=2.0)
        t2 = threading.Thread(target=worker)
        t2.start()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)

        errors = [r for r in results if isinstance(r, BaseException)]
        successes = [r for r in results if not isinstance(r, BaseException)]
        assert len(errors) == 1
        assert len(successes) == 1
        assert successes[0] == 99

    @pytest.mark.asyncio
    async def test_async_error_lets_next_caller_retry(self) -> None:
        """When the computing coroutine fails, a waiting coroutine should retry."""
        c = Cache(CacheConfig(max_size=10))
        call_count = 0

        async def flaky() -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                await asyncio.sleep(0.03)
                raise RuntimeError("first fails")
            return 99

        task1 = asyncio.create_task(c.execute_async("k", flaky))
        await asyncio.sleep(0.01)
        task2 = asyncio.create_task(c.execute_async("k", flaky))

        results = await asyncio.gather(task1, task2, return_exceptions=True)

        assert isinstance(results[0], RuntimeError)
        assert results[1] == 99
        assert call_count == 2


def _raise_value_error() -> None:
    raise ValueError("test error")
