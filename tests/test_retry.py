"""
Tests for the Retry pattern.

Run with: pytest tests/test_retry.py -v
"""

from __future__ import annotations

import sys

import pytest

from pysilience.retry import (
    RetriesExhausted,
    Retry,
    RetryConfig,
    RetryEvent,
    RetryEventType,
    retry,
)


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""

    def test_default_values(self) -> None:
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.initial_interval == 0.0
        assert config.multiplier == 2.0
        assert config.max_interval is None
        assert config.jitter is False
        assert config.retry_on == (Exception,)
        assert config.abort_on == ()

    def test_invalid_max_attempts(self) -> None:
        with pytest.raises(ValueError, match="max_attempts must be >= 1"):
            RetryConfig(max_attempts=0)

    def test_invalid_initial_interval(self) -> None:
        with pytest.raises(ValueError, match="initial_interval must be non-negative"):
            RetryConfig(initial_interval=-0.1)

    def test_frozen(self) -> None:
        config = RetryConfig()
        with pytest.raises(AttributeError):
            config.max_attempts = 5  # type: ignore[misc]


class TestSyncRetry:
    """Tests for synchronous retry behavior."""

    def test_success_first_attempt(self) -> None:
        @retry(max_attempts=3)
        def ok() -> str:
            return "ok"

        assert ok() == "ok"

    def test_retries_then_success(self) -> None:
        count = {"n": 0}

        @retry(max_attempts=5, initial_interval=0.0)
        def flaky() -> str:
            count["n"] += 1
            if count["n"] < 3:
                raise RuntimeError("fail")
            return "done"

        assert flaky() == "done"
        assert count["n"] == 3

    def test_exhausted_raises(self) -> None:
        @retry(max_attempts=2, initial_interval=0.0)
        def always_fail() -> None:
            raise ValueError("nope")

        with pytest.raises(RetriesExhausted) as ei:
            always_fail()
        assert ei.value.attempts == 2
        assert isinstance(ei.value.last_exception, ValueError)

    def test_abort_on_not_retried(self) -> None:
        @retry(max_attempts=5, initial_interval=0.0, abort_on=(ValueError,))
        def boom() -> None:
            raise ValueError("abort")

        with pytest.raises(ValueError, match="abort"):
            boom()

    def test_retry_on_subset(self) -> None:
        @retry(max_attempts=2, initial_interval=0.0, retry_on=(ConnectionError,))
        def wrong_type() -> None:
            raise ValueError("not retried")

        with pytest.raises(ValueError, match="not retried"):
            wrong_type()

    def test_preserves_metadata(self) -> None:
        @retry(max_attempts=2)
        def documented() -> None:
            """Doc."""

        assert documented.__name__ == "documented"
        assert documented.__doc__ == "Doc."


class TestAsyncRetry:
    """Tests for asynchronous retry behavior."""

    @pytest.mark.asyncio
    async def test_async_success(self) -> None:
        @retry(max_attempts=3)
        async def ok() -> int:
            return 42

        assert await ok() == 42

    @pytest.mark.asyncio
    async def test_async_retries(self) -> None:
        count = {"n": 0}

        @retry(max_attempts=4, initial_interval=0.0)
        async def flaky() -> str:
            count["n"] += 1
            if count["n"] < 2:
                raise OSError("transient")
            return "ok"

        assert await flaky() == "ok"
        assert count["n"] == 2


class TestRetryClass:
    """Tests for Retry.execute / events."""

    def test_execute_direct(self) -> None:
        r = Retry(RetryConfig(max_attempts=2, initial_interval=0.0))
        assert r.execute(lambda: 7) == 7

    def test_on_event(self) -> None:
        events: list[RetryEvent] = []
        r = Retry(RetryConfig(max_attempts=2, initial_interval=0.0), name="t")
        r.on_event(events.append)

        state = [0]

        def fail_once() -> str:
            state[0] += 1
            if state[0] < 2:
                raise RuntimeError("x")
            return "y"

        assert r.execute(fail_once) == "y"
        types = [e.event_type for e in events]
        assert RetryEventType.ATTEMPT_FAILURE in types
        assert RetryEventType.SUCCESS in types
        assert all(e.name == "t" for e in events)

    @pytest.mark.asyncio
    async def test_execute_async(self) -> None:
        n = 0

        async def factory() -> int:
            nonlocal n
            n += 1
            if n < 2:
                raise ConnectionError("retry")
            return 99

        r = Retry(RetryConfig(max_attempts=3, initial_interval=0.0))
        assert await r.execute_async(factory) == 99


class TestRetryDecoratorForms:
    """Bare @retry vs @retry(...)."""

    def test_bare_decorator(self) -> None:
        @retry
        def f() -> str:
            return "a"

        assert f() == "a"

    def test_decorator_with_parens(self) -> None:
        @retry(max_attempts=1)
        def g() -> str:
            return "b"

        assert g() == "b"


class TestBackoff:
    """Backoff timing (mocked sleep)."""

    def test_exponential_wait_sequence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sleeps: list[float] = []

        class _FakeTime:
            @staticmethod
            def sleep(s: float) -> None:
                sleeps.append(s)

        # Package re-exports `retry`, so `pysilience.retry` is the decorator; patch the real module.
        retry_py = sys.modules["pysilience.retry"]
        monkeypatch.setattr(retry_py, "time", _FakeTime())

        attempt = 0

        @retry(max_attempts=4, initial_interval=1.0, multiplier=2.0, max_interval=10.0)
        def flaky() -> None:
            nonlocal attempt
            attempt += 1
            if attempt < 4:
                raise RuntimeError("x")

        flaky()
        assert sleeps == [1.0, 2.0, 4.0]
