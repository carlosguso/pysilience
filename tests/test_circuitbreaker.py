"""
Tests for the Circuit Breaker pattern.

Run with: pytest tests/test_circuitbreaker.py -v
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from pysilience.circuitbreaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerEvent,
    CircuitBreakerEventType,
    CircuitBreakerOpen,
    CircuitBreakerState,
    circuit_breaker,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _boom(msg: str = "boom") -> None:
    raise RuntimeError(msg)


def _make_cb(**overrides: object) -> CircuitBreaker[..., ...]:
    """Create a CircuitBreaker with test-friendly defaults."""
    defaults: dict[str, object] = {
        "failure_rate_threshold": 0.5,
        "sliding_window_size": 4,
        "minimum_number_of_calls": 2,
        "wait_duration_in_open_state": 0.0,
        "permitted_number_of_calls_in_half_open_state": 2,
    }
    defaults.update(overrides)
    return CircuitBreaker(CircuitBreakerConfig(**defaults), name="test")  # type: ignore[arg-type]


def _trip(cb: CircuitBreaker[..., ...], failures: int = 2) -> None:
    """Force the circuit breaker into OPEN state by recording failures."""
    for _ in range(failures):
        with pytest.raises(RuntimeError):
            cb.execute(_boom)


# ============================================================================
# CONFIGURATION TESTS
# ============================================================================


class TestCircuitBreakerConfig:
    def test_default_values(self) -> None:
        config = CircuitBreakerConfig()
        assert config.failure_rate_threshold == 0.5
        assert config.sliding_window_size == 10
        assert config.minimum_number_of_calls == 5
        assert config.wait_duration_in_open_state == 60.0
        assert config.permitted_number_of_calls_in_half_open_state == 5
        assert config.record_exceptions == (Exception,)
        assert config.ignore_exceptions == ()

    def test_invalid_failure_rate_zero(self) -> None:
        with pytest.raises(ValueError, match="failure_rate_threshold"):
            CircuitBreakerConfig(failure_rate_threshold=0.0)

    def test_invalid_failure_rate_above_one(self) -> None:
        with pytest.raises(ValueError, match="failure_rate_threshold"):
            CircuitBreakerConfig(failure_rate_threshold=1.1)

    def test_valid_failure_rate_one(self) -> None:
        config = CircuitBreakerConfig(failure_rate_threshold=1.0)
        assert config.failure_rate_threshold == 1.0

    def test_invalid_sliding_window_size(self) -> None:
        with pytest.raises(ValueError, match="sliding_window_size"):
            CircuitBreakerConfig(sliding_window_size=0)

    def test_invalid_minimum_number_of_calls(self) -> None:
        with pytest.raises(ValueError, match="minimum_number_of_calls must be >= 1"):
            CircuitBreakerConfig(minimum_number_of_calls=0)

    def test_minimum_calls_exceeds_window(self) -> None:
        with pytest.raises(ValueError, match="minimum_number_of_calls.*sliding_window_size"):
            CircuitBreakerConfig(sliding_window_size=5, minimum_number_of_calls=6)

    def test_invalid_wait_duration(self) -> None:
        with pytest.raises(ValueError, match="wait_duration_in_open_state"):
            CircuitBreakerConfig(wait_duration_in_open_state=-1)

    def test_invalid_permitted_calls(self) -> None:
        with pytest.raises(ValueError, match="permitted_number_of_calls_in_half_open_state"):
            CircuitBreakerConfig(permitted_number_of_calls_in_half_open_state=0)

    def test_frozen(self) -> None:
        config = CircuitBreakerConfig()
        with pytest.raises(AttributeError):
            config.failure_rate_threshold = 0.9  # type: ignore[misc]


# ============================================================================
# SYNC CIRCUIT BREAKER TESTS
# ============================================================================


class TestSyncCircuitBreaker:
    def test_starts_closed(self) -> None:
        cb = _make_cb()
        assert cb.state == CircuitBreakerState.CLOSED

    def test_success_stays_closed(self) -> None:
        cb = _make_cb()
        for _ in range(10):
            cb.execute(lambda: "ok")
        assert cb.state == CircuitBreakerState.CLOSED

    def test_opens_when_threshold_exceeded(self) -> None:
        cb = _make_cb(failure_rate_threshold=0.5, minimum_number_of_calls=2)
        _trip(cb, failures=2)
        assert cb.state == CircuitBreakerState.OPEN

    def test_rejects_when_open(self) -> None:
        cb = _make_cb(wait_duration_in_open_state=999.0)
        _trip(cb)
        with pytest.raises(CircuitBreakerOpen) as exc_info:
            cb.execute(lambda: "rejected")
        assert exc_info.value.name == "test"
        assert exc_info.value.remaining_wait is not None
        assert exc_info.value.remaining_wait > 0

    def test_transitions_to_half_open_after_wait(self) -> None:
        cb = _make_cb(wait_duration_in_open_state=0.01)
        _trip(cb)
        assert cb.state == CircuitBreakerState.OPEN
        time.sleep(0.02)
        result = cb.execute(lambda: "probe")
        assert result == "probe"
        assert cb.state == CircuitBreakerState.HALF_OPEN

    def test_closes_after_successful_probes(self) -> None:
        cb = _make_cb(
            wait_duration_in_open_state=0.0,
            permitted_number_of_calls_in_half_open_state=2,
        )
        _trip(cb)
        cb.execute(lambda: "ok1")
        cb.execute(lambda: "ok2")
        assert cb.state == CircuitBreakerState.CLOSED

    def test_reopens_on_probe_failure(self) -> None:
        cb = _make_cb(
            wait_duration_in_open_state=0.0,
            permitted_number_of_calls_in_half_open_state=2,
            failure_rate_threshold=0.5,
        )
        _trip(cb)
        cb.execute(lambda: "ok")
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        assert cb.state == CircuitBreakerState.OPEN

    def test_preserves_return_value(self) -> None:
        cb = _make_cb()
        assert cb.execute(lambda: {"a": 1}) == {"a": 1}

    def test_preserves_exception(self) -> None:
        cb = _make_cb()
        with pytest.raises(ValueError, match="original"):
            cb.execute(lambda: (_ for _ in ()).throw(ValueError("original")))

    def test_decorator_success(self) -> None:
        @circuit_breaker(failure_rate_threshold=0.5, sliding_window_size=4, minimum_number_of_calls=2)
        def f(x: int) -> int:
            return x + 1

        assert f(1) == 2

    def test_preserves_metadata(self) -> None:
        @circuit_breaker(failure_rate_threshold=0.5)
        def documented() -> None:
            """Doc."""

        assert documented.__name__ == "documented"
        assert documented.__doc__ == "Doc."


# ============================================================================
# STATE MACHINE DETAILS
# ============================================================================


class TestStateMachine:
    def test_stays_closed_below_minimum_calls(self) -> None:
        cb = _make_cb(
            failure_rate_threshold=0.5,
            sliding_window_size=4,
            minimum_number_of_calls=3,
        )
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        assert cb.state == CircuitBreakerState.CLOSED

    def test_sliding_window_evicts_old_entries(self) -> None:
        cb = _make_cb(
            failure_rate_threshold=0.75,
            sliding_window_size=4,
            minimum_number_of_calls=4,
        )
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        for _ in range(3):
            cb.execute(lambda: "ok")
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.failure_rate == 0.25
        for _ in range(4):
            cb.execute(lambda: "ok")
        assert cb.failure_rate == 0.0

    def test_failure_rate_property(self) -> None:
        cb = _make_cb(sliding_window_size=4, minimum_number_of_calls=4)
        assert cb.failure_rate == 0.0
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        cb.execute(lambda: "ok")
        assert cb.failure_rate == 0.5

    def test_reset_force_closes(self) -> None:
        cb = _make_cb(wait_duration_in_open_state=999.0)
        _trip(cb)
        assert cb.state == CircuitBreakerState.OPEN
        cb.reset()
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.failure_rate == 0.0
        assert cb.execute(lambda: 42) == 42

    def test_window_cleared_on_close_from_half_open(self) -> None:
        cb = _make_cb(
            wait_duration_in_open_state=0.0,
            permitted_number_of_calls_in_half_open_state=1,
        )
        _trip(cb)
        cb.execute(lambda: "ok")
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.failure_rate == 0.0

    def test_half_open_rejects_when_permits_exhausted(self) -> None:
        cb = _make_cb(
            wait_duration_in_open_state=0.0,
            permitted_number_of_calls_in_half_open_state=1,
        )
        _trip(cb)
        hold = threading.Event()
        started = threading.Event()
        results: list[object] = []

        def blocking() -> str:
            started.set()
            hold.wait()
            return "done"

        t = threading.Thread(target=lambda: results.append(cb.execute(blocking)))
        t.start()
        started.wait(timeout=2.0)
        assert cb.state == CircuitBreakerState.HALF_OPEN

        with pytest.raises(CircuitBreakerOpen):
            cb.execute(lambda: "rejected")

        hold.set()
        t.join(timeout=2.0)
        assert results == ["done"]


# ============================================================================
# EXCEPTION CLASSIFICATION
# ============================================================================


class TestExceptionClassification:
    def test_ignore_exceptions_not_counted(self) -> None:
        cb = _make_cb(
            ignore_exceptions=(ValueError,),
            minimum_number_of_calls=2,
            sliding_window_size=4,
        )
        for _ in range(4):
            with pytest.raises(ValueError):
                cb.execute(lambda: (_ for _ in ()).throw(ValueError("ignored")))
        assert cb.state == CircuitBreakerState.CLOSED

    def test_record_exceptions_subset(self) -> None:
        cb = _make_cb(
            record_exceptions=(ConnectionError,),
            minimum_number_of_calls=2,
            sliding_window_size=4,
        )
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.execute(lambda: (_ for _ in ()).throw(ValueError("not recorded")))
        assert cb.state == CircuitBreakerState.CLOSED

        for _ in range(2):
            with pytest.raises(ConnectionError):
                cb.execute(lambda: (_ for _ in ()).throw(ConnectionError("recorded")))
        assert cb.state == CircuitBreakerState.OPEN

    def test_ignore_takes_precedence_over_record(self) -> None:
        cb = _make_cb(
            record_exceptions=(Exception,),
            ignore_exceptions=(ValueError,),
            minimum_number_of_calls=2,
            sliding_window_size=4,
        )
        for _ in range(4):
            with pytest.raises(ValueError):
                cb.execute(lambda: (_ for _ in ()).throw(ValueError("ignored")))
        assert cb.state == CircuitBreakerState.CLOSED


# ============================================================================
# ASYNC TESTS
# ============================================================================


class TestAsyncCircuitBreaker:
    @pytest.mark.asyncio
    async def test_async_success(self) -> None:
        cb = _make_cb()

        async def ok() -> int:
            return 42

        assert await cb.execute_async(ok) == 42

    @pytest.mark.asyncio
    async def test_async_opens_and_closes(self) -> None:
        cb = _make_cb(
            wait_duration_in_open_state=0.0,
            permitted_number_of_calls_in_half_open_state=1,
        )

        async def fail() -> None:
            raise RuntimeError("fail")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                await cb.execute_async(fail)
        assert cb.state == CircuitBreakerState.OPEN

        async def ok() -> str:
            return "recovered"

        result = await cb.execute_async(ok)
        assert result == "recovered"
        assert cb.state == CircuitBreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_async_rejects_when_open(self) -> None:
        cb = _make_cb(wait_duration_in_open_state=999.0)

        async def fail() -> None:
            raise RuntimeError("fail")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                await cb.execute_async(fail)

        with pytest.raises(CircuitBreakerOpen):
            await cb.execute_async(lambda: asyncio.sleep(0))

    @pytest.mark.asyncio
    async def test_async_decorator(self) -> None:
        @circuit_breaker(
            failure_rate_threshold=0.5,
            sliding_window_size=4,
            minimum_number_of_calls=2,
        )
        async def greet(name: str) -> str:
            return f"hello {name}"

        assert await greet("world") == "hello world"


# ============================================================================
# EVENT TESTS
# ============================================================================


class TestCircuitBreakerEvents:
    def test_success_event(self) -> None:
        events: list[CircuitBreakerEvent] = []
        cb = _make_cb()
        cb.on_event(events.append)
        cb.execute(lambda: "ok")
        success_events = [e for e in events if e.event_type == CircuitBreakerEventType.SUCCESS]
        assert len(success_events) == 1
        assert success_events[0].name == "test"
        assert success_events[0].state == CircuitBreakerState.CLOSED

    def test_error_event(self) -> None:
        events: list[CircuitBreakerEvent] = []
        cb = _make_cb()
        cb.on_event(events.append)
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        error_events = [e for e in events if e.event_type == CircuitBreakerEventType.ERROR]
        assert len(error_events) == 1
        assert isinstance(error_events[0].exception, RuntimeError)

    def test_state_transition_events(self) -> None:
        events: list[CircuitBreakerEvent] = []
        cb = _make_cb(wait_duration_in_open_state=0.0, permitted_number_of_calls_in_half_open_state=1)
        cb.on_event(events.append)
        _trip(cb)
        cb.execute(lambda: "probe")

        transitions = [e for e in events if e.event_type == CircuitBreakerEventType.STATE_TRANSITION]
        states = [(t.from_state, t.to_state) for t in transitions]
        assert (CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN) in states
        assert (CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN) in states
        assert (CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED) in states

    def test_rejected_event(self) -> None:
        events: list[CircuitBreakerEvent] = []
        cb = _make_cb(wait_duration_in_open_state=999.0)
        cb.on_event(events.append)
        _trip(cb)
        with pytest.raises(CircuitBreakerOpen):
            cb.execute(lambda: "rejected")
        rejected = [e for e in events if e.event_type == CircuitBreakerEventType.REJECTED]
        assert len(rejected) == 1
        assert isinstance(rejected[0].exception, CircuitBreakerOpen)

    def test_ignored_error_event(self) -> None:
        events: list[CircuitBreakerEvent] = []
        cb = _make_cb(ignore_exceptions=(ValueError,))
        cb.on_event(events.append)
        with pytest.raises(ValueError):
            cb.execute(lambda: (_ for _ in ()).throw(ValueError("ignored")))
        ignored = [e for e in events if e.event_type == CircuitBreakerEventType.IGNORED_ERROR]
        assert len(ignored) == 1
        assert isinstance(ignored[0].exception, ValueError)

    def test_listener_exception_does_not_break_flow(self) -> None:
        def bad_listener(event: CircuitBreakerEvent) -> None:
            raise RuntimeError("listener error")

        cb = _make_cb()
        cb.on_event(bad_listener)
        result = cb.execute(lambda: "ok")
        assert result == "ok"


# ============================================================================
# DECORATOR FORMS
# ============================================================================


class TestDecoratorForms:
    def test_bare_decorator(self) -> None:
        @circuit_breaker
        def f() -> int:
            return 3

        assert f() == 3

    def test_decorator_with_empty_parens(self) -> None:
        @circuit_breaker()
        def f() -> int:
            return 4

        assert f() == 4

    def test_decorator_with_params(self) -> None:
        @circuit_breaker(failure_rate_threshold=0.8, sliding_window_size=5, minimum_number_of_calls=3)
        def f() -> int:
            return 5

        assert f() == 5


# ============================================================================
# EDGE CASES
# ============================================================================


class TestEdgeCases:
    def test_threshold_one_requires_all_failures(self) -> None:
        cb = _make_cb(
            failure_rate_threshold=1.0,
            sliding_window_size=4,
            minimum_number_of_calls=2,
        )
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        cb.execute(lambda: "ok")
        assert cb.state == CircuitBreakerState.CLOSED
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        assert cb.state == CircuitBreakerState.CLOSED
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        with pytest.raises(RuntimeError):
            cb.execute(_boom)
        assert cb.state == CircuitBreakerState.OPEN

    def test_zero_wait_immediate_half_open(self) -> None:
        cb = _make_cb(
            wait_duration_in_open_state=0.0,
            permitted_number_of_calls_in_half_open_state=1,
        )
        _trip(cb)
        assert cb.state == CircuitBreakerState.OPEN
        result = cb.execute(lambda: "immediate probe")
        assert result == "immediate probe"
        assert cb.state == CircuitBreakerState.CLOSED

    def test_circuit_breaker_open_str(self) -> None:
        err = CircuitBreakerOpen("msg", name="svc", remaining_wait=5.3)
        assert "svc" in str(err)
        assert "5.3" in str(err)

    def test_circuit_breaker_open_str_no_wait(self) -> None:
        err = CircuitBreakerOpen("msg", name="svc")
        assert "svc" in str(err)

    def test_circuit_breaker_open_str_bare(self) -> None:
        err = CircuitBreakerOpen("msg")
        assert str(err) == "msg"

    def test_create_circuit_breaker_registers(self) -> None:
        from pysilience.core.registry import clear, get

        clear()
        from pysilience.circuitbreaker import create_circuit_breaker

        cb = create_circuit_breaker(name="svc-a")
        assert get("circuitbreaker", "svc-a") is cb
        clear()

    def test_multiple_calls_after_close(self) -> None:
        cb = _make_cb(
            wait_duration_in_open_state=0.0,
            permitted_number_of_calls_in_half_open_state=1,
        )
        for _ in range(3):
            _trip(cb)
            cb.execute(lambda: "recover")
            assert cb.state == CircuitBreakerState.CLOSED
