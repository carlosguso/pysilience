"""
Pysilience - Circuit Breaker Pattern
====================================
Prevents cascading failures by stopping calls to a dependency that is
likely down.  The circuit breaker tracks recent outcomes in a sliding
window and transitions between three states:

- **CLOSED** – normal operation.  Failures are recorded and the failure
  rate is evaluated after each call.  When the rate meets or exceeds
  ``failure_rate_threshold`` (and at least ``minimum_number_of_calls``
  have been recorded) the circuit **opens**.
- **OPEN** – every call is immediately rejected with
  :exc:`CircuitBreakerOpen`.  After ``wait_duration_in_open_state``
  seconds the circuit moves to **HALF_OPEN**.
- **HALF_OPEN** – up to ``permitted_number_of_calls_in_half_open_state``
  probe calls are allowed.  Once all probes complete the failure rate is
  re-evaluated: below threshold → **CLOSED**, at or above → **OPEN**.

Usage:
    from circuitbreaker import circuit_breaker, CircuitBreakerConfig, CircuitBreakerOpen

    @circuit_breaker(failure_rate_threshold=0.5)
    def call_service():
        ...

    @circuit_breaker(failure_rate_threshold=0.5, wait_duration_in_open_state=30.0)
    async def call_service_async():
        ...

License: MIT
"""

from __future__ import annotations

import asyncio
import functools
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generic, ParamSpec, TypeVar, overload

from pysilience.core.listeners import notify_listeners
from pysilience.core.registry import register as register_pattern

__all__ = [
    "circuit_breaker",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpen",
    "CircuitBreakerEvent",
    "CircuitBreakerEventType",
    "CircuitBreakerState",
    "create_circuit_breaker",
]

P = ParamSpec("P")
R = TypeVar("R")


# ============================================================================
# EXCEPTIONS
# ============================================================================


class CircuitBreakerOpen(Exception):  # noqa: N818
    """Raised when a call is rejected because the circuit breaker is open.

    Attributes:
        name: Name of the circuit breaker instance.
        remaining_wait: Approximate seconds until the circuit transitions
            to half-open (``None`` when rejected in half-open state because
            all probe permits are in use).
    """

    def __init__(
        self,
        message: str,
        *,
        name: str | None = None,
        remaining_wait: float | None = None,
    ) -> None:
        super().__init__(message)
        self.name = name
        self.remaining_wait = remaining_wait

    def __str__(self) -> str:
        base = super().__str__()
        if self.name and self.remaining_wait is not None:
            return f"[{self.name}] {base} (retry after {self.remaining_wait:.1f}s)"
        if self.name:
            return f"[{self.name}] {base}"
        return base


# ============================================================================
# CONFIGURATION
# ============================================================================


class CircuitBreakerState(Enum):
    """Possible states of a circuit breaker."""

    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior.

    Attributes:
        failure_rate_threshold: Failure ratio (0.0–1.0) at which the circuit
            opens.  0.5 means 50 %.
        sliding_window_size: Number of most-recent outcomes stored in the
            CLOSED-state sliding window.
        minimum_number_of_calls: Minimum outcomes recorded before the failure
            rate is evaluated.  Must be ``<= sliding_window_size``.
        wait_duration_in_open_state: Seconds the circuit stays OPEN before
            transitioning to HALF_OPEN.
        permitted_number_of_calls_in_half_open_state: How many probe calls
            are allowed in HALF_OPEN before re-evaluating the failure rate.
        record_exceptions: Exception types counted as failures.
        ignore_exceptions: Exception types that are never counted as failures
            (checked before ``record_exceptions``).

    Example:
        >>> config = CircuitBreakerConfig(
        ...     failure_rate_threshold=0.5,
        ...     sliding_window_size=10,
        ...     wait_duration_in_open_state=30.0,
        ... )
    """

    failure_rate_threshold: float = 0.5
    sliding_window_size: int = 10
    minimum_number_of_calls: int = 5
    wait_duration_in_open_state: float = 60.0
    permitted_number_of_calls_in_half_open_state: int = 5
    record_exceptions: tuple[type[BaseException], ...] = (Exception,)
    ignore_exceptions: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if not 0.0 < self.failure_rate_threshold <= 1.0:
            raise ValueError(
                f"failure_rate_threshold must be in (0.0, 1.0], got {self.failure_rate_threshold}"
            )
        if self.sliding_window_size < 1:
            raise ValueError(
                f"sliding_window_size must be >= 1, got {self.sliding_window_size}"
            )
        if self.minimum_number_of_calls < 1:
            raise ValueError(
                f"minimum_number_of_calls must be >= 1, got {self.minimum_number_of_calls}"
            )
        if self.minimum_number_of_calls > self.sliding_window_size:
            raise ValueError(
                f"minimum_number_of_calls ({self.minimum_number_of_calls}) must be "
                f"<= sliding_window_size ({self.sliding_window_size})"
            )
        if self.wait_duration_in_open_state < 0:
            raise ValueError(
                f"wait_duration_in_open_state must be non-negative, "
                f"got {self.wait_duration_in_open_state}"
            )
        if self.permitted_number_of_calls_in_half_open_state < 1:
            raise ValueError(
                f"permitted_number_of_calls_in_half_open_state must be >= 1, "
                f"got {self.permitted_number_of_calls_in_half_open_state}"
            )


# ============================================================================
# EVENTS (for observability)
# ============================================================================


class CircuitBreakerEventType(Enum):
    """Types of events emitted by CircuitBreaker."""

    SUCCESS = auto()
    ERROR = auto()
    IGNORED_ERROR = auto()
    REJECTED = auto()
    STATE_TRANSITION = auto()


@dataclass(frozen=True, slots=True)
class CircuitBreakerEvent:
    """Event emitted by CircuitBreaker for observability."""

    event_type: CircuitBreakerEventType
    name: str
    state: CircuitBreakerState
    exception: BaseException | None = None
    from_state: CircuitBreakerState | None = None
    to_state: CircuitBreakerState | None = None


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class CircuitBreaker(Generic[P, R]):
    """Circuit breaker that prevents cascading failures.

    Use as a decorator or call ``execute`` / ``execute_async`` directly.

    Example:
        >>> cb = CircuitBreaker(
        ...     CircuitBreakerConfig(failure_rate_threshold=0.5), name="api"
        ... )
        >>> result = cb.execute(lambda: fetch())
    """

    def __init__(
        self,
        config: CircuitBreakerConfig | None = None,
        *,
        name: str | None = None,
    ) -> None:
        self.config = config or CircuitBreakerConfig()
        self.name = name or "circuitbreaker"
        self._event_listeners: list[Callable[[CircuitBreakerEvent], None]] = []
        self._lock = threading.Lock()
        self._state = CircuitBreakerState.CLOSED
        self._window: deque[bool] = deque(maxlen=self.config.sliding_window_size)
        self._failure_count = 0
        self._half_open_results: list[bool] = []
        self._half_open_allowed = 0
        self._opened_at = 0.0

    # -- public properties ---------------------------------------------------

    @property
    def state(self) -> CircuitBreakerState:
        """Current state of the circuit breaker."""
        with self._lock:
            return self._state

    @property
    def failure_rate(self) -> float:
        """Current failure rate in the CLOSED-state sliding window (0.0–1.0)."""
        with self._lock:
            total = len(self._window)
            if total == 0:
                return 0.0
            return self._failure_count / total

    # -- public API -----------------------------------------------------------

    def on_event(self, listener: Callable[[CircuitBreakerEvent], None]) -> None:
        """Register a listener for circuit breaker events."""
        self._event_listeners.append(listener)

    def reset(self) -> None:
        """Force the circuit breaker to CLOSED and clear all recorded outcomes."""
        with self._lock:
            old = self._state
            self._state = CircuitBreakerState.CLOSED
            self._window.clear()
            self._failure_count = 0
            self._half_open_results.clear()
            self._half_open_allowed = 0
        if old != CircuitBreakerState.CLOSED:
            self._emit_event(
                CircuitBreakerEvent(
                    event_type=CircuitBreakerEventType.STATE_TRANSITION,
                    name=self.name,
                    state=CircuitBreakerState.CLOSED,
                    from_state=old,
                    to_state=CircuitBreakerState.CLOSED,
                )
            )

    def execute(self, func: Callable[[], R]) -> R:
        """Run ``func`` through the circuit breaker."""
        called_in, transitions = self._acquire_permission()
        for from_s, to_s in transitions:
            self._emit_transition(from_s, to_s)
        try:
            result = func()
        except Exception as exc:
            self._on_complete(called_in, exc)
            raise
        self._on_complete(called_in, None)
        return result

    async def execute_async(self, factory: Callable[[], Awaitable[R]]) -> R:
        """Run ``factory`` (must return an awaitable) through the circuit breaker."""
        called_in, transitions = self._acquire_permission()
        for from_s, to_s in transitions:
            self._emit_transition(from_s, to_s)
        try:
            result = await factory()
        except Exception as exc:
            self._on_complete(called_in, exc)
            raise
        self._on_complete(called_in, None)
        return result

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Use CircuitBreaker as a decorator."""
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return await self.execute_async(lambda: func(*args, **kwargs))

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return self.execute(lambda: func(*args, **kwargs))

        return sync_wrapper

    # -- internal helpers -----------------------------------------------------

    def _emit_event(self, event: CircuitBreakerEvent) -> None:
        notify_listeners(self._event_listeners, event)

    def _emit_transition(
        self, from_state: CircuitBreakerState, to_state: CircuitBreakerState
    ) -> None:
        self._emit_event(
            CircuitBreakerEvent(
                event_type=CircuitBreakerEventType.STATE_TRANSITION,
                name=self.name,
                state=to_state,
                from_state=from_state,
                to_state=to_state,
            )
        )

    def _make_open_error(self, remaining: float | None = None) -> CircuitBreakerOpen:
        return CircuitBreakerOpen(
            "Circuit breaker is OPEN",
            name=self.name,
            remaining_wait=remaining,
        )

    def _acquire_permission(
        self,
    ) -> tuple[CircuitBreakerState, list[tuple[CircuitBreakerState, CircuitBreakerState]]]:
        """Acquire permission to execute.

        Returns ``(called_in_state, transitions)`` on success.
        Raises :exc:`CircuitBreakerOpen` and emits a REJECTED event if not
        permitted.
        """
        transitions: list[tuple[CircuitBreakerState, CircuitBreakerState]] = []
        error: CircuitBreakerOpen | None = None
        acquired = CircuitBreakerState.CLOSED

        with self._lock:
            if self._state == CircuitBreakerState.CLOSED:
                acquired = CircuitBreakerState.CLOSED

            elif self._state == CircuitBreakerState.OPEN:
                elapsed = time.monotonic() - self._opened_at
                if elapsed >= self.config.wait_duration_in_open_state:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_results = []
                    self._half_open_allowed = (
                        self.config.permitted_number_of_calls_in_half_open_state - 1
                    )
                    transitions.append(
                        (CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN)
                    )
                    acquired = CircuitBreakerState.HALF_OPEN
                else:
                    remaining = self.config.wait_duration_in_open_state - elapsed
                    error = self._make_open_error(remaining)

            elif self._state == CircuitBreakerState.HALF_OPEN:
                if self._half_open_allowed > 0:
                    self._half_open_allowed -= 1
                    acquired = CircuitBreakerState.HALF_OPEN
                else:
                    error = self._make_open_error()

        if error is not None:
            self._emit_event(
                CircuitBreakerEvent(
                    event_type=CircuitBreakerEventType.REJECTED,
                    name=self.name,
                    state=self._state,
                    exception=error,
                )
            )
            raise error

        return acquired, transitions

    def _on_complete(
        self, called_in: CircuitBreakerState, exc: Exception | None
    ) -> None:
        """Record the outcome of a call and emit events."""
        if (
            exc is not None
            and self.config.ignore_exceptions
            and isinstance(exc, self.config.ignore_exceptions)
        ):
            self._emit_event(
                CircuitBreakerEvent(
                    event_type=CircuitBreakerEventType.IGNORED_ERROR,
                    name=self.name,
                    state=self._state,
                    exception=exc,
                )
            )
            return

        is_failure = exc is not None and isinstance(exc, self.config.record_exceptions)
        transitions = self._record_outcome(called_in, success=not is_failure)

        if is_failure:
            self._emit_event(
                CircuitBreakerEvent(
                    event_type=CircuitBreakerEventType.ERROR,
                    name=self.name,
                    state=self._state,
                    exception=exc,
                )
            )
        else:
            self._emit_event(
                CircuitBreakerEvent(
                    event_type=CircuitBreakerEventType.SUCCESS,
                    name=self.name,
                    state=self._state,
                )
            )

        for from_s, to_s in transitions:
            self._emit_transition(from_s, to_s)

    def _record_outcome(
        self, called_in: CircuitBreakerState, *, success: bool
    ) -> list[tuple[CircuitBreakerState, CircuitBreakerState]]:
        """Record an outcome under the lock and return any state transitions."""
        transitions: list[tuple[CircuitBreakerState, CircuitBreakerState]] = []

        with self._lock:
            if (
                called_in == CircuitBreakerState.CLOSED
                and self._state == CircuitBreakerState.CLOSED
            ):
                self._window_record(success)
                total = len(self._window)
                if total >= self.config.minimum_number_of_calls:
                    rate = self._failure_count / total
                    if rate >= self.config.failure_rate_threshold:
                        self._state = CircuitBreakerState.OPEN
                        self._opened_at = time.monotonic()
                        transitions.append(
                            (CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN)
                        )

            elif (
                called_in == CircuitBreakerState.HALF_OPEN
                and self._state == CircuitBreakerState.HALF_OPEN
            ):
                self._half_open_results.append(success)
                if (
                    len(self._half_open_results)
                    >= self.config.permitted_number_of_calls_in_half_open_state
                ):
                    failures = sum(1 for x in self._half_open_results if not x)
                    rate = failures / len(self._half_open_results)
                    if rate >= self.config.failure_rate_threshold:
                        self._state = CircuitBreakerState.OPEN
                        self._opened_at = time.monotonic()
                        transitions.append(
                            (CircuitBreakerState.HALF_OPEN, CircuitBreakerState.OPEN)
                        )
                    else:
                        self._state = CircuitBreakerState.CLOSED
                        self._window.clear()
                        self._failure_count = 0
                        transitions.append(
                            (CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED)
                        )

        return transitions

    def _window_record(self, success: bool) -> None:
        """Record an outcome in the sliding window.  Must be called under lock."""
        if len(self._window) == self._window.maxlen:
            evicted = self._window[0]
            if not evicted:
                self._failure_count -= 1
        self._window.append(success)
        if not success:
            self._failure_count += 1


# ============================================================================
# DECORATOR FACTORY
# ============================================================================


@overload
def circuit_breaker(
    func: Callable[P, R],
) -> Callable[P, R]: ...


@overload
def circuit_breaker(
    func: None = None,
    *,
    failure_rate_threshold: float = 0.5,
    sliding_window_size: int = 10,
    minimum_number_of_calls: int = 5,
    wait_duration_in_open_state: float = 60.0,
    permitted_number_of_calls_in_half_open_state: int = 5,
    record_exceptions: tuple[type[BaseException], ...] = (Exception,),
    ignore_exceptions: tuple[type[BaseException], ...] = (),
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def circuit_breaker(
    func: Callable[P, R] | None = None,
    *,
    failure_rate_threshold: float = 0.5,
    sliding_window_size: int = 10,
    minimum_number_of_calls: int = 5,
    wait_duration_in_open_state: float = 60.0,
    permitted_number_of_calls_in_half_open_state: int = 5,
    record_exceptions: tuple[type[BaseException], ...] = (Exception,),
    ignore_exceptions: tuple[type[BaseException], ...] = (),
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to wrap a function with circuit breaker protection.

    Bare ``@circuit_breaker`` uses defaults.  Use parameters for custom
    behavior::

        @circuit_breaker(failure_rate_threshold=0.5, wait_duration_in_open_state=30.0)
        def call_api():
            ...
    """
    config = CircuitBreakerConfig(
        failure_rate_threshold=failure_rate_threshold,
        sliding_window_size=sliding_window_size,
        minimum_number_of_calls=minimum_number_of_calls,
        wait_duration_in_open_state=wait_duration_in_open_state,
        permitted_number_of_calls_in_half_open_state=permitted_number_of_calls_in_half_open_state,
        record_exceptions=record_exceptions,
        ignore_exceptions=ignore_exceptions,
    )
    instance: CircuitBreaker[Any, Any] = CircuitBreaker(config, name=name)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        return instance(fn)

    if func is not None:
        return decorator(func)
    return decorator


def create_circuit_breaker(
    config: CircuitBreakerConfig | None = None,
    *,
    name: str,
    register: bool = True,
) -> CircuitBreaker[Any, Any]:
    """Create a :class:`CircuitBreaker` and optionally register it with :func:`pysilience.core.register`."""
    instance: CircuitBreaker[Any, Any] = CircuitBreaker(config, name=name)
    if register:
        register_pattern("circuitbreaker", name, instance)
    return instance
