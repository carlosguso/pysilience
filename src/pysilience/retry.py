"""
Pysilience - Retry Pattern
==========================
This file is self-contained and can be copied directly into your project.
No external dependencies required (Python 3.10+ stdlib only).

Usage:
    from retry import retry, RetryConfig, RetriesExhausted

    @retry(max_attempts=3, initial_interval=0.1)
    def flaky_call():
        ...

    @retry(max_attempts=5, initial_interval=0.05, multiplier=2.0)
    async def flaky_async():
        ...

    # RetriesExhausted is raised when all attempts fail.

License: MIT
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generic, ParamSpec, TypeVar, overload

__all__ = [
    "retry",
    "Retry",
    "RetryConfig",
    "RetriesExhausted",
    "RetryEvent",
    "RetryEventType",
]

P = ParamSpec("P")
R = TypeVar("R")


# ============================================================================
# EXCEPTIONS
# ============================================================================


class RetriesExhausted(Exception):  # noqa: N818
    """Raised when an operation fails after the configured number of attempts.

    Attributes:
        name: Name of the retry instance.
        attempts: Number of attempts that were made.
        last_exception: The exception raised on the final attempt.
    """

    def __init__(
        self,
        message: str,
        *,
        name: str | None = None,
        attempts: int | None = None,
        last_exception: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.name = name
        self.attempts = attempts
        self.last_exception = last_exception

    def __str__(self) -> str:
        base = super().__str__()
        if self.name and self.attempts is not None:
            return f"[{self.name}] {base} (after {self.attempts} attempts)"
        return base


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass(frozen=True, slots=True)
class RetryConfig:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Total number of attempts (including the first). Must be >= 1.
        initial_interval: Base wait time in seconds before the first retry after a
            failure. Subsequent waits use ``initial_interval * multiplier**k``,
            capped by max_interval when set.
        multiplier: Factor applied between attempts for exponential backoff.
            Use 1.0 for a fixed delay between retries.
        max_interval: Upper bound on wait time between attempts (seconds).
            None means no cap.
        jitter: If True, each wait is multiplied by a uniform random factor in
            [1 - jitter_ratio, 1 + jitter_ratio] to reduce synchronized retries.
        jitter_ratio: Half-width of the jitter band (0 to 1). Ignored if jitter
            is False.
        retry_on: Exception types that trigger a retry when raised.
        abort_on: Exception types that are never retried (checked first).

    Example:
        >>> config = RetryConfig(max_attempts=5, initial_interval=0.2, multiplier=2.0)
    """

    max_attempts: int = 3
    initial_interval: float = 0.0
    multiplier: float = 2.0
    max_interval: float | None = None
    jitter: bool = False
    jitter_ratio: float = 0.1
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    abort_on: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.initial_interval < 0:
            raise ValueError(f"initial_interval must be non-negative, got {self.initial_interval}")
        if self.multiplier < 0:
            raise ValueError(f"multiplier must be non-negative, got {self.multiplier}")
        if self.max_interval is not None and self.max_interval < 0:
            raise ValueError(f"max_interval must be non-negative, got {self.max_interval}")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError(f"jitter_ratio must be in [0, 1], got {self.jitter_ratio}")


# ============================================================================
# EVENTS (for observability)
# ============================================================================


class RetryEventType(Enum):
    """Types of events emitted by Retry."""

    SUCCESS = auto()
    ATTEMPT_FAILURE = auto()  # Failure that will be retried after a wait
    EXHAUSTED = auto()  # All attempts failed
    NON_RETRYABLE = auto()  # Failure that is not retried (abort or not in retry_on)


@dataclass(frozen=True, slots=True)
class RetryEvent:
    """Event emitted by Retry for observability."""

    event_type: RetryEventType
    name: str
    attempt: int
    max_attempts: int
    wait_before_next: float | None
    exception: BaseException | None = None


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class Retry(Generic[P, R]):
    """Retry failed operations up to a configured number of attempts.

    Use as a decorator or call ``execute`` / ``execute_async`` with a callable
    that performs one attempt.

    Example:
        >>> r = Retry(RetryConfig(max_attempts=3), name="http")
        >>> value = r.execute(lambda: fetch())
    """

    def __init__(
        self,
        config: RetryConfig | None = None,
        *,
        name: str | None = None,
    ) -> None:
        self.config = config or RetryConfig()
        self.name = name or "retry"
        self._event_listeners: list[Callable[[RetryEvent], None]] = []

    def on_event(self, listener: Callable[[RetryEvent], None]) -> None:
        """Register a listener for retry events."""
        self._event_listeners.append(listener)

    def _emit_event(self, event: RetryEvent) -> None:
        for listener in self._event_listeners:
            with contextlib.suppress(Exception):
                listener(event)

    def _classifies_retryable(self, exc: BaseException) -> bool:
        if self.config.abort_on and isinstance(exc, self.config.abort_on):
            return False
        return isinstance(exc, self.config.retry_on)

    def _wait_seconds_before_retry(self, failed_attempt_index: int) -> float:
        """``failed_attempt_index`` is 1-based (first failure = 1)."""
        if failed_attempt_index < 1:
            return 0.0
        k = failed_attempt_index - 1
        raw = self.config.initial_interval * (self.config.multiplier**k)
        if self.config.max_interval is not None:
            raw = min(raw, self.config.max_interval)
        if self.config.jitter and self.config.jitter_ratio > 0:
            lo = 1.0 - self.config.jitter_ratio
            hi = 1.0 + self.config.jitter_ratio
            raw *= random.uniform(lo, hi)
        return max(0.0, raw)

    def execute(self, func: Callable[[], R]) -> R:
        """Run ``func`` until success or attempts are exhausted."""
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = func()
                self._emit_event(
                    RetryEvent(
                        event_type=RetryEventType.SUCCESS,
                        name=self.name,
                        attempt=attempt,
                        max_attempts=self.config.max_attempts,
                        wait_before_next=None,
                        exception=None,
                    )
                )
                return result
            except Exception as exc:
                if not self._classifies_retryable(exc):
                    self._emit_event(
                        RetryEvent(
                            event_type=RetryEventType.NON_RETRYABLE,
                            name=self.name,
                            attempt=attempt,
                            max_attempts=self.config.max_attempts,
                            wait_before_next=None,
                            exception=exc,
                        )
                    )
                    raise
                if attempt >= self.config.max_attempts:
                    self._emit_event(
                        RetryEvent(
                            event_type=RetryEventType.EXHAUSTED,
                            name=self.name,
                            attempt=attempt,
                            max_attempts=self.config.max_attempts,
                            wait_before_next=None,
                            exception=exc,
                        )
                    )
                    raise RetriesExhausted(
                        "Maximum retry attempts reached",
                        name=self.name,
                        attempts=attempt,
                        last_exception=exc,
                    ) from exc
                wait = self._wait_seconds_before_retry(attempt)
                self._emit_event(
                    RetryEvent(
                        event_type=RetryEventType.ATTEMPT_FAILURE,
                        name=self.name,
                        attempt=attempt,
                        max_attempts=self.config.max_attempts,
                        wait_before_next=wait,
                        exception=exc,
                    )
                )
                time.sleep(wait)
        raise RuntimeError("retry loop exhausted without result")  # pragma: no cover

    async def execute_async(self, factory: Callable[[], Awaitable[R]]) -> R:
        """Run async factory (each call should start a fresh attempt) until success."""
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = await factory()
                self._emit_event(
                    RetryEvent(
                        event_type=RetryEventType.SUCCESS,
                        name=self.name,
                        attempt=attempt,
                        max_attempts=self.config.max_attempts,
                        wait_before_next=None,
                        exception=None,
                    )
                )
                return result
            except Exception as exc:
                if not self._classifies_retryable(exc):
                    self._emit_event(
                        RetryEvent(
                            event_type=RetryEventType.NON_RETRYABLE,
                            name=self.name,
                            attempt=attempt,
                            max_attempts=self.config.max_attempts,
                            wait_before_next=None,
                            exception=exc,
                        )
                    )
                    raise
                if attempt >= self.config.max_attempts:
                    self._emit_event(
                        RetryEvent(
                            event_type=RetryEventType.EXHAUSTED,
                            name=self.name,
                            attempt=attempt,
                            max_attempts=self.config.max_attempts,
                            wait_before_next=None,
                            exception=exc,
                        )
                    )
                    raise RetriesExhausted(
                        "Maximum retry attempts reached",
                        name=self.name,
                        attempts=attempt,
                        last_exception=exc,
                    ) from exc
                wait = self._wait_seconds_before_retry(attempt)
                self._emit_event(
                    RetryEvent(
                        event_type=RetryEventType.ATTEMPT_FAILURE,
                        name=self.name,
                        attempt=attempt,
                        max_attempts=self.config.max_attempts,
                        wait_before_next=wait,
                        exception=exc,
                    )
                )
                await asyncio.sleep(wait)
        raise RuntimeError("retry loop exhausted without result")  # pragma: no cover

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Use Retry as a decorator."""
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return await self.execute_async(lambda: func(*args, **kwargs))

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return self.execute(lambda: func(*args, **kwargs))

        return sync_wrapper


# ============================================================================
# DECORATOR FACTORY
# ============================================================================


@overload
def retry(
    func: Callable[P, R],
) -> Callable[P, R]: ...


@overload
def retry(
    func: None = None,
    *,
    max_attempts: int = 3,
    initial_interval: float = 0.0,
    multiplier: float = 2.0,
    max_interval: float | None = None,
    jitter: bool = False,
    jitter_ratio: float = 0.1,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    abort_on: tuple[type[BaseException], ...] = (),
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def retry(
    func: Callable[P, R] | None = None,
    *,
    max_attempts: int = 3,
    initial_interval: float = 0.0,
    multiplier: float = 2.0,
    max_interval: float | None = None,
    jitter: bool = False,
    jitter_ratio: float = 0.1,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    abort_on: tuple[type[BaseException], ...] = (),
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to retry a function on failure.

    Bare ``@retry`` uses defaults. Use parameters for custom behavior::

        @retry(max_attempts=5, initial_interval=0.1, multiplier=2.0)
        def call_api():
            ...
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        initial_interval=initial_interval,
        multiplier=multiplier,
        max_interval=max_interval,
        jitter=jitter,
        jitter_ratio=jitter_ratio,
        retry_on=retry_on,
        abort_on=abort_on,
    )
    instance: Retry[Any, Any] = Retry(config, name=name)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        return instance(fn)

    if func is not None:
        return decorator(func)
    return decorator


# ============================================================================
# OPTIONAL: INTEGRATION WITH PYSILIENCE CORE (if available)
# ============================================================================

try:
    from pysilience.core.registry import register as _register  # type: ignore[import-untyped]

    _HAS_CORE = True
except ImportError:
    _HAS_CORE = False
    _register = None


def create_retry(
    config: RetryConfig | None = None,
    *,
    name: str,
    register: bool = True,
) -> Retry[Any, Any]:
    """Create and optionally register a Retry instance."""
    instance: Retry[Any, Any] = Retry(config, name=name)
    if register and _HAS_CORE and _register is not None:
        _register("retry", name, instance)
    return instance
