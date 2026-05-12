"""
Pysilience - Rate Limiter Pattern
=================================
Controls the rate at which operations are executed using a token-bucket
algorithm with periodic refills (stdlib only; Python 3.10+).

The limiter tracks a fixed number of **permits** that refresh every
``limit_refresh_period`` seconds.  When a call arrives and a permit is
available it proceeds immediately; otherwise the caller blocks up to
``timeout_duration`` seconds waiting for the next refill.  If no permit
becomes available in time, :exc:`RateLimitExceeded` is raised.

Usage:
    from ratelimiter import rate_limiter, RateLimiterConfig, RateLimitExceeded

    @rate_limiter(limit_for_period=10, limit_refresh_period=1.0)
    def call_api():
        ...

    @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=2.0)
    async def call_api_async():
        ...

License: MIT
"""

from __future__ import annotations

import asyncio
import functools
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generic, ParamSpec, TypeVar, overload

from pysilience.core.listeners import notify_listeners
from pysilience.core.registry import register as register_pattern

__all__ = [
    "rate_limiter",
    "RateLimiter",
    "RateLimiterConfig",
    "RateLimitExceeded",
    "RateLimiterEvent",
    "RateLimiterEventType",
    "create_rate_limiter",
]

P = ParamSpec("P")
R = TypeVar("R")


# ============================================================================
# EXCEPTIONS
# ============================================================================


class RateLimitExceeded(Exception):  # noqa: N818
    """Raised when a call cannot acquire a permit within the timeout.

    Attributes:
        name: Name of the rate limiter instance.
        available_permits: Number of permits that were available (0).
        wait_time: Seconds the caller would need to wait for the next
            permit (``None`` when unknown).
    """

    def __init__(
        self,
        message: str,
        *,
        name: str | None = None,
        available_permits: int | None = None,
        wait_time: float | None = None,
    ) -> None:
        super().__init__(message)
        self.name = name
        self.available_permits = available_permits
        self.wait_time = wait_time

    def __str__(self) -> str:
        base = super().__str__()
        if self.name and self.wait_time is not None:
            return f"[{self.name}] {base} (wait {self.wait_time:.2f}s for next permit)"
        if self.name:
            return f"[{self.name}] {base}"
        return base


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass(frozen=True, slots=True)
class RateLimiterConfig:
    """Configuration for rate limiter behavior.

    Attributes:
        limit_for_period: Number of permits available in each period.
        limit_refresh_period: Duration of one period (seconds) after which
            permits are replenished to ``limit_for_period``.
        timeout_duration: Maximum time (seconds) a caller will block
            waiting for a permit.  ``0.0`` means reject immediately when
            no permit is available.

    Example:
        >>> config = RateLimiterConfig(limit_for_period=10, limit_refresh_period=1.0)
        >>> config = RateLimiterConfig(
        ...     limit_for_period=5,
        ...     limit_refresh_period=1.0,
        ...     timeout_duration=2.0,
        ... )
    """

    limit_for_period: int = 50
    limit_refresh_period: float = 0.5
    timeout_duration: float = 5.0

    def __post_init__(self) -> None:
        if self.limit_for_period < 1:
            raise ValueError(f"limit_for_period must be >= 1, got {self.limit_for_period}")
        if self.limit_refresh_period <= 0:
            raise ValueError(
                f"limit_refresh_period must be positive, got {self.limit_refresh_period}"
            )
        if self.timeout_duration < 0:
            raise ValueError(f"timeout_duration must be non-negative, got {self.timeout_duration}")


# ============================================================================
# EVENTS (for observability)
# ============================================================================


class RateLimiterEventType(Enum):
    """Types of events emitted by RateLimiter."""

    SUCCESS = auto()
    REJECTED = auto()
    ERROR = auto()


@dataclass(frozen=True, slots=True)
class RateLimiterEvent:
    """Event emitted by RateLimiter for observability.

    Attributes:
        event_type: The type of event that occurred.
        name: Name of the rate limiter instance.
        available_permits: Permits available at event time.
        wait_time: Time the caller waited to acquire a permit (seconds).
        exception: The exception if event_type is ERROR or REJECTED.
    """

    event_type: RateLimiterEventType
    name: str
    available_permits: int
    wait_time: float
    exception: BaseException | None = None


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class RateLimiter(Generic[P, R]):
    """Rate limiter that controls how many calls are permitted per period.

    Permits are replenished every ``limit_refresh_period`` seconds.  When
    all permits are consumed the caller blocks up to ``timeout_duration``
    waiting for the next refill cycle.

    Use as a decorator or call ``execute`` / ``execute_async`` with a
    callable.

    Example:
        >>> rl = RateLimiter(RateLimiterConfig(limit_for_period=5), name="api")
        >>> result = rl.execute(lambda: fetch())
    """

    def __init__(
        self,
        config: RateLimiterConfig | None = None,
        *,
        name: str | None = None,
    ) -> None:
        self.config = config or RateLimiterConfig()
        self.name = name or "ratelimiter"
        self._event_listeners: list[Callable[[RateLimiterEvent], None]] = []
        self._lock = threading.Lock()
        self._permits = self.config.limit_for_period
        self._period_start = time.monotonic()

    # -- public properties ---------------------------------------------------

    @property
    def available_permits(self) -> int:
        """Number of permits currently available (after refreshing if needed)."""
        with self._lock:
            self._maybe_refresh()
            return self._permits

    # -- public API -----------------------------------------------------------

    def on_event(self, listener: Callable[[RateLimiterEvent], None]) -> None:
        """Register a listener for rate limiter events."""
        self._event_listeners.append(listener)

    def _emit_event(self, event: RateLimiterEvent) -> None:
        notify_listeners(self._event_listeners, event)

    def _maybe_refresh(self) -> None:
        """Replenish permits if the current period has elapsed.  Must hold ``_lock``."""
        now = time.monotonic()
        elapsed = now - self._period_start
        if elapsed >= self.config.limit_refresh_period:
            periods = int(elapsed / self.config.limit_refresh_period)
            self._period_start += periods * self.config.limit_refresh_period
            self._permits = self.config.limit_for_period

    def _seconds_until_refresh(self) -> float:
        """Seconds until the next permit refresh.  Must hold ``_lock``."""
        elapsed = time.monotonic() - self._period_start
        return max(0.0, self.config.limit_refresh_period - elapsed)

    def _try_acquire(self) -> bool:
        """Try to acquire one permit.  Must hold ``_lock``."""
        self._maybe_refresh()
        if self._permits > 0:
            self._permits -= 1
            return True
        return False

    def _reject(self, wait_time: float) -> RateLimitExceeded:
        return RateLimitExceeded(
            "Rate limit exceeded",
            name=self.name,
            available_permits=0,
            wait_time=wait_time,
        )

    def acquire(self) -> float:
        """Block until a permit is acquired or timeout is reached.

        Returns:
            The time (seconds) the caller waited.

        Raises:
            RateLimitExceeded: If no permit becomes available within
                ``timeout_duration``.
        """
        start = time.monotonic()
        deadline = start + self.config.timeout_duration

        while True:
            with self._lock:
                if self._try_acquire():
                    return time.monotonic() - start

                if self.config.timeout_duration == 0.0:
                    wait_needed = self._seconds_until_refresh()
                    raise self._reject(wait_needed)

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    wait_needed = self._seconds_until_refresh()
                    raise self._reject(wait_needed)

                sleep_for = min(self._seconds_until_refresh(), remaining)

            time.sleep(sleep_for)

    async def acquire_async(self) -> float:
        """Async version of :meth:`acquire`."""
        start = time.monotonic()
        deadline = start + self.config.timeout_duration

        while True:
            with self._lock:
                if self._try_acquire():
                    return time.monotonic() - start

                if self.config.timeout_duration == 0.0:
                    wait_needed = self._seconds_until_refresh()
                    raise self._reject(wait_needed)

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    wait_needed = self._seconds_until_refresh()
                    raise self._reject(wait_needed)

                sleep_for = min(self._seconds_until_refresh(), remaining)

            await asyncio.sleep(sleep_for)

    def execute(self, func: Callable[[], R]) -> R:
        """Run ``func`` after acquiring a rate-limit permit."""
        try:
            wait_time = self.acquire()
        except RateLimitExceeded as err:
            self._emit_event(
                RateLimiterEvent(
                    event_type=RateLimiterEventType.REJECTED,
                    name=self.name,
                    available_permits=0,
                    wait_time=err.wait_time or 0.0,
                    exception=err,
                )
            )
            raise
        try:
            result = func()
        except Exception as exc:
            self._emit_event(
                RateLimiterEvent(
                    event_type=RateLimiterEventType.ERROR,
                    name=self.name,
                    available_permits=self.available_permits,
                    wait_time=wait_time,
                    exception=exc,
                )
            )
            raise
        else:
            self._emit_event(
                RateLimiterEvent(
                    event_type=RateLimiterEventType.SUCCESS,
                    name=self.name,
                    available_permits=self.available_permits,
                    wait_time=wait_time,
                )
            )
            return result

    async def execute_async(self, factory: Callable[[], Awaitable[R]]) -> R:
        """Run ``factory`` (each call must return an awaitable) after acquiring a permit."""
        try:
            wait_time = await self.acquire_async()
        except RateLimitExceeded as err:
            self._emit_event(
                RateLimiterEvent(
                    event_type=RateLimiterEventType.REJECTED,
                    name=self.name,
                    available_permits=0,
                    wait_time=err.wait_time or 0.0,
                    exception=err,
                )
            )
            raise
        try:
            result = await factory()
        except Exception as exc:
            self._emit_event(
                RateLimiterEvent(
                    event_type=RateLimiterEventType.ERROR,
                    name=self.name,
                    available_permits=self.available_permits,
                    wait_time=wait_time,
                    exception=exc,
                )
            )
            raise
        else:
            self._emit_event(
                RateLimiterEvent(
                    event_type=RateLimiterEventType.SUCCESS,
                    name=self.name,
                    available_permits=self.available_permits,
                    wait_time=wait_time,
                )
            )
            return result

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Use RateLimiter as a decorator."""
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
def rate_limiter(
    func: Callable[P, R],
) -> Callable[P, R]: ...


@overload
def rate_limiter(
    func: None = None,
    *,
    limit_for_period: int = 50,
    limit_refresh_period: float = 0.5,
    timeout_duration: float = 5.0,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def rate_limiter(
    func: Callable[P, R] | None = None,
    *,
    limit_for_period: int = 50,
    limit_refresh_period: float = 0.5,
    timeout_duration: float = 5.0,
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to wrap a function with rate-limit protection.

    Bare ``@rate_limiter`` uses defaults.  Use parameters for custom
    behavior::

        @rate_limiter(limit_for_period=10, limit_refresh_period=1.0)
        def call_api():
            ...
    """
    config = RateLimiterConfig(
        limit_for_period=limit_for_period,
        limit_refresh_period=limit_refresh_period,
        timeout_duration=timeout_duration,
    )
    instance: RateLimiter[Any, Any] = RateLimiter(config, name=name)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        return instance(fn)

    if func is not None:
        return decorator(func)
    return decorator


def create_rate_limiter(
    config: RateLimiterConfig | None = None,
    *,
    name: str,
    register: bool = True,
) -> RateLimiter[Any, Any]:
    """Create a :class:`RateLimiter` and optionally register it with :func:`pysilience.core.register`."""
    instance: RateLimiter[Any, Any] = RateLimiter(config, name=name)
    if register:
        register_pattern("ratelimiter", name, instance)
    return instance
