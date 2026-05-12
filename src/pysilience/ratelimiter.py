"""
Pysilience - Rate Limiter Pattern
=================================
Controls the rate at which operations are executed using configurable
algorithms (stdlib only; Python 3.10+).

Available algorithms (see :class:`RateLimitAlgorithm`):

- **TOKEN_BUCKET** *(default)*: Continuous token refill up to capacity.
  Allows bursting from idle state.
- **LEAKY_BUCKET**: Enforces smooth, evenly-spaced requests.  No bursting
  even after idle periods.
- **FIXED_WINDOW**: Counter resets at fixed period boundaries.  Simple but
  allows 2x burst at the boundary of two adjacent windows.
- **SLIDING_WINDOW**: Weighted blend of the current and previous fixed
  windows, smoothing the boundary burst.

Usage:
    from ratelimiter import rate_limiter, RateLimiterConfig, RateLimitExceeded

    @rate_limiter(limit_for_period=10, limit_refresh_period=1.0)
    def call_api():
        ...

    @rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=2.0)
    async def call_api_async():
        ...

    # Explicit algorithm selection:
    from ratelimiter import RateLimitAlgorithm

    @rate_limiter(
        limit_for_period=10,
        algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
    )
    def call_api():
        ...

License: MIT
"""

from __future__ import annotations

import asyncio
import functools
import threading
import time
from abc import ABC, abstractmethod
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
    "RateLimitAlgorithm",
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
# ALGORITHM ENUM
# ============================================================================


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm to use.

    Attributes:
        TOKEN_BUCKET: Tokens refill continuously at a steady rate up to
            ``limit_for_period``.  Allows bursting from idle state.
        LEAKY_BUCKET: Enforces a minimum interval of
            ``limit_refresh_period / limit_for_period`` seconds between
            requests.  Produces smooth, evenly-spaced throughput with no
            bursting, even after idle periods.
        FIXED_WINDOW: A simple counter that resets every
            ``limit_refresh_period`` seconds.  Allows up to 2x burst at
            the boundary of two adjacent windows.
        SLIDING_WINDOW: Blends the current window's count with a weighted
            portion of the previous window's count, smoothing out the
            boundary burst of the fixed-window approach.
    """

    TOKEN_BUCKET = auto()
    LEAKY_BUCKET = auto()
    FIXED_WINDOW = auto()
    SLIDING_WINDOW = auto()


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
        algorithm: Rate limiting algorithm to use.

    Example:
        >>> config = RateLimiterConfig(limit_for_period=10, limit_refresh_period=1.0)
        >>> config = RateLimiterConfig(
        ...     limit_for_period=5,
        ...     limit_refresh_period=1.0,
        ...     timeout_duration=2.0,
        ...     algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
        ... )
    """

    limit_for_period: int = 50
    limit_refresh_period: float = 0.5
    timeout_duration: float = 5.0
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET

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
# STRATEGIES (private)
# ============================================================================


class _RateLimitStrategy(ABC):
    """Interface for rate-limiting algorithms.

    All methods are called while the caller holds ``RateLimiter._lock``.
    """

    __slots__ = ()

    @abstractmethod
    def try_acquire(self) -> bool:
        """Attempt to acquire one permit.  Return ``True`` on success."""

    @abstractmethod
    def seconds_until_available(self) -> float:
        """Estimated seconds until the next permit becomes available."""

    @property
    @abstractmethod
    def permits(self) -> int:
        """Number of permits available right now."""


class _TokenBucketStrategy(_RateLimitStrategy):
    """Continuous-refill token bucket.  Allows bursting from idle state."""

    __slots__ = ("_capacity", "_refill_rate", "_tokens", "_last_refill")

    def __init__(self, limit: int, period: float) -> None:
        self._capacity = limit
        self._refill_rate = limit / period
        self._tokens = float(limit)
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self._capacity, self._tokens + (now - self._last_refill) * self._refill_rate)
        self._last_refill = now

    def try_acquire(self) -> bool:
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def seconds_until_available(self) -> float:
        self._refill()
        if self._tokens >= 1.0:
            return 0.0
        return (1.0 - self._tokens) / self._refill_rate

    @property
    def permits(self) -> int:
        self._refill()
        return int(self._tokens)


class _LeakyBucketStrategy(_RateLimitStrategy):
    """Leaky bucket enforcing minimum spacing between requests.

    Each request advances a virtual "next allowed" timestamp by
    ``period / limit`` seconds.  Idle time does **not** accumulate credit,
    so there is no bursting even after long pauses.
    """

    __slots__ = ("_interval", "_next_allowed")

    def __init__(self, limit: int, period: float) -> None:
        self._interval = period / limit
        self._next_allowed = time.monotonic()

    def try_acquire(self) -> bool:
        now = time.monotonic()
        if self._next_allowed <= now:
            self._next_allowed = now + self._interval
            return True
        return False

    def seconds_until_available(self) -> float:
        return max(0.0, self._next_allowed - time.monotonic())

    @property
    def permits(self) -> int:
        return 1 if time.monotonic() >= self._next_allowed else 0


class _FixedWindowStrategy(_RateLimitStrategy):
    """Counter that resets at fixed period boundaries."""

    __slots__ = ("_limit", "_period", "_count", "_window_start")

    def __init__(self, limit: int, period: float) -> None:
        self._limit = limit
        self._period = period
        self._count = 0
        self._window_start = time.monotonic()

    def _maybe_advance(self) -> None:
        now = time.monotonic()
        elapsed = now - self._window_start
        if elapsed >= self._period:
            windows = int(elapsed / self._period)
            self._window_start += windows * self._period
            self._count = 0

    def try_acquire(self) -> bool:
        self._maybe_advance()
        if self._count < self._limit:
            self._count += 1
            return True
        return False

    def seconds_until_available(self) -> float:
        self._maybe_advance()
        if self._count < self._limit:
            return 0.0
        elapsed = time.monotonic() - self._window_start
        return max(0.0, self._period - elapsed)

    @property
    def permits(self) -> int:
        self._maybe_advance()
        return max(0, self._limit - self._count)


class _SlidingWindowStrategy(_RateLimitStrategy):
    """Weighted blend of the current and previous fixed windows.

    At any point within the current window the effective count is::

        prev_count * (1 - elapsed/period) + curr_count

    This smooths out the 2x-burst edge case of fixed windows.
    """

    __slots__ = ("_limit", "_period", "_prev_count", "_curr_count", "_window_start")

    def __init__(self, limit: int, period: float) -> None:
        self._limit = limit
        self._period = period
        self._prev_count = 0
        self._curr_count = 0
        self._window_start = time.monotonic()

    def _maybe_advance(self) -> None:
        now = time.monotonic()
        elapsed = now - self._window_start
        if elapsed >= self._period:
            windows = int(elapsed / self._period)
            if windows == 1:
                self._prev_count = self._curr_count
            else:
                self._prev_count = 0
            self._curr_count = 0
            self._window_start += windows * self._period

    def _weighted_count(self) -> float:
        self._maybe_advance()
        elapsed = time.monotonic() - self._window_start
        prev_weight = 1.0 - elapsed / self._period
        return self._prev_count * prev_weight + self._curr_count

    def try_acquire(self) -> bool:
        if self._weighted_count() + 1.0 <= self._limit:
            self._curr_count += 1
            return True
        return False

    def seconds_until_available(self) -> float:
        wc = self._weighted_count()
        if wc + 1.0 <= self._limit:
            return 0.0
        elapsed = time.monotonic() - self._window_start
        remaining = max(0.0, self._period - elapsed)
        if self._prev_count > 0:
            excess = wc + 1.0 - self._limit
            decrease_rate = self._prev_count / self._period
            return min(max(0.0, excess / decrease_rate), remaining)
        return remaining

    @property
    def permits(self) -> int:
        return max(0, int(self._limit - self._weighted_count()))


def _create_strategy(config: RateLimiterConfig) -> _RateLimitStrategy:
    """Instantiate the strategy matching ``config.algorithm``."""
    limit = config.limit_for_period
    period = config.limit_refresh_period
    if config.algorithm is RateLimitAlgorithm.TOKEN_BUCKET:
        return _TokenBucketStrategy(limit, period)
    if config.algorithm is RateLimitAlgorithm.LEAKY_BUCKET:
        return _LeakyBucketStrategy(limit, period)
    if config.algorithm is RateLimitAlgorithm.FIXED_WINDOW:
        return _FixedWindowStrategy(limit, period)
    if config.algorithm is RateLimitAlgorithm.SLIDING_WINDOW:
        return _SlidingWindowStrategy(limit, period)
    raise ValueError(f"Unknown algorithm: {config.algorithm}")  # pragma: no cover


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class RateLimiter(Generic[P, R]):
    """Rate limiter that controls how many calls are permitted per period.

    The algorithm used to track permits is selected via
    :attr:`RateLimiterConfig.algorithm` (default: token bucket).

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
        self._strategy = _create_strategy(self.config)

    # -- public properties ---------------------------------------------------

    @property
    def available_permits(self) -> int:
        """Number of permits currently available."""
        with self._lock:
            return self._strategy.permits

    # -- public API -----------------------------------------------------------

    def on_event(self, listener: Callable[[RateLimiterEvent], None]) -> None:
        """Register a listener for rate limiter events."""
        self._event_listeners.append(listener)

    def _emit_event(self, event: RateLimiterEvent) -> None:
        notify_listeners(self._event_listeners, event)

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
                if self._strategy.try_acquire():
                    return time.monotonic() - start

                if self.config.timeout_duration == 0.0:
                    wait_needed = self._strategy.seconds_until_available()
                    raise self._reject(wait_needed)

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    wait_needed = self._strategy.seconds_until_available()
                    raise self._reject(wait_needed)

                sleep_for = min(self._strategy.seconds_until_available(), remaining)

            time.sleep(sleep_for)

    async def acquire_async(self) -> float:
        """Async version of :meth:`acquire`."""
        start = time.monotonic()
        deadline = start + self.config.timeout_duration

        while True:
            with self._lock:
                if self._strategy.try_acquire():
                    return time.monotonic() - start

                if self.config.timeout_duration == 0.0:
                    wait_needed = self._strategy.seconds_until_available()
                    raise self._reject(wait_needed)

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    wait_needed = self._strategy.seconds_until_available()
                    raise self._reject(wait_needed)

                sleep_for = min(self._strategy.seconds_until_available(), remaining)

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
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def rate_limiter(
    func: Callable[P, R] | None = None,
    *,
    limit_for_period: int = 50,
    limit_refresh_period: float = 0.5,
    timeout_duration: float = 5.0,
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET,
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to wrap a function with rate-limit protection.

    Bare ``@rate_limiter`` uses defaults.  Use parameters for custom
    behavior::

        @rate_limiter(limit_for_period=10, limit_refresh_period=1.0)
        def call_api():
            ...

        @rate_limiter(
            limit_for_period=10,
            algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
        )
        def call_api():
            ...
    """
    config = RateLimiterConfig(
        limit_for_period=limit_for_period,
        limit_refresh_period=limit_refresh_period,
        timeout_duration=timeout_duration,
        algorithm=algorithm,
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
