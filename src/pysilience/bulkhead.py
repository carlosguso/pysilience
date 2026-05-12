"""
Pysilience - Bulkhead Pattern
=============================
Limits concurrent executions so a failing dependency cannot exhaust all
threads or tasks. The sync path uses a threading semaphore; the async path
uses a lock, counter, and :class:`asyncio.Event` (not :class:`asyncio.Condition`
with :func:`asyncio.wait_for` on :meth:`asyncio.Condition.wait`, which can
corrupt the lock). Use one Bulkhead for sync-only or async-only workloads
to enforce a single shared limit.

Usage:
    from bulkhead import bulkhead, BulkheadConfig, BulkheadRejected

    @bulkhead(max_concurrent=4)
    def call_api():
        ...

    @bulkhead(max_concurrent=8, max_wait=2.0)
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
    "bulkhead",
    "Bulkhead",
    "BulkheadConfig",
    "BulkheadRejected",
    "BulkheadEvent",
    "BulkheadEventType",
    "create_bulkhead",
]

P = ParamSpec("P")
R = TypeVar("R")


# ============================================================================
# EXCEPTIONS
# ============================================================================


class BulkheadRejected(Exception):  # noqa: N818
    """Raised when the bulkhead has no capacity and waiting is not possible.

    Attributes:
        name: Name of the bulkhead instance.
        max_concurrent: Configured maximum concurrent calls.
    """

    def __init__(
        self,
        message: str,
        *,
        name: str | None = None,
        max_concurrent: int | None = None,
    ) -> None:
        super().__init__(message)
        self.name = name
        self.max_concurrent = max_concurrent

    def __str__(self) -> str:
        base = super().__str__()
        if self.name and self.max_concurrent is not None:
            return f"[{self.name}] {base} (max_concurrent={self.max_concurrent})"
        return base


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass(frozen=True, slots=True)
class BulkheadConfig:
    """Configuration for bulkhead behavior.

    Attributes:
        max_concurrent: Maximum number of calls executing at once (per path:
            sync uses a threading semaphore, async uses an asyncio semaphore).
        max_wait: Seconds to wait for a free slot before rejecting. ``0.0``
            means do not block: reject immediately when full.

    Example:
        >>> config = BulkheadConfig(max_concurrent=10, max_wait=1.5)
    """

    max_concurrent: int = 10
    max_wait: float = 0.0

    def __post_init__(self) -> None:
        if self.max_concurrent < 1:
            raise ValueError(f"max_concurrent must be >= 1, got {self.max_concurrent}")
        if self.max_wait < 0:
            raise ValueError(f"max_wait must be non-negative, got {self.max_wait}")


# ============================================================================
# EVENTS (for observability)
# ============================================================================


class BulkheadEventType(Enum):
    """Types of events emitted by Bulkhead."""

    SUCCESS = auto()
    REJECTED = auto()
    ERROR = auto()


@dataclass(frozen=True, slots=True)
class BulkheadEvent:
    """Event emitted by Bulkhead for observability."""

    event_type: BulkheadEventType
    name: str
    max_concurrent: int
    exception: BaseException | None = None


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class Bulkhead(Generic[P, R]):
    """Limit concurrent executions of wrapped operations.

    Use as a decorator or call ``execute`` / ``execute_async`` with a callable.

    Example:
        >>> bh = Bulkhead(BulkheadConfig(max_concurrent=4), name="db")
        >>> result = bh.execute(lambda: query())
    """

    def __init__(
        self,
        config: BulkheadConfig | None = None,
        *,
        name: str | None = None,
    ) -> None:
        self.config = config or BulkheadConfig()
        self.name = name or "bulkhead"
        self._event_listeners: list[Callable[[BulkheadEvent], None]] = []
        self._sync_sem = threading.Semaphore(self.config.max_concurrent)
        # Async side: lock + counter + Event (not asyncio.Condition). Wrapping
        # ``Condition.wait()`` in ``asyncio.wait_for()`` can cancel ``wait()`` during
        # lock cleanup and corrupt the condition (RuntimeError: Lock is not acquired).
        # Timed waits use ``wait_for`` only on ``Event.wait()``, which does not share
        # that failure mode.
        self._async_mu = asyncio.Lock()
        self._async_n = self.config.max_concurrent
        self._async_wake = asyncio.Event()
        if self._async_n > 0:
            self._async_wake.set()

    def on_event(self, listener: Callable[[BulkheadEvent], None]) -> None:
        """Register a listener for bulkhead events."""
        self._event_listeners.append(listener)

    def _emit_event(self, event: BulkheadEvent) -> None:
        notify_listeners(self._event_listeners, event)

    def _reject(self) -> BulkheadRejected:
        return BulkheadRejected(
            "Bulkhead is full and could not acquire a permit within max_wait",
            name=self.name,
            max_concurrent=self.config.max_concurrent,
        )

    def _sync_acquire(self) -> bool:
        if self.config.max_wait == 0.0:
            return self._sync_sem.acquire(blocking=False)
        return self._sync_sem.acquire(blocking=True, timeout=self.config.max_wait)

    def execute(self, func: Callable[[], R]) -> R:
        """Run ``func`` while holding a sync bulkhead permit."""
        if not self._sync_acquire():
            err = self._reject()
            self._emit_event(
                BulkheadEvent(
                    event_type=BulkheadEventType.REJECTED,
                    name=self.name,
                    max_concurrent=self.config.max_concurrent,
                    exception=err,
                )
            )
            raise err
        try:
            result = func()
        except Exception as exc:
            self._emit_event(
                BulkheadEvent(
                    event_type=BulkheadEventType.ERROR,
                    name=self.name,
                    max_concurrent=self.config.max_concurrent,
                    exception=exc,
                )
            )
            raise
        else:
            self._emit_event(
                BulkheadEvent(
                    event_type=BulkheadEventType.SUCCESS,
                    name=self.name,
                    max_concurrent=self.config.max_concurrent,
                    exception=None,
                )
            )
            return result
        finally:
            self._sync_sem.release()

    async def _async_acquire(self) -> bool:
        """Take one async permit."""
        deadline = time.monotonic() + self.config.max_wait if self.config.max_wait > 0 else None

        while True:
            async with self._async_mu:
                if self._async_n > 0:
                    self._async_n -= 1
                    if self._async_n == 0:
                        self._async_wake.clear()
                    return True
                if self.config.max_wait == 0.0:
                    return False
                assert deadline is not None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False

            try:
                await asyncio.wait_for(self._async_wake.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                continue

    async def _async_release(self) -> None:
        async with self._async_mu:
            self._async_n += 1
            self._async_wake.set()

    async def execute_async(self, factory: Callable[[], Awaitable[R]]) -> R:
        """Run ``factory`` (each call must return an awaitable) under an async permit."""
        if not await self._async_acquire():
            err = self._reject()
            self._emit_event(
                BulkheadEvent(
                    event_type=BulkheadEventType.REJECTED,
                    name=self.name,
                    max_concurrent=self.config.max_concurrent,
                    exception=err,
                )
            )
            raise err
        try:
            result = await factory()
        except Exception as exc:
            self._emit_event(
                BulkheadEvent(
                    event_type=BulkheadEventType.ERROR,
                    name=self.name,
                    max_concurrent=self.config.max_concurrent,
                    exception=exc,
                )
            )
            raise
        else:
            self._emit_event(
                BulkheadEvent(
                    event_type=BulkheadEventType.SUCCESS,
                    name=self.name,
                    max_concurrent=self.config.max_concurrent,
                    exception=None,
                )
            )
            return result
        finally:
            await self._async_release()

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Use Bulkhead as a decorator."""
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
def bulkhead(
    func: Callable[P, R],
) -> Callable[P, R]: ...


@overload
def bulkhead(
    func: None = None,
    *,
    max_concurrent: int = 10,
    max_wait: float = 0.0,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def bulkhead(
    func: Callable[P, R] | None = None,
    *,
    max_concurrent: int = 10,
    max_wait: float = 0.0,
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to limit concurrent executions.

    Bare ``@bulkhead`` uses defaults. Use parameters for custom behavior::

        @bulkhead(max_concurrent=4, max_wait=1.0)
        def call_db():
            ...
    """
    config = BulkheadConfig(max_concurrent=max_concurrent, max_wait=max_wait)
    instance: Bulkhead[Any, Any] = Bulkhead(config, name=name)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        return instance(fn)

    if func is not None:
        return decorator(func)
    return decorator


def create_bulkhead(
    config: BulkheadConfig | None = None,
    *,
    name: str,
    register: bool = True,
) -> Bulkhead[Any, Any]:
    """Create a :class:`Bulkhead` and optionally register it with :func:`pysilience.core.register`."""
    instance: Bulkhead[Any, Any] = Bulkhead(config, name=name)
    if register:
        register_pattern("bulkhead", name, instance)
    return instance
