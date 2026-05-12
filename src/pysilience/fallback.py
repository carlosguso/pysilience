"""
Pysilience - Fallback Pattern
=============================
Provides an alternative result when the primary operation fails (Python 3.10+, stdlib only).

Usage:
    from fallback import fallback, FallbackConfig

    @fallback(action=lambda exc: "default")
    def risky_call():
        ...

    @fallback(action=lambda exc: {"status": "degraded"})
    async def risky_async():
        ...

License: MIT
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generic, ParamSpec, TypeVar, overload

from pysilience.core.listeners import notify_listeners
from pysilience.core.registry import register as register_pattern

__all__ = [
    "fallback",
    "Fallback",
    "FallbackConfig",
    "FallbackEvent",
    "FallbackEventType",
    "create_fallback",
]

P = ParamSpec("P")
R = TypeVar("R")


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass(frozen=True, slots=True)
class FallbackConfig:
    """Configuration for fallback behavior.

    Attributes:
        fallback_on: Exception types that trigger the fallback action.
        raise_on: Exception types that bypass the fallback and propagate
            immediately. Checked before fallback_on.

    Example:
        >>> config = FallbackConfig(fallback_on=(IOError, TimeoutError))
        >>> config = FallbackConfig(raise_on=(KeyboardInterrupt,))
    """

    fallback_on: tuple[type[BaseException], ...] = (Exception,)
    raise_on: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if not self.fallback_on:
            raise ValueError("fallback_on must contain at least one exception type")


# ============================================================================
# EVENTS (for observability)
# ============================================================================


class FallbackEventType(Enum):
    """Types of events emitted by Fallback."""

    SUCCESS = auto()  # Primary operation succeeded without needing fallback
    RECOVERED = auto()  # Fallback action was invoked and returned a value
    FALLBACK_ERROR = auto()  # Fallback action itself raised an exception


@dataclass(frozen=True, slots=True)
class FallbackEvent:
    """Event emitted by Fallback for observability."""

    event_type: FallbackEventType
    name: str
    exception: BaseException | None = None
    fallback_exception: BaseException | None = None


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class Fallback(Generic[P, R]):
    """Provide an alternative result when the primary operation fails.

    Use as a decorator or call ``execute`` / ``execute_async`` with a callable.

    The ``action`` callable receives the caught exception and must return a
    value of the same type as the primary operation.

    Example:
        >>> fb = Fallback(action=lambda exc: 0, name="safe-int")
        >>> result = fb.execute(lambda: int("bad"))  # returns 0
    """

    def __init__(
        self,
        action: Callable[[BaseException], R],
        config: FallbackConfig | None = None,
        *,
        name: str | None = None,
    ) -> None:
        """Initialize the Fallback.

        Args:
            action: Callable invoked with the caught exception; its return value
                becomes the result of the protected operation.
            config: Configuration for fallback behavior. Uses defaults if None.
            name: Optional name for this fallback instance (for logging/metrics).
        """
        self.action = action
        self.config = config or FallbackConfig()
        self.name = name or "fallback"
        self._event_listeners: list[Callable[[FallbackEvent], None]] = []

    def on_event(self, listener: Callable[[FallbackEvent], None]) -> None:
        """Register a listener for fallback events."""
        self._event_listeners.append(listener)

    def _emit_event(self, event: FallbackEvent) -> None:
        notify_listeners(self._event_listeners, event)

    def _should_fallback(self, exc: BaseException) -> bool:
        if self.config.raise_on and isinstance(exc, self.config.raise_on):
            return False
        return isinstance(exc, self.config.fallback_on)

    def execute(self, func: Callable[[], R]) -> R:
        """Run ``func``; on eligible failure invoke the fallback action."""
        try:
            result = func()
        except BaseException as exc:
            if not self._should_fallback(exc):
                raise
            return self._invoke_fallback(exc)
        else:
            self._emit_event(
                FallbackEvent(
                    event_type=FallbackEventType.SUCCESS,
                    name=self.name,
                )
            )
            return result

    async def execute_async(self, factory: Callable[[], Awaitable[R]]) -> R:
        """Run ``factory`` (each call returns an awaitable); on eligible failure invoke the fallback."""
        try:
            result = await factory()
        except BaseException as exc:
            if not self._should_fallback(exc):
                raise
            return self._invoke_fallback(exc)
        else:
            self._emit_event(
                FallbackEvent(
                    event_type=FallbackEventType.SUCCESS,
                    name=self.name,
                )
            )
            return result

    def _invoke_fallback(self, exc: BaseException) -> R:
        """Call the fallback action and emit appropriate events."""
        try:
            fallback_result = self.action(exc)
        except Exception as fb_exc:
            self._emit_event(
                FallbackEvent(
                    event_type=FallbackEventType.FALLBACK_ERROR,
                    name=self.name,
                    exception=exc,
                    fallback_exception=fb_exc,
                )
            )
            raise fb_exc from exc
        self._emit_event(
            FallbackEvent(
                event_type=FallbackEventType.RECOVERED,
                name=self.name,
                exception=exc,
            )
        )
        return fallback_result

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Use Fallback as a decorator."""
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
def fallback(
    func: Callable[P, R],
    *,
    action: Callable[[BaseException], R],
) -> Callable[P, R]: ...


@overload
def fallback(
    func: None = None,
    *,
    action: Callable[[BaseException], Any],
    fallback_on: tuple[type[BaseException], ...] = (Exception,),
    raise_on: tuple[type[BaseException], ...] = (),
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def fallback(
    func: Callable[P, R] | None = None,
    *,
    action: Callable[[BaseException], Any],
    fallback_on: tuple[type[BaseException], ...] = (Exception,),
    raise_on: tuple[type[BaseException], ...] = (),
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to provide a fallback value when a function fails.

    Unlike other patterns ``action`` is required — it defines what value to
    return when the primary operation raises an eligible exception::

        @fallback(action=lambda exc: "default")
        def call_api():
            ...

        @fallback(action=lambda exc: None, fallback_on=(IOError,))
        async def fetch_data():
            ...

    Args:
        func: The function to wrap (when used without parentheses, but action
            must still be provided as a keyword argument).
        action: Callable receiving the caught exception; its return value
            becomes the result.
        fallback_on: Exception types that trigger the fallback.
        raise_on: Exception types that bypass the fallback and propagate.
        name: Optional name for this fallback instance.

    Returns:
        The wrapped function with fallback protection.
    """
    config = FallbackConfig(fallback_on=fallback_on, raise_on=raise_on)
    instance: Fallback[Any, Any] = Fallback(action, config, name=name)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        return instance(fn)

    if func is not None:
        return decorator(func)
    return decorator


# ============================================================================
# FACTORY FUNCTION
# ============================================================================


def create_fallback(
    action: Callable[[BaseException], Any],
    config: FallbackConfig | None = None,
    *,
    name: str,
    register: bool = True,
) -> Fallback[Any, Any]:
    """Create a :class:`Fallback` and optionally register it with :func:`pysilience.core.register`."""
    instance: Fallback[Any, Any] = Fallback(action, config, name=name)
    if register:
        register_pattern("fallback", name, instance)
    return instance
