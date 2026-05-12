"""
Pysilience - Timeout Pattern
============================
Limits how long a sync or async operation may run (Python 3.10+, stdlib only).

Usage:
    from timeout import timeout, TimeoutConfig, OperationTimeout

    @timeout(duration=5.0)
    def slow_function():
        ...

    @timeout(duration=10.0)
    async def slow_async_function():
        ...

    # OperationTimeout is the exception raised when the time limit is exceeded.

License: MIT
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import functools
import signal
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from pysilience.core.listeners import notify_listeners
from pysilience.core.registry import register as register_pattern

__all__ = [
    "timeout",
    "Timeout",
    "TimeoutConfig",
    "OperationTimeout",
    "TimeoutEvent",
    "TimeoutEventType",
    "create_timeout",
]

# Type variables for generic typing
P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")


# ============================================================================
# EXCEPTIONS
# ============================================================================


class OperationTimeout(Exception):  # noqa: N818
    """Raised when an operation exceeds its time limit.

    Attributes:
        name: Name of the timeout instance that raised this error.
        duration: The timeout duration that was exceeded (in seconds).
        elapsed: Actual time elapsed before timeout (in seconds).
    """

    def __init__(
        self,
        message: str,
        *,
        name: str | None = None,
        duration: float | None = None,
        elapsed: float | None = None,
    ) -> None:
        super().__init__(message)
        self.name = name
        self.duration = duration
        self.elapsed = elapsed

    def __str__(self) -> str:
        base = super().__str__()
        if self.name and self.duration:
            return f"[{self.name}] {base} (limit: {self.duration}s)"
        return base


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass(frozen=True, slots=True)
class TimeoutConfig:
    """Configuration for timeout behavior.

    Attributes:
        duration: Maximum time allowed for the operation (in seconds).
        cancel_running_future: For async operations, whether to cancel the
            underlying task when timeout occurs. Default True.
        use_signals: For sync operations on Unix, whether to use SIGALRM for
            timeout (more reliable but only works in main thread). If False,
            uses threading-based timeout. Default False.

    Example:
        >>> config = TimeoutConfig(duration=30.0)
        >>> config = TimeoutConfig(duration=5.0, cancel_running_future=False)
    """

    duration: float = 30.0
    cancel_running_future: bool = True
    use_signals: bool = False

    def __post_init__(self) -> None:
        if self.duration <= 0:
            raise ValueError(f"duration must be positive, got {self.duration}")


# ============================================================================
# EVENTS (for observability)
# ============================================================================


class TimeoutEventType(Enum):
    """Types of events emitted by the Timeout."""

    SUCCESS = auto()  # Operation completed within time limit
    TIMEOUT = auto()  # Operation exceeded time limit
    ERROR = auto()  # Operation raised an exception (not timeout)


@dataclass(frozen=True, slots=True)
class TimeoutEvent:
    """Event emitted by the Timeout for observability.

    Attributes:
        event_type: The type of event that occurred.
        name: Name of the timeout instance.
        duration_limit: The configured timeout duration.
        elapsed: Actual time elapsed.
        exception: The exception if event_type is ERROR or TIMEOUT.
    """

    event_type: TimeoutEventType
    name: str
    duration_limit: float
    elapsed: float
    exception: Exception | None = None


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class Timeout(Generic[P, R]):
    """Timeout implementation that limits the execution time of operations.

    Can be used as a decorator or as a context manager for wrapping operations.
    Supports both synchronous and asynchronous functions.

    Example as decorator:
        >>> @Timeout(TimeoutConfig(duration=5.0))
        ... def slow_operation():
        ...     time.sleep(10)  # Will raise OperationTimeout after 5s

    Example with execute:
        >>> t = Timeout(TimeoutConfig(duration=5.0), name="my-timeout")
        >>> result = t.execute(lambda: slow_operation())
    """

    def __init__(
        self,
        config: TimeoutConfig | None = None,
        *,
        name: str | None = None,
    ) -> None:
        """Initialize the Timeout.

        Args:
            config: Configuration for timeout behavior. Uses defaults if None.
            name: Optional name for this timeout instance (for logging/metrics).
        """
        self.config = config or TimeoutConfig()
        self.name = name or "timeout"
        self._event_listeners: list[Callable[[TimeoutEvent], None]] = []
        self._background_tasks: set[asyncio.Task[Any]] = set()

    @property
    def duration(self) -> float:
        """The configured timeout duration in seconds."""
        return self.config.duration

    def on_event(self, listener: Callable[[TimeoutEvent], None]) -> None:
        """Register a listener for timeout events.

        Args:
            listener: Callback function that receives TimeoutEvent objects.
        """
        self._event_listeners.append(listener)

    def _emit_event(self, event: TimeoutEvent) -> None:
        """Emit an event to all registered listeners."""
        notify_listeners(self._event_listeners, event)

    def _on_background_task_done(self, task: asyncio.Task[Any]) -> None:
        """Remove task from tracking set and consume any exception to avoid warnings."""
        self._background_tasks.discard(task)
        if not task.cancelled():
            with contextlib.suppress(asyncio.CancelledError):
                task.exception()

    def execute(self, func: Callable[[], R]) -> R:
        """Execute a callable with timeout protection.

        Args:
            func: A no-argument callable to execute.

        Returns:
            The return value of the callable.

        Raises:
            OperationTimeout: If the operation exceeds the time limit.
        """
        start_time = time.monotonic()

        # Choose timeout strategy
        if self.config.use_signals and _can_use_signals():
            return self._execute_with_signal(func, start_time)
        else:
            return self._execute_with_thread(func, start_time)

    def _execute_with_thread(self, func: Callable[[], R], start_time: float) -> R:
        """Execute using a thread pool for timeout (works everywhere)."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        timed_out = False
        try:
            future = executor.submit(func)
            result = future.result(timeout=self.config.duration)
            elapsed = time.monotonic() - start_time
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.SUCCESS,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                )
            )
            return result
        except concurrent.futures.TimeoutError:
            timed_out = True
            elapsed = time.monotonic() - start_time
            error = OperationTimeout(
                f"Operation timed out after {elapsed:.2f}s",
                name=self.name,
                duration=self.config.duration,
                elapsed=elapsed,
            )
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.TIMEOUT,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                    exception=error,
                )
            )
            raise error from None
        except Exception as e:
            elapsed = time.monotonic() - start_time
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.ERROR,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                    exception=e,
                )
            )
            raise
        finally:
            # wait=False on timeout returns control promptly; worker continues in background
            executor.shutdown(wait=not timed_out)

    def _execute_with_signal(self, func: Callable[[], R], start_time: float) -> R:
        """Execute using SIGALRM for timeout (Unix main thread only)."""

        def _timeout_handler(signum: int, frame: Any) -> None:
            elapsed = time.monotonic() - start_time
            raise OperationTimeout(
                f"Operation timed out after {elapsed:.2f}s",
                name=self.name,
                duration=self.config.duration,
                elapsed=elapsed,
            )

        # Set up signal handler
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, self.config.duration)

        try:
            result = func()
            elapsed = time.monotonic() - start_time
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.SUCCESS,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                )
            )
            return result
        except OperationTimeout as e:
            elapsed = time.monotonic() - start_time
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.TIMEOUT,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                    exception=e,
                )
            )
            raise
        except Exception as e:
            elapsed = time.monotonic() - start_time
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.ERROR,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                    exception=e,
                )
            )
            raise
        finally:
            # Restore original handler and cancel timer
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)

    async def execute_async(self, coro: Any) -> R:
        """Execute a coroutine with timeout protection.

        Args:
            coro: A coroutine to execute.

        Returns:
            The return value of the coroutine.

        Raises:
            OperationTimeout: If the operation exceeds the time limit.
        """
        start_time = time.monotonic()

        if not self.config.cancel_running_future:
            # Create task explicitly to avoid GC: the event loop keeps only weak refs.
            # Without a strong ref, a task created internally by shield() can be
            # garbage-collected when the shield is cancelled on timeout.
            task = asyncio.create_task(coro)
            self._background_tasks.add(task)
            task.add_done_callback(self._on_background_task_done)
            awaitable = asyncio.shield(task)
        else:
            awaitable = coro

        try:
            result = await asyncio.wait_for(awaitable, timeout=self.config.duration)
            elapsed = time.monotonic() - start_time
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.SUCCESS,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                )
            )
            return cast(R, result)
        except asyncio.TimeoutError:
            elapsed = time.monotonic() - start_time
            error = OperationTimeout(
                f"Operation timed out after {elapsed:.2f}s",
                name=self.name,
                duration=self.config.duration,
                elapsed=elapsed,
            )
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.TIMEOUT,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                    exception=error,
                )
            )
            raise error from None
        except Exception as e:
            elapsed = time.monotonic() - start_time
            self._emit_event(
                TimeoutEvent(
                    event_type=TimeoutEventType.ERROR,
                    name=self.name,
                    duration_limit=self.config.duration,
                    elapsed=elapsed,
                    exception=e,
                )
            )
            raise

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Use Timeout as a decorator.

        Example:
            >>> t = Timeout(TimeoutConfig(duration=5.0))
            >>> @t
            ... def my_function():
            ...     ...
        """
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return await self.execute_async(func(*args, **kwargs))

            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return self.execute(lambda: func(*args, **kwargs))

            return sync_wrapper


# ============================================================================
# DECORATOR FACTORY
# ============================================================================


@overload
def timeout(
    func: Callable[P, R],
) -> Callable[P, R]: ...


@overload
def timeout(
    func: None = None,
    *,
    duration: float = 30.0,
    name: str | None = None,
    cancel_running_future: bool = True,
    use_signals: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def timeout(
    func: Callable[P, R] | None = None,
    *,
    duration: float = 30.0,
    name: str | None = None,
    cancel_running_future: bool = True,
    use_signals: bool = False,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to wrap a function with timeout protection.

    Can be used with or without arguments:

        @timeout
        def my_func():
            ...

        @timeout(duration=5.0)
        def my_func():
            ...

        @timeout(duration=10.0, name="api-call")
        async def my_async_func():
            ...

    Args:
        func: The function to wrap (when used without parentheses).
        duration: Maximum time allowed for the operation (in seconds).
        name: Optional name for this timeout instance.
        cancel_running_future: For async, whether to cancel on timeout.
        use_signals: For sync on Unix, whether to use SIGALRM.

    Returns:
        The wrapped function that will raise OperationTimeout if duration exceeded.

        Raises:
            OperationTimeout: When the wrapped function exceeds the time limit.
    """
    config = TimeoutConfig(
        duration=duration,
        cancel_running_future=cancel_running_future,
        use_signals=use_signals,
    )
    timeout_instance: Timeout[P, R] = Timeout(config, name=name)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        return timeout_instance(fn)

    if func is not None:
        # Called as @timeout without parentheses
        return decorator(func)

    # Called as @timeout() or @timeout(duration=5.0)
    return decorator


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _can_use_signals() -> bool:
    """Check if we can use signal-based timeout (Unix main thread only)."""
    if sys.platform == "win32":
        return False
    try:
        return threading.current_thread() is threading.main_thread()
    except RuntimeError:
        return False


def create_timeout(
    config: TimeoutConfig | None = None,
    *,
    name: str,
    register: bool = True,
) -> Timeout[Any, Any]:
    """Create a :class:`Timeout` and optionally register it with :func:`pysilience.core.register`."""
    instance: Timeout[Any, Any] = Timeout(config, name=name)
    if register:
        register_pattern("timeout", name, instance)
    return instance
