"""
Pysilience - Cache Pattern
==========================
Caches function results with configurable max size (LRU eviction) and
optional TTL (stdlib only; Python 3.10+).

Usage:
    from cache import cache, CacheConfig

    @cache(max_size=128, ttl=60.0)
    def fetch_user(user_id: int) -> dict:
        ...

    @cache(max_size=64, ttl=30.0)
    async def fetch_user_async(user_id: int) -> dict:
        ...

    # Use ``Cache`` directly for explicit key control:
    c = Cache(CacheConfig(max_size=100, ttl=60.0), name="users")
    result = c.execute("user:42", lambda: fetch_user(42))

License: MIT
"""

from __future__ import annotations

import asyncio
import functools
import threading
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generic, ParamSpec, TypeVar, overload

from pysilience.core.listeners import notify_listeners
from pysilience.core.registry import register as register_pattern

__all__ = [
    "cache",
    "Cache",
    "CacheConfig",
    "CacheEvent",
    "CacheEventType",
    "create_cache",
]

P = ParamSpec("P")
R = TypeVar("R")


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass(frozen=True, slots=True)
class CacheConfig:
    """Configuration for cache behavior.

    Attributes:
        max_size: Maximum number of entries in the cache.  When exceeded the
            least-recently-used entry is evicted.  Must be >= 1.
        ttl: Time-to-live for cache entries in seconds.  ``None`` means entries
            never expire based on time.

    Example:
        >>> config = CacheConfig(max_size=256, ttl=120.0)
    """

    max_size: int = 128
    ttl: float | None = None

    def __post_init__(self) -> None:
        if self.max_size < 1:
            raise ValueError(f"max_size must be >= 1, got {self.max_size}")
        if self.ttl is not None and self.ttl <= 0:
            raise ValueError(f"ttl must be positive when set, got {self.ttl}")


# ============================================================================
# EVENTS (for observability)
# ============================================================================


class CacheEventType(Enum):
    """Types of events emitted by Cache."""

    HIT = auto()
    MISS = auto()
    ERROR = auto()


@dataclass(frozen=True, slots=True)
class CacheEvent:
    """Event emitted by Cache for observability."""

    event_type: CacheEventType
    name: str
    key: Hashable
    exception: BaseException | None = None


# ============================================================================
# INTERNAL
# ============================================================================


@dataclass(slots=True)
class _CacheEntry:
    value: Any
    cached_at: float


_MISS: Any = object()


# ============================================================================
# IMPLEMENTATION
# ============================================================================


class Cache(Generic[P, R]):
    """Cache function results with LRU eviction and optional TTL.

    Use as a decorator (keys are derived from arguments automatically) or call
    ``execute`` / ``execute_async`` with an explicit key and callable.

    Concurrent callers requesting the same key are coalesced: only one
    invocation of the underlying function runs while the others wait for
    its result (thundering-herd protection).

    Example:
        >>> c = Cache(CacheConfig(max_size=100, ttl=60.0), name="users")
        >>> result = c.execute("user:42", lambda: fetch_user(42))
    """

    def __init__(
        self,
        config: CacheConfig | None = None,
        *,
        name: str | None = None,
    ) -> None:
        self.config = config or CacheConfig()
        self.name = name or "cache"
        self._event_listeners: list[Callable[[CacheEvent], None]] = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._store: OrderedDict[Hashable, _CacheEntry] = OrderedDict()
        self._inflight: set[Hashable] = set()
        self._async_inflight: dict[Hashable, asyncio.Event] = {}

    @property
    def size(self) -> int:
        """Current number of entries in the cache."""
        with self._lock:
            return len(self._store)

    def on_event(self, listener: Callable[[CacheEvent], None]) -> None:
        """Register a listener for cache events."""
        self._event_listeners.append(listener)

    def _emit_event(self, event: CacheEvent) -> None:
        notify_listeners(self._event_listeners, event)

    def _is_expired(self, entry: _CacheEntry) -> bool:
        if self.config.ttl is None:
            return False
        return (time.monotonic() - entry.cached_at) >= self.config.ttl

    # -- lock-internal helpers (caller must hold ``_lock``) ------------------

    def _lookup(self, key: Hashable) -> _CacheEntry | None:
        """Return cached entry for *key* or ``None``.  Caller must hold ``_lock``."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            del self._store[key]
            return None
        self._store.move_to_end(key)
        return entry

    def _insert(self, key: Hashable, value: Any) -> None:
        """Store *value* under *key* with LRU eviction.  Caller must hold ``_lock``."""
        if key in self._store:
            self._store.move_to_end(key)
            self._store[key] = _CacheEntry(value=value, cached_at=time.monotonic())
        else:
            while len(self._store) >= self.config.max_size:
                self._store.popitem(last=False)
            self._store[key] = _CacheEntry(value=value, cached_at=time.monotonic())

    # -- sync coordination ---------------------------------------------------

    def _sync_check_or_reserve(self, key: Hashable) -> Any:
        """Return cached value, or mark *key* in-flight and return ``_MISS``.

        If another thread is already computing *key*, blocks until that
        thread finishes and then re-checks the cache.
        """
        with self._condition:
            while True:
                entry = self._lookup(key)
                if entry is not None:
                    return entry.value
                if key not in self._inflight:
                    self._inflight.add(key)
                    return _MISS
                self._condition.wait()

    def _sync_unreserve(self, key: Hashable) -> None:
        """Remove *key* from the in-flight set and wake waiters (error path)."""
        with self._condition:
            self._inflight.discard(key)
            self._condition.notify_all()

    def _sync_store_and_unreserve(self, key: Hashable, value: Any) -> None:
        """Cache *value*, remove *key* from in-flight, and wake waiters."""
        with self._condition:
            self._insert(key, value)
            self._inflight.discard(key)
            self._condition.notify_all()

    # -- async coordination --------------------------------------------------

    async def _async_check_or_reserve(self, key: Hashable) -> Any:
        """Async counterpart of :meth:`_sync_check_or_reserve`.

        Uses per-key :class:`asyncio.Event` objects so waiting coroutines
        yield to the event loop instead of blocking a thread.
        """
        while True:
            with self._lock:
                entry = self._lookup(key)
                if entry is not None:
                    return entry.value
                if key not in self._async_inflight:
                    self._async_inflight[key] = asyncio.Event()
                    return _MISS
                event = self._async_inflight[key]
            await event.wait()

    def _async_unreserve(self, key: Hashable) -> None:
        """Remove *key* from async in-flight and wake waiting coroutines."""
        with self._lock:
            event = self._async_inflight.pop(key, None)
        if event is not None:
            event.set()

    def _async_store_and_unreserve(self, key: Hashable, value: Any) -> None:
        """Cache *value*, remove *key* from async in-flight, and wake waiters."""
        with self._lock:
            self._insert(key, value)
            event = self._async_inflight.pop(key, None)
        if event is not None:
            event.set()

    # -- public execute methods ----------------------------------------------

    def execute(self, key: Hashable, func: Callable[[], R]) -> R:
        """Return the cached value for *key*, or call *func* and cache the result.

        Concurrent threads requesting the same *key* are coalesced: only
        one thread invokes *func* while the others block and receive its
        cached result.
        """
        cached = self._sync_check_or_reserve(key)
        if cached is not _MISS:
            self._emit_event(
                CacheEvent(event_type=CacheEventType.HIT, name=self.name, key=key)
            )
            return cached  # type: ignore[no-any-return]
        try:
            result = func()
        except Exception as exc:
            self._sync_unreserve(key)
            self._emit_event(
                CacheEvent(
                    event_type=CacheEventType.ERROR,
                    name=self.name,
                    key=key,
                    exception=exc,
                )
            )
            raise
        self._sync_store_and_unreserve(key, result)
        self._emit_event(
            CacheEvent(event_type=CacheEventType.MISS, name=self.name, key=key)
        )
        return result

    async def execute_async(self, key: Hashable, factory: Callable[[], Awaitable[R]]) -> R:
        """Async version of :meth:`execute`.

        Concurrent coroutines requesting the same *key* are coalesced:
        only one coroutine invokes *factory* while the others await its
        cached result.
        """
        cached = await self._async_check_or_reserve(key)
        if cached is not _MISS:
            self._emit_event(
                CacheEvent(event_type=CacheEventType.HIT, name=self.name, key=key)
            )
            return cached  # type: ignore[no-any-return]
        try:
            result = await factory()
        except Exception as exc:
            self._async_unreserve(key)
            self._emit_event(
                CacheEvent(
                    event_type=CacheEventType.ERROR,
                    name=self.name,
                    key=key,
                    exception=exc,
                )
            )
            raise
        self._async_store_and_unreserve(key, result)
        self._emit_event(
            CacheEvent(event_type=CacheEventType.MISS, name=self.name, key=key)
        )
        return result

    def invalidate(self, key: Hashable) -> bool:
        """Remove *key* from the cache.  Returns ``True`` if the key existed."""
        with self._lock:
            try:
                del self._store[key]
            except KeyError:
                return False
            return True

    def invalidate_all(self) -> None:
        """Remove all entries from the cache."""
        with self._lock:
            self._store.clear()

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Use Cache as a decorator.  Keys are derived from function arguments.

        Arguments must be hashable; a ``TypeError`` is raised otherwise.
        """
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                key = _make_key(args, kwargs)
                return await self.execute_async(key, lambda: func(*args, **kwargs))

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            key = _make_key(args, kwargs)
            return self.execute(key, lambda: func(*args, **kwargs))

        return sync_wrapper


# ============================================================================
# DECORATOR FACTORY
# ============================================================================


@overload
def cache(
    func: Callable[P, R],
) -> Callable[P, R]: ...


@overload
def cache(
    func: None = None,
    *,
    max_size: int = 128,
    ttl: float | None = None,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def cache(
    func: Callable[P, R] | None = None,
    *,
    max_size: int = 128,
    ttl: float | None = None,
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to cache function results with LRU eviction and optional TTL.

    Bare ``@cache`` uses defaults.  Use parameters for custom behavior::

        @cache(max_size=256, ttl=30.0)
        def fetch_data(key: str) -> dict:
            ...

    Arguments to the decorated function must be hashable (used as cache key).
    """
    config = CacheConfig(max_size=max_size, ttl=ttl)
    instance: Cache[Any, Any] = Cache(config, name=name)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        return instance(fn)

    if func is not None:
        return decorator(func)
    return decorator


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _make_key(args: Any, kwargs: Any) -> Hashable:
    """Derive a hashable cache key from function arguments."""
    if kwargs:
        return (*args, tuple(sorted(kwargs.items())))
    return tuple(args)


def create_cache(
    config: CacheConfig | None = None,
    *,
    name: str,
    register: bool = True,
) -> Cache[Any, Any]:
    """Create a :class:`Cache` and optionally register it with :func:`pysilience.core.register`."""
    instance: Cache[Any, Any] = Cache(config, name=name)
    if register:
        register_pattern("cache", name, instance)
    return instance
