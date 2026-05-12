"""
Pysilience - Cache Pattern
==========================
Caches function results with pluggable storage backends.  Ships with an
in-memory LRU backend (``MemoryBackend``) and supports external backends
such as Redis (see ``pysilience.cache_redis``).

Usage:
    from pysilience import cache, CacheConfig

    @cache(max_size=128, ttl=60.0)
    def fetch_user(user_id: int) -> dict:
        ...

    @cache(max_size=64, ttl=30.0)
    async def fetch_user_async(user_id: int) -> dict:
        ...

    # Use ``Cache`` directly for explicit key control:
    c = Cache(CacheConfig(max_size=100, ttl=60.0), name="users")
    result = c.execute("user:42", lambda: fetch_user(42))

    # Plug in a Redis backend:
    from pysilience.cache_redis import RedisBackend
    rb = RedisBackend(sync_client=redis_conn, prefix="app:")
    c = Cache(CacheConfig(ttl=60.0), backend=rb, name="users")

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
from typing import Any, Generic, ParamSpec, Protocol, TypeVar, overload, runtime_checkable

from pysilience.core.listeners import notify_listeners
from pysilience.core.registry import register as register_pattern

__all__ = [
    "cache",
    "Cache",
    "CacheBackend",
    "CacheConfig",
    "CacheEvent",
    "CacheEventType",
    "create_cache",
    "MemoryBackend",
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
            least-recently-used entry is evicted.  Must be >= 1.  Only used
            by :class:`MemoryBackend`; external backends manage their own
            eviction.
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
# INTERNAL HELPERS
# ============================================================================


@dataclass(slots=True)
class _CacheEntry:
    value: Any
    cached_at: float


_MISS: Any = object()
"""Sentinel returned by :class:`CacheBackend` methods to signal a cache miss.

External backend implementations should import this sentinel and return it
from :meth:`~CacheBackend.get` / :meth:`~CacheBackend.aget` when the
requested key is not present.
"""


# ============================================================================
# BACKEND PROTOCOL
# ============================================================================


@runtime_checkable
class CacheBackend(Protocol):
    """Protocol that cache storage backends must satisfy.

    Sync methods (``get``, ``put``, ``delete``, ``clear``) are used by
    :meth:`Cache.execute`.  Async methods (``aget``, ``aput``, ``adelete``,
    ``aclear``) are used by :meth:`Cache.execute_async`.

    Return ``_MISS`` (importable from ``pysilience.cache``) from ``get`` /
    ``aget`` when the key is absent so that cached ``None`` values can be
    distinguished from true misses.
    """

    def get(self, key: Hashable) -> Any: ...
    def put(self, key: Hashable, value: Any, ttl: float | None = None) -> None: ...
    def delete(self, key: Hashable) -> bool: ...
    def clear(self) -> None: ...
    async def aget(self, key: Hashable) -> Any: ...
    async def aput(self, key: Hashable, value: Any, ttl: float | None = None) -> None: ...
    async def adelete(self, key: Hashable) -> bool: ...
    async def aclear(self) -> None: ...


# ============================================================================
# MEMORY BACKEND (default)
# ============================================================================


class MemoryBackend:
    """In-memory LRU cache backend with optional TTL.

    This is the default backend created by :class:`Cache` when no explicit
    backend is provided.  It stores entries in an :class:`~collections.OrderedDict`
    and evicts the least-recently-used entry when ``max_size`` is exceeded.

    .. note::
        This backend is **not** independently thread-safe.  The
        :class:`Cache` class serialises access via its own lock.

    Args:
        max_size: Maximum entries before LRU eviction.  Must be >= 1.
        ttl: Time-to-live in seconds, or ``None`` for no expiry.
    """

    __slots__ = ("_max_size", "_ttl", "_store")

    def __init__(self, max_size: int = 128, ttl: float | None = None) -> None:
        self._max_size = max_size
        self._ttl = ttl
        self._store: OrderedDict[Hashable, _CacheEntry] = OrderedDict()

    @property
    def size(self) -> int:
        """Number of entries currently stored."""
        return len(self._store)

    def _is_expired(self, entry: _CacheEntry) -> bool:
        if self._ttl is None:
            return False
        return (time.monotonic() - entry.cached_at) >= self._ttl

    # -- sync ----------------------------------------------------------------

    def get(self, key: Hashable) -> Any:
        """Return cached value or ``_MISS``."""
        entry = self._store.get(key)
        if entry is None:
            return _MISS
        if self._is_expired(entry):
            del self._store[key]
            return _MISS
        self._store.move_to_end(key)
        return entry.value

    def put(self, key: Hashable, value: Any, ttl: float | None = None) -> None:
        """Store *value* under *key*, evicting LRU entries as needed."""
        if key in self._store:
            self._store.move_to_end(key)
            self._store[key] = _CacheEntry(value=value, cached_at=time.monotonic())
        else:
            while len(self._store) >= self._max_size:
                self._store.popitem(last=False)
            self._store[key] = _CacheEntry(value=value, cached_at=time.monotonic())

    def delete(self, key: Hashable) -> bool:
        """Remove *key*.  Returns ``True`` if it existed."""
        try:
            del self._store[key]
        except KeyError:
            return False
        return True

    def clear(self) -> None:
        """Remove all entries."""
        self._store.clear()

    # -- async (delegates to sync; no I/O) -----------------------------------

    async def aget(self, key: Hashable) -> Any:
        return self.get(key)

    async def aput(self, key: Hashable, value: Any, ttl: float | None = None) -> None:
        self.put(key, value, ttl)

    async def adelete(self, key: Hashable) -> bool:
        return self.delete(key)

    async def aclear(self) -> None:
        self.clear()


# ============================================================================
# CACHE IMPLEMENTATION
# ============================================================================


class Cache(Generic[P, R]):
    """Cache function results with pluggable storage backends.

    Use as a decorator (keys are derived from arguments automatically) or call
    ``execute`` / ``execute_async`` with an explicit key and callable.

    Concurrent callers requesting the same key are coalesced: only one
    invocation of the underlying function runs while the others wait for
    its result (thundering-herd protection).

    Args:
        config: Cache configuration.  Defaults to ``CacheConfig()``.
        name: Human-readable name for event reporting.
        backend: Storage backend.  When ``None`` a :class:`MemoryBackend` is
            created using ``config.max_size`` and ``config.ttl``.

    Example:
        >>> c = Cache(CacheConfig(max_size=100, ttl=60.0), name="users")
        >>> result = c.execute("user:42", lambda: fetch_user(42))
    """

    def __init__(
        self,
        config: CacheConfig | None = None,
        *,
        name: str | None = None,
        backend: CacheBackend | None = None,
    ) -> None:
        self.config = config or CacheConfig()
        self.name = name or "cache"
        if backend is None:
            self._backend: CacheBackend = MemoryBackend(
                max_size=self.config.max_size,
                ttl=self.config.ttl,
            )
        else:
            self._backend = backend
        self._is_memory = isinstance(self._backend, MemoryBackend)
        self._event_listeners: list[Callable[[CacheEvent], None]] = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._inflight: set[Hashable] = set()
        self._async_inflight: dict[Hashable, asyncio.Event] = {}

    @property
    def backend(self) -> CacheBackend:
        """The storage backend in use."""
        return self._backend

    @property
    def size(self) -> int:
        """Current number of entries in the cache.

        Only supported when the backend exposes a ``size`` attribute
        (e.g. :class:`MemoryBackend`).
        """
        with self._lock:
            return self._backend.size  # type: ignore[union-attr]

    def on_event(self, listener: Callable[[CacheEvent], None]) -> None:
        """Register a listener for cache events."""
        self._event_listeners.append(listener)

    def _emit_event(self, event: CacheEvent) -> None:
        notify_listeners(self._event_listeners, event)

    # -- sync coordination ---------------------------------------------------

    def _sync_check_or_reserve(self, key: Hashable) -> Any:
        """Return cached value, or mark *key* in-flight and return ``_MISS``.

        If another thread is already computing *key*, blocks until that
        thread finishes and then re-checks the cache.
        """
        with self._condition:
            while True:
                value = self._backend.get(key)
                if value is not _MISS:
                    return value
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
            self._backend.put(key, value, ttl=self.config.ttl)
            self._inflight.discard(key)
            self._condition.notify_all()

    # -- async coordination --------------------------------------------------

    async def _async_check_or_reserve(self, key: Hashable) -> Any:
        """Async counterpart of :meth:`_sync_check_or_reserve`.

        For :class:`MemoryBackend` (no I/O) the sync ``get`` is called under
        ``self._lock`` for thread safety.  For I/O backends the async ``aget``
        is called outside the lock to avoid blocking the event loop.
        """
        if self._is_memory:
            while True:
                with self._lock:
                    value = self._backend.get(key)
                    if value is not _MISS:
                        return value
                    if key not in self._async_inflight:
                        self._async_inflight[key] = asyncio.Event()
                        return _MISS
                    event = self._async_inflight[key]
                await event.wait()
        else:
            value = await self._backend.aget(key)
            if value is not _MISS:
                return value
            while True:
                with self._lock:
                    if key not in self._async_inflight:
                        self._async_inflight[key] = asyncio.Event()
                        return _MISS
                    event = self._async_inflight[key]
                await event.wait()
                value = await self._backend.aget(key)
                if value is not _MISS:
                    return value

    def _async_unreserve(self, key: Hashable) -> None:
        """Remove *key* from async in-flight and wake waiting coroutines."""
        with self._lock:
            event = self._async_inflight.pop(key, None)
        if event is not None:
            event.set()

    async def _async_store_and_unreserve(self, key: Hashable, value: Any) -> None:
        """Cache *value*, remove *key* from async in-flight, and wake waiters.

        For :class:`MemoryBackend` the sync ``put`` is called under the lock.
        For I/O backends the async ``aput`` is awaited outside the lock to keep
        the event loop responsive.
        """
        if self._is_memory:
            with self._lock:
                self._backend.put(key, value, ttl=self.config.ttl)
                event = self._async_inflight.pop(key, None)
        else:
            await self._backend.aput(key, value, ttl=self.config.ttl)
            with self._lock:
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
        await self._async_store_and_unreserve(key, result)
        self._emit_event(
            CacheEvent(event_type=CacheEventType.MISS, name=self.name, key=key)
        )
        return result

    def invalidate(self, key: Hashable) -> bool:
        """Remove *key* from the cache.  Returns ``True`` if the key existed."""
        with self._lock:
            return self._backend.delete(key)

    def invalidate_all(self) -> None:
        """Remove all entries from the cache."""
        with self._lock:
            self._backend.clear()

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
    backend: CacheBackend | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def cache(
    func: Callable[P, R] | None = None,
    *,
    max_size: int = 128,
    ttl: float | None = None,
    name: str | None = None,
    backend: CacheBackend | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to cache function results with LRU eviction and optional TTL.

    Bare ``@cache`` uses defaults.  Use parameters for custom behavior::

        @cache(max_size=256, ttl=30.0)
        def fetch_data(key: str) -> dict:
            ...

    Arguments to the decorated function must be hashable (used as cache key).
    """
    config = CacheConfig(max_size=max_size, ttl=ttl)
    instance: Cache[Any, Any] = Cache(config, name=name, backend=backend)

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
    backend: CacheBackend | None = None,
) -> Cache[Any, Any]:
    """Create a :class:`Cache` and optionally register it with :func:`pysilience.core.register`."""
    instance: Cache[Any, Any] = Cache(config, name=name, backend=backend)
    if register:
        register_pattern("cache", name, instance)
    return instance
