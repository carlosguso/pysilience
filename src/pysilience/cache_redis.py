"""
Pysilience - Redis Cache Backend
=================================
A :class:`~pysilience.cache.CacheBackend` backed by Redis, providing
distributed caching with pickle serialisation.

Requires the ``redis`` package (``pip install pysilience[redis]``).

Usage::

    import redis
    from pysilience import Cache, CacheConfig
    from pysilience.cache_redis import RedisBackend

    # Sync-only
    backend = RedisBackend(sync_client=redis.Redis())
    c = Cache(CacheConfig(ttl=300), backend=backend, name="users")
    val = c.execute("user:42", lambda: fetch_user(42))

    # Async-only
    import redis.asyncio as aioredis
    backend = RedisBackend(async_client=aioredis.Redis())
    val = await c.execute_async("user:42", lambda: async_fetch_user(42))

License: MIT
"""

from __future__ import annotations

import pickle
from collections.abc import Hashable
from typing import Any

try:
    import redis
    import redis.asyncio
except ImportError as _import_err:
    raise ImportError(
        "The 'redis' package is required for RedisBackend. "
        "Install it with: pip install pysilience[redis]"
    ) from _import_err

from pysilience.cache import _MISS

__all__ = ["RedisBackend"]


def _serialise_key(key: Hashable, prefix: str) -> str:
    """Convert a hashable cache key to a Redis string key."""
    return prefix + pickle.dumps(key).hex()


class RedisBackend:
    """Redis-backed cache storage.

    At least one of *sync_client* and *async_client* must be provided.
    Calling a sync method when only an async client was given (or vice
    versa) raises :class:`RuntimeError`.

    Args:
        sync_client: A ``redis.Redis`` instance for synchronous operations.
        async_client: A ``redis.asyncio.Redis`` instance for async operations.
        prefix: Key prefix for namespacing within the Redis database.

    Notes:
        * Values are serialised with :mod:`pickle` (protocol 5).
        * ``max_size`` from :class:`~pysilience.cache.CacheConfig` is **not**
          enforced by this backend -- Redis manages its own memory and
          eviction policy.
        * ``ttl`` is applied natively via Redis ``SETEX``.
    """

    __slots__ = ("_sync", "_async", "_prefix")

    def __init__(
        self,
        sync_client: redis.Redis | None = None,  # type: ignore[type-arg]
        async_client: redis.asyncio.Redis | None = None,  # type: ignore[type-arg]
        *,
        prefix: str = "pysilience:",
    ) -> None:
        if sync_client is None and async_client is None:
            raise ValueError("At least one of sync_client or async_client must be provided")
        self._sync = sync_client
        self._async = async_client
        self._prefix = prefix

    def _require_sync(self) -> redis.Redis:  # type: ignore[type-arg]
        if self._sync is None:
            raise RuntimeError(
                "Sync Redis client not available. "
                "Provide sync_client to RedisBackend or use async methods."
            )
        return self._sync

    def _require_async(self) -> redis.asyncio.Redis:  # type: ignore[type-arg]
        if self._async is None:
            raise RuntimeError(
                "Async Redis client not available. "
                "Provide async_client to RedisBackend or use sync methods."
            )
        return self._async

    def _key(self, key: Hashable) -> str:
        return _serialise_key(key, self._prefix)

    # -- sync ----------------------------------------------------------------

    def get(self, key: Hashable) -> Any:
        """Fetch *key* from Redis.  Returns ``_MISS`` if absent."""
        client = self._require_sync()
        raw: bytes | None = client.get(self._key(key))
        if raw is None:
            return _MISS
        return pickle.loads(raw)  # noqa: S301

    def put(self, key: Hashable, value: Any, ttl: float | None = None) -> None:
        """Store *value* under *key*, optionally with a TTL (seconds)."""
        client = self._require_sync()
        data = pickle.dumps(value, protocol=5)
        if ttl is not None:
            client.setex(self._key(key), int(max(ttl, 1)), data)
        else:
            client.set(self._key(key), data)

    def delete(self, key: Hashable) -> bool:
        """Remove *key*.  Returns ``True`` if it existed."""
        client = self._require_sync()
        return client.delete(self._key(key)) > 0

    def clear(self) -> None:
        """Remove all keys matching this backend's prefix.

        Uses ``SCAN`` to avoid blocking Redis with ``KEYS``.
        """
        client = self._require_sync()
        cursor: int = 0
        pattern = self._prefix + "*"
        while True:
            cursor, keys = client.scan(cursor=cursor, match=pattern, count=500)
            if keys:
                client.delete(*keys)
            if cursor == 0:
                break

    # -- async ---------------------------------------------------------------

    async def aget(self, key: Hashable) -> Any:
        """Async :meth:`get`."""
        client = self._require_async()
        raw: bytes | None = await client.get(self._key(key))
        if raw is None:
            return _MISS
        return pickle.loads(raw)  # noqa: S301

    async def aput(self, key: Hashable, value: Any, ttl: float | None = None) -> None:
        """Async :meth:`put`."""
        client = self._require_async()
        data = pickle.dumps(value, protocol=5)
        if ttl is not None:
            await client.setex(self._key(key), int(max(ttl, 1)), data)
        else:
            await client.set(self._key(key), data)

    async def adelete(self, key: Hashable) -> bool:
        """Async :meth:`delete`."""
        client = self._require_async()
        return await client.delete(self._key(key)) > 0

    async def aclear(self) -> None:
        """Async :meth:`clear`."""
        client = self._require_async()
        cursor: int = 0
        pattern = self._prefix + "*"
        while True:
            cursor, keys = await client.scan(cursor=cursor, match=pattern, count=500)
            if keys:
                await client.delete(*keys)
            if cursor == 0:
                break
