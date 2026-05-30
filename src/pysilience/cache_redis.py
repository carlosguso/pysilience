"""
Pysilience - Redis Cache Backend
=================================
A :class:`~pysilience.cache.CacheBackend` backed by Redis, providing
distributed caching with pluggable value serialisation.

Requires the ``redis`` package (``pip install pysilience[redis]``).

Usage::

    import redis
    from pysilience import Cache, CacheConfig
    from pysilience.cache_redis import RedisBackend
    from pysilience.cache_serializer_hmac import HmacPickleSerializer

    # Default: JSON (safe for untrusted Redis data)
    backend = RedisBackend(sync_client=redis.Redis())
    c = Cache(CacheConfig(ttl=300), backend=backend, name="users")
    val = c.execute("user:42", lambda: fetch_user(42))

    # HMAC-signed pickle for arbitrary Python objects
    serializer = HmacPickleSerializer(secret=b"my-secret-key")
    backend = RedisBackend(sync_client=redis.Redis(), serializer=serializer)

    # MessagePack (compact binary; pip install pysilience[msgpack])
    from datetime import datetime
    from pysilience.cache_serializer_msgpack import MsgpackSerializer
    from pysilience.cache_serializer_msgpack_builtins import (
        pack_datetime, unpack_datetime,
    )
    serializer = MsgpackSerializer()
    serializer.register_type(datetime, type_id=64, pack=pack_datetime, unpack=unpack_datetime)
    backend = RedisBackend(sync_client=redis.Redis(), serializer=serializer)

    # Custom serializer
    class PlainSerializer:
        def dumps(self, value): return str(value).encode()
        def loads(self, raw): return raw.decode()

    backend = RedisBackend(sync_client=redis.Redis(), serializer=PlainSerializer())

    # Async-only
    import redis.asyncio as aioredis
    backend = RedisBackend(async_client=aioredis.Redis())
    val = await c.execute_async("user:42", lambda: async_fetch_user(42))

Security note
-------------
The default :class:`~pysilience.cache_serializer_json.JsonSerializer` uses
:mod:`json`, which is safe to deserialize from an untrusted Redis server.
For arbitrary Python objects, use
:class:`~pysilience.cache_serializer_hmac.HmacPickleSerializer` instead — it
signs pickle payloads with HMAC-SHA256 so tampered data is rejected before
unpickling.

**HmacPickleSerializer requires a signing secret.**  Pass ``secret=b'...'`` or
set the ``PYSILIENCE_CACHE_SECRET`` environment variable.  Generate a suitable
value with::

    python -c "import secrets; print(secrets.token_hex(32))"

License: MIT
"""

from __future__ import annotations

import hashlib
import logging
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
from pysilience.cache_serializer import CacheSerializer
from pysilience.cache_serializer_hmac import HmacPickleSerializer
from pysilience.cache_serializer_json import JsonSerializer

__all__ = ["RedisBackend", "CacheSerializer", "HmacPickleSerializer", "JsonSerializer"]

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Key serialisation
# ---------------------------------------------------------------------------


def _serialise_key(key: Hashable, prefix: str) -> str:
    """Convert a hashable cache key to a stable Redis string key.

    Uses a SHA-256 hex digest of the key's ``repr`` so the output is:
    - version-independent (no pickle format drift across Python releases)
    - fixed-length (64 hex chars) regardless of key complexity
    - safe for use as a Redis key without escaping
    """
    key_bytes = repr(key).encode()
    digest = hashlib.sha256(key_bytes).hexdigest()
    return prefix + digest


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class RedisBackend:
    """Redis-backed cache storage with pluggable value serialisation.

    At least one of *sync_client* and *async_client* must be provided.
    Calling a sync method when only an async client was given (or vice
    versa) raises :class:`RuntimeError`.

    Args:
        sync_client: A ``redis.Redis`` instance for synchronous operations.
        async_client: A ``redis.asyncio.Redis`` instance for async operations.
        prefix: Key prefix for namespacing within the Redis database.
        serializer: A :class:`~pysilience.cache_serializer.CacheSerializer` used to
            encode and decode stored values.  Defaults to
            :class:`~pysilience.cache_serializer_json.JsonSerializer`.

    Notes:
        * By default, values are serialised as JSON.  Invalid entries are logged
          as a warning and treated as a cache miss.
        * For arbitrary Python objects, pass
          :class:`~pysilience.cache_serializer_hmac.HmacPickleSerializer` as
          *serializer*.
        * ``max_size`` from :class:`~pysilience.cache.CacheConfig` is **not**
          enforced by this backend — Redis manages its own memory and eviction
          policy.
        * ``ttl`` is applied natively via Redis ``SETEX``.
    """

    __slots__ = ("_sync", "_async", "_prefix", "_serializer")

    def __init__(
        self,
        sync_client: redis.Redis | None = None,  # type: ignore[type-arg]
        async_client: redis.asyncio.Redis | None = None,  # type: ignore[type-arg]
        *,
        prefix: str = "pysilience:",
        serializer: CacheSerializer | None = None,
    ) -> None:
        if sync_client is None and async_client is None:
            raise ValueError("At least one of sync_client or async_client must be provided")

        if serializer is not None and not isinstance(serializer, CacheSerializer):
            raise TypeError(
                f"serializer must implement CacheSerializer (dumps/loads), "
                f"got {type(serializer).__name__!r}"
            )

        self._sync = sync_client
        self._async = async_client
        self._prefix = prefix
        self._serializer: CacheSerializer = (
            serializer if serializer is not None else JsonSerializer()
        )

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

    def _load(self, raw: bytes, key: Hashable) -> Any:
        """Deserialize *raw*; return ``_MISS`` on failure."""
        try:
            return self._serializer.loads(raw)
        except ValueError as exc:
            _log.warning(
                "RedisBackend: discarding cache entry for key %r — %s", key, exc
            )
            return _MISS

    # -- sync ----------------------------------------------------------------

    def get(self, key: Hashable) -> Any:
        """Fetch *key* from Redis.  Returns ``_MISS`` if absent or invalid."""
        client = self._require_sync()
        raw: bytes | None = client.get(self._key(key))
        if raw is None:
            return _MISS
        return self._load(raw, key)

    def put(self, key: Hashable, value: Any, ttl: float | None = None) -> None:
        """Store *value* under *key*, optionally with a TTL (seconds)."""
        client = self._require_sync()
        data = self._serializer.dumps(value)
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

        Note: keys inserted between SCAN pages may not be removed.
        For production use, consider prefix rotation instead of ``clear()``:
        ``backend._prefix = f"pysilience:{int(time.time())}:"``
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
        return self._load(raw, key)

    async def aput(self, key: Hashable, value: Any, ttl: float | None = None) -> None:
        """Async :meth:`put`."""
        client = self._require_async()
        data = self._serializer.dumps(value)
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
