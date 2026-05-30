"""
Tests for the Redis cache backend.

All Redis I/O is mocked -- no running Redis instance is needed.

Run with: pytest tests/test_cache_redis.py -v
"""

from __future__ import annotations

import asyncio
import json
import os
import pickle
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pysilience.cache import (
    _MISS,
    Cache,
    CacheConfig,
    CacheEvent,
    CacheEventType,
)
from pysilience.cache_redis import RedisBackend, _serialise_key
from pysilience.cache_serializer import CacheSerializer, HmacPickleSerializer

# ---------------------------------------------------------------------------
# Shared test secret
# ---------------------------------------------------------------------------

_SECRET = b"test-secret-key-for-unit-tests"
_SERIALIZER = HmacPickleSerializer(secret=_SECRET)


def _signed(value: Any) -> bytes:
    """Helper: pickle *value* and sign it as the backend would."""
    return _SERIALIZER.dumps(value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sync_client() -> MagicMock:
    """Return a mock ``redis.Redis`` instance."""
    client = MagicMock()
    client.get.return_value = None
    client.set.return_value = True
    client.setex.return_value = True
    client.delete.return_value = 0
    client.scan.return_value = (0, [])
    return client


def _make_async_client() -> MagicMock:
    """Return a mock ``redis.asyncio.Redis`` instance with async methods."""
    client = MagicMock()
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.setex = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=0)
    client.scan = AsyncMock(return_value=(0, []))
    return client


# ============================================================================
# CONSTRUCTION
# ============================================================================


class TestRedisBackendConstruction:
    def test_requires_at_least_one_client(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            RedisBackend(secret=_SECRET)

    def test_requires_secret(self) -> None:
        """Construction must fail when no secret is provided and env var is absent."""
        env_backup = os.environ.pop("PYSILIENCE_CACHE_SECRET", None)
        try:
            with pytest.raises(ValueError, match="signing secret"):
                RedisBackend(sync_client=_make_sync_client())
        finally:
            if env_backup is not None:
                os.environ["PYSILIENCE_CACHE_SECRET"] = env_backup

    def test_secret_from_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Construction succeeds when secret comes from the environment variable."""
        monkeypatch.setenv("PYSILIENCE_CACHE_SECRET", "env-secret-value")
        backend = RedisBackend(sync_client=_make_sync_client())
        assert isinstance(backend._serializer, HmacPickleSerializer)
        assert backend._serializer._secret == b"env-secret-value"

    def test_explicit_secret_beats_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The constructor *secret* arg takes priority over the env var."""
        monkeypatch.setenv("PYSILIENCE_CACHE_SECRET", "env-secret")
        backend = RedisBackend(sync_client=_make_sync_client(), secret=b"explicit-secret")
        assert backend._serializer._secret == b"explicit-secret"

    def test_secret_str_is_encoded(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client(), secret="str-secret")
        assert backend._serializer._secret == b"str-secret"

    def test_explicit_serializer(self) -> None:
        serializer = HmacPickleSerializer(secret=b"custom")
        backend = RedisBackend(sync_client=_make_sync_client(), serializer=serializer)
        assert backend._serializer is serializer

    def test_custom_serializer(self) -> None:
        """Users can plug in their own CacheSerializer implementation."""

        class JsonSerializer:
            def dumps(self, value: Any) -> bytes:
                return json.dumps(value).encode()

            def loads(self, raw: bytes) -> Any:
                return json.loads(raw.decode())

        client = _make_sync_client()
        serializer = JsonSerializer()
        backend = RedisBackend(sync_client=client, serializer=serializer)

        backend.put("k", {"a": 1})
        raw = client.set.call_args[0][1]
        assert raw == b'{"a": 1}'

        client.get.return_value = raw
        assert backend.get("k") == {"a": 1}

    def test_invalid_serializer_rejected(self) -> None:
        with pytest.raises(TypeError, match="CacheSerializer"):
            RedisBackend(sync_client=_make_sync_client(), serializer=object())  # type: ignore[arg-type]

    def test_sync_only(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client(), secret=_SECRET)
        assert backend._sync is not None
        assert backend._async is None

    def test_async_only(self) -> None:
        backend = RedisBackend(async_client=_make_async_client(), secret=_SECRET)
        assert backend._sync is None
        assert backend._async is not None

    def test_both_clients(self) -> None:
        backend = RedisBackend(
            sync_client=_make_sync_client(),
            async_client=_make_async_client(),
            secret=_SECRET,
        )
        assert backend._sync is not None
        assert backend._async is not None

    def test_default_prefix(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client(), secret=_SECRET)
        assert backend._prefix == "pysilience:"

    def test_custom_prefix(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client(), prefix="myapp:", secret=_SECRET)
        assert backend._prefix == "myapp:"


# ============================================================================
# KEY SERIALISATION
# ============================================================================


class TestKeySerialization:
    def test_string_key(self) -> None:
        result = _serialise_key("hello", "pfx:")
        assert result.startswith("pfx:")
        assert len(result) > len("pfx:")

    def test_tuple_key(self) -> None:
        result = _serialise_key((1, "a", 2.0), "pfx:")
        assert result.startswith("pfx:")

    def test_deterministic(self) -> None:
        a = _serialise_key(("user", 42), "p:")
        b = _serialise_key(("user", 42), "p:")
        assert a == b

    def test_different_keys_differ(self) -> None:
        a = _serialise_key("key_a", "p:")
        b = _serialise_key("key_b", "p:")
        assert a != b

    def test_fixed_length_hash_suffix(self) -> None:
        """The suffix is always a 64-char hex SHA-256 digest."""
        result = _serialise_key(("user", 42), "pfx:")
        suffix = result[len("pfx:"):]
        assert len(suffix) == 64
        int(suffix, 16)  # must be valid hex


# ============================================================================
# HMAC PICKLE SERIALIZER
# ============================================================================


class TestHmacPickleSerializer:
    def test_implements_cache_serializer_protocol(self) -> None:
        assert isinstance(HmacPickleSerializer(secret=_SECRET), CacheSerializer)

    def test_requires_secret(self) -> None:
        env_backup = os.environ.pop("PYSILIENCE_CACHE_SECRET", None)
        try:
            with pytest.raises(ValueError, match="signing secret"):
                HmacPickleSerializer()
        finally:
            if env_backup is not None:
                os.environ["PYSILIENCE_CACHE_SECRET"] = env_backup

    def test_sign_then_verify(self) -> None:
        value = {"hello": "world", "n": 42}
        signed = _SERIALIZER.dumps(value)
        assert signed[32:] == pickle.dumps(value, protocol=5)
        assert _SERIALIZER.loads(signed) == value

    def test_tampered_payload_raises(self) -> None:
        signed = _SERIALIZER.dumps({"x": 1})
        tampered = signed[:32] + b"\xff" + signed[33:]
        with pytest.raises(ValueError, match="signature mismatch"):
            _SERIALIZER.loads(tampered)

    def test_wrong_secret_raises(self) -> None:
        signed = _SERIALIZER.dumps(42)
        other = HmacPickleSerializer(secret=b"wrong-secret")
        with pytest.raises(ValueError, match="signature mismatch"):
            other.loads(signed)

    def test_too_short_raises(self) -> None:
        with pytest.raises(ValueError, match="too short"):
            _SERIALIZER.loads(b"short")

    def test_unsigned_pickle_raises(self) -> None:
        """Raw unsigned pickle data (legacy) raises ValueError, not a security bypass."""
        raw_pickle = pickle.dumps({"key": "val"}, protocol=5)
        with pytest.raises(ValueError):
            _SERIALIZER.loads(raw_pickle)


# ============================================================================
# SYNC OPERATIONS
# ============================================================================


class TestSyncOperations:
    def test_get_miss(self) -> None:
        client = _make_sync_client()
        client.get.return_value = None
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        assert backend.get("k") is _MISS
        client.get.assert_called_once()

    def test_get_hit(self) -> None:
        client = _make_sync_client()
        value = {"user": "alice"}
        client.get.return_value = _signed(value)
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        result = backend.get("k")
        assert result == value

    def test_get_tampered_returns_miss(self) -> None:
        """A tampered cache entry should be treated as a cache miss, not raise."""
        client = _make_sync_client()
        client.get.return_value = b"\xff" * 32 + pickle.dumps({"evil": True})
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        assert backend.get("k") is _MISS

    def test_get_unsigned_legacy_returns_miss(self) -> None:
        """Unsigned legacy data (no HMAC prefix) is discarded as a miss."""
        client = _make_sync_client()
        client.get.return_value = pickle.dumps("old_value", protocol=5)
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        assert backend.get("k") is _MISS

    def test_put_without_ttl(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        backend.put("k", "v")
        client.set.assert_called_once()
        client.setex.assert_not_called()
        raw = client.set.call_args[0][1]
        # Raw bytes are now signed; verify and unpickle to confirm value
        assert _SERIALIZER.loads(raw) == "v"

    def test_put_with_ttl(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        backend.put("k", "v", ttl=60.0)
        client.setex.assert_called_once()
        client.set.assert_not_called()
        _, ttl_arg, raw = client.setex.call_args[0]
        assert ttl_arg == 60
        assert _SERIALIZER.loads(raw) == "v"

    def test_put_ttl_minimum_one_second(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        backend.put("k", "v", ttl=0.1)
        _, ttl_arg, _ = client.setex.call_args[0]
        assert ttl_arg == 1

    def test_delete_existing(self) -> None:
        client = _make_sync_client()
        client.delete.return_value = 1
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        assert backend.delete("k") is True

    def test_delete_missing(self) -> None:
        client = _make_sync_client()
        client.delete.return_value = 0
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        assert backend.delete("k") is False

    def test_clear_no_keys(self) -> None:
        client = _make_sync_client()
        client.scan.return_value = (0, [])
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        backend.clear()
        client.scan.assert_called_once()
        client.delete.assert_not_called()

    def test_clear_with_keys(self) -> None:
        client = _make_sync_client()
        client.scan.side_effect = [
            (42, [b"pysilience:k1", b"pysilience:k2"]),
            (0, [b"pysilience:k3"]),
        ]
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        backend.clear()
        assert client.delete.call_count == 2

    def test_sync_raises_without_client(self) -> None:
        backend = RedisBackend(async_client=_make_async_client(), secret=_SECRET)
        with pytest.raises(RuntimeError, match="Sync Redis client not available"):
            backend.get("k")


# ============================================================================
# ASYNC OPERATIONS
# ============================================================================


class TestAsyncOperations:
    @pytest.mark.asyncio
    async def test_aget_miss(self) -> None:
        client = _make_async_client()
        client.get.return_value = None
        backend = RedisBackend(async_client=client, secret=_SECRET)

        assert await backend.aget("k") is _MISS

    @pytest.mark.asyncio
    async def test_aget_hit(self) -> None:
        client = _make_async_client()
        value = [1, 2, 3]
        client.get.return_value = _signed(value)
        backend = RedisBackend(async_client=client, secret=_SECRET)

        assert await backend.aget("k") == value

    @pytest.mark.asyncio
    async def test_aget_tampered_returns_miss(self) -> None:
        client = _make_async_client()
        client.get.return_value = b"\x00" * 32 + pickle.dumps("evil")
        backend = RedisBackend(async_client=client, secret=_SECRET)

        assert await backend.aget("k") is _MISS

    @pytest.mark.asyncio
    async def test_aput_without_ttl(self) -> None:
        client = _make_async_client()
        backend = RedisBackend(async_client=client, secret=_SECRET)

        await backend.aput("k", "v")
        client.set.assert_awaited_once()
        client.setex.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_aput_with_ttl(self) -> None:
        client = _make_async_client()
        backend = RedisBackend(async_client=client, secret=_SECRET)

        await backend.aput("k", "v", ttl=120.0)
        client.setex.assert_awaited_once()
        client.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_adelete(self) -> None:
        client = _make_async_client()
        client.delete.return_value = 1
        backend = RedisBackend(async_client=client, secret=_SECRET)

        assert await backend.adelete("k") is True

    @pytest.mark.asyncio
    async def test_aclear(self) -> None:
        client = _make_async_client()
        client.scan.side_effect = [
            (10, [b"pysilience:a"]),
            (0, []),
        ]
        backend = RedisBackend(async_client=client, secret=_SECRET)

        await backend.aclear()
        assert client.scan.await_count == 2

    @pytest.mark.asyncio
    async def test_async_raises_without_client(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client(), secret=_SECRET)
        with pytest.raises(RuntimeError, match="Async Redis client not available"):
            await backend.aget("k")


# ============================================================================
# PICKLE ROUND-TRIP
# ============================================================================


class TestPickleRoundTrip:
    def test_none_value(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        backend.put("k", None)
        raw = client.set.call_args[0][1]
        client.get.return_value = raw
        assert backend.get("k") is None

    def test_complex_value(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        value: dict[str, Any] = {"nested": {"a": [1, 2.0, True, None]}}
        backend.put("k", value)
        raw = client.set.call_args[0][1]
        client.get.return_value = raw
        assert backend.get("k") == value

    def test_bytes_value(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)

        value = b"\x00\x01\xff"
        backend.put("k", value)
        raw = client.set.call_args[0][1]
        client.get.return_value = raw
        assert backend.get("k") == value


# ============================================================================
# PREFIX NAMESPACING
# ============================================================================


class TestPrefixNamespacing:
    def test_prefix_in_key(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, prefix="myapp:", secret=_SECRET)

        backend.get("k")
        redis_key = client.get.call_args[0][0]
        assert redis_key.startswith("myapp:")

    def test_clear_uses_prefix_pattern(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, prefix="myapp:", secret=_SECRET)

        backend.clear()
        pattern = client.scan.call_args[1].get("match") or client.scan.call_args[0][1]
        assert pattern == "myapp:*"


# ============================================================================
# CACHE + REDIS BACKEND INTEGRATION
# ============================================================================


class TestCacheWithRedisBackend:
    """Integration tests using Cache with a mocked RedisBackend."""

    def test_sync_miss_then_hit(self) -> None:
        client = _make_sync_client()
        stored: dict[str, bytes] = {}

        def mock_set(key: str, value: bytes) -> bool:
            stored[key] = value
            return True

        def mock_setex(key: str, ttl: int, value: bytes) -> bool:
            stored[key] = value
            return True

        def mock_get(key: str) -> bytes | None:
            return stored.get(key)

        client.set.side_effect = mock_set
        client.setex.side_effect = mock_setex
        client.get.side_effect = mock_get

        backend = RedisBackend(sync_client=client, secret=_SECRET)
        c = Cache(CacheConfig(ttl=60.0), backend=backend, name="test")

        call_count = 0

        def compute() -> str:
            nonlocal call_count
            call_count += 1
            return "hello"

        assert c.execute("k", compute) == "hello"
        assert call_count == 1
        assert c.execute("k", compute) == "hello"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_async_miss_then_hit(self) -> None:
        client = _make_async_client()
        stored: dict[str, bytes] = {}

        async def mock_set(key: str, value: bytes) -> bool:
            stored[key] = value
            return True

        async def mock_setex(key: str, ttl: int, value: bytes) -> bool:
            stored[key] = value
            return True

        async def mock_get(key: str) -> bytes | None:
            return stored.get(key)

        client.set = AsyncMock(side_effect=mock_set)
        client.setex = AsyncMock(side_effect=mock_setex)
        client.get = AsyncMock(side_effect=mock_get)

        backend = RedisBackend(async_client=client, secret=_SECRET)
        c = Cache(CacheConfig(ttl=60.0), backend=backend, name="test")

        call_count = 0

        async def compute() -> str:
            nonlocal call_count
            call_count += 1
            return "hello"

        assert await c.execute_async("k", compute) == "hello"
        assert call_count == 1
        assert await c.execute_async("k", compute) == "hello"
        assert call_count == 1

    def test_events_emitted(self) -> None:
        client = _make_sync_client()
        stored: dict[str, bytes] = {}

        def mock_set(key: str, value: bytes) -> bool:
            stored[key] = value
            return True

        def mock_setex(key: str, ttl: int, value: bytes) -> bool:
            stored[key] = value
            return True

        def mock_get(key: str) -> bytes | None:
            return stored.get(key)

        client.set.side_effect = mock_set
        client.setex.side_effect = mock_setex
        client.get.side_effect = mock_get

        backend = RedisBackend(sync_client=client, secret=_SECRET)
        c = Cache(CacheConfig(ttl=60.0), backend=backend, name="ev")

        events: list[CacheEvent] = []
        c.on_event(events.append)

        c.execute("k", lambda: "v")
        c.execute("k", lambda: "v")

        assert len(events) == 2
        assert events[0].event_type == CacheEventType.MISS
        assert events[1].event_type == CacheEventType.HIT

    def test_invalidate_delegates_to_backend(self) -> None:
        client = _make_sync_client()
        client.delete.return_value = 1
        backend = RedisBackend(sync_client=client, secret=_SECRET)
        c = Cache(backend=backend, name="inv")

        assert c.invalidate("k") is True
        client.delete.assert_called_once()

    def test_invalidate_all_delegates_to_backend(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)
        c = Cache(backend=backend, name="inv")

        c.invalidate_all()
        client.scan.assert_called()

    @pytest.mark.asyncio
    async def test_async_thundering_herd(self) -> None:
        """Multiple coroutines for the same key should only compute once."""
        client = _make_async_client()
        stored: dict[str, bytes] = {}

        async def mock_set(key: str, value: bytes) -> bool:
            stored[key] = value
            return True

        async def mock_setex(key: str, ttl: int, value: bytes) -> bool:
            stored[key] = value
            return True

        async def mock_get(key: str) -> bytes | None:
            return stored.get(key)

        client.set = AsyncMock(side_effect=mock_set)
        client.setex = AsyncMock(side_effect=mock_setex)
        client.get = AsyncMock(side_effect=mock_get)

        backend = RedisBackend(async_client=client, secret=_SECRET)
        c = Cache(CacheConfig(ttl=60.0), backend=backend, name="herd")

        compute_count = 0

        async def expensive() -> int:
            nonlocal compute_count
            compute_count += 1
            await asyncio.sleep(0.05)
            return 42

        results = await asyncio.gather(
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
            c.execute_async("shared", expensive),
        )

        assert all(r == 42 for r in results)
        assert compute_count == 1, f"Expected 1 computation, got {compute_count}"

    def test_sync_thundering_herd(self) -> None:
        """Multiple threads for the same key should only compute once."""
        import threading

        client = _make_sync_client()
        stored: dict[str, bytes] = {}
        store_lock = threading.Lock()

        def mock_set(key: str, value: bytes) -> bool:
            with store_lock:
                stored[key] = value
            return True

        def mock_setex(key: str, ttl: int, value: bytes) -> bool:
            with store_lock:
                stored[key] = value
            return True

        def mock_get(key: str) -> bytes | None:
            with store_lock:
                return stored.get(key)

        client.set.side_effect = mock_set
        client.setex.side_effect = mock_setex
        client.get.side_effect = mock_get

        backend = RedisBackend(sync_client=client, secret=_SECRET)
        c = Cache(CacheConfig(ttl=60.0), backend=backend, name="herd")

        import time

        compute_count = 0
        count_lock = threading.Lock()
        barrier = threading.Barrier(5)

        def expensive() -> int:
            nonlocal compute_count
            with count_lock:
                compute_count += 1
            time.sleep(0.05)
            return 42

        results: list[int] = []
        results_lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            result = c.execute("shared", expensive)
            with results_lock:
                results.append(result)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert len(results) == 5
        assert all(r == 42 for r in results)
        assert compute_count == 1, f"Expected 1 computation, got {compute_count}"

    def test_backend_property(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, secret=_SECRET)
        c = Cache(backend=backend, name="test")
        assert c.backend is backend
