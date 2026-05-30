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
from datetime import date, datetime
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
from pysilience.cache_serializer import CacheSerializer
from pysilience.cache_serializer_hmac import HmacPickleSerializer, _sign
from pysilience.cache_serializer_json import JsonSerializer

# ---------------------------------------------------------------------------
# Shared serializers
# ---------------------------------------------------------------------------

_SECRET = b"test-secret-key-for-unit-tests"
_JSON = JsonSerializer()
_HMAC = HmacPickleSerializer(secret=_SECRET)


def _encoded(value: Any) -> bytes:
    """Helper: serialize *value* as the default JSON backend would."""
    return _JSON.dumps(value)


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
            RedisBackend()

    def test_default_serializer_is_json(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client())
        assert isinstance(backend._serializer, JsonSerializer)

    def test_explicit_serializer(self) -> None:
        serializer = HmacPickleSerializer(secret=b"custom")
        backend = RedisBackend(sync_client=_make_sync_client(), serializer=serializer)
        assert backend._serializer is serializer

    def test_custom_serializer(self) -> None:
        """Users can plug in their own CacheSerializer implementation."""

        class PlainTextSerializer:
            def dumps(self, value: Any) -> bytes:
                return str(value).encode()

            def loads(self, raw: bytes) -> Any:
                return raw.decode()

        client = _make_sync_client()
        serializer = PlainTextSerializer()
        backend = RedisBackend(sync_client=client, serializer=serializer)

        backend.put("k", "hello")
        raw = client.set.call_args[0][1]
        assert raw == b"hello"

        client.get.return_value = raw
        assert backend.get("k") == "hello"

    def test_invalid_serializer_rejected(self) -> None:
        with pytest.raises(TypeError, match="CacheSerializer"):
            RedisBackend(sync_client=_make_sync_client(), serializer=object())  # type: ignore[arg-type]

    def test_sync_only(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client())
        assert backend._sync is not None
        assert backend._async is None

    def test_async_only(self) -> None:
        backend = RedisBackend(async_client=_make_async_client())
        assert backend._sync is None
        assert backend._async is not None

    def test_both_clients(self) -> None:
        backend = RedisBackend(
            sync_client=_make_sync_client(),
            async_client=_make_async_client(),
        )
        assert backend._sync is not None
        assert backend._async is not None

    def test_default_prefix(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client())
        assert backend._prefix == "pysilience:"

    def test_custom_prefix(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client(), prefix="myapp:")
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
# JSON SERIALIZER
# ============================================================================


class TestJsonSerializer:
    def test_implements_cache_serializer_protocol(self) -> None:
        assert isinstance(JsonSerializer(), CacheSerializer)

    def test_round_trip(self) -> None:
        value = {"hello": "world", "n": 42, "ok": True, "empty": None}
        assert _JSON.loads(_JSON.dumps(value)) == value

    def test_primitives_have_no_envelope(self) -> None:
        raw = _JSON.dumps({"a": 1, "b": [True, None]})
        assert b"__pysilience_type__" not in raw

    def test_datetime_round_trip(self) -> None:
        value = datetime(2025, 6, 15, 14, 30, 0)
        assert _JSON.loads(_JSON.dumps(value)) == value

    def test_date_round_trip(self) -> None:
        value = date(2025, 6, 15)
        assert _JSON.loads(_JSON.dumps(value)) == value

    def test_nested_datetime_in_dict(self) -> None:
        ts = datetime(2025, 1, 1, 12, 0, 0)
        value = {"user": "alice", "created_at": ts}
        assert _JSON.loads(_JSON.dumps(value)) == value

    def test_custom_type_registration(self) -> None:
        class UserProfile:
            __slots__ = ("name",)

            def __init__(self, name: str) -> None:
                self.name = name

            def __eq__(self, other: object) -> bool:
                return isinstance(other, UserProfile) and self.name == other.name

        JsonSerializer.register(
            UserProfile,
            encode=lambda u: {"name": u.name},
            decode=lambda d: UserProfile(d["name"]),
        )
        try:
            profile = UserProfile("alice")
            assert _JSON.loads(_JSON.dumps(profile)) == profile
        finally:
            JsonSerializer.unregister(UserProfile)

    def test_override_builtin_datetime(self) -> None:
        """Built-in handlers can be replaced via register()."""
        original = _JSON.loads(_JSON.dumps(datetime(2025, 1, 1, 12, 0, 0)))

        JsonSerializer.register(
            datetime,
            encode=lambda d: d.timestamp(),
            decode=lambda ts: datetime.fromtimestamp(ts),
        )
        try:
            value = datetime(2025, 6, 15, 14, 30, 0)
            raw = _JSON.dumps(value)
            assert b"__pysilience_ver__" not in raw
            assert _JSON.loads(raw) == value
        finally:
            JsonSerializer.register(
                datetime,
                encode=lambda d: d.isoformat(),
                decode=lambda s: datetime.fromisoformat(s),
            )

        assert _JSON.loads(_JSON.dumps(original)) == original

    def test_unknown_envelope_raises(self) -> None:
        payload = json.dumps(
            {
                "__pysilience_type__": "myapp.models.UserProfile",
                "data": {"name": "alice"},
            }
        ).encode()
        with pytest.raises(ValueError, match="Unknown pysilience type"):
            _JSON.loads(payload)

    def test_bytes_round_trip(self) -> None:
        value = b"\x00\x01\xff"
        assert _JSON.loads(_JSON.dumps(value)) == value

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(ValueError):
            _JSON.loads(b"not-json")


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
        signed = _HMAC.dumps(value)
        assert signed[32:] == pickle.dumps(value, protocol=5)
        assert _HMAC.loads(signed) == value

    def test_tampered_payload_raises(self) -> None:
        signed = _HMAC.dumps({"x": 1})
        tampered = signed[:32] + b"\xff" + signed[33:]
        with pytest.raises(ValueError, match="signature mismatch"):
            _HMAC.loads(tampered)

    def test_wrong_secret_raises(self) -> None:
        signed = _HMAC.dumps(42)
        other = HmacPickleSerializer(secret=b"wrong-secret")
        with pytest.raises(ValueError, match="signature mismatch"):
            other.loads(signed)

    def test_too_short_raises(self) -> None:
        with pytest.raises(ValueError, match="too short"):
            _HMAC.loads(b"short")

    def test_unsigned_pickle_raises(self) -> None:
        """Raw unsigned pickle data (legacy) raises ValueError, not a security bypass."""
        raw_pickle = pickle.dumps({"key": "val"}, protocol=5)
        with pytest.raises(ValueError):
            _HMAC.loads(raw_pickle)

    def test_corrupt_pickle_payload_raises_value_error(self) -> None:
        """Valid HMAC over invalid pickle bytes raises ValueError, not UnpicklingError."""
        signed = _sign(b"not-valid-pickle", _SECRET)
        with pytest.raises(ValueError, match="invalid load key"):
            _HMAC.loads(signed)


# ============================================================================
# SYNC OPERATIONS
# ============================================================================


class TestSyncOperations:
    def test_get_miss(self) -> None:
        client = _make_sync_client()
        client.get.return_value = None
        backend = RedisBackend(sync_client=client)

        assert backend.get("k") is _MISS
        client.get.assert_called_once()

    def test_get_hit(self) -> None:
        client = _make_sync_client()
        value = {"user": "alice"}
        client.get.return_value = _encoded(value)
        backend = RedisBackend(sync_client=client)

        result = backend.get("k")
        assert result == value

    def test_get_invalid_json_returns_miss(self) -> None:
        """Corrupt JSON is discarded as a cache miss."""
        client = _make_sync_client()
        client.get.return_value = b"not-valid-json"
        backend = RedisBackend(sync_client=client)

        assert backend.get("k") is _MISS

    def test_get_hmac_tampered_returns_miss(self) -> None:
        """Tampered HMAC entries are discarded as a cache miss."""
        client = _make_sync_client()
        client.get.return_value = b"\xff" * 32 + pickle.dumps({"evil": True})
        backend = RedisBackend(
            sync_client=client,
            serializer=HmacPickleSerializer(secret=_SECRET),
        )

        assert backend.get("k") is _MISS

    def test_get_hmac_unsigned_legacy_returns_miss(self) -> None:
        """Unsigned legacy pickle data is discarded as a miss."""
        client = _make_sync_client()
        client.get.return_value = pickle.dumps("old_value", protocol=5)
        backend = RedisBackend(
            sync_client=client,
            serializer=HmacPickleSerializer(secret=_SECRET),
        )

        assert backend.get("k") is _MISS

    def test_get_hmac_corrupt_pickle_returns_miss(self) -> None:
        """Valid HMAC over corrupt pickle is discarded as a cache miss."""
        client = _make_sync_client()
        client.get.return_value = _sign(b"not-valid-pickle", _SECRET)
        backend = RedisBackend(
            sync_client=client,
            serializer=HmacPickleSerializer(secret=_SECRET),
        )

        assert backend.get("k") is _MISS

    def test_put_without_ttl(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client)

        backend.put("k", "v")
        client.set.assert_called_once()
        client.setex.assert_not_called()
        raw = client.set.call_args[0][1]
        # Raw bytes are JSON-encoded; verify round-trip
        assert _JSON.loads(raw) == "v"

    def test_put_with_ttl(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client)

        backend.put("k", "v", ttl=60.0)
        client.setex.assert_called_once()
        client.set.assert_not_called()
        _, ttl_arg, raw = client.setex.call_args[0]
        assert ttl_arg == 60
        assert _JSON.loads(raw) == "v"

    def test_put_ttl_minimum_one_second(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client)

        backend.put("k", "v", ttl=0.1)
        _, ttl_arg, _ = client.setex.call_args[0]
        assert ttl_arg == 1

    def test_delete_existing(self) -> None:
        client = _make_sync_client()
        client.delete.return_value = 1
        backend = RedisBackend(sync_client=client)

        assert backend.delete("k") is True

    def test_delete_missing(self) -> None:
        client = _make_sync_client()
        client.delete.return_value = 0
        backend = RedisBackend(sync_client=client)

        assert backend.delete("k") is False

    def test_clear_no_keys(self) -> None:
        client = _make_sync_client()
        client.scan.return_value = (0, [])
        backend = RedisBackend(sync_client=client)

        backend.clear()
        client.scan.assert_called_once()
        client.delete.assert_not_called()

    def test_clear_with_keys(self) -> None:
        client = _make_sync_client()
        client.scan.side_effect = [
            (42, [b"pysilience:k1", b"pysilience:k2"]),
            (0, [b"pysilience:k3"]),
        ]
        backend = RedisBackend(sync_client=client)

        backend.clear()
        assert client.delete.call_count == 2

    def test_sync_raises_without_client(self) -> None:
        backend = RedisBackend(async_client=_make_async_client())
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
        backend = RedisBackend(async_client=client)

        assert await backend.aget("k") is _MISS

    @pytest.mark.asyncio
    async def test_aget_hit(self) -> None:
        client = _make_async_client()
        value = [1, 2, 3]
        client.get.return_value = _encoded(value)
        backend = RedisBackend(async_client=client)

        assert await backend.aget("k") == value

    async def test_aget_invalid_json_returns_miss(self) -> None:
        client = _make_async_client()
        client.get.return_value = b"{bad json"
        backend = RedisBackend(async_client=client)

        assert await backend.aget("k") is _MISS

    @pytest.mark.asyncio
    async def test_aget_hmac_tampered_returns_miss(self) -> None:
        client = _make_async_client()
        client.get.return_value = b"\x00" * 32 + pickle.dumps("evil")
        backend = RedisBackend(
            async_client=client,
            serializer=HmacPickleSerializer(secret=_SECRET),
        )

        assert await backend.aget("k") is _MISS

    @pytest.mark.asyncio
    async def test_aput_without_ttl(self) -> None:
        client = _make_async_client()
        backend = RedisBackend(async_client=client)

        await backend.aput("k", "v")
        client.set.assert_awaited_once()
        client.setex.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_aput_with_ttl(self) -> None:
        client = _make_async_client()
        backend = RedisBackend(async_client=client)

        await backend.aput("k", "v", ttl=120.0)
        client.setex.assert_awaited_once()
        client.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_adelete(self) -> None:
        client = _make_async_client()
        client.delete.return_value = 1
        backend = RedisBackend(async_client=client)

        assert await backend.adelete("k") is True

    @pytest.mark.asyncio
    async def test_aclear(self) -> None:
        client = _make_async_client()
        client.scan.side_effect = [
            (10, [b"pysilience:a"]),
            (0, []),
        ]
        backend = RedisBackend(async_client=client)

        await backend.aclear()
        assert client.scan.await_count == 2

    @pytest.mark.asyncio
    async def test_async_raises_without_client(self) -> None:
        backend = RedisBackend(sync_client=_make_sync_client())
        with pytest.raises(RuntimeError, match="Async Redis client not available"):
            await backend.aget("k")


# ============================================================================
# VALUE ROUND-TRIP
# ============================================================================


class TestValueRoundTrip:
    def test_none_value(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client)

        backend.put("k", None)
        raw = client.set.call_args[0][1]
        client.get.return_value = raw
        assert backend.get("k") is None

    def test_complex_value(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client)

        value: dict[str, Any] = {"nested": {"a": [1, 2.0, True, None]}}
        backend.put("k", value)
        raw = client.set.call_args[0][1]
        client.get.return_value = raw
        assert backend.get("k") == value

    def test_bytes_value(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client)

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
        backend = RedisBackend(sync_client=client, prefix="myapp:")

        backend.get("k")
        redis_key = client.get.call_args[0][0]
        assert redis_key.startswith("myapp:")

    def test_clear_uses_prefix_pattern(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client, prefix="myapp:")

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

        backend = RedisBackend(sync_client=client)
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

        backend = RedisBackend(async_client=client)
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

        backend = RedisBackend(sync_client=client)
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
        backend = RedisBackend(sync_client=client)
        c = Cache(backend=backend, name="inv")

        assert c.invalidate("k") is True
        client.delete.assert_called_once()

    def test_invalidate_all_delegates_to_backend(self) -> None:
        client = _make_sync_client()
        backend = RedisBackend(sync_client=client)
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

        backend = RedisBackend(async_client=client)
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

        backend = RedisBackend(sync_client=client)
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
        backend = RedisBackend(sync_client=client)
        c = Cache(backend=backend, name="test")
        assert c.backend is backend
