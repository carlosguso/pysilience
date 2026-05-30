"""
Pysilience - Cache Serializers
==============================
Pluggable serializers for external cache backends such as
:class:`~pysilience.cache_redis.RedisBackend`.

License: MIT
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import pickle
from collections.abc import Callable
from datetime import date, datetime, time
from typing import Any, Protocol, runtime_checkable

__all__ = ["CacheSerializer", "HmacPickleSerializer", "JsonSerializer"]

# SHA-256 produces a 32-byte digest; we prepend it to every stored value.
_HMAC_DIGEST_SIZE = 32

# Type-envelope keys (non-primitive values only; plain JSON types are stored as-is).
_TYPE_KEY = "__pysilience_type__"
_DATA_KEY = "data"

# Registry entry: (encode_fn, decode_fn)
_TypeHandler = tuple[Callable[[Any], Any], Callable[[Any], Any]]
_type_registry: dict[str, _TypeHandler] = {}


def _type_key(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _register_type(
    cls: type,
    encode: Callable[[Any], Any],
    decode: Callable[[Any], Any],
) -> None:
    _type_registry[_type_key(cls)] = (encode, decode)


# Built-in type handlers (may be replaced via :meth:`JsonSerializer.register`).
_register_type(bytes, lambda b: base64.b64encode(b).decode("ascii"), lambda s: base64.b64decode(s))
_register_type(datetime, lambda d: d.isoformat(), lambda s: datetime.fromisoformat(s))
_register_type(date, lambda d: d.isoformat(), lambda s: date.fromisoformat(s))
_register_type(time, lambda t: t.isoformat(), lambda s: time.fromisoformat(s))


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class CacheSerializer(Protocol):
    """Protocol for encoding and decoding values stored in external backends.

    Implementations are passed to :class:`~pysilience.cache_redis.RedisBackend`
    via its *serializer* argument.  :meth:`loads` should raise
    :class:`ValueError` when *raw* is invalid or corrupted; backends treat that
    as a cache miss rather than propagating the error.

    Subclass this protocol (``class MySerializer(CacheSerializer): ...``) so
    type checkers verify ``dumps`` / ``loads`` at compile time.  Because the
    protocol is :func:`~typing.runtime_checkable`, ``isinstance(obj,
    CacheSerializer)`` also works at runtime.
    """

    def dumps(self, value: Any) -> bytes:
        """Serialize *value* to bytes for storage."""
        ...

    def loads(self, raw: bytes) -> Any:
        """Deserialize *raw* bytes back to a Python object."""
        ...


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


class JsonSerializer(CacheSerializer):
    """JSON serialisation for cache values.

    Standard JSON types (``dict``, ``list``, ``str``, ``int``, ``float``,
    ``bool``, ``None``) are stored as-is with no wrapper.  Non-primitive
    types (``datetime``, ``date``, ``time``, ``bytes``, and user-registered
    classes) are wrapped in a type envelope::

        {
            "__pysilience_type__": "datetime.datetime",
            "data": "2025-01-01T12:00:00"
        }

    Built-in handlers for ``datetime``, ``date``, ``time``, and ``bytes`` are
    registered at import time but may be **overridden** by calling
    :meth:`register` with the same type.  Register custom types the same way.
    """

    __slots__ = ()

    def dumps(self, value: Any) -> bytes:
        """Serialize *value* to UTF-8 JSON bytes."""
        return json.dumps(value, default=self._encode, separators=(",", ":")).encode()

    def loads(self, raw: bytes) -> Any:
        """Deserialize UTF-8 JSON bytes back to a Python object."""
        try:
            return json.loads(raw.decode(), object_hook=self._decode)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(str(exc)) from exc

    @classmethod
    def register(
        cls,
        typ: type,
        *,
        encode: Callable[[Any], Any],
        decode: Callable[[Any], Any],
    ) -> None:
        """Register or override envelope serialisation for *typ*.

        Replaces any existing handler for *typ*, including built-in handlers
        for ``datetime``, ``date``, ``time``, and ``bytes``.  *encode* must
        return JSON-serialisable *data*; *decode* reconstructs the object
        from that *data*.
        """
        _register_type(typ, encode, decode)

    @classmethod
    def unregister(cls, typ: type) -> None:
        """Remove *typ* from the type registry (mainly for tests)."""
        _type_registry.pop(_type_key(typ), None)

    @staticmethod
    def _encode(obj: Any) -> Any:
        key = _type_key(obj.__class__)
        entry = _type_registry.get(key)
        if entry is None:
            raise TypeError(
                f"Object of type {obj.__class__.__qualname__!r} is not JSON serializable. "
                f"Register it with JsonSerializer.register() or use HmacPickleSerializer."
            )
        encode_fn, _ = entry
        return {
            _TYPE_KEY: key,
            _DATA_KEY: encode_fn(obj),
        }

    @staticmethod
    def _decode(obj: dict[str, Any]) -> Any:
        if set(obj) != {_TYPE_KEY, _DATA_KEY}:
            return obj
        type_key = obj[_TYPE_KEY]
        if not isinstance(type_key, str):
            return obj
        entry = _type_registry.get(type_key)
        if entry is None:
            raise ValueError(f"Unknown pysilience type envelope: {type_key!r}")
        _, decode_fn = entry
        return decode_fn(obj[_DATA_KEY])


# ---------------------------------------------------------------------------
# HMAC-signed pickle
# ---------------------------------------------------------------------------

def _sign(data: bytes, secret: bytes) -> bytes:
    """Return *data* prefixed with its HMAC-SHA256 signature."""
    sig = hmac.new(secret, data, hashlib.sha256).digest()
    return sig + data


def _verify_and_load(raw: bytes, secret: bytes) -> Any:
    """Verify the HMAC-SHA256 signature and unpickle the payload.

    Raises :class:`ValueError` if the signature is absent, too short, or
    does not match — indicating corruption, tampering, or unsigned legacy data.
    """
    if len(raw) < _HMAC_DIGEST_SIZE:
        raise ValueError(
            "Cache entry is too short to contain a signature "
            "(possible corruption or unsigned legacy data)"
        )
    sig, payload = raw[:_HMAC_DIGEST_SIZE], raw[_HMAC_DIGEST_SIZE:]
    expected = hmac.new(secret, payload, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        raise ValueError(
            "Cache entry signature mismatch — possible tampering or secret rotation"
        )
    return pickle.loads(payload)  # noqa: S301


class HmacPickleSerializer(CacheSerializer):
    """Pickle (protocol 5) serialisation with HMAC-SHA256 integrity protection.

    Values are serialised with :mod:`pickle` and prefixed with a 32-byte
    HMAC-SHA256 signature.  :meth:`loads` verifies the signature before
    unpickling, so a compromised Redis server cannot achieve remote code
    execution through crafted payloads.

    Args:
        secret: HMAC signing secret (``bytes`` or ``str``).  If omitted the
            ``PYSILIENCE_CACHE_SECRET`` environment variable is used.  A
            :class:`ValueError` is raised at construction time if neither
            source provides a non-empty value.

    Example::

        serializer = HmacPickleSerializer(secret=b"my-secret-key")
        backend = RedisBackend(sync_client=redis.Redis(), serializer=serializer)
    """

    __slots__ = ("_secret",)

    def __init__(self, *, secret: bytes | str | None = None) -> None:
        _secret: bytes | str | None = secret
        if not _secret:
            env_val = os.environ.get("PYSILIENCE_CACHE_SECRET", "")
            if env_val:
                _secret = env_val
        if not _secret:
            raise ValueError(
                "HmacPickleSerializer requires a signing secret to prevent unsafe "
                "deserialization. Pass secret=b'...' or set the "
                "PYSILIENCE_CACHE_SECRET environment variable. "
                "Generate a suitable value with:\n"
                "    python -c \"import secrets; print(secrets.token_hex(32))\""
            )
        self._secret: bytes = (
            _secret if isinstance(_secret, bytes) else _secret.encode()
        )

    def dumps(self, value: Any) -> bytes:
        """Pickle *value* and return HMAC-signed bytes."""
        return _sign(pickle.dumps(value, protocol=5), self._secret)

    def loads(self, raw: bytes) -> Any:
        """Verify the HMAC signature and unpickle *raw*."""
        return _verify_and_load(raw, self._secret)
