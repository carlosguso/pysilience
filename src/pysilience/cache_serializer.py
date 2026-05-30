"""
Pysilience - Cache Serializer Protocol
======================================
Shared protocol and type-envelope registry for cache serializers.

Concrete implementations live in sibling modules:

- :mod:`pysilience.cache_serializer_json` — :class:`~pysilience.cache_serializer_json.JsonSerializer`
- :mod:`pysilience.cache_serializer_hmac` — :class:`~pysilience.cache_serializer_hmac.HmacPickleSerializer`
- :mod:`pysilience.cache_serializer_msgpack` — :class:`~pysilience.cache_serializer_msgpack.MsgpackSerializer`

License: MIT
"""

from __future__ import annotations

import base64
from collections.abc import Callable
from datetime import date, datetime, time
from typing import Any, Protocol, runtime_checkable

__all__ = ["CacheSerializer"]

# Type-envelope keys (non-primitive values only; plain JSON/msgpack types stored as-is).
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


# Built-in type handlers (may be replaced via :meth:`~pysilience.cache_serializer_json.JsonSerializer.register`).
_register_type(bytes, lambda b: base64.b64encode(b).decode("ascii"), lambda s: base64.b64decode(s))
_register_type(datetime, lambda d: d.isoformat(), lambda s: datetime.fromisoformat(s))
_register_type(date, lambda d: d.isoformat(), lambda s: date.fromisoformat(s))
_register_type(time, lambda t: t.isoformat(), lambda s: time.fromisoformat(s))


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


def _envelope_encode(obj: Any) -> Any:
    """Wrap a non-JSON/msgpack-native object in a type envelope."""
    key = _type_key(obj.__class__)
    entry = _type_registry.get(key)
    if entry is None:
        raise TypeError(
            f"Object of type {obj.__class__.__qualname__!r} is not serializable. "
            f"Register it with JsonSerializer.register() or use HmacPickleSerializer."
        )
    encode_fn, _ = entry
    return {
        _TYPE_KEY: key,
        _DATA_KEY: encode_fn(obj),
    }


def _envelope_decode(obj: dict[str, Any]) -> Any:
    """Restore a type-envelope dict to its original object."""
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
