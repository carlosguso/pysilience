"""
Pysilience - JSON Cache Serializer
===================================
A :class:`~pysilience.cache_serializer.CacheSerializer` backed by :mod:`json`.

License: MIT
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from datetime import date, datetime, time
from typing import Any

from pysilience.cache_serializer import CacheSerializer

__all__ = ["JsonSerializer"]

# Type-envelope keys (non-primitive values only; plain JSON types stored as-is).
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


def _envelope_encode(obj: Any) -> Any:
    """Wrap a non-JSON-native object in a type envelope."""
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
    :meth:`register` with the same type.

    Example::

        from pysilience.cache_redis import RedisBackend
        from pysilience.cache_serializer_json import JsonSerializer

        backend = RedisBackend(sync_client=redis.Redis(), serializer=JsonSerializer())
    """

    __slots__ = ()

    def dumps(self, value: Any) -> bytes:
        """Serialize *value* to UTF-8 JSON bytes."""
        return json.dumps(value, default=_envelope_encode, separators=(",", ":")).encode()

    def loads(self, raw: bytes) -> Any:
        """Deserialize UTF-8 JSON bytes back to a Python object."""
        try:
            return json.loads(raw.decode(), object_hook=_envelope_decode)
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
