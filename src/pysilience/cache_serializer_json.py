"""
Pysilience - JSON Cache Serializer
===================================
A :class:`~pysilience.cache_serializer.CacheSerializer` backed by :mod:`json`.

License: MIT
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from pysilience.cache_serializer import (
    CacheSerializer,
    _envelope_decode,
    _envelope_encode,
    _register_type,
    _type_key,
    _type_registry,
)

__all__ = ["JsonSerializer"]


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
    :meth:`register` with the same type.  The same registry is shared with
    :class:`~pysilience.cache_serializer_msgpack.MsgpackSerializer`.
    Register custom types the same way.

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
