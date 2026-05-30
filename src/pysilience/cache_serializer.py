"""
Pysilience - Cache Serializer Protocol
======================================
Shared protocol for cache serializers.

Concrete implementations live in sibling modules:

- :mod:`pysilience.cache_serializer_json` — :class:`~pysilience.cache_serializer_json.JsonSerializer`
- :mod:`pysilience.cache_serializer_hmac` — :class:`~pysilience.cache_serializer_hmac.HmacPickleSerializer`
- :mod:`pysilience.cache_serializer_msgpack` — :class:`~pysilience.cache_serializer_msgpack.MsgpackSerializer`

License: MIT
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

__all__ = ["CacheSerializer"]


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
