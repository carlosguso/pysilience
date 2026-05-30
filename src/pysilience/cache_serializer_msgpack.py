"""
Pysilience - MessagePack Cache Serializer
==========================================
A :class:`~pysilience.cache_serializer.CacheSerializer` backed by MessagePack.

Requires the ``msgpack`` package (``pip install pysilience[msgpack]``).

License: MIT
"""

from __future__ import annotations

from typing import Any

try:
    import msgpack
except ImportError as _import_err:
    raise ImportError(
        "The 'msgpack' package is required for MsgpackSerializer. "
        "Install it with: pip install pysilience[msgpack]"
    ) from _import_err

from pysilience.cache_serializer import (
    CacheSerializer,
    _envelope_decode,
    _envelope_encode,
)

__all__ = ["MsgpackSerializer"]


class MsgpackSerializer(CacheSerializer):
    """MessagePack serialisation for cache values.

    Uses the same type-envelope scheme as
    :class:`~pysilience.cache_serializer_json.JsonSerializer` for ``datetime``,
    ``date``, ``time``, ``bytes``, and types registered via
    :meth:`~pysilience.cache_serializer_json.JsonSerializer.register`.

    Example::

        from pysilience.cache_redis import RedisBackend
        from pysilience.cache_serializer_msgpack import MsgpackSerializer

        backend = RedisBackend(
            sync_client=redis.Redis(),
            serializer=MsgpackSerializer(),
        )
    """

    __slots__ = ()

    def dumps(self, value: Any) -> bytes:
        """Serialize *value* to MessagePack bytes."""
        return msgpack.packb(value, default=_envelope_encode, use_bin_type=True)

    def loads(self, raw: bytes) -> Any:
        """Deserialize MessagePack bytes back to a Python object."""
        try:
            return msgpack.unpackb(raw, raw=False, object_hook=_envelope_decode)
        except (msgpack.UnpackException, msgpack.FormatError, ValueError) as exc:
            raise ValueError(str(exc)) from exc
