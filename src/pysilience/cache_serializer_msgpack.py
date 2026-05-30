"""
Pysilience - MessagePack Cache Serializer
==========================================
A :class:`~pysilience.cache_serializer.CacheSerializer` backed by MessagePack.

Requires the ``msgpack`` package (``pip install pysilience[msgpack]``).

License: MIT
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

try:
    import msgpack
except ImportError as _import_err:
    raise ImportError(
        "The 'msgpack' package is required for MsgpackSerializer. "
        "Install it with: pip install pysilience[msgpack]"
    ) from _import_err

from pysilience.cache_serializer import CacheSerializer

__all__ = ["MsgpackSerializer"]

_T = TypeVar("_T", bound=type)

_MIN_TYPE_ID = 64
_MAX_TYPE_ID = 127


def _is_instance_method(cls: type, name: str) -> bool:
    for base in cls.__mro__:
        if name in base.__dict__:
            descriptor = base.__dict__[name]
            return callable(descriptor) and not isinstance(
                descriptor, (classmethod, staticmethod)
            )
    return False


def _is_classmethod(cls: type, name: str) -> bool:
    for base in cls.__mro__:
        if name in base.__dict__:
            return isinstance(base.__dict__[name], classmethod)
    return False


class MsgpackSerializer(CacheSerializer):
    """MessagePack serialisation for cache values.

    Standard msgpack types (``dict``, ``list``, ``str``, ``int``, ``float``,
    ``bool``, ``None``, ``bytes``) are encoded natively.  Custom classes are
    serialised as msgpack extension types via :meth:`register`::

        serializer = MsgpackSerializer()

        @serializer.register(type_id=64)   # must be 64–127
        class GeoPoint:
            def __pack__(self) -> bytes:
                return struct.pack("dd", self.lat, self.lon)

            @classmethod
            def __unpack__(cls, data: bytes) -> "GeoPoint":
                lat, lon = struct.unpack("dd", data)
                return cls(lat, lon)

    Example::

        from pysilience.cache_redis import RedisBackend
        from pysilience.cache_serializer_msgpack import MsgpackSerializer

        backend = RedisBackend(
            sync_client=redis.Redis(),
            serializer=MsgpackSerializer(),
        )
    """

    __slots__ = ("_ext_by_type", "_type_by_id")

    def __init__(self) -> None:
        self._ext_by_type: dict[type, int] = {}
        self._type_by_id: dict[int, type] = {}

    def register(self, type_id: int) -> Callable[[_T], _T]:
        """Decorator that registers *type_id* (64–127) for a custom class.

        The class must implement ``__pack__(self) -> bytes`` (instance method)
        and ``@classmethod __unpack__(cls, data: bytes)``.
        """
        if not _MIN_TYPE_ID <= type_id <= _MAX_TYPE_ID:
            raise ValueError(
                f"type_id must be between {_MIN_TYPE_ID} and {_MAX_TYPE_ID}, got {type_id}"
            )

        def decorator(cls: _T) -> _T:
            if type_id in self._type_by_id:
                existing = self._type_by_id[type_id]
                raise ValueError(
                    f"type_id {type_id} is already registered to {existing.__qualname__!r}"
                )
            if cls in self._ext_by_type:
                raise ValueError(
                    f"{cls.__qualname__!r} is already registered "
                    f"with type_id {self._ext_by_type[cls]}"
                )
            if not _is_instance_method(cls, "__pack__"):
                raise TypeError(f"{cls.__qualname__!r} must define __pack__(self) -> bytes")
            if not _is_classmethod(cls, "__unpack__"):
                raise TypeError(
                    f"{cls.__qualname__!r} must define "
                    f"@classmethod __unpack__(cls, data: bytes)"
                )

            self._ext_by_type[cls] = type_id
            self._type_by_id[type_id] = cls
            return cls

        return decorator

    def _default(self, obj: Any) -> msgpack.ExtType:
        type_id = self._ext_by_type.get(type(obj))
        if type_id is None:
            raise TypeError(
                f"Object of type {obj.__class__.__qualname__!r} is not serializable. "
                f"Register it with @serializer.register(type_id=...) where type_id is 64–127."
            )
        return msgpack.ExtType(type_id, obj.__pack__())

    def _ext_hook(self, code: int, data: bytes) -> Any:
        cls = self._type_by_id.get(code)
        if cls is None:
            raise ValueError(f"Unknown msgpack extension type: {code}")
        return cls.__unpack__(data)

    def dumps(self, value: Any) -> bytes:
        """Serialize *value* to MessagePack bytes."""
        return msgpack.packb(value, default=self._default, use_bin_type=True)

    def loads(self, raw: bytes) -> Any:
        """Deserialize MessagePack bytes back to a Python object."""
        try:
            return msgpack.unpackb(raw, raw=False, ext_hook=self._ext_hook)
        except (msgpack.UnpackException, msgpack.FormatError, ValueError) as exc:
            raise ValueError(str(exc)) from exc
