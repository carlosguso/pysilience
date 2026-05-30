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

_PackFn = Callable[[Any], bytes]
_UnpackFn = Callable[[bytes], Any]


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
    ``bool``, ``None``, ``bytes``) are encoded natively.  Everything else must
    be registered explicitly as a msgpack extension type.

    Extension type IDs are **your** choice — pysilience does not reserve any.
    Optional handlers for ``datetime``, ``date``, and ``time`` live in
    :mod:`pysilience.cache_serializer_msgpack_builtins`::

        from datetime import datetime

        from pysilience.cache_serializer_msgpack import MsgpackSerializer
        from pysilience.cache_serializer_msgpack_builtins import (
            pack_datetime,
            unpack_datetime,
        )

        serializer = MsgpackSerializer()
        serializer.register_type(
            datetime, type_id=64, pack=pack_datetime, unpack=unpack_datetime
        )

        @serializer.register(type_id=65)
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

    __slots__ = ("_ext_by_type", "_handlers_by_id")

    def __init__(self) -> None:
        self._ext_by_type: dict[type, int] = {}
        self._handlers_by_id: dict[int, tuple[_PackFn, _UnpackFn]] = {}

    def register_type(
        self,
        typ: type,
        *,
        pack: _PackFn,
        unpack: _UnpackFn,
        type_id: int | None = None,
    ) -> None:
        """Register serialisation for *typ*.

        *type_id* is required on first registration.  When replacing handlers
        for an already-registered *typ* at the same ID, *type_id* may be
        omitted.  If *type_id* is already registered to a **different** type,
        :class:`ValueError` is raised — call :meth:`unregister_type` first so
        existing cache entries are not deserialized with the wrong handler.

        *pack* serialises a value to bytes; *unpack* reconstructs it.
        """
        resolved_id = type_id if type_id is not None else self._ext_by_type.get(typ)
        if resolved_id is None:
            raise ValueError(
                f"type_id is required to register {typ.__qualname__!r}"
            )

        if resolved_id in self._handlers_by_id:
            owner = next(
                registered_typ
                for registered_typ, registered_id in self._ext_by_type.items()
                if registered_id == resolved_id
            )
            if owner is not typ:
                raise ValueError(
                    f"type_id {resolved_id} is already registered to "
                    f"{owner.__qualname__!r}; call unregister_type() first"
                )

        old_id = self._ext_by_type.get(typ)
        if old_id is not None and old_id != resolved_id:
            self._handlers_by_id.pop(old_id, None)

        self._ext_by_type[typ] = resolved_id
        self._handlers_by_id[resolved_id] = (pack, unpack)

    def unregister_type(self, typ: type) -> None:
        """Remove *typ* from the registry (mainly for tests)."""
        type_id = self._ext_by_type.pop(typ, None)
        if type_id is not None:
            self._handlers_by_id.pop(type_id, None)

    def register(self, type_id: int) -> Callable[[_T], _T]:
        """Decorator that registers *type_id* for a custom class.

        The class must implement ``__pack__(self) -> bytes`` (instance method)
        and ``@classmethod __unpack__(cls, data: bytes)``.
        """

        def decorator(cls: _T) -> _T:
            if not _is_instance_method(cls, "__pack__"):
                raise TypeError(f"{cls.__qualname__!r} must define __pack__(self) -> bytes")
            if not _is_classmethod(cls, "__unpack__"):
                raise TypeError(
                    f"{cls.__qualname__!r} must define "
                    f"@classmethod __unpack__(cls, data: bytes)"
                )

            self.register_type(
                cls,
                pack=cls.__pack__,
                unpack=cls.__unpack__,
                type_id=type_id,
            )
            return cls

        return decorator

    def _default(self, obj: Any) -> msgpack.ExtType:
        type_id = self._ext_by_type.get(type(obj))
        if type_id is None:
            raise TypeError(
                f"Object of type {obj.__class__.__qualname__!r} is not serializable. "
                f"Register it with register_type() or @serializer.register(type_id=...)."
            )
        pack, _ = self._handlers_by_id[type_id]
        return msgpack.ExtType(type_id, pack(obj))

    def _ext_hook(self, code: int, data: bytes) -> Any:
        handlers = self._handlers_by_id.get(code)
        if handlers is None:
            raise ValueError(f"Unknown msgpack extension type: {code}")
        _, unpack = handlers
        return unpack(data)

    def dumps(self, value: Any) -> bytes:
        """Serialize *value* to MessagePack bytes."""
        return msgpack.packb(value, default=self._default, use_bin_type=True)

    def loads(self, raw: bytes) -> Any:
        """Deserialize MessagePack bytes back to a Python object."""
        try:
            return msgpack.unpackb(raw, raw=False, ext_hook=self._ext_hook)
        except (msgpack.UnpackException, msgpack.FormatError, ValueError) as exc:
            raise ValueError(str(exc)) from exc
